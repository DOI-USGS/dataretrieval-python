"""Functions for downloading data from the Water Data APIs, including the USGS
Aquarius Samples database.

See https://api.waterdata.usgs.gov/ for API reference.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from io import StringIO
from typing import Any, get_args
from urllib.parse import quote

import httpx
import pandas as pd

from dataretrieval.ogc.filters import FILTER_LANG
from dataretrieval.utils import (
    HTTPX_DEFAULTS,
    BaseMetadata,
    _attach_datetime_columns,
    _get,
    to_str,
)
from dataretrieval.waterdata import stats
from dataretrieval.waterdata.types import (
    CODE_SERVICES,
    METADATA_COLLECTIONS,
    PROFILES,
    SERVICES,
    WATERDATA_SERVICES,
)
from dataretrieval.waterdata.utils import (
    _OUTPUT_ID_BY_SERVICE,
    GEOPANDAS,
    SAMPLES_URL,
    _accept_legacy_kwargs,
    _as_str_list,
    _check_ogc_requests,
    _check_profiles,
    _construct_cql_request,
    _default_headers,
    _finalize_ogc,
    _get_args,
    _raise_for_non_200,
    _run_sync,
    _switch_properties_id,
    _walk_pages,
    _with_state,
    get_ogc_data,
)

# Set up logger for this module
logger = logging.getLogger(__name__)


def get_daily(
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    statistic_id: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    time_series_id: str | Iterable[str] | None = None,
    daily_id: str | Iterable[str] | None = None,
    approval_status: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    qualifier: str | Iterable[str] | None = None,
    value: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    time: str | Iterable[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Daily data provide one data value to represent water conditions for the
    day.

    Throughout much of the history of the USGS, the primary water data available
    was daily data collected manually at the monitoring location once each day.
    With improved availability of computer storage and automated transmission of
    data, the daily data published today are generally a statistical summary or
    metric of the continuous data collected each day, such as the daily mean,
    minimum, or maximum value. Daily data are automatically calculated from the
    continuous data of the same parameter code and are described by parameter
    code and a statistic code. These data have also been referred to as “daily
    values” or “DV”.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of
        the agency responsible for the monitoring location (e.g. USGS) with
        the ID number of the monitoring location (e.g. 02238500), separated
        by a hyphen (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter
        codes and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or iterable of strings, optional
        A code corresponding to the statistic an observation represents.
        Example codes include 00001 (max), 00002 (min), and 00003 (mean).
        A complete list of codes and their descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or iterable of strings, optional
        A list of requested columns to be returned from the query.
        Available options are: geometry, id, time_series_id,
        monitoring_location_id, parameter_code, statistic_id, time, value,
        unit_of_measure, approval_status, qualifier, last_modified
    time_series_id : string or iterable of strings, optional
        A unique identifier representing a single time series. This
        corresponds to the id field in the time-series-metadata endpoint.
    daily_id : string or iterable of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. It is not stable over time. Every time the record is refreshed
        in our database (which may happen as part of normal operations and does
        not imply any change to the data itself) a new ID will be generated. To
        uniquely identify a single observation over time, compare the time and
        time_series_id fields; each time series will only have a single
        observation at a given time.
    approval_status : string or iterable of strings, optional
        Some of the data that you have obtained from this U.S. Geological Survey
        database may not have received Director's approval. Any such data values
        are qualified as provisional and are subject to revision. Provisional
        data are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from its
        use. This field reflects the approval status of each record, and is either
        "Approved", meaning processing review has been completed and the data is
        approved for publication, or "Provisional" and subject to revision. For
        more information about provisional data, go to:
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or iterable of strings, optional
        This field indicates any qualifiers associated with an observation, for
        instance if a sensor may have been impacted by ice or if values were
        estimated.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format in order to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        that anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end).
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

        Only features that have a last_modified that intersects the value of
        datetime are selected.
    skip_geometry : boolean, optional
        This option can be used to skip response geometries for each feature.
        The returning object will be a data frame with no spatial information.
        Note that the USGS Water Data APIs use camelCase "skipGeometry" in
        CQL2 queries.
    time : string, optional
        The date an observation represents. You can query this field using
        date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features that have a time that intersects the
        value of datetime are selected. If a feature has multiple temporal
        properties, it is the decision of the server whether only a single
        temporal property is used to determine the extent or all relevant
        temporal properties.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features that have a geometry that intersects the bounding box are
        selected.  The bounding box is provided as four or six numbers,
        depending on whether the coordinate reference system includes a vertical
        axis (height or depth). Coordinates are assumed to be in crs 4326. The
        expected format is ``[xmin, ymin, xmax, ymax]``, i.e.
        ``[Western-most longitude, Southern-most latitude, Eastern-most
        longitude, Northern-most latitude]``.
    limit : int, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Many collections share the monitoring-location
        attributes (e.g. ``state_name="Wisconsin"``, ``site_type_code="ST"``),
        but the available queryables differ per collection -- call
        :func:`get_queryables` for the ones this collection actually supports.
        An unknown name is rejected by the service with a ``DataRetrievalError``
        (HTTP 400). This passthrough is provisional and may be superseded by
        explicit per-property keyword arguments.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get daily flow data from a single site
        >>> # over a yearlong period
        >>> df, md = dataretrieval.waterdata.get_daily(
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00060",
        ...     time="2021-01-01T00:00:00Z/2022-01-01T00:00:00Z",
        ... )

        >>> # Quick "show me the last week" idiom (ISO 8601 duration)
        >>> df, md = dataretrieval.waterdata.get_daily(
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00060",
        ...     time="P7D",
        ... )

        >>> # Get approved daily flow data from multiple sites
        >>> df, md = dataretrieval.waterdata.get_daily(
        ...     monitoring_location_id=["USGS-05114000", "USGS-09423350"],
        ...     approval_status="Approved",
        ...     time="2024-01-01/..",
        ... )

        >>> # Pull only rows whose underlying record was refreshed in the
        >>> # last 7 days — handy for incremental ETL polling
        >>> df, md = dataretrieval.waterdata.get_daily(
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00060",
        ...     last_modified="P7D",
        ... )

        >>> # Chain queries: pull all stream sites in a state, then their
        >>> # daily discharge for the last week. The site list can be hundreds
        >>> # of values long — the request is transparently chunked across
        >>> # multiple sub-requests so the URL stays under the server's byte
        >>> # limit. Combined output looks like a single query.
        >>> sites_df, _ = dataretrieval.waterdata.get_monitoring_locations(
        ...     state="Ohio",
        ...     site_type="Stream",
        ... )
        >>> df, md = dataretrieval.waterdata.get_daily(
        ...     monitoring_location_id=sites_df["monitoring_location_id"].tolist(),
        ...     parameter_code="00060",
        ...     time="P7D",
        ... )
    """
    service = "daily"

    # Build argument dictionary, omitting None values
    args = _get_args(locals())

    return get_ogc_data(args, service)


def get_continuous(
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    statistic_id: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    time_series_id: str | Iterable[str] | None = None,
    continuous_id: str | Iterable[str] | None = None,
    approval_status: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    qualifier: str | Iterable[str] | None = None,
    value: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    time: str | Iterable[str] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Continuous data provide instantaneous water conditions.

    This is an early version of the continuous endpoint that is feature-complete
    and is being made available for limited use.  Geometries are not included
    with the continuous endpoint. If the "time" input is left blank, the service
    will return the most recent year of measurements. Users may request no more
    than three years of data with each function call.

    Continuous data are collected at a high frequency, typically 15-minute
    intervals. Depending on the specific monitoring location, the data may be
    transmitted automatically via telemetry and be available on WDFN within
    minutes of collection, while other times the delivery of data may be delayed
    if the monitoring location does not have the capacity to automatically
    transmit data.  Continuous data are described by parameter name and
    parameter code (pcode). These data might also be referred to as
    "instantaneous values" or "IV".

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of
        the agency responsible for the monitoring location (e.g. USGS) with
        the ID number of the monitoring location (e.g. 02238500), separated
        by a hyphen (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter
        codes and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or iterable of strings, optional
        A code corresponding to the statistic an observation represents.
        Continuous data are nearly always associated with statistic id
        00011. Using a different code (such as 00003 for mean) will
        typically return no results. A complete list of codes and their
        descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or iterable of strings, optional
        A list of requested columns to be returned from the query.
        Available options are: geometry, id, time_series_id,
        monitoring_location_id, parameter_code, statistic_id, time, value,
        unit_of_measure, approval_status, qualifier, last_modified
    time_series_id : string or iterable of strings, optional
        A unique identifier representing a single time series. This
        corresponds to the id field in the time-series-metadata endpoint.
    continuous_id : string or iterable of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. It is not stable over time. Every time the record is refreshed
        in our database (which may happen as part of normal operations and does
        not imply any change to the data itself) a new ID will be generated. To
        uniquely identify a single observation over time, compare the time and
        time_series_id fields; each time series will only have a single
        observation at a given time.
    approval_status : string or iterable of strings, optional
        Some of the data that you have obtained from this U.S. Geological Survey
        database may not have received Director's approval. Any such data values
        are qualified as provisional and are subject to revision. Provisional
        data are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from its
        use. This field reflects the approval status of each record, and is either
        "Approved", meaning processing review has been completed and the data is
        approved for publication, or "Provisional" and subject to revision. For
        more information about provisional data, go to:
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or iterable of strings, optional
        This field indicates any qualifiers associated with an observation, for
        instance if a sensor may have been impacted by ice or if values were
        estimated.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format in order to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        that anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end).
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

        Only features that have a last_modified that intersects the value of
        datetime are selected.
    time : string, optional
        The date an observation represents. You can query this field using
        date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features that have a time that intersects the
        value of datetime are selected. If a feature has multiple temporal
        properties, it is the decision of the server whether only a single
        temporal property is used to determine the extent or all relevant
        temporal properties.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    limit : int, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 10000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Many collections share the monitoring-location
        attributes (e.g. ``state_name="Wisconsin"``, ``site_type_code="ST"``),
        but the available queryables differ per collection -- call
        :func:`get_queryables` for the ones this collection actually supports.
        An unknown name is rejected by the service with a ``DataRetrievalError``
        (HTTP 400). This passthrough is provisional and may be superseded by
        explicit per-property keyword arguments.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get instantaneous gage height data from a
        >>> # single site from a single year
        >>> df, md = dataretrieval.waterdata.get_continuous(
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00065",
        ...     time="2021-01-01T00:00:00Z/2022-01-01T00:00:00Z",
        ... )

        >>> # Pull several disjoint time windows in one call via a CQL
        >>> # ``filter``. See ``dataretrieval.ogc.filters`` for the
        >>> # full grammar, auto-chunking, and pitfalls.
        >>> df, md = dataretrieval.waterdata.get_continuous(
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00060",
        ...     filter=(
        ...         "(time >= '2023-06-01T12:00:00Z' "
        ...         "AND time <= '2023-06-01T13:00:00Z') "
        ...         "OR (time >= '2023-06-15T12:00:00Z' "
        ...         "AND time <= '2023-06-15T13:00:00Z')"
        ...     ),
        ...     filter_lang="cql-text",
        ... )
    """
    service = "continuous"

    # Build argument dictionary, omitting None values
    args = _get_args(locals())

    return get_ogc_data(args, service)


def get_monitoring_locations(
    monitoring_location_id: str | Iterable[str] | None = None,
    agency_code: str | Iterable[str] | None = None,
    agency_name: str | Iterable[str] | None = None,
    monitoring_location_number: str | Iterable[str] | None = None,
    monitoring_location_name: str | Iterable[str] | None = None,
    district_code: str | Iterable[str] | None = None,
    country_code: str | Iterable[str] | None = None,
    country_name: str | Iterable[str] | None = None,
    state: str | Iterable[str] | None = None,
    state_code: str | Iterable[str] | None = None,
    state_name: str | Iterable[str] | None = None,
    county_code: str | Iterable[str] | None = None,
    county_name: str | Iterable[str] | None = None,
    minor_civil_division_code: str | Iterable[str] | None = None,
    site_type_code: str | Iterable[str] | None = None,
    site_type: str | Iterable[str] | None = None,
    hydrologic_unit_code: str | Iterable[str] | None = None,
    basin_code: str | Iterable[str] | None = None,
    altitude: str | Iterable[str] | None = None,
    altitude_accuracy: str | Iterable[str] | None = None,
    altitude_method_code: str | Iterable[str] | None = None,
    altitude_method_name: str | Iterable[str] | None = None,
    vertical_datum: str | Iterable[str] | None = None,
    vertical_datum_name: str | Iterable[str] | None = None,
    horizontal_positional_accuracy_code: str | Iterable[str] | None = None,
    horizontal_positional_accuracy: str | Iterable[str] | None = None,
    horizontal_position_method_code: str | Iterable[str] | None = None,
    horizontal_position_method_name: str | Iterable[str] | None = None,
    original_horizontal_datum: str | Iterable[str] | None = None,
    original_horizontal_datum_name: str | Iterable[str] | None = None,
    drainage_area: str | Iterable[str] | None = None,
    contributing_drainage_area: str | Iterable[str] | None = None,
    time_zone_abbreviation: str | Iterable[str] | None = None,
    uses_daylight_savings: str | Iterable[str] | None = None,
    construction_date: str | Iterable[str] | None = None,
    aquifer_code: str | Iterable[str] | None = None,
    national_aquifer_code: str | Iterable[str] | None = None,
    aquifer_type_code: str | Iterable[str] | None = None,
    well_constructed_depth: str | Iterable[str] | None = None,
    hole_constructed_depth: str | Iterable[str] | None = None,
    depth_source_code: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Location information is basic information about the monitoring location
    including the name, identifier, agency responsible for data collection, and
    the date the location was established. It also includes information about
    the type of location, such as stream, lake, or groundwater, and geographic
    information about the location, such as state, county, latitude and
    longitude, and hydrologic unit code (HUC).

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of
        the agency responsible for the monitoring location (e.g. USGS) with
        the ID number of the monitoring location (e.g. 02238500), separated
        by a hyphen (e.g. USGS-02238500).
    agency_code : string or iterable of strings, optional
        The agency that is reporting the data. Agency codes are fixed values
        assigned by the National Water Information System (NWIS).
    agency_name : string or iterable of strings, optional
        The name of the agency that is reporting the data.
    monitoring_location_number : string or iterable of strings, optional
        Each monitoring location in the USGS data base has a unique 8- to
        15-digit identification number. Monitoring location numbers are
        assigned based on this logic:
        https://help.waterdata.usgs.gov/faq/sites/do-station-numbers-have-any-particular-meaning.
    monitoring_location_name : string or iterable of strings, optional
        This is the official name of the monitoring location in the database.
        For well information this can be a district-assigned local number.
    district_code : string or iterable of strings, optional
        The Water Science Centers (WSCs) across the United States use the FIPS
        state code as the district code. In some cases, monitoring locations and
        samples may be managed by a water science center that is adjacent to the
        state in which the monitoring location actually resides. For example a
        monitoring location may have a district code of 30 which translates to
        Montana, but the state code could be 56 for Wyoming because that is where
        the monitoring location actually is located.
    country_code : string or iterable of strings, optional
        The code for the country in which the monitoring location is located.
    country_name : string or iterable of strings, optional
        The name of the country in which the monitoring location is located.
    state : string or iterable of strings, optional
        State/territory filter (the recommended parameter). Accepts a full name
        (``"Wisconsin"``), a two-letter postal code (``"WI"``), or a two-digit
        ANSI/FIPS code (``"55"``).
    state_code : string or iterable of strings, optional
        State code. A two-digit ANSI code (formerly FIPS code) as defined by
        the American National Standards Institute, to define States and
        equivalents. A three-digit ANSI code is used to define counties and
        county equivalents. A `lookup table
        <https://www.census.gov/library/reference/code-lists/ansi.html#states>`_
        is available. The only countries with
        political subdivisions other than the US are Mexico and Canada. The Mexican
        states have US state codes ranging from 81-86 and Canadian provinces have
        state codes ranging from 90-98.
    state_name : string or iterable of strings, optional
        The name of the state or state equivalent in which the monitoring location
        is located.
    county_code : string or iterable of strings, optional
        The code for the county or county equivalent (parish, borough, etc.) in which
        the monitoring location is located. A `list of codes
        <https://help.waterdata.usgs.gov/code/county_query?fmt=html>`__ is available.
    county_name : string or iterable of strings, optional
        The name of the county or county equivalent (parish, borough, etc.) in which
        the monitoring location is located. A `list of codes
        <https://help.waterdata.usgs.gov/code/county_query?fmt=html>`__ is available.
    minor_civil_division_code : string or iterable of strings, optional
        Codes for primary governmental or administrative divisions of the county or
        county equivalent in which the monitoring location is located.
    site_type_code : string or iterable of strings, optional
        A code describing the hydrologic setting of the monitoring location.
    site_type : string or iterable of strings, optional
        A description of the hydrologic setting of the monitoring location.
    hydrologic_unit_code : string or iterable of strings, optional
        The United States is divided and sub-divided into successively smaller
        hydrologic units which are classified into four levels: regions,
        sub-regions, accounting units, and cataloging units. The hydrologic
        units are arranged within each other, from the smallest (cataloging
        units) to the largest (regions). Each hydrologic unit is identified by a
        unique hydrologic unit code (HUC) consisting of two to eight digits
        based on the four levels of classification in the hydrologic unit
        system.
    basin_code : string or iterable of strings, optional
        The Basin Code or "drainage basin code" is a two-digit code that further
        subdivides the 8-digit hydrologic-unit code. The drainage basin code is
        defined by the USGS State Office where the monitoring location is
        located.
    altitude : string or iterable of strings, optional
        Altitude of the monitoring location referenced to the specified Vertical
        Datum.
    altitude_accuracy : string or iterable of strings, optional
        Accuracy of the altitude, in feet. An accuracy of +/- 0.1 foot would be
        entered as “.1”. Many altitudes are interpolated from the contours on
        topographic maps; accuracies determined in this way are generally
        entered as one-half of the contour interval.
    altitude_method_code : string or iterable of strings, optional
        Codes representing the method used to measure altitude.
    altitude_method_name : string or iterable of strings, optional
        The name of the method used to measure altitude.
    vertical_datum : string or iterable of strings, optional
        The datum used to determine altitude and vertical position at the
        monitoring location.
    vertical_datum_name : string or iterable of strings, optional
        The datum used to determine altitude and vertical position at the
        monitoring location.
    horizontal_positional_accuracy_code : string or iterable of strings, optional
        Indicates the accuracy of the latitude longitude values.
    horizontal_positional_accuracy : string or iterable of strings, optional
        Indicates the accuracy of the latitude longitude values.
    horizontal_position_method_code : string or iterable of strings, optional
        Indicates the method used to determine latitude longitude values.
    horizontal_position_method_name : string or iterable of strings, optional
        Indicates the method used to determine latitude longitude values.
    original_horizontal_datum : string or iterable of strings, optional
        Coordinates are published in EPSG:4326 / WGS84 / World Geodetic System
        1984. This field indicates the original datum used to determine
        coordinates before they were converted.
    original_horizontal_datum_name : string or iterable of strings, optional
        Coordinates are published in EPSG:4326 / WGS84 / World Geodetic System
        1984. This field indicates the original datum used to determine coordinates
        before they were converted.
    drainage_area : string or iterable of strings, optional
        The area enclosed by a topographic divide from which direct surface runoff
        from precipitation normally drains by gravity into the stream above that
        point.
    contributing_drainage_area : string or iterable of strings, optional
        The contributing drainage area of a lake, stream, wetland, or estuary
        monitoring location, in square miles. This item should be present only
        if the contributing area is different from the total drainage area. This
        situation can occur when part of the drainage area consists of very
        porous soil or depressions that either allow all runoff to enter the
        groundwater or trap the water in ponds so that rainfall does not
        contribute to runoff.  A transbasin diversion can also affect the total
        drainage area.
    time_zone_abbreviation : string or iterable of strings, optional
        A short code describing the time zone used by a monitoring location.
    uses_daylight_savings : string or iterable of strings, optional
        A flag indicating whether or not a monitoring location uses daylight savings.
    construction_date : string or iterable of strings, optional
        Date the well was completed.
    aquifer_code : string or iterable of strings, optional
        Local aquifers in the USGS water resources data base are identified by a
        geohydrologic unit code (a three-digit number related to the age of the
        formation, followed by a 4 or 5 character abbreviation for the geologic
        unit or aquifer name). Additional information is available
        `at this link <https://help.waterdata.usgs.gov/faq/groundwater/local-aquifer-description>`_.
    national_aquifer_code : string or iterable of strings, optional
        National aquifers are the principal aquifers or aquifer systems in the United
        States, defined as regionally extensive aquifers or aquifer systems that have
        the potential to be used as a source of potable water. Not all groundwater
        monitoring locations can be associated with a National Aquifer. Such
        monitoring locations will not be retrieved using this search criteria. A `list
        of National aquifer codes and names <https://help.waterdata.usgs.gov/code/nat_aqfr_query?fmt=html>`_
        is available.
    aquifer_type_code : string or iterable of strings, optional
        Groundwater occurs in aquifers under two different conditions. Where water
        only partly fills an aquifer, the upper surface is free to rise and decline.
        These aquifers are referred to as unconfined (or water-table) aquifers. Where
        water completely fills an aquifer that is overlain by a confining bed, the
        aquifer is referred to as a confined (or artesian) aquifer. When a confined
        aquifer is penetrated by a well, the water level in the well will rise above
        the top of the aquifer (but not necessarily above land surface). Additional
        information is available `at this link <https://help.waterdata.usgs.gov/faq/groundwater/local-aquifer-description>`_.
    well_constructed_depth : string or iterable of strings, optional
        The depth of the finished well, in feet below land surface datum. Note: Not
        all groundwater monitoring locations have information on Well Depth. Such
        monitoring locations will not be retrieved using this search criteria.
    hole_constructed_depth : string or iterable of strings, optional
        The total depth to which the hole is drilled, in feet below land surface datum.
        Note: Not all groundwater monitoring locations have information on Hole Depth.
        Such monitoring locations will not be retrieved using this search criteria.
    depth_source_code : string or iterable of strings, optional
        A code indicating the source of water-level data. A `list of
        codes <https://help.waterdata.usgs.gov/code/water_level_src_cd_query?fmt=html>`_
        is available.
    properties : string or iterable of strings, optional
        A list of requested columns to be returned from the query. Available
        options are: geometry, id, agency_code, agency_name,
        monitoring_location_number, monitoring_location_name, district_code,
        country_code, country_name, state_code, state_name, county_code,
        county_name, minor_civil_division_code, site_type_code, site_type,
        hydrologic_unit_code, basin_code, altitude, altitude_accuracy,
        altitude_method_code, altitude_method_name, vertical_datum,
        vertical_datum_name, horizontal_positional_accuracy_code,
        horizontal_positional_accuracy, horizontal_position_method_code,
        horizontal_position_method_name, original_horizontal_datum,
        original_horizontal_datum_name, drainage_area,
        contributing_drainage_area, time_zone_abbreviation,
        uses_daylight_savings, construction_date, aquifer_code,
        national_aquifer_code, aquifer_type_code, well_constructed_depth,
        hole_constructed_depth, depth_source_code.
    bbox : list of numbers, optional
        Only features that have a geometry that intersects the bounding box are
        selected.  The bounding box is provided as four or six numbers,
        depending on whether the coordinate reference system includes a vertical
        axis (height or depth). Coordinates are assumed to be in crs 4326. The
        expected format is ``[xmin, ymin, xmax, ymax]``, i.e.
        ``[Western-most longitude, Southern-most latitude, Eastern-most
        longitude, Northern-most latitude]``.
    limit : int, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    skip_geometry : boolean, optional
        This option can be used to skip response geometries for each feature.
        The returning object will be a data frame with no spatial information.
        Note that the USGS Water Data APIs use camelCase "skipGeometry" in
        CQL2 queries.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Many collections share the monitoring-location
        attributes (e.g. ``state_name="Wisconsin"``, ``site_type_code="ST"``),
        but the available queryables differ per collection -- call
        :func:`get_queryables` for the ones this collection actually supports.
        An unknown name is rejected by the service with a ``DataRetrievalError``
        (HTTP 400). This passthrough is provisional and may be superseded by
        explicit per-property keyword arguments.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get monitoring locations within a bounding box
        >>> # and leave out geometry
        >>> df, md = dataretrieval.waterdata.get_monitoring_locations(
        ...     bbox=[-90.2, 42.6, -88.7, 43.2], skip_geometry=True
        ... )

        >>> # Get monitoring location info for specific sites
        >>> # and only specific properties
        >>> df, md = dataretrieval.waterdata.get_monitoring_locations(
        ...     monitoring_location_id=["USGS-05114000", "USGS-09423350"],
        ...     properties=["monitoring_location_id", "state_name", "country_name"],
        ... )
    """
    service = "monitoring-locations"

    # Build argument dictionary, omitting None values (resolving the unified
    # `state` argument into the OGC `state_name` queryable).
    args = _get_args(_with_state(locals(), to="name", into="state_name"))

    return get_ogc_data(args, service)


def get_time_series_metadata(
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    parameter_name: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    statistic_id: str | Iterable[str] | None = None,
    hydrologic_unit_code: str | Iterable[str] | None = None,
    state: str | Iterable[str] | None = None,
    state_name: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    begin: str | Iterable[str] | None = None,
    end: str | Iterable[str] | None = None,
    begin_utc: str | Iterable[str] | None = None,
    end_utc: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    computation_period_identifier: str | Iterable[str] | None = None,
    computation_identifier: str | Iterable[str] | None = None,
    thresholds: float | list[float] | None = None,
    sublocation_identifier: str | Iterable[str] | None = None,
    primary: str | Iterable[str] | None = None,
    parent_time_series_id: str | Iterable[str] | None = None,
    time_series_id: str | Iterable[str] | None = None,
    web_description: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Daily data and continuous measurements are grouped into time series,
    which represent a collection of observations of a single parameter,
    potentially aggregated using a standard statistic, at a single monitoring
    location. This endpoint provides metadata about those time series,
    including their operational thresholds, units of measurement, and when
    the earliest and most recent observations in a time series occurred.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of
        the agency responsible for the monitoring location (e.g. USGS) with
        the ID number of the monitoring location (e.g. 02238500), separated
        by a hyphen (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter
        codes and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    parameter_name : string or iterable of strings, optional
        A human-understandable name corresponding to parameter_code.
    properties : string or iterable of strings, optional
        A list of requested columns to be returned from the query.
        Available options are: begin, begin_utc, computation_identifier,
        computation_period_identifier, end, end_utc, geometry,
        hydrologic_unit_code, id, last_modified, monitoring_location_id,
        parameter_code, parameter_description, parameter_name,
        parent_time_series_id, primary, state_name, statistic_id,
        sublocation_identifier, thresholds, unit_of_measure, web_description
    statistic_id : string or iterable of strings, optional
        A code corresponding to the statistic an observation represents.
        Example codes include 00001 (max), 00002 (min), and 00003 (mean).
        A complete list of codes and their descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    hydrologic_unit_code : string or iterable of strings, optional
        The United States is divided and sub-divided into successively smaller
        hydrologic units which are classified into four levels: regions,
        sub-regions, accounting units, and cataloging units. The hydrologic
        units are arranged within each other, from the smallest (cataloging units)
        to the largest (regions). Each hydrologic unit is identified by a unique
        hydrologic unit code (HUC) consisting of two to eight digits based on the
        four levels of classification in the hydrologic unit system.
    state : string or iterable of strings, optional
        State/territory filter (the recommended parameter). Accepts a full name
        (``"Wisconsin"``), a two-letter postal code (``"WI"``), or a two-digit
        ANSI/FIPS code (``"55"``).
    state_name : string or iterable of strings, optional
        The name of the state or state equivalent in which the monitoring location
        is located.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        that anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features that have a last_modified that
        intersects the value of datetime are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H"
                for the last 36 hours

    begin : string or iterable of strings, optional
        This field contains the same information as "begin_utc", but in the
        local time of the monitoring location. It is retained for backwards
        compatibility, but will be removed in V1 of these APIs.
    end : string or iterable of strings, optional
        This field contains the same information as "end_utc", but in the
        local time of the monitoring location. It is retained for backwards
        compatibility, but will be removed in V1 of these APIs.
    begin_utc : string or iterable of strings, optional
        The datetime of the earliest observation in the time series. Together
        with end, this field represents the period of record of a time series.
        Note that some time series may have large gaps in their collection
        record. This field is currently in the local time of the monitoring
        location. We intend to update this in version v0 to use UTC with a time
        zone. You can query this field using date-times or intervals, adhering
        to RFC 3339, or using ISO 8601 duration objects. Intervals may be
        bounded or half-bounded (double-dots at start or end). Only features
        that have a begin that intersects the value of datetime are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    end_utc : string or iterable of strings, optional
        The datetime of the most recent observation in the time series. Data returned by
        this endpoint updates at most once per day, and potentially less frequently than
        that, and as such there may be more recent observations within a time series
        than the time series end value reflects. Together with begin, this field
        represents the period of record of a time series. It is additionally used to
        determine whether a time series is "active". We intend to update this in
        version v0 to use UTC with a time zone.
        You can query this field using date-times or intervals,
        adhering to RFC 3339, or using ISO 8601 duration objects. Intervals
        may be bounded or half-bounded (double-dots at start or end). Only
        features that have an end that intersects the value of datetime are
        selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    computation_period_identifier : string or iterable of strings, optional
        Indicates the period of data used for any statistical computations.
    computation_identifier : string or iterable of strings, optional
        Indicates whether the data from this time series represent a specific
        statistical computation.
    thresholds : number or list of numbers, optional
        Thresholds represent known numeric limits for a time series, for example
        the historic maximum value for a parameter or a level below which a
        sensor is non-operative. These thresholds are sometimes used to
        automatically determine if an observation is erroneous due to sensor
        error, and therefore shouldn't be included in the time series.
    sublocation_identifier : string or iterable of strings, optional
    primary : string or iterable of strings, optional
    parent_time_series_id : string or iterable of strings, optional
    time_series_id : string or iterable of strings, optional
        A unique identifier representing a single time series. This
        corresponds to the id field in the time-series-metadata endpoint.
    web_description : string or iterable of strings, optional
        A description of what this time series represents, as used by WDFN and
        other USGS data dissemination products.
    skip_geometry : boolean, optional
        This option can be used to skip response geometries for each feature.
        The returning object will be a data frame with no spatial information.
        Note that the USGS Water Data APIs use camelCase "skipGeometry" in
        CQL2 queries.
    bbox : list of numbers, optional
        Only features that have a geometry that intersects the bounding box are
        selected.  The bounding box is provided as four or six numbers,
        depending on whether the coordinate reference system includes a vertical
        axis (height or depth). Coordinates are assumed to be in crs 4326. The
        expected format is ``[xmin, ymin, xmax, ymax]``, i.e.
        ``[Western-most longitude, Southern-most latitude, Eastern-most
        longitude, Northern-most latitude]``.
    limit : int, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Many collections share the monitoring-location
        attributes (e.g. ``state_name="Wisconsin"``, ``site_type_code="ST"``),
        but the available queryables differ per collection -- call
        :func:`get_queryables` for the ones this collection actually supports.
        An unknown name is rejected by the service with a ``DataRetrievalError``
        (HTTP 400). This passthrough is provisional and may be superseded by
        explicit per-property keyword arguments.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get timeseries metadata information from a single site
        >>> # over a yearlong period
        >>> df, md = dataretrieval.waterdata.get_time_series_metadata(
        ...     monitoring_location_id="USGS-02238500"
        ... )

        >>> # Get timeseries metadata information from multiple sites
        >>> # that begin after January 1, 1990.
        >>> df, md = dataretrieval.waterdata.get_time_series_metadata(
        ...     monitoring_location_id=["USGS-05114000", "USGS-09423350"],
        ...     begin="1990-01-01/..",
        ... )
    """
    service = "time-series-metadata"

    # Build argument dictionary, omitting None values (resolving the unified
    # `state` argument into the OGC `state_name` queryable).
    args = _get_args(_with_state(locals(), to="name", into="state_name"))

    return get_ogc_data(args, service)


def get_combined_metadata(
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    parameter_name: str | Iterable[str] | None = None,
    parameter_description: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    statistic_id: str | Iterable[str] | None = None,
    data_type: str | Iterable[str] | None = None,
    computation_identifier: str | Iterable[str] | None = None,
    thresholds: float | list[float] | None = None,
    sublocation_identifier: str | Iterable[str] | None = None,
    primary: str | Iterable[str] | None = None,
    parent_time_series_id: str | Iterable[str] | None = None,
    web_description: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    begin: str | Iterable[str] | None = None,
    end: str | Iterable[str] | None = None,
    agency_code: str | Iterable[str] | None = None,
    agency_name: str | Iterable[str] | None = None,
    monitoring_location_number: str | Iterable[str] | None = None,
    monitoring_location_name: str | Iterable[str] | None = None,
    district_code: str | Iterable[str] | None = None,
    country_code: str | Iterable[str] | None = None,
    country_name: str | Iterable[str] | None = None,
    state: str | Iterable[str] | None = None,
    state_code: str | Iterable[str] | None = None,
    state_name: str | Iterable[str] | None = None,
    county_code: str | Iterable[str] | None = None,
    county_name: str | Iterable[str] | None = None,
    minor_civil_division_code: str | Iterable[str] | None = None,
    site_type_code: str | Iterable[str] | None = None,
    site_type: str | Iterable[str] | None = None,
    hydrologic_unit_code: str | Iterable[str] | None = None,
    basin_code: str | Iterable[str] | None = None,
    altitude: str | Iterable[str] | None = None,
    altitude_accuracy: str | Iterable[str] | None = None,
    altitude_method_code: str | Iterable[str] | None = None,
    altitude_method_name: str | Iterable[str] | None = None,
    vertical_datum: str | Iterable[str] | None = None,
    vertical_datum_name: str | Iterable[str] | None = None,
    horizontal_positional_accuracy_code: str | Iterable[str] | None = None,
    horizontal_positional_accuracy: str | Iterable[str] | None = None,
    horizontal_position_method_code: str | Iterable[str] | None = None,
    horizontal_position_method_name: str | Iterable[str] | None = None,
    original_horizontal_datum: str | Iterable[str] | None = None,
    original_horizontal_datum_name: str | Iterable[str] | None = None,
    drainage_area: str | Iterable[str] | None = None,
    contributing_drainage_area: str | Iterable[str] | None = None,
    time_zone_abbreviation: str | Iterable[str] | None = None,
    uses_daylight_savings: str | Iterable[str] | None = None,
    construction_date: str | Iterable[str] | None = None,
    aquifer_code: str | Iterable[str] | None = None,
    national_aquifer_code: str | Iterable[str] | None = None,
    aquifer_type_code: str | Iterable[str] | None = None,
    well_constructed_depth: str | Iterable[str] | None = None,
    hole_constructed_depth: str | Iterable[str] | None = None,
    depth_source_code: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get combined monitoring-location and time-series metadata.

    The ``combined-metadata`` collection joins the monitoring-locations
    catalog with the time-series-metadata catalog so that one row is
    returned per (location, parameter, statistic) inventory entry,
    carrying every column from both source endpoints. This makes it the
    most flexible "what data is available" endpoint in the Water Data
    API: any monitoring-location attribute (state, HUC, site type,
    drainage area, well-construction depth, …) can be combined with any
    time-series attribute (parameter code, statistic, data type, period
    of record, …) in a single query.

    See the OpenAPI reference for the full list of supported fields:
    https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/combined-metadata

    All ~35 location-catalog kwargs are accepted (``agency_code``,
    ``state_name``, ``drainage_area``, ``aquifer_code``, …) but only
    the most-used ones are documented below; see
    :func:`get_monitoring_locations` for per-field descriptions.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location.
        Created by combining the agency code (e.g. ``USGS``) with the ID
        number (e.g. ``02238500``), separated by a hyphen
        (e.g. ``"USGS-02238500"``).
    parameter_code : string or iterable of strings, optional
        5-digit codes used to identify the constituent measured and the
        units of measure. See
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    parameter_name : string or iterable of strings, optional
        A human-understandable name corresponding to ``parameter_code``.
    parameter_description : string or iterable of strings, optional
        A human-readable description of what is being measured.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement
        associated with an observation.
    statistic_id : string or iterable of strings, optional
        A code corresponding to the statistic an observation represents
        (e.g. ``00001`` max, ``00002`` min, ``00003`` mean). Full list at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    data_type : string or iterable of strings, optional
        The type of data the time series represents, e.g.
        ``"Continuous values"``, ``"Daily values"``,
        ``"Field measurements"``.
    computation_identifier : string or iterable of strings, optional
        Indicates whether the data from this time series represent a
        specific statistical computation.
    thresholds : number or list of numbers, optional
        Numeric limits known for a time series (e.g. historic maximum,
        below-which-the-sensor-is-non-operative).
    sublocation_identifier : string or iterable of strings, optional
    primary : string or iterable of strings, optional
        A flag identifying whether the time series is "primary". Primary
        time series are standard observations that have undergone Bureau
        review and approval. Non-primary (provisional) time series have a
        missing ``primary`` value, are produced for timely best-science
        use, and are retained by this system for only 120 days.
    parent_time_series_id : string or iterable of strings, optional
    web_description : string or iterable of strings, optional
        A description of what this time series represents, as used by
        WDFN and other USGS data dissemination products.
    last_modified, begin, end : string, optional
        Datetime fields that accept either an RFC 3339 datetime, an
        interval (``"start/end"``, optionally half-bounded with ``..``),
        or an ISO 8601 duration (e.g. ``"P1M"``, ``"PT36H"``). See
        :func:`get_time_series_metadata` for the full grammar.
    state : string or iterable of strings, optional
        State/territory filter (the recommended parameter). Accepts a full
        name (``"Wisconsin"``), a two-letter postal code (``"WI"``), or a
        two-digit ANSI/FIPS code (``"55"``).
    state_name, county_name, hydrologic_unit_code, site_type, \
site_type_code : string or iterable of strings, optional
        Common location-catalog filters carried over from the
        ``monitoring-locations`` collection. The function also accepts
        the full list of location-catalog kwargs (agency, district,
        altitude, vertical/horizontal datum, drainage area, aquifer,
        well construction, …); see :func:`get_monitoring_locations` for
        descriptions of each.
    properties : string or iterable of strings, optional
        Subset of columns to return. Defaults to every available
        property.
    skip_geometry : boolean, optional
        Skip per-feature geometries; the returned object will be a plain
        ``DataFrame`` with no spatial information. The Water Data APIs
        use camelCase ``skipGeometry`` in CQL2 queries.
    bbox : list of numbers, optional
        Only features whose geometry intersects the bounding box are
        selected. Format: ``[xmin, ymin, xmax, ymax]`` in CRS 4326
        (longitude/latitude, west-south-east-north).
    limit : int, optional
        Page size; the maximum allowable value is 50000. Default
        (``None``) requests the maximum allowable limit.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Many collections share the monitoring-location
        attributes (e.g. ``state_name="Wisconsin"``, ``site_type_code="ST"``),
        but the available queryables differ per collection -- call
        :func:`get_queryables` for the ones this collection actually supports.
        An unknown name is rejected by the service with a ``DataRetrievalError``
        (HTTP 400). This passthrough is provisional and may be superseded by
        explicit per-property keyword arguments.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object pertaining to the query.

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # All time series and field measurements at a single surface-water site
        >>> df, md = dataretrieval.waterdata.get_combined_metadata(
        ...     monitoring_location_id="USGS-05407000"
        ... )

        >>> # Same, for a groundwater well — water-level and aquifer columns
        >>> # are populated where the surface-water example has nulls
        >>> df, md = dataretrieval.waterdata.get_combined_metadata(
        ...     monitoring_location_id="USGS-375907091432201"
        ... )

        >>> # Every series in a single county, useful for area-of-interest workflows
        >>> df, md = dataretrieval.waterdata.get_combined_metadata(
        ...     state="Wisconsin", county_name="Dane County"
        ... )

        >>> # Inventory across multiple HUCs, restricted to streams and springs
        >>> df, md = dataretrieval.waterdata.get_combined_metadata(
        ...     hydrologic_unit_code=["11010008", "11010009"],
        ...     site_type=["Stream", "Spring"],
        ... )

        >>> # Discharge time series at three sites with at least one
        >>> # observation in the past month
        >>> df, md = dataretrieval.waterdata.get_combined_metadata(
        ...     monitoring_location_id=[
        ...         "USGS-07069000",
        ...         "USGS-07064000",
        ...         "USGS-07068000",
        ...     ],
        ...     end="P1M",
        ...     parameter_code="00060",
        ... )

        >>> # Two-step "what's available?" → "fetch it" workflow:
        >>> # 1. inventory the sites in two HUCs
        >>> hucs, _ = dataretrieval.waterdata.get_combined_metadata(
        ...     hydrologic_unit_code=["11010008", "11010009"],
        ...     site_type="Stream",
        ... )
        >>> # 2. pull continuous discharge at every distinct site found
        >>> sites = hucs["monitoring_location_id"].unique().tolist()
        >>> df, md = dataretrieval.waterdata.get_continuous(
        ...     monitoring_location_id=sites,
        ...     parameter_code="00060",
        ...     time="P1D",
        ... )

    """
    service = "combined-metadata"

    # Resolve the unified `state` argument into the OGC `state_name` queryable.
    args = _get_args(_with_state(locals(), to="name", into="state_name"))

    return get_ogc_data(args, service)


def get_latest_continuous(
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    statistic_id: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    time_series_id: str | Iterable[str] | None = None,
    latest_continuous_id: str | Iterable[str] | None = None,
    approval_status: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    qualifier: str | Iterable[str] | None = None,
    value: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    time: str | Iterable[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """This endpoint provides the most recent observation for each time series
    of continuous data. Continuous data are collected via automated sensors
    installed at a monitoring location. They are collected at a high frequency
    and often at a fixed 15-minute interval. Depending on the specific monitoring
    location, the data may be transmitted automatically via telemetry and be
    available on WDFN within minutes of collection, while other times the delivery
    of data may be delayed if the monitoring location does not have the capacity to
    automatically transmit data. Continuous data are described by parameter name
    and parameter code. These data might also be referred to as "instantaneous
    values" or "IV".

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of the
        agency responsible for the monitoring location (e.g. USGS) with the ID
        number of the monitoring location (e.g. 02238500), separated by a hyphen
        (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter codes
        and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or iterable of strings, optional
        A code corresponding to the statistic an observation represents.
        Example codes include 00001 (max), 00002 (min), and 00003 (mean).
        A complete list of codes and their descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or iterable of strings, optional
        A list of requested columns to be returned from the query.  Available
        options are: geometry, id, time_series_id, monitoring_location_id,
        parameter_code, statistic_id, time, value, unit_of_measure,
        approval_status, qualifier, last_modified
    time_series_id : string or iterable of strings, optional
        A unique identifier representing a single time series. This
        corresponds to the id field in the time-series-metadata endpoint.
    latest_continuous_id : string or iterable of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. It is not stable over time. Every time the record is refreshed
        in our database (which may happen as part of normal operations and does
        not imply any change to the data itself) a new ID will be generated. To
        uniquely identify a single observation over time, compare the time and
        time_series_id fields; each time series will only have a single
        observation at a given time.
    approval_status : string or iterable of strings, optional
        Some of the data that you have obtained from this U.S. Geological Survey
        database may not have received Director's approval. Any such data values
        are qualified as provisional and are subject to revision. Provisional
        data are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from its
        use. This field reflects the approval status of each record, and is either
        "Approved", meaning processing review has been completed and the data is
        approved for publication, or "Provisional" and subject to revision. For
        more information about provisional data, go to:
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or iterable of strings, optional
        This field indicates any qualifiers associated with an observation, for
        instance if a sensor may have been impacted by ice or if values were
        estimated.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format in order to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        that anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features that have a last_modified that
        intersects the value of datetime are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    skip_geometry : boolean, optional
        This option can be used to skip response geometries for each feature.
        The returning object will be a data frame with no spatial information.
        Note that the USGS Water Data APIs use camelCase "skipGeometry" in
        CQL2 queries.
    time : string, optional
        The date an observation represents. You can query this field using
        date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects.  Intervals may be bounded or half-bounded (double-dots
        at start or end).  Only features that have a time that intersects the
        value of datetime are selected. If a feature has multiple temporal
        properties, it is the decision of the server whether only a single
        temporal property is used to determine the extent or all relevant
        temporal properties.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features that have a geometry that intersects the bounding box are
        selected.  The bounding box is provided as four or six numbers,
        depending on whether the coordinate reference system includes a vertical
        axis (height or depth). Coordinates are assumed to be in crs 4326. The
        expected format is ``[xmin, ymin, xmax, ymax]``, i.e.
        ``[Western-most longitude, Southern-most latitude, Eastern-most
        longitude, Northern-most latitude]``.
    limit : int, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Many collections share the monitoring-location
        attributes (e.g. ``state_name="Wisconsin"``, ``site_type_code="ST"``),
        but the available queryables differ per collection -- call
        :func:`get_queryables` for the ones this collection actually supports.
        An unknown name is rejected by the service with a ``DataRetrievalError``
        (HTTP 400). This passthrough is provisional and may be superseded by
        explicit per-property keyword arguments.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get latest flow data from a single site
        >>> df, md = dataretrieval.waterdata.get_latest_continuous(
        ...     monitoring_location_id="USGS-02238500", parameter_code="00060"
        ... )

        >>> # Restrict to the last 7 days; sites with no observation in that
        >>> # window are dropped instead of returned with stale values
        >>> df, md = dataretrieval.waterdata.get_latest_continuous(
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00060",
        ...     time="P7D",
        ... )

        >>> # Pull only rows whose underlying record was refreshed in the
        >>> # last 7 days, across multiple sites and parameters
        >>> df, md = dataretrieval.waterdata.get_latest_continuous(
        ...     monitoring_location_id=["USGS-451605097071701", "USGS-14181500"],
        ...     parameter_code=["00060", "72019"],
        ...     last_modified="P7D",
        ... )

        >>> # Get latest continuous measurements for multiple sites
        >>> df, md = dataretrieval.waterdata.get_latest_continuous(
        ...     monitoring_location_id=["USGS-05114000", "USGS-09423350"]
        ... )
    """
    service = "latest-continuous"

    # Build argument dictionary, omitting None values
    args = _get_args(locals())

    return get_ogc_data(args, service)


def get_latest_daily(
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    statistic_id: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    time_series_id: str | Iterable[str] | None = None,
    latest_daily_id: str | Iterable[str] | None = None,
    approval_status: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    qualifier: str | Iterable[str] | None = None,
    value: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    time: str | Iterable[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Daily data provide one data value to represent water conditions for the
    day.

    Throughout much of the history of the USGS, the primary water data available
    was daily data collected manually at the monitoring location once each day.
    With improved availability of computer storage and automated transmission of
    data, the daily data published today are generally a statistical summary or
    metric of the continuous data collected each day, such as the daily mean,
    minimum, or maximum value. Daily data are automatically calculated from the
    continuous data of the same parameter code and are described by parameter
    code and a statistic code. These data have also been referred to as “daily
    values” or “DV”.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of the
        agency responsible for the monitoring location (e.g. USGS) with the ID
        number of the monitoring location (e.g. 02238500), separated by a hyphen
        (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter codes
        and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or iterable of strings, optional
        A code corresponding to the statistic an observation represents.
        Example codes include 00001 (max), 00002 (min), and 00003 (mean).
        A complete list of codes and their descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or iterable of strings, optional
        A list of requested columns to be returned from the query.  Available
        options are: geometry, id, time_series_id, monitoring_location_id,
        parameter_code, statistic_id, time, value, unit_of_measure,
        approval_status, qualifier, last_modified
    time_series_id : string or iterable of strings, optional
        A unique identifier representing a single time series. This
        corresponds to the id field in the time-series-metadata endpoint.
    latest_daily_id : string or iterable of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. It is not stable over time. Every time the record is refreshed
        in our database (which may happen as part of normal operations and does
        not imply any change to the data itself) a new ID will be generated. To
        uniquely identify a single observation over time, compare the time and
        time_series_id fields; each time series will only have a single
        observation at a given time.
    approval_status : string or iterable of strings, optional
        Some of the data that you have obtained from this U.S. Geological Survey
        database may not have received Director's approval. Any such data values
        are qualified as provisional and are subject to revision. Provisional
        data are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from its
        use. This field reflects the approval status of each record, and is either
        "Approved", meaning processing review has been completed and the data is
        approved for publication, or "Provisional" and subject to revision. For
        more information about provisional data, go to:
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or iterable of strings, optional
        This field indicates any qualifiers associated with an observation, for
        instance if a sensor may have been impacted by ice or if values were
        estimated.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format in order to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        that anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features that have a last_modified that
        intersects the value of datetime are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    skip_geometry : boolean, optional
        This option can be used to skip response geometries for each feature.
        The returning object will be a data frame with no spatial information.
        Note that the USGS Water Data APIs use camelCase "skipGeometry" in
        CQL2 queries.
    time : string, optional
        The date an observation represents. You can query this field using
        date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects.  Intervals may be bounded or half-bounded (double-dots
        at start or end).  Only features that have a time that intersects the
        value of datetime are selected. If a feature has multiple temporal
        properties, it is the decision of the server whether only a single
        temporal property is used to determine the extent or all relevant
        temporal properties.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features that have a geometry that intersects the bounding box are
        selected.  The bounding box is provided as four or six numbers,
        depending on whether the coordinate reference system includes a vertical
        axis (height or depth). Coordinates are assumed to be in crs 4326. The
        expected format is ``[xmin, ymin, xmax, ymax]``, i.e.
        ``[Western-most longitude, Southern-most latitude, Eastern-most
        longitude, Northern-most latitude]``.
    limit : int, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Many collections share the monitoring-location
        attributes (e.g. ``state_name="Wisconsin"``, ``site_type_code="ST"``),
        but the available queryables differ per collection -- call
        :func:`get_queryables` for the ones this collection actually supports.
        An unknown name is rejected by the service with a ``DataRetrievalError``
        (HTTP 400). This passthrough is provisional and may be superseded by
        explicit per-property keyword arguments.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get most recent daily flow data from a single site
        >>> df, md = dataretrieval.waterdata.get_latest_daily(
        ...     monitoring_location_id="USGS-02238500", parameter_code="00060"
        ... )

        >>> # Restrict to rows whose underlying record was refreshed in the
        >>> # last 7 days
        >>> df, md = dataretrieval.waterdata.get_latest_daily(
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00060",
        ...     last_modified="P7D",
        ... )

        >>> # Multi-site, multi-parameter — discharge and water temperature
        >>> # at two sites in a single round-trip
        >>> df, md = dataretrieval.waterdata.get_latest_daily(
        ...     monitoring_location_id=["USGS-01491000", "USGS-01645000"],
        ...     parameter_code=["00060", "00010"],
        ... )

        >>> # Get most recent daily measurements for two sites
        >>> df, md = dataretrieval.waterdata.get_latest_daily(
        ...     monitoring_location_id=["USGS-05114000", "USGS-09423350"]
        ... )
    """
    service = "latest-daily"

    # Build argument dictionary, omitting None values
    args = _get_args(locals())

    return get_ogc_data(args, service)


def get_field_measurements(
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    observing_procedure_code: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    field_visit_id: str | Iterable[str] | None = None,
    approval_status: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    qualifier: str | Iterable[str] | None = None,
    value: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    observing_procedure: str | Iterable[str] | None = None,
    vertical_datum: str | Iterable[str] | None = None,
    measuring_agency: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    time: str | Iterable[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Field measurements are physically measured values collected during a
    visit to the monitoring location. Field measurements consist of measurements
    of gage height and discharge, and readings of groundwater levels, and are
    primarily used as calibration readings for the automated sensors collecting
    continuous data. They are collected at a low frequency, and delivery of the
    data in WDFN may be delayed due to data processing time.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of the
        agency responsible for the monitoring location (e.g. USGS) with the ID
        number of the monitoring location (e.g. 02238500), separated by a hyphen
        (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter codes
        and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    observing_procedure_code : string or iterable of strings, optional
        A short code corresponding to the observing procedure for the field
        measurement.
    properties : string or iterable of strings, optional
        A list of requested columns to be returned from the query. See the
        field-measurements schema in the OpenAPI reference for the available
        columns (e.g. geometry, id, monitoring_location_id, parameter_code,
        value, unit_of_measure, approval_status, qualifier, last_modified):
        https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/field-measurements
    field_visit_id : string or iterable of strings, optional
        A universally unique identifier (UUID) for the field visit.
        Multiple measurements may be made during a single field visit.
    approval_status : string or iterable of strings, optional
        Some of the data that you have obtained from this U.S. Geological Survey
        database may not have received Director's approval. Any such data values
        are qualified as provisional and are subject to revision. Provisional
        data are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from its
        use. This field reflects the approval status of each record, and is either
        "Approved", meaning processing review has been completed and the data is
        approved for publication, or "Provisional" and subject to revision. For
        more information about provisional data, go to:
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or iterable of strings, optional
        This field indicates any qualifiers associated with an observation, for
        instance if a sensor may have been impacted by ice or if values were
        estimated.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format in order to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        that anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features that have a last_modified that
        intersects the value of datetime are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    observing_procedure : string or iterable of strings, optional
        Water measurement or water-quality observing procedure descriptions.
    vertical_datum : string or iterable of strings, optional
        The datum used to determine altitude and vertical position at the
        monitoring location.
    measuring_agency : string or iterable of strings, optional
        The agency performing the measurement.
    skip_geometry : boolean, optional
        This option can be used to skip response geometries for each feature.
        The returning object will be a data frame with no spatial information.
        Note that the USGS Water Data APIs use camelCase "skipGeometry" in
        CQL2 queries.
    time : string, optional
        The date an observation represents. You can query this field using date-times
        or intervals, adhering to RFC 3339, or using ISO 8601 duration objects.
        Intervals may be bounded or half-bounded (double-dots at start or end).
        Only features that have a time that intersects the value of datetime are
        selected. If a feature has multiple temporal properties, it is the
        decision of the server whether only a single temporal property is used
        to determine the extent or all relevant temporal properties.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features that have a geometry that intersects the bounding box are
        selected.  The bounding box is provided as four or six numbers,
        depending on whether the coordinate reference system includes a vertical
        axis (height or depth). Coordinates are assumed to be in crs 4326. The
        expected format is ``[xmin, ymin, xmax, ymax]``, i.e.
        ``[Western-most longitude, Southern-most latitude, Eastern-most
        longitude, Northern-most latitude]``.
    limit : int, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Many collections share the monitoring-location
        attributes (e.g. ``state_name="Wisconsin"``, ``site_type_code="ST"``),
        but the available queryables differ per collection -- call
        :func:`get_queryables` for the ones this collection actually supports.
        An unknown name is rejected by the service with a ``DataRetrievalError``
        (HTTP 400). This passthrough is provisional and may be superseded by
        explicit per-property keyword arguments.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get field measurements from a single groundwater site
        >>> # and parameter code, and do not return geometry
        >>> df, md = dataretrieval.waterdata.get_field_measurements(
        ...     monitoring_location_id="USGS-375907091432201",
        ...     parameter_code="72019",
        ...     skip_geometry=True,
        ... )

        >>> # Half-bounded time range: every measurement at this site since
        >>> # 1980 (open-ended end). Use ``"../<date>"`` for the inverse
        >>> # (everything up to a date).
        >>> df, md = dataretrieval.waterdata.get_field_measurements(
        ...     monitoring_location_id="USGS-425957088141001",
        ...     time="1980-01-01/..",
        ... )

        >>> # Get field measurements from multiple sites and
        >>> # parameter codes from the last 20 years
        >>> df, md = dataretrieval.waterdata.get_field_measurements(
        ...     monitoring_location_id=[
        ...         "USGS-451605097071701",
        ...         "USGS-263819081585801",
        ...     ],
        ...     parameter_code=["62611", "72019"],
        ...     time="P20Y",
        ... )
    """
    service = "field-measurements"

    # Build argument dictionary, omitting None values
    args = _get_args(locals())

    return get_ogc_data(args, service)


def get_field_measurements_metadata(
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    parameter_name: str | Iterable[str] | None = None,
    parameter_description: str | Iterable[str] | None = None,
    begin: str | Iterable[str] | None = None,
    end: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get field-measurement metadata: one row per (location, parameter) series.

    Each row describes a single field-measurement series — what parameter is
    measured at the location, the period of record (``begin`` / ``end``), the
    units, and so on — without returning the underlying observations
    themselves. Use :func:`get_field_measurements` to fetch the values.

    This is the discrete-measurement analogue to
    :func:`get_time_series_metadata` (which describes daily and continuous
    series). It's primarily useful for inventory queries: "what
    field-measurement parameters does this site have, and over what date
    range?"

    See the OpenAPI reference for the full list of supported fields:
    https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/field-measurements-metadata

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location, in
        ``AGENCY-ID`` form (e.g. ``"USGS-02238500"``).
    parameter_code : string or iterable of strings, optional
        5-digit parameter code. See
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    parameter_name : string or iterable of strings, optional
        A human-understandable name corresponding to ``parameter_code``.
    parameter_description : string or iterable of strings, optional
        A human-readable description of what is being measured.
    begin, end, last_modified : string, optional
        Datetime fields that accept either an RFC 3339 datetime, an
        interval (``"start/end"``, optionally half-bounded with ``..``),
        or an ISO 8601 duration (e.g. ``"P1M"``, ``"PT36H"``). See
        :func:`get_time_series_metadata` for the full grammar.
    properties : string or iterable of strings, optional
        Subset of columns to return. Defaults to every available property.
    skip_geometry : boolean, optional
        Skip per-feature geometries; the returned object will be a plain
        ``DataFrame`` with no spatial information.
    bbox : list of numbers, optional
        Only features whose geometry intersects the bounding box are
        selected. Format: ``[xmin, ymin, xmax, ymax]`` in CRS 4326
        (longitude / latitude, west-south-east-north).
    limit : int, optional
        Page size; the maximum allowable value is 50000. Default
        (``None``) requests the maximum allowable limit.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Many collections share the monitoring-location
        attributes (e.g. ``state_name="Wisconsin"``, ``site_type_code="ST"``),
        but the available queryables differ per collection -- call
        :func:`get_queryables` for the ones this collection actually supports.
        An unknown name is rejected by the service with a ``DataRetrievalError``
        (HTTP 400). This passthrough is provisional and may be superseded by
        explicit per-property keyword arguments.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object pertaining to the query.

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # All field-measurement series at a surface-water site
        >>> df, md = dataretrieval.waterdata.get_field_measurements_metadata(
        ...     monitoring_location_id="USGS-02238500"
        ... )

        >>> # Same, for a groundwater well
        >>> df, md = dataretrieval.waterdata.get_field_measurements_metadata(
        ...     monitoring_location_id="USGS-375907091432201"
        ... )

        >>> # Multi-site, narrowed to two parameter codes
        >>> df, md = dataretrieval.waterdata.get_field_measurements_metadata(
        ...     monitoring_location_id=[
        ...         "USGS-451605097071701",
        ...         "USGS-263819081585801",
        ...     ],
        ...     parameter_code=["62611", "72019"],
        ... )

        >>> # Series modified in the last year — useful for incremental ETL
        >>> df, md = dataretrieval.waterdata.get_field_measurements_metadata(
        ...     monitoring_location_id="USGS-375907091432201",
        ...     parameter_code="72019",
        ...     last_modified="P1Y",
        ... )

    """
    service = "field-measurements-metadata"

    args = _get_args(locals())

    return get_ogc_data(args, service)


def get_peaks(
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    time_series_id: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    time: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    water_year: int | list[int] | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    day: int | list[int] | None = None,
    peak_since: int | list[int] | None = None,
    properties: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get the annual peak streamflow / stage record for a monitoring location.

    Peaks are the largest values observed at a site each water year and are
    the standard input to flood-frequency analysis (e.g. log-Pearson Type III
    fits). The endpoint returns one row per (monitoring location, parameter,
    water year), with the peak ``value`` and the ``time`` it occurred.

    The collection covers both stage (parameter ``"00065"``, ``ft``) and
    discharge (parameter ``"00060"``, ``ft^3/s``); a typical streamgage has a
    series for each. Reference docs:
    https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/peaks

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location, in
        ``AGENCY-ID`` form (e.g. ``"USGS-02238500"``).
    parameter_code : string or iterable of strings, optional
        5-digit parameter code. Most peaks records are ``"00060"`` (discharge)
        or ``"00065"`` (stage / gage height). Full list at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    time_series_id : string or iterable of strings, optional
        ID of the time series the peak belongs to.
    unit_of_measure : string or iterable of strings, optional
        Human-readable units (e.g. ``"ft^3/s"``, ``"ft"``).
    time : string, optional
        Datetime, interval, or duration filter on the peak's date.
        See :func:`get_time_series_metadata` for the full grammar.
    last_modified : string, optional
        Same datetime grammar as ``time``; filters on the database
        last-modified timestamp (useful for incremental ETL polling).
    water_year, year, month, day : int or list of ints, optional
        Calendar / water-year filters on the peak event. The water year ends
        September 30 (e.g. WY2024 = Oct 1, 2023 – Sep 30, 2024).
    peak_since : int or list of ints, optional
        Filter on the year since which the peak value has stood as the
        record (the API serves this field as an integer; many rows are
        ``null``).
    properties : string or iterable of strings, optional
        Subset of columns to return. Defaults to every available property.
    skip_geometry : boolean, optional
        Skip per-feature geometries; the returned object will be a plain
        ``DataFrame`` with no spatial information.
    bbox : list of numbers, optional
        Only features whose geometry intersects the bounding box are
        selected. Format: ``[xmin, ymin, xmax, ymax]`` in CRS 4326
        (longitude / latitude, west-south-east-north).
    limit : int, optional
        Page size; the maximum allowable value is 50000. Default
        (``None``) requests the maximum allowable limit.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Many collections share the monitoring-location
        attributes (e.g. ``state_name="Wisconsin"``, ``site_type_code="ST"``),
        but the available queryables differ per collection -- call
        :func:`get_queryables` for the ones this collection actually supports.
        An unknown name is rejected by the service with a ``DataRetrievalError``
        (HTTP 400). This passthrough is provisional and may be superseded by
        explicit per-property keyword arguments.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object pertaining to the query.

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Full annual peak record at one site (both stage and discharge)
        >>> df, md = dataretrieval.waterdata.get_peaks(
        ...     monitoring_location_id="USGS-02238500"
        ... )

        >>> # Discharge peaks only
        >>> df, md = dataretrieval.waterdata.get_peaks(
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00060",
        ... )

        >>> # Multi-site peaks for a parameter, narrowed to a water-year range
        >>> df, md = dataretrieval.waterdata.get_peaks(
        ...     monitoring_location_id=[
        ...         "USGS-07069000",
        ...         "USGS-07064000",
        ...         "USGS-07068000",
        ...     ],
        ...     parameter_code="00060",
        ...     water_year=[2020, 2021, 2022, 2023],
        ... )

    """
    service = "peaks"

    args = _get_args(locals())

    return get_ogc_data(args, service)


def get_reference_table(
    collection: str,
    limit: int | None = None,
    query: dict[str, Any] | None = None,
    max_rows: int | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get metadata reference tables for the USGS Water Data API.

    Reference tables provide the range of allowable values for parameter
    arguments in the waterdata module.

    Parameters
    ----------
    collection : string
        One of the following options: "agency-codes", "altitude-datums",
        "aquifer-codes", "aquifer-types", "coordinate-accuracy-codes",
        "coordinate-datum-codes", "coordinate-method-codes", "counties",
        "hydrologic-unit-codes", "medium-codes", "national-aquifer-codes",
        "parameter-codes", "reliability-codes", "site-types", "states",
        "statistic-codes", "topographic-codes", "time-zone-codes"
    limit : int, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    query: dictionary, optional
        The optional query parameter can be used to pass a dictionary of
        query parameters to the collection API call.
    max_rows : int, optional
        Cap the total number of rows returned, stopping pagination early
        instead of downloading the whole table. Useful for cheaply
        previewing large tables (e.g. ``hydrologic-unit-codes`` has ~125k
        rows). Unlike ``limit`` (the per-page size), this bounds the total
        result. The default (None) downloads every page.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query. The primary metadata
        of each reference table will show up in the first column, where
        the name of the column is the singular form of the collection name,
        separated by underscores (e.g. the "medium-codes" reference table
        has a column called "medium_code", which contains all possible
        medium code values).
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object including the URL request and query time.

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get table of USGS parameter codes
        >>> ref, md = dataretrieval.waterdata.get_reference_table(
        ...     collection="parameter-codes"
        ... )

        >>> # Get table of selected USGS parameter codes
        >>> ref, md = dataretrieval.waterdata.get_reference_table(
        ...     collection="parameter-codes",
        ...     query={"id": "00001,00002"},
        ... )
    """
    valid_code_services = get_args(METADATA_COLLECTIONS)
    if collection not in valid_code_services:
        raise ValueError(
            f"Invalid code service: '{collection}'. "
            f"Valid options are: {valid_code_services}."
        )

    # Give the ID column the collection name, singularized and underscored.
    if collection == "counties":
        output_id = "county"
    elif collection.endswith("s"):
        output_id = collection[:-1].replace("-", "_")
    else:
        output_id = collection.replace("-", "_")

    query_args = dict(query) if query else {}
    if limit is not None:
        query_args["limit"] = limit
    return get_ogc_data(
        args=query_args, output_id=output_id, service=collection, max_rows=max_rows
    )


def get_queryables(collection: str) -> tuple[pd.DataFrame, BaseMetadata]:
    """List the queryable properties of a Water Data API collection.

    Every OGC collection (``daily``, ``continuous``, ``monitoring-locations``,
    ...) advertises the set of properties that can be filtered on -- exposed as
    the typed keyword arguments of the matching ``get_*`` function, and usable
    directly in a CQL2 ``filter``. This returns that set, so the available
    filters can be discovered programmatically and monitored for upstream
    additions.

    Parameters
    ----------
    collection : string
        The collection id, e.g. ``"daily"``, ``"continuous"``,
        ``"monitoring-locations"``, or ``"time-series-metadata"``. See
        :data:`dataretrieval.waterdata.types.WATERDATA_SERVICES` for the data
        collections; reference collections (e.g. ``"parameter-codes"``) work
        too.

    Returns
    -------
    df : ``pandas.DataFrame``
        One row per queryable, sorted by name, with columns ``queryable`` (the
        property name), ``type``, ``title``, and ``description``.
    md : :class:`dataretrieval.utils.BaseMetadata`
        Metadata describing the request (URL, query time, response headers).

    Raises
    ------
    DataRetrievalError
        On an HTTP error response (e.g. an unknown ``collection`` yields a 404),
        the typed subclass for the status.

    Examples
    --------
    .. doctest::
        :skipif: True  # network

        >>> from dataretrieval import waterdata
        >>> df, md = waterdata.get_queryables("daily")
        >>> df.set_index("queryable").loc["state_name", "type"]
        'string'
    """
    # The OGC queryables document is a JSON Schema whose ``properties`` map each
    # filterable property name to a ``{title, type, description}`` definition.
    body, response = _check_ogc_requests(endpoint=collection, req_type="queryables")
    properties: dict[str, Any] = body.get("properties", {})
    df = pd.DataFrame(
        [
            {
                "queryable": name,
                "type": prop.get("type"),
                "title": prop.get("title"),
                "description": (prop.get("description") or "").strip(),
            }
            for name, prop in sorted(properties.items())
        ],
        columns=["queryable", "type", "title", "description"],
    )
    return df, BaseMetadata(response)


def get_codes(code_service: CODE_SERVICES) -> tuple[pd.DataFrame, BaseMetadata]:
    """Return codes from a Samples code service.

    Parameters
    ----------
    code_service : string
        One of the following options: "states", "counties", "countries",
        "sitetype", "samplemedia", "characteristicgroup", "characteristics",
        or "observedproperty"

    Returns
    -------
    df : ``pandas.DataFrame``
        The requested code table.
    md : :obj:`dataretrieval.utils.BaseMetadata`
        Metadata for the query (URL, query time, response headers).
    """
    valid_code_services = get_args(CODE_SERVICES)
    if code_service not in valid_code_services:
        raise ValueError(
            f"Invalid code service: '{code_service}'. "
            f"Valid options are: {valid_code_services}."
        )

    url = f"{SAMPLES_URL}/codeservice/{code_service}?mimeType=application%2Fjson"

    response = _get(url, headers=_default_headers(), **HTTPX_DEFAULTS)

    _raise_for_non_200(response)

    data_dict = json.loads(response.text)
    data_list = data_dict["data"]

    df = pd.DataFrame(data_list)

    return df, BaseMetadata(response)


def _get_samples_csv(
    url: str, params: dict[str, Any], ssl_check: bool
) -> tuple[pd.DataFrame, httpx.Response]:
    """Issue a Samples CSV request and parse the body into a DataFrame.

    Shared tail for the Samples getters: sends the GET with the standard
    headers (including ``X-Api-Key``), raises a typed error on a non-200
    (consistent with the OGC/stats path) instead of a bare
    ``HTTPStatusError``, and reads the CSV. The caller wraps the response
    as metadata and applies any per-getter post-step.
    """
    logger.debug("Request: %s", httpx.URL(url).copy_merge_params(params))
    response = _get(
        url,
        params=params,
        verify=ssl_check,
        headers=_default_headers(),
        **HTTPX_DEFAULTS,
    )
    _raise_for_non_200(response)
    df = pd.read_csv(StringIO(response.text), delimiter=",")
    return df, response


# Map the public snake_case ``get_samples`` parameters to the camelCase query
# parameter names the Samples API expects on the wire. ``characteristic`` is
# already snake_case-compatible (single word) and is sent unchanged. The
# remaining snake_case params are bookkeeping (``service``/``profile``/
# ``ssl_check``) and never reach the request.
_SAMPLES_PARAM_TO_API = {
    "activity_media_name": "activityMediaName",
    "activity_start_date_lower": "activityStartDateLower",
    "activity_start_date_upper": "activityStartDateUpper",
    "activity_type_code": "activityTypeCode",
    "characteristic_group": "characteristicGroup",
    "characteristic_user_supplied": "characteristicUserSupplied",
    "bbox": "boundingBox",
    "country_code": "countryFips",
    "state_code": "stateFips",
    "county_code": "countyFips",
    "site_type_code": "siteTypeCode",
    "site_type_name": "siteTypeName",
    "usgs_pcode": "usgsPCode",
    "hydrologic_unit": "hydrologicUnit",
    "monitoring_location_id": "monitoringLocationIdentifier",
    "organization_id": "organizationIdentifier",
    "point_location_latitude": "pointLocationLatitude",
    "point_location_longitude": "pointLocationLongitude",
    "point_location_within_miles": "pointLocationWithinMiles",
    "project_id": "projectIdentifier",
    "record_identifier_user_supplied": "recordIdentifierUserSupplied",
}

# Deprecated camelCase keyword names (the Samples-API spelling) accepted for
# backward compatibility, mapped to the new snake_case parameter names. Derived
# from ``_SAMPLES_PARAM_TO_API`` so the two never drift apart.
_SAMPLES_LEGACY_KWARGS = {
    api_name: py_name for py_name, api_name in _SAMPLES_PARAM_TO_API.items()
}


@_accept_legacy_kwargs(_SAMPLES_LEGACY_KWARGS)
def get_samples(
    ssl_check: bool = True,
    service: SERVICES = "results",
    profile: PROFILES = "fullphyschem",
    activity_media_name: str | Iterable[str] | None = None,
    activity_start_date_lower: str | None = None,
    activity_start_date_upper: str | None = None,
    activity_type_code: str | Iterable[str] | None = None,
    characteristic_group: str | Iterable[str] | None = None,
    characteristic: str | Iterable[str] | None = None,
    characteristic_user_supplied: str | Iterable[str] | None = None,
    bbox: list[float] | None = None,
    country_code: str | Iterable[str] | None = None,
    state_code: str | Iterable[str] | None = None,
    county_code: str | Iterable[str] | None = None,
    site_type_code: str | Iterable[str] | None = None,
    site_type_name: str | Iterable[str] | None = None,
    usgs_pcode: str | Iterable[str] | None = None,
    hydrologic_unit: str | Iterable[str] | None = None,
    monitoring_location_id: str | Iterable[str] | None = None,
    organization_id: str | Iterable[str] | None = None,
    point_location_latitude: float | None = None,
    point_location_longitude: float | None = None,
    point_location_within_miles: float | None = None,
    project_id: str | Iterable[str] | None = None,
    record_identifier_user_supplied: str | Iterable[str] | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Search Samples database for USGS water quality data.
    This is a wrapper function for the Samples database API. All potential
    filters are provided as arguments to the function, but please do not
    populate all possible filters; leave as many as feasible with their default
    value (None). This is important because overcomplicated web service queries
    can bog down the database's ability to return an applicable dataset before
    it times out.

    The web GUI for the Samples database can be found here:
    https://waterdata.usgs.gov/download-samples/#dataProfile=site

    If you would like more details on feasible query parameters (complete with
    examples), please visit the Samples database swagger docs, here:
    https://api.waterdata.usgs.gov/samples-data/docs#/

    Parameters
    ----------
    ssl_check : bool, optional
        Check the SSL certificate.
    service : string
        One of the available Samples services: "results", "locations", "activities",
        "projects", or "organizations". Defaults to "results".
    profile : string
        One of the available profiles associated with a service. Options for each
        service are:
        results - "fullphyschem", "basicphyschem",
        "fullbio", "basicbio", "narrow",
        "resultdetectionquantitationlimit",
        "labsampleprep", "count"
        locations - "site", "count"
        activities - "sampact", "actmetric",
        "actgroup", "count"
        projects - "project", "projectmonitoringlocationweight"
        organizations - "organization", "count"
    activity_media_name : string or iterable of strings, optional
        Name or code indicating environmental medium in which sample was taken.
        Call ``get_codes("samplemedia")`` for the valid inputs.
        Example: "Water". (Samples API: ``activityMediaName``)
    activity_start_date_lower : string, optional
        The start date if using a date range. Takes the format YYYY-MM-DD.
        The logic is inclusive, i.e. it will also return results that
        match the date. If left as None, will pull all data on or before
        ``activity_start_date_upper``, if populated.
        (Samples API: ``activityStartDateLower``)
    activity_start_date_upper : string, optional
        The end date if using a date range. Takes the format YYYY-MM-DD.
        The logic is inclusive, i.e. it will also return results that
        match the date. If left as None, will pull all data after
        ``activity_start_date_lower`` up to the most recent available results.
        (Samples API: ``activityStartDateUpper``)
    activity_type_code : string or iterable of strings, optional
        Text code that describes type of field activity performed.
        Example: "Sample-Routine, regular". (Samples API: ``activityTypeCode``)
    characteristic_group : string or iterable of strings, optional
        Characteristic group is a broad category of characteristics
        describing one or more results. Call ``get_codes("characteristicgroup")``
        for the valid inputs.
        Example: "Organics, PFAS" (Samples API: ``characteristicGroup``)
    characteristic : string or iterable of strings, optional
        Characteristic is a specific category describing one or more results.
        Call ``get_codes("characteristics")`` for the valid inputs.
        Example: "Suspended Sediment Discharge" (Samples API: ``characteristic``)
    characteristic_user_supplied : string or iterable of strings, optional
        A user supplied characteristic name describing one or more results.
        (Samples API: ``characteristicUserSupplied``)
    bbox : list of four floats, optional
        Filters on the associated monitoring location's point location
        by checking if it is located within the specified geographic area.
        The logic is inclusive, i.e. it will include locations that overlap
        with the edge of the bounding box. Values are separated by commas,
        expressed in decimal degrees, NAD83, and longitudes west of Greenwich
        are negative. The format is a list consisting of:

            * Western-most longitude
            * Southern-most latitude
            * Eastern-most longitude
            * Northern-most latitude

        Example: [-92.8,44.2,-88.9,46.0] (Samples API: ``boundingBox``)
    country_code : string or iterable of strings, optional
        Example: "US" (United States) (Samples API: ``countryFips``)
    state_code : string or iterable of strings, optional
        Call ``get_codes("states")`` for the valid inputs.
        Example: "US:15" (United States: Hawaii) (Samples API: ``stateFips``)
    county_code : string or iterable of strings, optional
        Call ``get_codes("counties")`` for the valid inputs.
        Example: "US:15:001" (United States: Hawaii, Hawaii County)
        (Samples API: ``countyFips``)
    site_type_code : string or iterable of strings, optional
        An abbreviation for a certain site type. Call ``get_codes("sitetype")``
        for the valid inputs.
        Example: "GW" (Groundwater site) (Samples API: ``siteTypeCode``)
    site_type_name : string or iterable of strings, optional
        A full name for a certain site type. Call ``get_codes("sitetype")``
        for the valid inputs.
        Example: "Well" (Samples API: ``siteTypeName``)
    usgs_pcode : string or iterable of strings, optional
        5-digit number used in the US Geological Survey computerized
        data system, National Water Information System (NWIS), to
        uniquely identify a specific constituent (the ``parameterCode`` column
        of ``get_codes("characteristics")``).
        Example: "00060" (Discharge, cubic feet per second)
        (Samples API: ``usgsPCode``)
    hydrologic_unit : string or iterable of strings, optional
        Max 12-digit number used to describe a hydrologic unit.
        Example: "070900020502" (Samples API: ``hydrologicUnit``)
    monitoring_location_id : string or iterable of strings, optional
        A monitoring location identifier has two parts: the agency code
        and the location number, separated by a dash (-).
        Example: "USGS-040851385"
        (Samples API: ``monitoringLocationIdentifier``)
    organization_id : string or iterable of strings, optional
        Designator used to uniquely identify a specific organization.
        Currently only accepting the organization "USGS".
        (Samples API: ``organizationIdentifier``)
    point_location_latitude : float, optional
        Latitude for a point/radius query (decimal degrees). Must be used
        with ``point_location_longitude`` and ``point_location_within_miles``.
        (Samples API: ``pointLocationLatitude``)
    point_location_longitude : float, optional
        Longitude for a point/radius query (decimal degrees). Must be used
        with ``point_location_latitude`` and ``point_location_within_miles``.
        (Samples API: ``pointLocationLongitude``)
    point_location_within_miles : float, optional
        Radius for a point/radius query. Must be used with
        ``point_location_latitude`` and ``point_location_longitude``.
        (Samples API: ``pointLocationWithinMiles``)
    project_id : string or iterable of strings, optional
        Designator used to uniquely identify a data collection project. Project
        identifiers are specific to an organization (e.g. USGS).
        Example: "ZH003QW03" (Samples API: ``projectIdentifier``)
    record_identifier_user_supplied : string or iterable of strings, optional
        Internal AQS record identifier that returns 1 entry. Only available
        for the "results" service.
        (Samples API: ``recordIdentifierUserSupplied``)

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query. For each
        ``<prefix>Date`` / ``<prefix>Time`` / ``<prefix>TimeZone`` triplet in
        the response (e.g. ``Activity_StartDate``, ``Activity_StartTime``,
        ``Activity_StartTimeZone``), an additional ``<prefix>DateTime`` column
        is appended holding a UTC ``Timestamp`` derived from the three. The
        original Date/Time/TimeZone columns are left intact; rows whose
        timezone abbreviation is not recognized resolve to ``NaT``. Rows are
        sorted by ``Activity_StartDateTime`` when present (the API's default
        order is unstable).
    md : :obj:`dataretrieval.utils.BaseMetadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get PFAS results within a bounding box
        >>> df, md = dataretrieval.waterdata.get_samples(
        ...     bbox=[-90.2, 42.6, -88.7, 43.2],
        ...     characteristic_group="Organics, PFAS",
        ... )

        >>> # Get all activities for the Commonwealth of Virginia over a date range
        >>> df, md = dataretrieval.waterdata.get_samples(
        ...     service="activities",
        ...     profile="sampact",
        ...     activity_start_date_lower="2023-10-01",
        ...     activity_start_date_upper="2024-01-01",
        ...     state_code="US:51",
        ... )

        >>> # Get all pH samples for two sites in Utah
        >>> df, md = dataretrieval.waterdata.get_samples(
        ...     monitoring_location_id=[
        ...         "USGS-393147111462301",
        ...         "USGS-393343111454101",
        ...     ],
        ...     usgs_pcode="00400",
        ... )

    """

    _check_profiles(service, profile)

    # Build argument dictionary, omitting None values. Parameters are the
    # public snake_case names here; translate them to the camelCase names the
    # Samples API expects just before building the request.
    args = _get_args(locals(), exclude={"ssl_check", "profile"})
    params = {_SAMPLES_PARAM_TO_API.get(key, key): value for key, value in args.items()}

    params.update({"mimeType": "text/csv"})

    if "boundingBox" in params:
        params["boundingBox"] = to_str(params["boundingBox"])

    url = f"{SAMPLES_URL}/{service}/{profile}"

    df, response = _get_samples_csv(url, params, ssl_check)
    df = _attach_datetime_columns(df)

    return df, BaseMetadata(response)


@_accept_legacy_kwargs({"monitoringLocationIdentifier": "monitoring_location_id"})
def get_samples_summary(
    monitoring_location_id: str,
    ssl_check: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get a summary of discrete water-quality samples at a single monitoring location.

    Wraps the Samples database summary service described at
    https://api.waterdata.usgs.gov/samples-data/docs. The service returns one
    row per (characteristic group, characteristic, user-supplied characteristic)
    combination with result and activity counts and the first / most recent
    activity dates — useful for taking inventory of what discrete-sample data
    exists at a site before pulling the underlying observations with
    :func:`get_samples`.

    The summary service is single-site only: it accepts exactly one monitoring
    location per request.

    Parameters
    ----------
    monitoring_location_id : string
        A monitoring location identifier has two parts, separated by a dash
        (``-``): the agency code and the location number. Examples:
        ``"USGS-040851385"``, ``"AZ014-320821110580701"``,
        ``"CAX01-15304600"``. Bare location numbers without an agency prefix
        are accepted by the service but return an empty result, so a prefix
        is effectively required. (Samples API: ``monitoringLocationIdentifier``)
    ssl_check : bool, optional
        Check the SSL certificate. Default is True.

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.BaseMetadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # What discrete-sample data is available at this site?
        >>> df, md = dataretrieval.waterdata.get_samples_summary(
        ...     monitoring_location_id="USGS-04074950"
        ... )

    """
    if not isinstance(monitoring_location_id, str):
        raise TypeError(
            "monitoring_location_id must be a string; the Samples "
            "summary service accepts exactly one monitoring location per "
            f"request, got {type(monitoring_location_id).__name__}."
        )

    url = f"{SAMPLES_URL}/summary/{quote(monitoring_location_id, safe='')}"
    params = {"mimeType": "text/csv"}

    df, response = _get_samples_csv(url, params, ssl_check)

    return df, BaseMetadata(response)


def get_stats_por(
    approval_status: str | None = None,
    computation_type: str | Iterable[str] | None = None,
    country_code: str | Iterable[str] | None = None,
    state: str | Iterable[str] | None = None,
    state_code: str | Iterable[str] | None = None,
    county_code: str | Iterable[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    monitoring_location_id: str | Iterable[str] | None = None,
    page_size: int = 1000,
    parent_time_series_id: str | Iterable[str] | None = None,
    site_type_code: str | Iterable[str] | None = None,
    site_type_name: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    normal_type: str | None = None,
    expand_percentiles: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get day-of-year and month-of-year water data statistics from the
    USGS Water Data API.
    This service (called the "observationNormals" endpoint on api.waterdata.usgs.gov)
    provides endpoints for access to computations on the historical record regarding
    water conditions, including minimum, maximum, mean, median, and percentiles for
    day of year and month of year. For more information regarding the calculation of
    statistics and other details, please visit the Statistics documentation page:
    https://waterdata.usgs.gov/statistics-documentation/.

    Note: This API is under active beta development and subject to
    change. Improved handling of significant figures will be
    addressed in a future release.

    Parameters
    ----------
    approval_status: string, optional
        Whether to include approved and/or provisional observations.
        At this time, only approved observations are returned.
    computation_type: string, optional
        Desired statistical computation method. Available values are:
        arithmetic_mean, maximum, median, minimum, percentile.
    country_code: string, optional
        Country query parameter. API defaults to "US".
    state: string or iterable of strings, optional
        State/territory filter (the recommended parameter). Accepts a full name
        ("Wisconsin"), a two-letter postal code ("WI"), or a two-digit
        ANSI/FIPS code ("55").
    state_code: string, optional
        State query parameter. Takes the format "US:XX", where XX is
        the two-digit state code. API defaults to "US:42" (Pennsylvania).
    county_code: string, optional
        County query parameter. Takes the format "US:XX:YYY", where XX is
        the two-digit state code and YYY is the three-digit county code.
        API defaults to "US:42:103" (Pennsylvania, Pike County).
    start_date: string or datetime, optional
        Start day for the query in the month-day format (MM-DD).
    end_date: string or datetime, optional
        End day for the query in the month-day format (MM-DD).
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of the
        agency responsible for the monitoring location (e.g. USGS) with the ID
        number of the monitoring location (e.g. 02238500), separated by a hyphen
        (e.g. USGS-02238500).
    page_size : int, optional
        The number of results to return per page, where one result represents a
        monitoring location. The default is 1000.
    parent_time_series_id: string, optional
        The parent_time_series_id returns statistics tied to a
        particular database entry.
    site_type_code: string, optional
        Site type code query parameter.
        A list of valid site type codes is available at:
        https://api.waterdata.usgs.gov/ogcapi/v0/collections/site-types/items.
        Example: "GW" (Groundwater site)
    site_type_name: string, optional
        Site type name query parameter.
    parameter_code : string or iterable of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter codes
        and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    normal_type : string, optional
        Filter the returned normals to a single period. If unspecified
        (default), all matching data are returned. Available values:
        "DOY" (day-of-year) and "MOY" (month-of-year).
    expand_percentiles : boolean
        Percentile data for a given day of year or month of year by default
        are returned from the service as lists of string values and percentile
        thresholds in the "values" and "percentiles" columns, respectively.
        When `expand_percentiles` is set to True (default), each value and
        percentile threshold specific to a computation id are returned as
        individual rows in the dataframe, with the value reported in the
        "value" column and the corresponding percentile reported in a
        "percentile" column (and the "values" and "percentiles" columns
        are removed). Missing percentile values expressed as 'nan' in the
        list of string values are removed from the dataframe to save space.
        Setting `expand_percentiles` to False retains the "values" and
        "percentiles" columns produced by the service. Including
        both 'percentiles' and one or more other statistics ('median',
        'minimum', 'maximum', or 'arithmetic_mean') in the `computation_type`
        argument will return both the "values" column, containing the list
        of percentile threshold values, and a "value" column, containing
        the singular summary value for the other statistics.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object.

    Examples
    --------
    .. code::

        >>> # Get daily, monthly, and annual percentiles for streamflow at
        >>> # a monitoring location of interest
        >>> df, md = dataretrieval.waterdata.get_stats_por(
        ...     monitoring_location_id="USGS-05114000",
        ...     parameter_code="00060",
        ...     computation_type="percentile",
        ... )

        >>> # Get all daily and monthly statistics for the month of January
        >>> # over the entire period of record for streamflow and gage height
        >>> # at a monitoring location of interest
        >>> df, md = dataretrieval.waterdata.get_stats_por(
        ...     monitoring_location_id="USGS-05114000",
        ...     parameter_code=["00060", "00065"],
        ...     start_date="01-01",
        ...     end_date="01-31",
        ... )
    """
    # Build argument dictionary, omitting None values
    params = _get_args(
        _with_state(locals(), to="fips_us", into="state_code"),
        exclude={"expand_percentiles"},
    )

    return stats.get_data(
        args=params, service="observationNormals", expand_percentiles=expand_percentiles
    )


def get_stats_date_range(
    approval_status: str | None = None,
    computation_type: str | Iterable[str] | None = None,
    country_code: str | Iterable[str] | None = None,
    state: str | Iterable[str] | None = None,
    state_code: str | Iterable[str] | None = None,
    county_code: str | Iterable[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    monitoring_location_id: str | Iterable[str] | None = None,
    page_size: int = 1000,
    parent_time_series_id: str | Iterable[str] | None = None,
    site_type_code: str | Iterable[str] | None = None,
    site_type_name: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    interval_type: str | Iterable[str] | None = None,
    expand_percentiles: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get monthly and annual water data statistics from the USGS Water Data API.
    This service (called the "observationIntervals" endpoint on api.waterdata.usgs.gov)
    provides endpoints for access to computations on the historical record regarding
    water conditions, including minimum, maximum, mean, median, and percentiles for
    month-year, and water/calendar years. For more information regarding the calculation
    of statistics and other details, please visit the Statistics documentation page:
    https://waterdata.usgs.gov/statistics-documentation/.

    Note: This API is under active beta development and subject to
    change. Improved handling of significant figures will be
    addressed in a future release.

    Parameters
    ----------
    approval_status: string, optional
        Whether to include approved and/or provisional observations.
        At this time, only approved observations are returned.
    computation_type: string, optional
        Desired statistical computation method. Available values are:
        arithmetic_mean, maximum, median, minimum, percentile.
    country_code: string, optional
        Country query parameter. API defaults to "US".
    state: string or iterable of strings, optional
        State/territory filter (the recommended parameter). Accepts a full name
        ("Wisconsin"), a two-letter postal code ("WI"), or a two-digit
        ANSI/FIPS code ("55").
    state_code: string, optional
        State query parameter. Takes the format "US:XX", where XX is
        the two-digit state code. API defaults to "US:42" (Pennsylvania).
    county_code: string, optional
        County query parameter. Takes the format "US:XX:YYY", where XX is
        the two-digit state code and YYY is the three-digit county code.
        API defaults to "US:42:103" (Pennsylvania, Pike County).
    start_date: string or datetime, optional
        Start date for the query in the year-month-day format
        (YYYY-MM-DD).
    end_date: string or datetime, optional
        End date for the query in the year-month-day format
        (YYYY-MM-DD).
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of the
        agency responsible for the monitoring location (e.g. USGS) with the ID
        number of the monitoring location (e.g. 02238500), separated by a hyphen
        (e.g. USGS-02238500).
    page_size : int, optional
        The number of results to return per page, where one result represents a
        monitoring location. The default is 1000.
    parent_time_series_id: string, optional
        The parent_time_series_id returns statistics tied to a
        particular database entry.
    site_type_code: string, optional
        Site type code query parameter.
        You can see a list of valid site type codes here:
        https://api.waterdata.usgs.gov/ogcapi/v0/collections/site-types/items.
        Example: "GW" (Groundwater site)
    site_type_name: string, optional
        Site type name query parameter.
        You can see a list of valid site type names here:
        https://api.waterdata.usgs.gov/ogcapi/v0/collections/site-types/items.
        Example: "Well"
    parameter_code : string or iterable of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter codes
        and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    interval_type : string or iterable of strings, optional
        Filter the returned intervals to one or more periods. If unspecified
        (default), all matching data are returned. Available values:
        "M" (month), "CY" (calendar year), and "WY" (water year).
    expand_percentiles : boolean
        Percentile data for a given day of year or month of year by default
        are returned from the service as lists of string values and percentile
        thresholds in the "values" and "percentiles" columns, respectively.
        When `expand_percentiles` is set to True (default), each value and
        percentile threshold specific to a computation id are returned as
        individual rows in the dataframe, with the value reported in the
        "value" column and the corresponding percentile reported in a
        "percentile" column (and the "values" and "percentiles" columns
        are removed). Missing percentile values expressed as 'nan' in the
        list of string values are removed from the dataframe to save space.
        Setting `expand_percentiles` to False retains the "values" and
        "percentiles" columns produced by the service. Including
        both 'percentiles' and one or more other statistics ('median',
        'minimum', 'maximum', or 'arithmetic_mean') in the `computation_type`
        argument will return both the "values" column, containing the list
        of percentile threshold values, and a "value" column, containing
        the singular summary value for the other statistics.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object.

    Examples
    --------
    .. code::

        >>> # Get monthly and yearly medians for streamflow at streams in Rhode Island
        >>> # from calendar year 2024.
        >>> df, md = dataretrieval.waterdata.get_stats_date_range(
        ...     state="RI",  # Rhode Island (postal code, name, or FIPS all work)
        ...     parameter_code="00060",
        ...     site_type_code="ST",
        ...     start_date="2024-01-01",
        ...     end_date="2024-12-31",
        ...     computation_type="median",
        ... )

        >>> # Get monthly and yearly minimum and maximums for gage height at
        >>> # a monitoring location of interest
        >>> df, md = dataretrieval.waterdata.get_stats_date_range(
        ...     monitoring_location_id="USGS-05114000",
        ...     parameter_code="00065",
        ...     computation_type=["minimum", "maximum"],
        ... )
    """
    # Build argument dictionary, omitting None values
    params = _get_args(
        _with_state(locals(), to="fips_us", into="state_code"),
        exclude={"expand_percentiles"},
    )

    return stats.get_data(
        args=params,
        service="observationIntervals",
        expand_percentiles=expand_percentiles,
    )


def get_channel(
    monitoring_location_id: str | Iterable[str] | None = None,
    field_visit_id: str | Iterable[str] | None = None,
    measurement_number: str | Iterable[str] | None = None,
    time: str | Iterable[str] | None = None,
    channel_name: str | Iterable[str] | None = None,
    channel_flow: str | Iterable[str] | None = None,
    channel_flow_unit: str | Iterable[str] | None = None,
    channel_width: str | Iterable[str] | None = None,
    channel_width_unit: str | Iterable[str] | None = None,
    channel_area: str | Iterable[str] | None = None,
    channel_area_unit: str | Iterable[str] | None = None,
    channel_velocity: str | Iterable[str] | None = None,
    channel_velocity_unit: str | Iterable[str] | None = None,
    channel_location_distance: str | Iterable[str] | None = None,
    channel_location_distance_unit: str | Iterable[str] | None = None,
    channel_stability: str | Iterable[str] | None = None,
    channel_material: str | Iterable[str] | None = None,
    channel_evenness: str | Iterable[str] | None = None,
    horizontal_velocity_description: str | Iterable[str] | None = None,
    vertical_velocity_description: str | Iterable[str] | None = None,
    longitudinal_velocity_description: str | Iterable[str] | None = None,
    measurement_type: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    channel_measurement_type: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Channel measurements taken as part of streamflow field measurements.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of
        the agency responsible for the monitoring location (e.g. USGS) with
        the ID number of the monitoring location (e.g. 02238500), separated
        by a hyphen (e.g. USGS-02238500).
    field_visit_id : string or iterable of strings, optional
        A universally unique identifier (UUID) for the field visit.
        Multiple measurements
        may be made during a single field visit.
    measurement_number : string or iterable of strings, optional
        Measurement number.
    time : string or iterable of strings, optional
        The date an observation represents. You can query this field using
        date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features that have a time that intersects the
        value of datetime are selected. If a feature has multiple temporal
        properties, it is the decision of the server whether only a single
        temporal property is used to determine the extent or all relevant
        temporal properties.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
              "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H"
              for the last 36 hours

    channel_name : string or iterable of strings, optional
        The channel name.
    channel_flow : string or iterable of strings, optional
        The channel discharge (flow).
    channel_flow_unit : string or iterable of strings, optional
        The units for channel discharge.
    channel_width : string or iterable of strings, optional
        The channel width.
    channel_width_unit : string or iterable of strings, optional
        The units for channel width.
    channel_area : string or iterable of strings, optional
        The channel area.
    channel_area_unit : string or iterable of strings, optional
        The units for channel area.
    channel_velocity :  string or iterable of strings, optional
        The mean channel velocity.
    channel_velocity_unit : string or iterable of strings, optional
        The units for channel velocity.
    channel_location_distance : string or iterable of strings, optional
        The channel location distance.
    channel_location_distance_unit : string or iterable of strings, optional
        The units for channel location distance.
    channel_stability : string or iterable of strings, optional
        The stability of the channel material.
    channel_material : string or iterable of strings, optional
        The channel material.
    channel_evenness : string or iterable of strings, optional
        The channel evenness from bank to bank.
    horizontal_velocity_description : string or iterable of strings, optional
        The horizontal velocity description.
    vertical_velocity_description : string or iterable of strings, optional
        The vertical velocity description.
    longitudinal_velocity_description : string or iterable of strings, optional
        The longitudinal velocity description.
    measurement_type : string or iterable of strings, optional
        The type of channel measurement.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        that anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features that have a last_modified that
        intersects the value of datetime are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    skip_geometry : boolean, optional
        This option can be used to skip response geometries for each feature.
        The returning object will be a data frame with no spatial information.
        Note that the USGS Water Data APIs use camelCase "skipGeometry" in
        CQL2 queries.
    channel_measurement_type : string or iterable of strings, optional
        The channel measurement type.
    properties : string or iterable of strings, optional
        A list of requested columns to be returned from the query. Available
        options are: geometry, channel_measurements_id, monitoring_location_id,
        field_visit_id, measurement_number, time, channel_name, channel_flow,
        channel_flow_unit, channel_width, channel_width_unit, channel_area,
        channel_area_unit, channel_velocity, channel_velocity_unit,
        channel_location_distance, channel_location_distance_unit, channel_stability,
        channel_material, channel_evenness, horizontal_velocity_description,
        vertical_velocity_description, longitudinal_velocity_description,
        measurement_type, last_modified, channel_measurement_type. The default
        (None) will return all columns of the data.
    bbox : list of numbers, optional
        Only features that have a geometry that intersects the bounding box are
        selected.  The bounding box is provided as four or six numbers,
        depending on whether the coordinate reference system includes a vertical
        axis (height or depth). Coordinates are assumed to be in crs 4326. The
        expected format is ``[xmin, ymin, xmax, ymax]``, i.e.
        ``[Western-most longitude, Southern-most latitude, Eastern-most
        longitude, Northern-most latitude]``.
    limit : int, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Many collections share the monitoring-location
        attributes (e.g. ``state_name="Wisconsin"``, ``site_type_code="ST"``),
        but the available queryables differ per collection -- call
        :func:`get_queryables` for the ones this collection actually supports.
        An unknown name is rejected by the service with a ``DataRetrievalError``
        (HTTP 400). This passthrough is provisional and may be superseded by
        explicit per-property keyword arguments.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get channel data from a
        >>> # single site from a single year
        >>> df, md = dataretrieval.waterdata.get_channel(
        ...     monitoring_location_id="USGS-02238500",
        ... )
    """
    service = "channel-measurements"

    args = _get_args(locals())

    return get_ogc_data(args, service)


def get_cql(
    service: WATERDATA_SERVICES,
    cql: str | dict[str, Any],
    *,
    properties: str | Iterable[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    skip_geometry: bool | None = None,
    convert_type: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Query a Water Data OGC API collection with an arbitrary CQL2 filter.

    Sends ``cql`` as a CQL2 filter against ``service`` and returns the matching
    features, shaped like the typed getters (``get_daily``, ``get_continuous``,
    …): the wire ``id`` renamed to the service's id column, columns ordered and
    sorted, and dtypes coerced. Use it when you need a predicate the typed
    getters can't express — a top-level ``or``, ``like`` with ``%`` wildcards,
    comparison operators, nested boolean trees, or a geometry predicate beyond a
    bounding box; prefer a typed getter when one covers the query.

    The request is a single POST with the ``cql`` body sent verbatim, so there
    are no multi-value arguments to chunk: narrow a query whose URL or body
    would exceed the server's size cap rather than relying on automatic
    chunking.

    The CQL2 grammar is documented at
    https://api.waterdata.usgs.gov/docs/ogcapi/complex-queries/.

    Parameters
    ----------
    service : str
        OGC collection name. Must be one of
        :data:`dataretrieval.waterdata.types.WATERDATA_SERVICES`
        (e.g. ``"daily"``, ``"monitoring-locations"``).
    cql : str or dict
        CQL2 query. A ``dict`` is JSON-serialized for transport; a ``str`` is
        sent through unchanged. The query goes into the HTTP POST body with
        ``Content-Type: application/query-cql-json``.
    properties : str or iterable of str, optional
        Server-side property whitelist (passed as ``properties=`` on the URL).
        Reduces payload size. ``"id"`` resolves to the service's ``output_id``
        (e.g. ``daily_id``) the same way it does in the typed wrappers.
    bbox : list of float, optional
        Bounding box ``[xmin, ymin, xmax, ymax]`` in CRS 4326. Combines with the
        CQL filter as an additional spatial predicate.
    limit : int, optional
        Page size, clamped server-side to 50,000.
    skip_geometry : bool, optional
        If True, the server omits geometry from each feature
        (``skipGeometry=true``).
    convert_type : bool, default True
        Coerce date/datetime/numeric columns to typed dtypes after the
        DataFrame is built.

    Returns
    -------
    df : pandas.DataFrame or geopandas.GeoDataFrame
        Result of the query. GeoDataFrame when ``geopandas`` is installed and
        geometry is present.
    md : :class:`dataretrieval.utils.BaseMetadata`
        Request metadata (URL, query time, response headers).

    Examples
    --------
    .. code::

        >>> # Daily values for two parameter codes at two sites
        >>> # (compound AND-of-INs).
        >>> from dataretrieval import waterdata
        >>> cql = {
        ...     "op": "and",
        ...     "args": [
        ...         {
        ...             "op": "in",
        ...             "args": [
        ...                 {"property": "parameter_code"},
        ...                 ["00060", "00065"],
        ...             ],
        ...         },
        ...         {
        ...             "op": "in",
        ...             "args": [
        ...                 {"property": "monitoring_location_id"},
        ...                 ["USGS-07367300", "USGS-03277200"],
        ...             ],
        ...         },
        ...     ],
        ... }
        >>> df, md = waterdata.get_cql(service="daily", cql=cql)

        >>> # Monitoring locations whose HUC starts with "02070010"
        >>> # (LIKE with the CQL2 ``%`` wildcard).
        >>> df, md = waterdata.get_cql(
        ...     service="monitoring-locations",
        ...     cql='{"op": "like", "args": ['
        ...     '{"property": "hydrologic_unit_code"},'
        ...     ' "02070010%"]}',
        ... )
    """
    if service not in _OUTPUT_ID_BY_SERVICE:
        raise ValueError(
            f"Unknown service {service!r}. Valid services: "
            f"{sorted(_OUTPUT_ID_BY_SERVICE)}."
        )
    output_id = _OUTPUT_ID_BY_SERVICE[service]

    # ``dict`` is the pythonic input — serialize on the way out. ``str`` is sent
    # verbatim so callers who already have a CQL2 doc (e.g. imported from a
    # config file) don't need to re-parse it.
    body = json.dumps(cql, separators=(",", ":")) if isinstance(cql, dict) else cql

    properties_list = _as_str_list(properties, "properties")

    # Drop id aliases (``daily_id``/``id``) and ``geometry`` from the wire
    # ``properties`` (the feature ``id`` is always returned and renamed
    # downstream), matching the typed getters.
    wire_properties = _switch_properties_id(properties_list, output_id, service)

    req = _construct_cql_request(
        service,
        body,
        properties=wire_properties,
        bbox=bbox,
        limit=limit,
        skip_geometry=skip_geometry,
    )

    async def _run() -> tuple[pd.DataFrame, httpx.Response]:
        return await _walk_pages(geopd=GEOPANDAS, req=req)

    df, response = _run_sync(_run, service=service)

    return _finalize_ogc(
        df,
        response,
        properties=properties_list,
        output_id=output_id,
        convert_type=convert_type,
        service=service,
    )
