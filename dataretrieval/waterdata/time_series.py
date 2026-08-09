"""Getters for observations that form a time series.

Daily and continuous values, their most-recent counterparts, and the
period-of-record statistics computed over them. What unites them is shape: a
monitoring location and a parameter, repeated over time.

Metadata *about* these series -- what a location measures, over what period --
lives in :mod:`~dataretrieval.waterdata.metadata`, so a caller can discover what
exists before asking for the observations.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd

from dataretrieval.ogc.filters import FILTER_LANG
from dataretrieval.response_metadata import BaseMetadata
from dataretrieval.waterdata import stats
from dataretrieval.waterdata.utils import (
    _get_args,
    _with_state,
    get_ogc_data,
)


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
    max_rows: int | None = None,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get daily values: one value per monitoring location, parameter, and day.

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
        A unique identifier representing a single monitoring location,
        corresponding to the id field in the monitoring-locations endpoint. IDs
        combine the agency code of the agency responsible for the monitoring
        location (e.g. USGS) with the location's ID number (e.g. 02238500),
        separated by a hyphen (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        A 5-digit code identifying the constituent measured and the units of
        measure. A complete list of parameter codes and associated groupings is
        available at https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or iterable of strings, optional
        A code corresponding to the statistic an observation represents.
        Example codes include 00001 (max), 00002 (min), and 00003 (mean).
        A complete list of codes and their descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or iterable of strings, optional
        The columns to return from the query.
        Available options are: geometry, id, time_series_id,
        monitoring_location_id, parameter_code, statistic_id, time, value,
        unit_of_measure, approval_status, qualifier, last_modified
    time_series_id : string or iterable of strings, optional
        A unique identifier representing a single time series, corresponding to
        the id field in the time-series-metadata endpoint.
    daily_id : string or iterable of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. The UUID is not stable over time: every time the record is
        refreshed in our database, a new ID is generated. A refresh may happen
        as part of normal operations and does not imply any change to the data
        itself. To uniquely identify a single observation over time, compare the
        time and time_series_id fields; each time series has only a single
        observation at a given time.
    approval_status : string or iterable of strings, optional
        The approval status of each record: either "Approved", meaning
        processing review has been completed and the data are approved for
        publication, or "Provisional", meaning the data are subject to revision.
        Some of the data you obtain from this U.S. Geological Survey database
        may not have received Director's approval. Any such data values are
        qualified as provisional and are subject to revision. Provisional data
        are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from
        their use. For more information about provisional data, see
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or iterable of strings, optional
        Any qualifiers associated with an observation, for instance whether a
        sensor may have been impacted by ice or whether values were estimated.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. A refresh may
        happen due to regular operational processes and does not necessarily
        indicate that anything about the measurement has changed. You can query
        this field using date-times or intervals, adhering to RFC 3339, or using
        ISO 8601 duration objects. Intervals may be bounded or half-bounded
        (double-dots at start or end). Only features whose last_modified
        intersects the requested value are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    skip_geometry : boolean, optional
        If True, the response omits the geometry of each feature and the
        returned object is a data frame with no spatial information. The USGS
        Water Data APIs use camelCase "skipGeometry" in CQL2 queries.
    time : string, optional
        The date an observation represents. You can query this field using
        date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features whose time intersects the requested
        value are selected. If a feature has multiple temporal properties, the
        server decides whether to use a single property or all relevant ones to
        determine the extent.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features whose geometry intersects the bounding box are selected.
        The bounding box is provided as four or six numbers, depending on
        whether the coordinate reference system includes a vertical axis (height
        or depth). Coordinates are assumed to be in crs 4326. The expected
        format is ``[xmin, ymin, xmax, ymax]``, i.e. ``[Western-most longitude,
        Southern-most latitude, Eastern-most longitude, Northern-most
        latitude]``.
    limit : int, optional
        The number of features returned in each page. The maximum allowable
        limit is 50000; the default (None) requests that maximum. Set a lower
        number if your internet connection is spotty. This is a per-page size,
        not a cap on the total result: a query matching more rows than ``limit``
        still returns every matching row across multiple pages. Use ``max_rows``
        to cap the total instead.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    max_rows : int, optional
        Cap the total number of rows returned, stopping pagination early
        instead of downloading the whole result. Unlike ``limit`` (the
        per-page size), this bounds the total result across every page.
        The default (None) follows pagination to completion.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Call :func:`get_queryables` to see the queryables a
        collection supports.

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
    args = _get_args(locals(), exclude={"max_rows"})

    return get_ogc_data(args, service, max_rows=max_rows)


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
    max_rows: int | None = None,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get continuous sensor observations, typically at a 15-minute interval.

    This is an early version of the continuous endpoint that is feature-complete
    and is being made available for limited use. Geometries are not included
    with the continuous endpoint. If the "time" input is left blank, the service
    returns the most recent year of measurements. Users may request no more than
    three years of data with each function call.

    Continuous data are collected at a high frequency, typically 15-minute
    intervals. Depending on the monitoring location, the data may be transmitted
    automatically via telemetry and be available on WDFN within minutes of
    collection. Delivery may be delayed where the monitoring location cannot
    transmit data automatically. Continuous data are described by parameter name
    and parameter code (pcode). These data might also be referred to as
    "instantaneous values" or "IV".

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location,
        corresponding to the id field in the monitoring-locations endpoint. IDs
        combine the agency code of the agency responsible for the monitoring
        location (e.g. USGS) with the location's ID number (e.g. 02238500),
        separated by a hyphen (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        A 5-digit code identifying the constituent measured and the units of
        measure. A complete list of parameter codes and associated groupings is
        available at https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or iterable of strings, optional
        A code corresponding to the statistic an observation represents.
        Continuous data are nearly always associated with statistic id
        00011. Using a different code (such as 00003 for mean) will
        typically return no results. A complete list of codes and their
        descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or iterable of strings, optional
        The columns to return from the query.
        Available options are: geometry, id, time_series_id,
        monitoring_location_id, parameter_code, statistic_id, time, value,
        unit_of_measure, approval_status, qualifier, last_modified
    time_series_id : string or iterable of strings, optional
        A unique identifier representing a single time series, corresponding to
        the id field in the time-series-metadata endpoint.
    continuous_id : string or iterable of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. The UUID is not stable over time: every time the record is
        refreshed in our database, a new ID is generated. A refresh may happen
        as part of normal operations and does not imply any change to the data
        itself. To uniquely identify a single observation over time, compare the
        time and time_series_id fields; each time series has only a single
        observation at a given time.
    approval_status : string or iterable of strings, optional
        The approval status of each record: either "Approved", meaning
        processing review has been completed and the data are approved for
        publication, or "Provisional", meaning the data are subject to revision.
        Some of the data you obtain from this U.S. Geological Survey database
        may not have received Director's approval. Any such data values are
        qualified as provisional and are subject to revision. Provisional data
        are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from
        their use. For more information about provisional data, see
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or iterable of strings, optional
        Any qualifiers associated with an observation, for instance whether a
        sensor may have been impacted by ice or whether values were estimated.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. A refresh may
        happen due to regular operational processes and does not necessarily
        indicate that anything about the measurement has changed. You can query
        this field using date-times or intervals, adhering to RFC 3339, or using
        ISO 8601 duration objects. Intervals may be bounded or half-bounded
        (double-dots at start or end). Only features whose last_modified
        intersects the requested value are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    time : string, optional
        The date an observation represents. You can query this field using
        date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features whose time intersects the requested
        value are selected. If a feature has multiple temporal properties, the
        server decides whether to use a single property or all relevant ones to
        determine the extent.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    limit : int, optional
        The number of features returned in each page. The maximum allowable
        limit is 10000; the default (None) requests that maximum. Set a lower
        number if your internet connection is spotty. This is a per-page size,
        not a cap on the total result: a query matching more rows than ``limit``
        still returns every matching row across multiple pages. Use ``max_rows``
        to cap the total instead.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    max_rows : int, optional
        Cap the total number of rows returned, stopping pagination early
        instead of downloading the whole result. Unlike ``limit`` (the
        per-page size), this bounds the total result across every page.
        The default (None) follows pagination to completion.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Call :func:`get_queryables` to see the queryables a
        collection supports.

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
    args = _get_args(locals(), exclude={"max_rows"})

    return get_ogc_data(args, service, max_rows=max_rows)


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
    max_rows: int | None = None,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get only the most recent observation of each continuous time series.

    Use this for a current-conditions view; use :func:`get_continuous` for a
    history.

    Continuous data are collected via automated sensors installed at a
    monitoring location, at a high frequency and often at a fixed 15-minute
    interval. Depending on the monitoring location, the data may be transmitted
    automatically via telemetry and be available on WDFN within minutes of
    collection. Delivery may be delayed where the monitoring location cannot
    transmit data automatically. Continuous data are described by parameter name
    and parameter code. These data might also be referred to as "instantaneous
    values" or "IV".

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location,
        corresponding to the id field in the monitoring-locations endpoint. IDs
        combine the agency code of the agency responsible for the monitoring
        location (e.g. USGS) with the location's ID number (e.g. 02238500),
        separated by a hyphen (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        A 5-digit code identifying the constituent measured and the units of
        measure. A complete list of parameter codes and associated groupings is
        available at https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or iterable of strings, optional
        A code corresponding to the statistic an observation represents.
        Example codes include 00001 (max), 00002 (min), and 00003 (mean).
        A complete list of codes and their descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or iterable of strings, optional
        The columns to return from the query. Available
        options are: geometry, id, time_series_id, monitoring_location_id,
        parameter_code, statistic_id, time, value, unit_of_measure,
        approval_status, qualifier, last_modified
    time_series_id : string or iterable of strings, optional
        A unique identifier representing a single time series, corresponding to
        the id field in the time-series-metadata endpoint.
    latest_continuous_id : string or iterable of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. The UUID is not stable over time: every time the record is
        refreshed in our database, a new ID is generated. A refresh may happen
        as part of normal operations and does not imply any change to the data
        itself. To uniquely identify a single observation over time, compare the
        time and time_series_id fields; each time series has only a single
        observation at a given time.
    approval_status : string or iterable of strings, optional
        The approval status of each record: either "Approved", meaning
        processing review has been completed and the data are approved for
        publication, or "Provisional", meaning the data are subject to revision.
        Some of the data you obtain from this U.S. Geological Survey database
        may not have received Director's approval. Any such data values are
        qualified as provisional and are subject to revision. Provisional data
        are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from
        their use. For more information about provisional data, see
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or iterable of strings, optional
        Any qualifiers associated with an observation, for instance whether a
        sensor may have been impacted by ice or whether values were estimated.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. A refresh may
        happen due to regular operational processes and does not necessarily
        indicate that anything about the measurement has changed. You can query
        this field using date-times or intervals, adhering to RFC 3339, or using
        ISO 8601 duration objects. Intervals may be bounded or half-bounded
        (double-dots at start or end). Only features whose last_modified
        intersects the requested value are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    skip_geometry : boolean, optional
        If True, the response omits the geometry of each feature and the
        returned object is a data frame with no spatial information. The USGS
        Water Data APIs use camelCase "skipGeometry" in CQL2 queries.
    time : string, optional
        The date an observation represents. You can query this field using
        date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features whose time intersects the requested
        value are selected. If a feature has multiple temporal properties, the
        server decides whether to use a single property or all relevant ones to
        determine the extent.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features whose geometry intersects the bounding box are selected.
        The bounding box is provided as four or six numbers, depending on
        whether the coordinate reference system includes a vertical axis (height
        or depth). Coordinates are assumed to be in crs 4326. The expected
        format is ``[xmin, ymin, xmax, ymax]``, i.e. ``[Western-most longitude,
        Southern-most latitude, Eastern-most longitude, Northern-most
        latitude]``.
    limit : int, optional
        The number of features returned in each page. The maximum allowable
        limit is 50000; the default (None) requests that maximum. Set a lower
        number if your internet connection is spotty. This is a per-page size,
        not a cap on the total result: a query matching more rows than ``limit``
        still returns every matching row across multiple pages. Use ``max_rows``
        to cap the total instead.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    max_rows : int, optional
        Cap the total number of rows returned, stopping pagination early
        instead of downloading the whole result. Unlike ``limit`` (the
        per-page size), this bounds the total result across every page.
        The default (None) follows pagination to completion.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Call :func:`get_queryables` to see the queryables a
        collection supports.

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
    args = _get_args(locals(), exclude={"max_rows"})

    return get_ogc_data(args, service, max_rows=max_rows)


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
    max_rows: int | None = None,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get only the most recent daily value of each time series.

    Use this for a current-conditions view; use :func:`get_daily` for a
    history.

    Daily data provide one data value to represent water conditions for the day.
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
        A unique identifier representing a single monitoring location,
        corresponding to the id field in the monitoring-locations endpoint. IDs
        combine the agency code of the agency responsible for the monitoring
        location (e.g. USGS) with the location's ID number (e.g. 02238500),
        separated by a hyphen (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        A 5-digit code identifying the constituent measured and the units of
        measure. A complete list of parameter codes and associated groupings is
        available at https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or iterable of strings, optional
        A code corresponding to the statistic an observation represents.
        Example codes include 00001 (max), 00002 (min), and 00003 (mean).
        A complete list of codes and their descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or iterable of strings, optional
        The columns to return from the query. Available
        options are: geometry, id, time_series_id, monitoring_location_id,
        parameter_code, statistic_id, time, value, unit_of_measure,
        approval_status, qualifier, last_modified
    time_series_id : string or iterable of strings, optional
        A unique identifier representing a single time series, corresponding to
        the id field in the time-series-metadata endpoint.
    latest_daily_id : string or iterable of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. The UUID is not stable over time: every time the record is
        refreshed in our database, a new ID is generated. A refresh may happen
        as part of normal operations and does not imply any change to the data
        itself. To uniquely identify a single observation over time, compare the
        time and time_series_id fields; each time series has only a single
        observation at a given time.
    approval_status : string or iterable of strings, optional
        The approval status of each record: either "Approved", meaning
        processing review has been completed and the data are approved for
        publication, or "Provisional", meaning the data are subject to revision.
        Some of the data you obtain from this U.S. Geological Survey database
        may not have received Director's approval. Any such data values are
        qualified as provisional and are subject to revision. Provisional data
        are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from
        their use. For more information about provisional data, see
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or iterable of strings, optional
        Any qualifiers associated with an observation, for instance whether a
        sensor may have been impacted by ice or whether values were estimated.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. A refresh may
        happen due to regular operational processes and does not necessarily
        indicate that anything about the measurement has changed. You can query
        this field using date-times or intervals, adhering to RFC 3339, or using
        ISO 8601 duration objects. Intervals may be bounded or half-bounded
        (double-dots at start or end). Only features whose last_modified
        intersects the requested value are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    skip_geometry : boolean, optional
        If True, the response omits the geometry of each feature and the
        returned object is a data frame with no spatial information. The USGS
        Water Data APIs use camelCase "skipGeometry" in CQL2 queries.
    time : string, optional
        The date an observation represents. You can query this field using
        date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features whose time intersects the requested
        value are selected. If a feature has multiple temporal properties, the
        server decides whether to use a single property or all relevant ones to
        determine the extent.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features whose geometry intersects the bounding box are selected.
        The bounding box is provided as four or six numbers, depending on
        whether the coordinate reference system includes a vertical axis (height
        or depth). Coordinates are assumed to be in crs 4326. The expected
        format is ``[xmin, ymin, xmax, ymax]``, i.e. ``[Western-most longitude,
        Southern-most latitude, Eastern-most longitude, Northern-most
        latitude]``.
    limit : int, optional
        The number of features returned in each page. The maximum allowable
        limit is 50000; the default (None) requests that maximum. Set a lower
        number if your internet connection is spotty. This is a per-page size,
        not a cap on the total result: a query matching more rows than ``limit``
        still returns every matching row across multiple pages. Use ``max_rows``
        to cap the total instead.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    max_rows : int, optional
        Cap the total number of rows returned, stopping pagination early
        instead of downloading the whole result. Unlike ``limit`` (the
        per-page size), this bounds the total result across every page.
        The default (None) follows pagination to completion.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Call :func:`get_queryables` to see the queryables a
        collection supports.

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
    args = _get_args(locals(), exclude={"max_rows"})

    return get_ogc_data(args, service, max_rows=max_rows)


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
    """Get day-of-year and month-of-year statistics over the historical record.

    Answers "how does today compare to a normal day here?" -- minimum, maximum,
    mean, median, and percentiles computed per day of year and month of year
    (the ``observationNormals`` endpoint). For more on how these statistics are
    calculated, see the Statistics documentation page:
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
        A unique identifier representing a single monitoring location,
        corresponding to the id field in the monitoring-locations endpoint. IDs
        combine the agency code of the agency responsible for the monitoring
        location (e.g. USGS) with the location's ID number (e.g. 02238500),
        separated by a hyphen (e.g. USGS-02238500).
    page_size : int, optional
        The number of results to return per page, where one result represents a
        monitoring location. The default is 1000.
    parent_time_series_id: string, optional
        Returns statistics tied to a particular database entry.
    site_type_code: string, optional
        Site type code query parameter. A list of valid site type codes is
        available at
        https://api.waterdata.usgs.gov/ogcapi/v0/collections/site-types/items.
        Example: "GW" (Groundwater site)
    site_type_name: string, optional
        Site type name query parameter.
    parameter_code : string or iterable of strings, optional
        A 5-digit code identifying the constituent measured and the units of
        measure. A complete list of parameter codes and associated groupings is
        available at https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    normal_type : string, optional
        Filter the returned normals to a single period. If unspecified
        (default), all matching data are returned. Available values:
        "DOY" (day-of-year) and "MOY" (month-of-year).
    expand_percentiles : boolean
        Whether to expand percentile lists into one row per percentile.
        By default, the service returns percentile data for a given day of year
        or month of year as lists of string values and percentile thresholds, in
        the "values" and "percentiles" columns respectively. When
        `expand_percentiles` is True (default), each value and percentile
        threshold specific to a computation id becomes its own row in the
        dataframe: the value is reported in a "value" column and the
        corresponding percentile in a "percentile" column, and the "values" and
        "percentiles" columns are removed. Missing percentile values, expressed
        as 'nan' in the list of string values, are removed from the dataframe to
        save space. Setting `expand_percentiles` to False retains the "values"
        and "percentiles" columns produced by the service. Including both
        'percentiles' and one or more other statistics ('median', 'minimum',
        'maximum', or 'arithmetic_mean') in the `computation_type` argument
        returns both the "values" column, containing the list of percentile
        threshold values, and a "value" column, containing the singular summary
        value for the other statistics.

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
    """Get statistics summarizing whole months and years of the record.

    Answers "how did this month or year compare to others?" -- minimum, maximum,
    mean, median, and percentiles per month-year and per water or calendar year
    (the ``observationIntervals`` endpoint). For more on how these statistics are
    calculated, see the Statistics documentation page:
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
        A unique identifier representing a single monitoring location,
        corresponding to the id field in the monitoring-locations endpoint. IDs
        combine the agency code of the agency responsible for the monitoring
        location (e.g. USGS) with the location's ID number (e.g. 02238500),
        separated by a hyphen (e.g. USGS-02238500).
    page_size : int, optional
        The number of results to return per page, where one result represents a
        monitoring location. The default is 1000.
    parent_time_series_id: string, optional
        Returns statistics tied to a particular database entry.
    site_type_code: string, optional
        Site type code query parameter. A list of valid site type codes is
        available at
        https://api.waterdata.usgs.gov/ogcapi/v0/collections/site-types/items.
        Example: "GW" (Groundwater site)
    site_type_name: string, optional
        Site type name query parameter. A list of valid site type names is
        available at
        https://api.waterdata.usgs.gov/ogcapi/v0/collections/site-types/items.
        Example: "Well"
    parameter_code : string or iterable of strings, optional
        A 5-digit code identifying the constituent measured and the units of
        measure. A complete list of parameter codes and associated groupings is
        available at https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    interval_type : string or iterable of strings, optional
        Filter the returned intervals to one or more periods. If unspecified
        (default), all matching data are returned. Available values:
        "M" (month), "CY" (calendar year), and "WY" (water year).
    expand_percentiles : boolean
        Whether to expand percentile lists into one row per percentile.
        By default, the service returns percentile data for a given day of year
        or month of year as lists of string values and percentile thresholds, in
        the "values" and "percentiles" columns respectively. When
        `expand_percentiles` is True (default), each value and percentile
        threshold specific to a computation id becomes its own row in the
        dataframe: the value is reported in a "value" column and the
        corresponding percentile in a "percentile" column, and the "values" and
        "percentiles" columns are removed. Missing percentile values, expressed
        as 'nan' in the list of string values, are removed from the dataframe to
        save space. Setting `expand_percentiles` to False retains the "values"
        and "percentiles" columns produced by the service. Including both
        'percentiles' and one or more other statistics ('median', 'minimum',
        'maximum', or 'arithmetic_mean') in the `computation_type` argument
        returns both the "values" column, containing the list of percentile
        threshold values, and a "value" column, containing the singular summary
        value for the other statistics.

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


__all__ = [
    "get_daily",
    "get_continuous",
    "get_latest_continuous",
    "get_latest_daily",
    "get_stats_por",
    "get_stats_date_range",
]
