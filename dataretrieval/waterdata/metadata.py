"""Getters that answer "what data exists?" rather than returning it.

The monitoring-location catalog, the time-series inventory, and the joins over
them. These are the discovery step: narrow down which locations and parameters
are worth requesting before pulling observations from
:mod:`~dataretrieval.waterdata.time_series`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd

from dataretrieval.ogc.filters import FILTER_LANG
from dataretrieval.response_metadata import BaseMetadata
from dataretrieval.waterdata.utils import (
    _get_args,
    _with_state,
    get_ogc_data,
)


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
    max_rows: int | None = None,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get the catalog of monitoring locations and their attributes.

    Location information includes the name, identifier, agency responsible for
    data collection, and the date the location was established. It also includes
    the type of location, such as stream, lake, or groundwater, and geographic
    information such as state, county, latitude and longitude, and hydrologic
    unit code (HUC).

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location,
        corresponding to the id field in the monitoring-locations endpoint. IDs
        combine the agency code of the agency responsible for the monitoring
        location (e.g. USGS) with the location's ID number (e.g. 02238500),
        separated by a hyphen (e.g. USGS-02238500).
    agency_code : string or iterable of strings, optional
        The agency that is reporting the data. Agency codes are fixed values
        assigned by the National Water Information System (NWIS).
    agency_name : string or iterable of strings, optional
        The name of the agency that is reporting the data.
    monitoring_location_number : string or iterable of strings, optional
        A unique 8- to 15-digit identification number. Every monitoring location
        in the USGS database has one, assigned according to this logic:
        https://help.waterdata.usgs.gov/faq/sites/do-station-numbers-have-any-particular-meaning.
    monitoring_location_name : string or iterable of strings, optional
        This is the official name of the monitoring location in the database.
        For well information this can be a district-assigned local number.
    district_code : string or iterable of strings, optional
        The Water Science Centers (WSCs) across the United States use the FIPS
        state code as the district code. In some cases, monitoring locations and
        samples may be managed by a water science center that is adjacent to the
        state in which the monitoring location actually resides. For example, a
        monitoring location may have a district code of 30, which translates to
        Montana, but a state code of 56 for Wyoming, because that is where the
        monitoring location is actually located.
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
        A unique hydrologic unit code (HUC) of two to eight digits, based on the
        four levels of classification in the hydrologic unit system. The United
        States is divided and sub-divided into successively smaller hydrologic
        units, classified into four levels: regions, sub-regions, accounting
        units, and cataloging units. The hydrologic units are arranged within
        each other, from the smallest (cataloging units) to the largest
        (regions).
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
        contribute to runoff. A transbasin diversion can also affect the total
        drainage area.
    time_zone_abbreviation : string or iterable of strings, optional
        A short code describing the time zone used by a monitoring location.
    uses_daylight_savings : string or iterable of strings, optional
        A flag indicating whether a monitoring location uses daylight savings.
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
        The columns to return from the query. Available
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
    skip_geometry : boolean, optional
        If True, the response omits the geometry of each feature and the
        returned object is a data frame with no spatial information. The USGS
        Water Data APIs use camelCase "skipGeometry" in CQL2 queries.
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
    args = _get_args(
        _with_state(locals(), to="name", into="state_name"), exclude={"max_rows"}
    )

    return get_ogc_data(args, service, max_rows=max_rows)


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
    max_rows: int | None = None,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get metadata describing the time series available at a location.

    Use this to discover what a location measures before requesting the
    observations themselves. Daily data and continuous measurements are grouped
    into time series, which represent a collection of observations of a single
    parameter, potentially aggregated using a standard statistic, at a single
    monitoring location. This endpoint provides metadata about those time
    series, including their operational thresholds, units of measurement, and
    when the earliest and most recent observations in a time series occurred.

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
    parameter_name : string or iterable of strings, optional
        A human-understandable name corresponding to parameter_code.
    properties : string or iterable of strings, optional
        The columns to return from the query.
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
        A unique hydrologic unit code (HUC) of two to eight digits, based on the
        four levels of classification in the hydrologic unit system. The United
        States is divided and sub-divided into successively smaller hydrologic
        units, classified into four levels: regions, sub-regions, accounting
        units, and cataloging units. The hydrologic units are arranged within
        each other, from the smallest (cataloging units) to the largest
        (regions).
    state : string or iterable of strings, optional
        State/territory filter (the recommended parameter). Accepts a full name
        (``"Wisconsin"``), a two-letter postal code (``"WI"``), or a two-digit
        ANSI/FIPS code (``"55"``).
    state_name : string or iterable of strings, optional
        The name of the state or state equivalent in which the monitoring location
        is located.
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
        A unique identifier representing a single time series, corresponding to
        the id field in the time-series-metadata endpoint.
    web_description : string or iterable of strings, optional
        A description of what this time series represents, as used by WDFN and
        other USGS data dissemination products.
    skip_geometry : boolean, optional
        If True, the response omits the geometry of each feature and the
        returned object is a data frame with no spatial information. The USGS
        Water Data APIs use camelCase "skipGeometry" in CQL2 queries.
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
    args = _get_args(
        _with_state(locals(), to="name", into="state_name"), exclude={"max_rows"}
    )

    return get_ogc_data(args, service, max_rows=max_rows)


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
    max_rows: int | None = None,
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
        (``None``) requests the maximum allowable limit. This is a
        per-page size, not a cap on the total result: a query matching more
        rows than ``limit`` still returns every matching row across
        multiple pages. Use ``max_rows`` to cap the total instead.
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
    args = _get_args(
        _with_state(locals(), to="name", into="state_name"), exclude={"max_rows"}
    )

    return get_ogc_data(args, service, max_rows=max_rows)


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
    max_rows: int | None = None,
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
        (``None``) requests the maximum allowable limit. This is a
        per-page size, not a cap on the total result: a query matching more
        rows than ``limit`` still returns every matching row across
        multiple pages. Use ``max_rows`` to cap the total instead.
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

    args = _get_args(locals(), exclude={"max_rows"})

    return get_ogc_data(args, service, max_rows=max_rows)


__all__ = [
    "get_monitoring_locations",
    "get_time_series_metadata",
    "get_combined_metadata",
    "get_field_measurements_metadata",
]
