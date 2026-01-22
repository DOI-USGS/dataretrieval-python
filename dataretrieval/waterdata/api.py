"""Functions for downloading data from the Water Data APIs, including the USGS
Aquarius Samples database.

See https://api.waterdata.usgs.gov/ for API reference.
"""

import json
import logging
from io import StringIO
from typing import List, Optional, Tuple, Union, get_args

import pandas as pd
import requests
from requests.models import PreparedRequest

from dataretrieval.utils import BaseMetadata, to_str
from dataretrieval.waterdata.types import (
    CODE_SERVICES,
    METADATA_COLLECTIONS,
    PROFILES,
    SERVICES,
)
from dataretrieval.waterdata.utils import (
    SAMPLES_URL,
    get_ogc_data,
    _construct_api_requests,
    _walk_pages,
    _check_profiles
)

# Set up logger for this module
logger = logging.getLogger(__name__)


def get_daily(
    monitoring_location_id: Optional[Union[str, List[str]]] = None,
    parameter_code: Optional[Union[str, List[str]]] = None,
    statistic_id: Optional[Union[str, List[str]]] = None,
    properties: Optional[List[str]] = None,
    time_series_id: Optional[Union[str, List[str]]] = None,
    daily_id: Optional[Union[str, List[str]]] = None,
    approval_status: Optional[Union[str, List[str]]] = None,
    unit_of_measure: Optional[Union[str, List[str]]] = None,
    qualifier: Optional[Union[str, List[str]]] = None,
    value: Optional[Union[str, List[str]]] = None,
    last_modified: Optional[str] = None,
    skip_geometry: Optional[bool] = None,
    time: Optional[Union[str, List[str]]] = None,
    bbox: Optional[List[float]] = None,
    limit: Optional[int] = None,
    convert_type: bool = True,
) -> Tuple[pd.DataFrame, BaseMetadata]:
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
    monitoring_location_id : string or list of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of
        the agency responsible for the monitoring location (e.g. USGS) with
        the ID number of the monitoring location (e.g. 02238500), separated
        by a hyphen (e.g. USGS-02238500).
    parameter_code : string or list of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter
        codes and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or list of strings, optional
        A code corresponding to the statistic an observation represents.
        Example codes include 00001 (max), 00002 (min), and 00003 (mean).
        A complete list of codes and their descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or list of strings, optional
        A vector of requested columns to be returned from the query.
        Available options are: geometry, id, time_series_id,
        monitoring_location_id, parameter_code, statistic_id, time, value,
        unit_of_measure, approval_status, qualifier, last_modified
    time_series_id : string or list of strings, optional
        A unique identifier representing a single time series. This
        corresponds to the id field in the time-series-metadata endpoint.
    daily_id : string or list of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. It is not stable over time. Every time the record is refreshed
        in our database (which may happen as part of normal operations and does
        not imply any change to the data itself) a new ID will be generated. To
        uniquely identify a single observation over time, compare the time and
        time_series_id fields; each time series will only have a single
        observation at a given time.
    approval_status : string or list of strings, optional
        Some of the data that you have obtained from this U.S. Geological Survey
        database may not have received Director's approval. Any such data values
        are qualified as provisional and are subject to revision. Provisional
        data are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from its
        use. This field reflects the approval status of each record, and is either
        "Approved", meaining processing review has been completed and the data is
        approved for publication, or "Provisional" and subject to revision. For
        more information about provisional data, go to:
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or list of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or list of strings, optional
        This field indicates any qualifiers associated with an observation, for
        instance if a sensor may have been impacted by ice or if values were
        estimated.
    value : string or list of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format in order to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end).
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

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
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features that have a geometry that intersects the bounding box are
        selected.  The bounding box is provided as four or six numbers,
        depending on whether the coordinate reference system includes a vertical
        axis (height or depth). Coordinates are assumed to be in crs 4326. The
        expected format is a numeric vector structured: c(xmin,ymin,xmax,ymax).
        Another way to think of it is c(Western-most longitude, Southern-most
        latitude, Eastern-most longitude, Northern-most longitude).
    limit : numeric, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (NA) will set the
        limit to the maximum allowable limit for the service.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

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

        >>> # Get approved daily flow data from multiple sites
        >>> df, md = dataretrieval.waterdata.get_daily(
        ...     monitoring_location_id = ["USGS-05114000", "USGS-09423350"],
        ...     approval_status = "Approved",
        ...     time = "2024-01-01/.."
    """
    service = "daily"
    output_id = "daily_id"

    # Build argument dictionary, omitting None values
    args = {
        k: v
        for k, v in locals().items()
        if k not in {"service", "output_id"} and v is not None
    }

    return get_ogc_data(args, output_id, service)

def get_continuous(
    monitoring_location_id: Optional[Union[str, List[str]]] = None,
    parameter_code: Optional[Union[str, List[str]]] = None,
    statistic_id: Optional[Union[str, List[str]]] = None,
    properties: Optional[List[str]] = None,
    time_series_id: Optional[Union[str, List[str]]] = None,
    continuous_id: Optional[Union[str, List[str]]] = None,
    approval_status: Optional[Union[str, List[str]]] = None,
    unit_of_measure: Optional[Union[str, List[str]]] = None,
    qualifier: Optional[Union[str, List[str]]] = None,
    value: Optional[Union[str, List[str]]] = None,
    last_modified: Optional[str] = None,
    time: Optional[Union[str, List[str]]] = None,
    limit: Optional[int] = None,
    convert_type: bool = True,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Continuous data provide instantanous water conditions.

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
    monitoring_location_id : string or list of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of
        the agency responsible for the monitoring location (e.g. USGS) with
        the ID number of the monitoring location (e.g. 02238500), separated
        by a hyphen (e.g. USGS-02238500).
    parameter_code : string or list of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter
        codes and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or list of strings, optional
        A code corresponding to the statistic an observation represents.
        Continuous data are nearly always associated with statistic id
        00011. Using a different code (such as 00003 for mean) will
        typically return no results. A complete list of codes and their
        descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or list of strings, optional
        A vector of requested columns to be returned from the query.
        Available options are: geometry, id, time_series_id,
        monitoring_location_id, parameter_code, statistic_id, time, value,
        unit_of_measure, approval_status, qualifier, last_modified
    time_series_id : string or list of strings, optional
        A unique identifier representing a single time series. This
        corresponds to the id field in the time-series-metadata endpoint.
    continuous_id : string or list of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. It is not stable over time. Every time the record is refreshed
        in our database (which may happen as part of normal operations and does
        not imply any change to the data itself) a new ID will be generated. To
        uniquely identify a single observation over time, compare the time and
        time_series_id fields; each time series will only have a single
        observation at a given time.
    approval_status : string or list of strings, optional
        Some of the data that you have obtained from this U.S. Geological Survey
        database may not have received Director's approval. Any such data values
        are qualified as provisional and are subject to revision. Provisional
        data are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from its
        use. This field reflects the approval status of each record, and is either
        "Approved", meaining processing review has been completed and the data is
        approved for publication, or "Provisional" and subject to revision. For
        more information about provisional data, go to:
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or list of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or list of strings, optional
        This field indicates any qualifiers associated with an observation, for
        instance if a sensor may have been impacted by ice or if values were
        estimated.
    value : string or list of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format in order to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end).
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

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
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

    limit : numeric, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 10000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (NA) will set the
        limit to the maximum allowable limit for the service.
    convert_type : boolean, optional
        If True, the function will convert the data to dates and qualifier to
        string vector

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

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
    """
    service = "continuous"
    output_id = "continuous_id"

    # Build argument dictionary, omitting None values
    args = {
        k: v
        for k, v in locals().items()
        if k not in {"service", "output_id"} and v is not None
    }

    return get_ogc_data(args, output_id, service)


def get_monitoring_locations(
    monitoring_location_id: Optional[List[str]] = None,
    agency_code: Optional[List[str]] = None,
    agency_name: Optional[List[str]] = None,
    monitoring_location_number: Optional[List[str]] = None,
    monitoring_location_name: Optional[List[str]] = None,
    district_code: Optional[List[str]] = None,
    country_code: Optional[List[str]] = None,
    country_name: Optional[List[str]] = None,
    state_code: Optional[List[str]] = None,
    state_name: Optional[List[str]] = None,
    county_code: Optional[List[str]] = None,
    county_name: Optional[List[str]] = None,
    minor_civil_division_code: Optional[List[str]] = None,
    site_type_code: Optional[List[str]] = None,
    site_type: Optional[List[str]] = None,
    hydrologic_unit_code: Optional[List[str]] = None,
    basin_code: Optional[List[str]] = None,
    altitude: Optional[List[str]] = None,
    altitude_accuracy: Optional[List[str]] = None,
    altitude_method_code: Optional[List[str]] = None,
    altitude_method_name: Optional[List[str]] = None,
    vertical_datum: Optional[List[str]] = None,
    vertical_datum_name: Optional[List[str]] = None,
    horizontal_positional_accuracy_code: Optional[List[str]] = None,
    horizontal_positional_accuracy: Optional[List[str]] = None,
    horizontal_position_method_code: Optional[List[str]] = None,
    horizontal_position_method_name: Optional[List[str]] = None,
    original_horizontal_datum: Optional[List[str]] = None,
    original_horizontal_datum_name: Optional[List[str]] = None,
    drainage_area: Optional[List[str]] = None,
    contributing_drainage_area: Optional[List[str]] = None,
    time_zone_abbreviation: Optional[List[str]] = None,
    uses_daylight_savings: Optional[List[str]] = None,
    construction_date: Optional[List[str]] = None,
    aquifer_code: Optional[List[str]] = None,
    national_aquifer_code: Optional[List[str]] = None,
    aquifer_type_code: Optional[List[str]] = None,
    well_constructed_depth: Optional[List[str]] = None,
    hole_constructed_depth: Optional[List[str]] = None,
    depth_source_code: Optional[List[str]] = None,
    properties: Optional[List[str]] = None,
    skip_geometry: Optional[bool] = None,
    time: Optional[Union[str, List[str]]] = None,
    bbox: Optional[List[float]] = None,
    limit: Optional[int] = None,
    convert_type: bool = True,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """Location information is basic information about the monitoring location
    including the name, identifier, agency responsible for data collection, and
    the date the location was established. It also includes information about
    the type of location, such as stream, lake, or groundwater, and geographic
    information about the location, such as state, county, latitude and
    longitude, and hydrologic unit code (HUC).

    Parameters
    ----------
    monitoring_location_id : string or list of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of
        the agency responsible for the monitoring location (e.g. USGS) with
        the ID number of the monitoring location (e.g. 02238500), separated
        by a hyphen (e.g. USGS-02238500).
    agency_code : string or list of strings, optional
        The agency that is reporting the data. Agency codes are fixed values
        assigned by the National Water Information System (NWIS). A list of
        agency codes is available at:
        https://help.waterdata.usgs.gov/code/agency_cd_query?fmt=html.
    agency_name : string or list of strings, optional
        The name of the agency that is reporting the data.
    monitoring_location_number : string or list of strings, optional
        Each monitoring location in the USGS data base has a unique 8- to
        15-digit identification number. Monitoring location numbers are
        assigned based on this logic:
        https://help.waterdata.usgs.gov/faq/sites/do-station-numbers-have-any-particular-meaning.
    monitoring_location_name : string or list of strings, optional
        This is the official name of the monitoring location in the database.
        For well information this can be a district-assigned local number.
    district_code : string or list of strings, optional
        The Water Science Centers (WSCs) across the United States use the FIPS
        state code as the district code. In some case, monitoring locations and
        samples may be managed by a water science center that is adjacent to the
        state in which the monitoring location actually resides. For example a
        monitoring location may have a district code of 30 which translates to
        Montana, but the state code could be 56 for Wyoming because that is where
        the monitoring location actually is located.
    country_code : string or list of strings, optional
        The code for the country in which the monitoring location is located.
    country_name : string or list of strings, optional
        The name of the country in which the monitoring location is located.
    state_code : string or list of strings, optional
        State code. A two-digit ANSI code (formerly FIPS code) as defined by
        the American National Standards Institute, to define States and
        equivalents. A three-digit ANSI code is used to define counties and
        county equivalents. A `lookup table
        <https://www.census.gov/library/reference/code-lists/ansi.html#states>`_
        is available. The only countries with
        political subdivisions other than the US are Mexico and Canada. The Mexican
        states have US state codes ranging from 81-86 and Canadian provinces have
        state codes ranging from 90-98.
    state_name : string or list of strings, optional
        The name of the state or state equivalent in which the monitoring location
        is located.
    county_code : string or list of strings, optional
        The code for the county or county equivalent (parish, borough, etc.) in which
        the monitoring location is located. A `list of codes 
        <https://help.waterdata.usgs.gov/code/county_query?fmt=html>`_ is available.
    county_name : string or list of strings, optional
        The name of the county or county equivalent (parish, borough, etc.) in which
        the monitoring location is located. A `list of codes 
        <https://help.waterdata.usgs.gov/code/county_query?fmt=html>`_ is available.
    minor_civil_division_code : string or list of strings, optional
        Codes for primary governmental or administrative divisions of the county or
        county equivalent in which the monitoring location is located.
    site_type_code : string or list of strings, optional
        A code describing the hydrologic setting of the monitoring location. A `list of
        codes <https://help.waterdata.usgs.gov/code/site_tp_query?fmt=html>`_ is available.
        Example: "US:15:001" (United States: Hawaii, Hawaii County)
    site_type : string or list of strings, optional
        A description of the hydrologic setting of the monitoring location. A `list of
        codes <https://help.waterdata.usgs.gov/code/site_tp_query?fmt=html>`_ is available.
    hydrologic_unit_code : string or list of strings, optional
        The United States is divided and sub-divided into successively smaller
        hydrologic units which are classified into four levels: regions,
        sub-regions, accounting units, and cataloging units. The hydrologic
        units are arranged within each other, from the smallest (cataloging
        units) to the largest (regions). Each hydrologic unit is identified by a
        unique hydrologic unit code (HUC) consisting of two to eight digits
        based on the four levels of classification in the hydrologic unit
        system.
    basin_code : string or list of strings, optional
        The Basin Code or "drainage basin code" is a two-digit code that further
        subdivides the 8-digit hydrologic-unit code. The drainage basin code is
        defined by the USGS State Office where the monitoring location is
        located.
    altitude : string or list of strings, optional
        Altitude of the monitoring location referenced to the specified Vertical
        Datum.
    altitude_accuracy : string or list of strings, optional
        Accuracy of the altitude, in feet. An accuracy of +/- 0.1 foot would be
        entered as “.1”. Many altitudes are interpolated from the contours on
        topographic maps; accuracies determined in this way are generally
        entered as one-half of the contour interval.
    altitude_method_code : string or list of strings, optional
        Codes representing the method used to measure altitude. A `list of
        codes <https://help.waterdata.usgs.gov/code/alt_meth_cd_query?fmt=html>`_ is available.
    altitude_method_name : float, optional
        The name of the the method used to measure altitude. A `list of
        codes <https://help.waterdata.usgs.gov/code/alt_meth_cd_query?fmt=html>`_ is available.
    vertical_datum : float, optional
        The datum used to determine altitude and vertical position at the
        monitoring location. A `list of
        codes <https://help.waterdata.usgs.gov/code/alt_datum_cd_query?fmt=html>`_ is available.
    vertical_datum_name : float, optional
        The datum used to determine altitude and vertical position at the
        monitoring location. A `list of
        codes <https://help.waterdata.usgs.gov/code/alt_datum_cd_query?fmt=html>`_ is available.
    horizontal_positional_accuracy_code : string or list of strings, optional
        Indicates the accuracy of the latitude longitude values. A `list of
        codes <https://help.waterdata.usgs.gov/code/coord_acy_cd_query?fmt=html>`_ is available.
    horizontal_positional_accuracy : string or list of strings, optional
        Indicates the accuracy of the latitude longitude values. A `list of
        codes <https://help.waterdata.usgs.gov/code/coord_acy_cd_query?fmt=html>`_ is available.
    horizontal_position_method_code : string or list of strings, optional
        Indicates the method used to determine latitude longitude values. A `list of
        codes <https://help.waterdata.usgs.gov/code/coord_meth_cd_query?fmt=html>`_ is available.
    horizontal_position_method_name : string or list of strings, optional
        Indicates the method used to determine latitude longitude values. A `list of
        codes <https://help.waterdata.usgs.gov/code/coord_meth_cd_query?fmt=html>`_ is available.
    original_horizontal_datum : string or list of strings, optional
        Coordinates are published in EPSG:4326 / WGS84 / World Geodetic System
        1984. This field indicates the original datum used to determine
        coordinates before they were converted. A `list of
        codes <https://help.waterdata.usgs.gov/code/coord_datum_cd_query?fmt=html>`_ is available.
    original_horizontal_datum_name : string or list of strings, optional
        Coordinates are published in EPSG:4326 / WGS84 / World Geodetic System
        1984. This field indicates the original datum used to determine coordinates
        before they were converted. A `list of
        codes <https://help.waterdata.usgs.gov/code/coord_datum_cd_query?fmt=html>`_ is available.
    drainage_area : string or list of strings, optional
        The area enclosed by a topographic divide from which direct surface runoff
        from precipitation normally drains by gravity into the stream above that
        point.
    contributing_drainage_area : string or list of strings, optional
        The contributing drainage area of a lake, stream, wetland, or estuary
        monitoring location, in square miles. This item should be present only
        if the contributing area is different from the total drainage area. This
        situation can occur when part of the drainage area consists of very
        porous soil or depressions that either allow all runoff to enter the
        groundwater or traps the water in ponds so that rainfall does not
        contribute to runoff.  A transbasin diversion can also affect the total
        drainage area.
    time_zone_abbreviation : string or list of strings, optional
        A short code describing the time zone used by a monitoring location.
    uses_daylight_savings : string or list of strings, optional
        A flag indicating whether or not a monitoring location uses daylight savings.
    construction_date : string or list of strings, optional
        Date the well was completed.
    aquifer_code : string or list of strings, optional
        Local aquifers in the USGS water resources data base are identified by a
        geohydrologic unit code (a three-digit number related to the age of the
        formation, followed by a 4 or 5 character abbreviation for the geologic
        unit or aquifer name). Additional information is available
        `at this link <https://help.waterdata.usgs.gov/faq/groundwater/local-aquifer-description>`_.
    national_aquifer_code : string or list of strings, optional
        National aquifers are the principal aquifers or aquifer systems in the United
        States, defined as regionally extensive aquifers or aquifer systems that have
        the potential to be used as a source of potable water. Not all groundwater
        monitoring locations can be associated with a National Aquifer. Such
        monitoring locations will not be retrieved using this search criteria. A `list
        of National aquifer codes and names <https://help.waterdata.usgs.gov/code/nat_aqfr_query?fmt=html>`_
        is available.
    aquifer_type_code : string or list of strings, optional
        Groundwater occurs in aquifers under two different conditions. Where water
        only partly fills an aquifer, the upper surface is free to rise and decline.
        These aquifers are referred to as unconfined (or water-table) aquifers. Where
        water completely fills an aquifer that is overlain by a confining bed, the
        aquifer is referred to as a confined (or artesian) aquifer. When a confined
        aquifer is penetrated by a well, the water level in the well will rise above
        the top of the aquifer (but not necessarily above land surface). Additional
        information is available `at this link <https://help.waterdata.usgs.gov/faq/groundwater/local-aquifer-description>`_.
    well_constructed_depth : string or list of strings, optional
        The depth of the finished well, in feet below land surface datum. Note: Not
        all groundwater monitoring locations have information on Well Depth. Such
        monitoring locations will not be retrieved using this search criteria.
    hole_constructed_depth : string or list of strings, optional
        The total depth to which the hole is drilled, in feet below land surface datum.
        Note: Not all groundwater monitoring locations have information on Hole Depth.
        Such monitoring locations will not be retrieved using this search criteria.
    depth_source_code : string or list of strings, optional
        A code indicating the source of water-level data. A `list of
        codes <https://help.waterdata.usgs.gov/code/water_level_src_cd_query?fmt=html>`_
        is available.
    properties : string or list of strings, optional
        A vector of requested columns to be returned from the query. Available
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
        expected format is a numeric vector structured: c(xmin,ymin,xmax,ymax).
        Another way to think of it is c(Western-most longitude, Southern-most
        latitude, Eastern-most longitude, Northern-most longitude).
    limit : numeric, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (NA) will set the
        limit to the maximum allowable limit for the service.
    skip_geometry : boolean, optional
        This option can be used to skip response geometries for each feature.
        The returning object will be a data frame with no spatial information.
        Note that the USGS Water Data APIs use camelCase "skipGeometry" in
        CQL2 queries.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

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
    output_id = "monitoring_location_id"

    # Build argument dictionary, omitting None values
    args = {
        k: v
        for k, v in locals().items()
        if k not in {"service", "output_id"} and v is not None
    }

    return get_ogc_data(args, output_id, service)


def get_time_series_metadata(
    monitoring_location_id: Optional[Union[str, List[str]]] = None,
    parameter_code: Optional[Union[str, List[str]]] = None,
    parameter_name: Optional[Union[str, List[str]]] = None,
    properties: Optional[Union[str, List[str]]] = None,
    statistic_id: Optional[Union[str, List[str]]] = None,
    hydrologic_unit_code: Optional[Union[str, List[str]]] = None,
    state_name: Optional[Union[str, List[str]]] = None,
    last_modified: Optional[Union[str, List[str]]] = None,
    begin: Optional[Union[str, List[str]]] = None,
    end: Optional[Union[str, List[str]]] = None,
    begin_utc: Optional[Union[str, List[str]]] = None,
    end_utc: Optional[Union[str, List[str]]] = None,
    unit_of_measure: Optional[Union[str, List[str]]] = None,
    computation_period_identifier: Optional[Union[str, List[str]]] = None,
    computation_identifier: Optional[Union[str, List[str]]] = None,
    thresholds: Optional[int] = None,
    sublocation_identifier: Optional[Union[str, List[str]]] = None,
    primary: Optional[Union[str, List[str]]] = None,
    parent_time_series_id: Optional[Union[str, List[str]]] = None,
    time_series_id: Optional[Union[str, List[str]]] = None,
    web_description: Optional[Union[str, List[str]]] = None,
    skip_geometry: Optional[bool] = None,
    time: Optional[Union[str, List[str]]] = None,
    bbox: Optional[List[float]] = None,
    limit: Optional[int] = None,
    convert_type: bool = True,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """Daily data and continuous measurements are grouped into time series,
    which represent a collection of observations of a single parameter,
    potentially aggregated using a standard statistic, at a single monitoring
    location. This endpoint provides metadata about those time series,
    including their operational thresholds, units of measurement, and when
    the earliest and most recent observations in a time series occurred.

    Parameters
    ----------
    monitoring_location_id : string or list of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of
        the agency responsible for the monitoring location (e.g. USGS) with
        the ID number of the monitoring location (e.g. 02238500), separated
        by a hyphen (e.g. USGS-02238500).
    parameter_code : string or list of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter
        codes and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    parameter_name : string or list of strings, optional
        A human-understandable name corresponding to parameter_code.
    properties : string or list of strings, optional
        A vector of requested columns to be returned from the query.
        Available options are: geometry, id, time_series_id,
        monitoring_location_id, parameter_code, statistic_id, time, value,
        unit_of_measure, approval_status, qualifier, last_modified
    statistic_id : string or list of strings, optional
        A code corresponding to the statistic an observation represents.
        Example codes include 00001 (max), 00002 (min), and 00003 (mean).
        A complete list of codes and their descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    hydrologic_unit_code : string or list of strings, optional
        The United States is divided and sub-divided into successively smaller
        hydrologic units which are classified into four levels: regions,
        sub-regions, accounting units, and cataloging units. The hydrologic
        units are arranged within each other, from the smallest (cataloging units)
        to the largest (regions). Each hydrologic unit is identified by a unique
        hydrologic unit code (HUC) consisting of two to eight digits based on the
        four levels of classification in the hydrologic unit system.
    state_name : string or list of strings, optional
        The name of the state or state equivalent in which the monitoring location
        is located.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        anything about the measurement has changed. You can query this field
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

    begin : string or list of strings, optional
        This field contains the same information as "begin_utc", but in the
        local time of the monitoring location. It is retained for backwards
        compatibility, but will be removed in V1 of these APIs.
    end : string or list of strings, optional
        This field contains the same information as "end_utc", but in the
        local time of the monitoring location. It is retained for backwards
        compatibility, but will be removed in V1 of these APIs.
    begin_utc : string or list of strings, optional
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
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

    end_utc : string or list of strings, optional
        The datetime of the most recent observation in the time series. Data returned by
        this endpoint updates at most once per day, and potentially less frequently than
        that, and as such there may be more recent observations within a time series
        than the time series end value reflects. Together with begin, this field
        represents the period of record of a time series. It is additionally used to
        determine whether a time series is "active". We intend to update this in
        version v0 to use UTC with a time zone. You can query this field using date-times
        or intervals, adhering to RFC 3339, or using ISO 8601 duration objects. Intervals
        may be bounded or half-bounded (double-dots at start or end). Only
        features that have a end that intersects the value of datetime are
        selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

    unit_of_measure : string or list of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    computation_period_identifier : string or list of strings, optional
        Indicates the period of data used for any statistical computations.
    computation_identifier : string or list of strings, optional
        Indicates whether the data from this time series represent a specific
        statistical computation.
    thresholds : numeric or list of numbers, optional
        Thresholds represent known numeric limits for a time series, for example
        the historic maximum value for a parameter or a level below which a
        sensor is non-operative. These thresholds are sometimes used to
        automatically determine if an observation is erroneous due to sensor
        error, and therefore shouldn't be included in the time series.
    sublocation_identifier : string or list of strings, optional
    primary : string or list of strings, optional
    parent_time_series_id : string or list of strings, optional
    time_series_id : string or list of strings, optional
        A unique identifier representing a single time series. This
        corresponds to the id field in the time-series-metadata endpoint.
    web_description : string or list of strings, optional
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
        expected format is a numeric vector structured: c(xmin,ymin,xmax,ymax).
        Another way to think of it is c(Western-most longitude, Southern-most
        latitude, Eastern-most longitude, Northern-most longitude).
    limit : numeric, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

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
        ...     monitoring_location_id = ["USGS-05114000", "USGS-09423350"],
        ...     begin = "1990-01-01/.."
        ... )
    """
    service = "time-series-metadata"
    output_id = "time_series_id"

    # Build argument dictionary, omitting None values
    args = {
        k: v
        for k, v in locals().items()
        if k not in {"service", "output_id"} and v is not None
    }

    return get_ogc_data(args, output_id, service)


def get_latest_continuous(
    monitoring_location_id: Optional[Union[str, List[str]]] = None,
    parameter_code: Optional[Union[str, List[str]]] = None,
    statistic_id: Optional[Union[str, List[str]]] = None,
    properties: Optional[Union[str, List[str]]] = None,
    time_series_id: Optional[Union[str, List[str]]] = None,
    latest_continuous_id: Optional[Union[str, List[str]]] = None,
    approval_status: Optional[Union[str, List[str]]] = None,
    unit_of_measure: Optional[Union[str, List[str]]] = None,
    qualifier: Optional[Union[str, List[str]]] = None,
    value: Optional[int] = None,
    last_modified: Optional[Union[str, List[str]]] = None,
    skip_geometry: Optional[bool] = None,
    time: Optional[Union[str, List[str]]] = None,
    bbox: Optional[List[float]] = None,
    limit: Optional[int] = None,
    convert_type: bool = True,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """This endpoint provides the most recent observation for each time series
    of continuous data. Continuous data are collected via automated sensors
    installed at a monitoring location. They are collected at a high frequency
    and often at a fixed 15-minute interval. Depending on the specific monitoring
    location, the data may be transmitted automatically via telemetry and be
    available on WDFN within minutes of collection, while other times the delivery
    of data may be delayed if the monitoring location does not have the capacity to
    automatically transmit data. Continuous data are described by parameter name
    and parameter code. These data might also be referred to as "instantaneous
    values" or "IV"

    Parameters
    ----------
    monitoring_location_id : string or list of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of the
        agency responsible for the monitoring location (e.g. USGS) with the ID
        number of the monitoring location (e.g. 02238500), separated by a hyphen
        (e.g. USGS-02238500).
    parameter_code : string or list of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter codes
        and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or list of strings, optional
        A code corresponding to the statistic an observation represents.
        Example codes include 00001 (max), 00002 (min), and 00003 (mean).
        A complete list of codes and their descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or list of strings, optional
        A vector of requested columns to be returned from the query.  Available
        options are: geometry, id, time_series_id, monitoring_location_id,
        parameter_code, statistic_id, time, value, unit_of_measure,
        approval_status, qualifier, last_modified
    time_series_id : string or list of strings, optional
        A unique identifier representing a single time series. This
        corresponds to the id field in the time-series-metadata endpoint.
    latest_continuous_id : string or list of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. It is not stable over time. Every time the record is refreshed
        in our database (which may happen as part of normal operations and does
        not imply any change to the data itself) a new ID will be generated. To
        uniquely identify a single observation over time, compare the time and
        time_series_id fields; each time series will only have a single
        observation at a given time.
    approval_status : string or list of strings, optional
        Some of the data that you have obtained from this U.S. Geological Survey
        database may not have received Director's approval. Any such data values
        are qualified as provisional and are subject to revision. Provisional
        data are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from its
        use. This field reflects the approval status of each record, and is either
        "Approved", meaining processing review has been completed and the data is
        approved for publication, or "Provisional" and subject to revision. For
        more information about provisional data, go to:
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or list of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or list of strings, optional
        This field indicates any qualifiers associated with an observation, for
        instance if a sensor may have been impacted by ice or if values were
        estimated.
    value : string or list of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format in order to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features that have a last_modified that
        intersects the value of datetime are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

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
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features that have a geometry that intersects the bounding box are
        selected.  The bounding box is provided as four or six numbers,
        depending on whether the coordinate reference system includes a vertical
        axis (height or depth). Coordinates are assumed to be in crs 4326. The
        expected format is a numeric vector structured: c(xmin,ymin,xmax,ymax).
        Another way to think of it is c(Western-most longitude, Southern-most
        latitude, Eastern-most longitude, Northern-most longitude).
    limit : numeric, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. code::

        >>> # Get latest flow data from a single site
        >>> df, md = dataretrieval.waterdata.get_latest_continuous(
        ...     monitoring_location_id="USGS-02238500", parameter_code="00060"
        ... )

        >>> # Get latest continuous measurements for multiple sites
        >>> df, md = dataretrieval.waterdata.get_latest_continuous(
        ...     monitoring_location_id=["USGS-05114000", "USGS-09423350"]
        ... )
    """
    service = "latest-continuous"
    output_id = "latest_continuous_id"

    # Build argument dictionary, omitting None values
    args = {
        k: v
        for k, v in locals().items()
        if k not in {"service", "output_id"} and v is not None
    }

    return get_ogc_data(args, output_id, service)


def get_latest_daily(
    monitoring_location_id: Optional[Union[str, List[str]]] = None,
    parameter_code: Optional[Union[str, List[str]]] = None,
    statistic_id: Optional[Union[str, List[str]]] = None,
    properties: Optional[Union[str, List[str]]] = None,
    time_series_id: Optional[Union[str, List[str]]] = None,
    latest_daily_id: Optional[Union[str, List[str]]] = None,
    approval_status: Optional[Union[str, List[str]]] = None,
    unit_of_measure: Optional[Union[str, List[str]]] = None,
    qualifier: Optional[Union[str, List[str]]] = None,
    value: Optional[int] = None,
    last_modified: Optional[Union[str, List[str]]] = None,
    skip_geometry: Optional[bool] = None,
    time: Optional[Union[str, List[str]]] = None,
    bbox: Optional[List[float]] = None,
    limit: Optional[int] = None,
    convert_type: bool = True,
) -> Tuple[pd.DataFrame, BaseMetadata]:
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
    monitoring_location_id : string or list of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of the
        agency responsible for the monitoring location (e.g. USGS) with the ID
        number of the monitoring location (e.g. 02238500), separated by a hyphen
        (e.g. USGS-02238500).
    parameter_code : string or list of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter codes
        and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or list of strings, optional
        A code corresponding to the statistic an observation represents.
        Example codes include 00001 (max), 00002 (min), and 00003 (mean).
        A complete list of codes and their descriptions can be found at
        https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=html.
    properties : string or list of strings, optional
        A vector of requested columns to be returned from the query.  Available
        options are: geometry, id, time_series_id, monitoring_location_id,
        parameter_code, statistic_id, time, value, unit_of_measure,
        approval_status, qualifier, last_modified
    time_series_id : string or list of strings, optional
        A unique identifier representing a single time series. This
        corresponds to the id field in the time-series-metadata endpoint.
    latest_daily_id : string or list of strings, optional
        A universally unique identifier (UUID) representing a single version of
        a record. It is not stable over time. Every time the record is refreshed
        in our database (which may happen as part of normal operations and does
        not imply any change to the data itself) a new ID will be generated. To
        uniquely identify a single observation over time, compare the time and
        time_series_id fields; each time series will only have a single
        observation at a given time.
    approval_status : string or list of strings, optional
        Some of the data that you have obtained from this U.S. Geological Survey
        database may not have received Director's approval. Any such data values
        are qualified as provisional and are subject to revision. Provisional
        data are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from its
        use. This field reflects the approval status of each record, and is either
        "Approved", meaining processing review has been completed and the data is
        approved for publication, or "Provisional" and subject to revision. For
        more information about provisional data, go to:
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or list of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or list of strings, optional
        This field indicates any qualifiers associated with an observation, for
        instance if a sensor may have been impacted by ice or if values were
        estimated.
    value : string or list of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format in order to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features that have a last_modified that
        intersects the value of datetime are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

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
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features that have a geometry that intersects the bounding box are
        selected.  The bounding box is provided as four or six numbers,
        depending on whether the coordinate reference system includes a vertical
        axis (height or depth). Coordinates are assumed to be in crs 4326. The
        expected format is a numeric vector structured: c(xmin,ymin,xmax,ymax).
        Another way to think of it is c(Western-most longitude, Southern-most
        latitude, Eastern-most longitude, Northern-most longitude).
    limit : numeric, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. code::

        >>> # Get most recent daily flow data from a single site
        >>> df, md = dataretrieval.waterdata.get_latest_daily(
        ...     monitoring_location_id="USGS-02238500", parameter_code="00060"
        ... )

        >>> # Get most recent daily measurements for two sites
        >>> df, md = dataretrieval.waterdata.get_latest_daily(
        ...     monitoring_location_id=["USGS-05114000", "USGS-09423350"]
        ... )
    """
    service = "latest-daily"
    output_id = "latest_daily_id"

    # Build argument dictionary, omitting None values
    args = {
        k: v
        for k, v in locals().items()
        if k not in {"service", "output_id"} and v is not None
    }

    return get_ogc_data(args, output_id, service)

def get_field_measurements(
    monitoring_location_id: Optional[Union[str, List[str]]] = None,
    parameter_code: Optional[Union[str, List[str]]] = None,
    observing_procedure_code: Optional[Union[str, List[str]]] = None,
    properties: Optional[List[str]] = None,
    field_visit_id: Optional[Union[str, List[str]]] = None,
    approval_status: Optional[Union[str, List[str]]] = None,
    unit_of_measure: Optional[Union[str, List[str]]] = None,
    qualifier: Optional[Union[str, List[str]]] = None,
    value: Optional[Union[str, List[str]]] = None,
    last_modified: Optional[Union[str, List[str]]] = None,
    observing_procedure: Optional[Union[str, List[str]]] = None,
    vertical_datum: Optional[Union[str, List[str]]] = None,
    measuring_agency: Optional[Union[str, List[str]]] = None,
    skip_geometry: Optional[bool] = None,
    time: Optional[Union[str, List[str]]] = None,
    bbox: Optional[List[float]] = None,
    limit: Optional[int] = None,
    convert_type: bool = True,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """Field measurements are physically measured values collected during a
    visit to the monitoring location. Field measurements consist of measurements
    of gage height and discharge, and readings of groundwater levels, and are
    primarily used as calibration readings for the automated sensors collecting
    continuous data. They are collected at a low frequency, and delivery of the
    data in WDFN may be delayed due to data processing time.

    Parameters
    ----------
    monitoring_location_id : string or list of strings, optional
        A unique identifier representing a single monitoring location. This
        corresponds to the id field in the monitoring-locations endpoint.
        Monitoring location IDs are created by combining the agency code of the
        agency responsible for the monitoring location (e.g. USGS) with the ID
        number of the monitoring location (e.g. 02238500), separated by a hyphen
        (e.g. USGS-02238500).
    parameter_code : string or list of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent
        measured and the units of measure. A complete list of parameter codes
        and associated groupings can be found at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    observing_procedure_code : string or list of strings, optional
        A short code corresponding to the observing procedure for the field
        measurement.
    properties : string or list of strings, optional
        A vector of requested columns to be returned from the query.  Available
        options are: geometry, id, time_series_id, monitoring_location_id,
        parameter_code, statistic_id, time, value, unit_of_measure,
        approval_status, qualifier, last_modified
    field_visit_id : string or list of strings, optional
        A universally unique identifier (UUID) for the field visit.
        Multiple measurements may be made during a single field visit.
    approval_status : string or list of strings, optional
        Some of the data that you have obtained from this U.S. Geological Survey
        database may not have received Director's approval. Any such data values
        are qualified as provisional and are subject to revision. Provisional
        data are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from its
        use. This field reflects the approval status of each record, and is either
        "Approved", meaining processing review has been completed and the data is
        approved for publication, or "Provisional" and subject to revision. For
        more information about provisional data, go to:
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or list of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or list of strings, optional
        This field indicates any qualifiers associated with an observation, for
        instance if a sensor may have been impacted by ice or if values were
        estimated.
    value : string or list of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format in order to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen
        due to regular operational processes and does not necessarily indicate
        anything about the measurement has changed. You can query this field
        using date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features that have a last_modified that
        intersects the value of datetime are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

    observing_procedure : string or list of strings, optional
        Water measurement or water-quality observing procedure descriptions.
    vertical_datum : string or list of strings, optional
        The datum used to determine altitude and vertical position at the monitoring location.
        A list of codes is available.
    measuring_agency : string or list of strings, optional
        The agency performing the measurement.
    skip_geometry : boolean, optional
        This option can be used to skip response geometries for each feature. The returning
        object will be a data frame with no spatial information.
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
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features that have a geometry that intersects the bounding box are
        selected.  The bounding box is provided as four or six numbers,
        depending on whether the coordinate reference system includes a vertical
        axis (height or depth). Coordinates are assumed to be in crs 4326. The
        expected format is a numeric vector structured: c(xmin,ymin,xmax,ymax).
        Another way to think of it is c(Western-most longitude, Southern-most
        latitude, Eastern-most longitude, Northern-most longitude).
    limit : numeric, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

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

        >>> # Get field measurements from multiple sites and
        >>> # parameter codes from the last 20 years
        >>> df, md = dataretrieval.waterdata.get_field_measurements(
        ...     monitoring_location_id = ["USGS-451605097071701",
                                          "USGS-263819081585801"],
        ...     parameter_code = ["62611", "72019"],
        ...     time = "P20Y"
        ... )
    """
    service = "field-measurements"
    output_id = "field_measurement_id"

    # Build argument dictionary, omitting None values
    args = {
        k: v
        for k, v in locals().items()
        if k not in {"service", "output_id"} and v is not None
    }

    return get_ogc_data(args, output_id, service)


def get_reference_table(
        collection: str,
        limit: Optional[int] = None,
        ) -> Tuple[pd.DataFrame, BaseMetadata]:
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
    limit : numeric, optional
        The optional limit parameter is used to control the subset of the
        selected features that should be returned in each page. The maximum
        allowable limit is 50000. It may be beneficial to set this number lower
        if your internet connection is spotty. The default (None) will set the
        limit to the maximum allowable limit for the service.
    
    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query. The primary metadata
        of each reference table will show up in the first column, where
        the name of the column is the singular form of the collection name,
        separated by underscores (e.g. the "medium-codes" reference table
        has a column called "medium_code", which contains all possible
        medium code values).
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object including the URL request and query time.
    
    Examples
    --------
    .. code::

        >>> # Get table of USGS parameter codes
        >>> ref, md = dataretrieval.waterdata.get_reference_table(
        ...     collection="parameter-codes"
        ... )
    """
    valid_code_services = get_args(METADATA_COLLECTIONS)
    if collection not in valid_code_services:
        raise ValueError(
            f"Invalid code service: '{collection}'. "
            f"Valid options are: {valid_code_services}."
        )
    
    # Give ID column the collection name with underscores
    if collection.endswith("s") and collection != "counties":
        output_id = f"{collection[:-1].replace('-', '_')}"
    elif collection == "counties":
        output_id = "county"
    else:
        output_id = f"{collection.replace('-', '_')}"
    
    return get_ogc_data(
        args={},
        output_id=output_id,
        service=collection
        )


def get_codes(code_service: CODE_SERVICES) -> pd.DataFrame:
    """Return codes from a Samples code service.

    Parameters
    ----------
    code_service : string
        One of the following options: "states", "counties", "countries"
        "sitetype", "samplemedia", "characteristicgroup", "characteristics",
        or "observedproperty"
    """
    valid_code_services = get_args(CODE_SERVICES)
    if code_service not in valid_code_services:
        raise ValueError(
            f"Invalid code service: '{code_service}'. "
            f"Valid options are: {valid_code_services}."
        )

    url = f"{SAMPLES_URL}/codeservice/{code_service}?mimeType=application%2Fjson"

    response = requests.get(url)

    response.raise_for_status()

    data_dict = json.loads(response.text)
    data_list = data_dict["data"]

    df = pd.DataFrame(data_list)

    return df


def get_samples(
    ssl_check: bool = True,
    service: SERVICES = "results",
    profile: PROFILES = "fullphyschem",
    activityMediaName: Optional[Union[str, list[str]]] = None,
    activityStartDateLower: Optional[str] = None,
    activityStartDateUpper: Optional[str] = None,
    activityTypeCode: Optional[Union[str, list[str]]] = None,
    characteristicGroup: Optional[Union[str, list[str]]] = None,
    characteristic: Optional[Union[str, list[str]]] = None,
    characteristicUserSupplied: Optional[Union[str, list[str]]] = None,
    boundingBox: Optional[list[float]] = None,
    countryFips: Optional[Union[str, list[str]]] = None,
    stateFips: Optional[Union[str, list[str]]] = None,
    countyFips: Optional[Union[str, list[str]]] = None,
    siteTypeCode: Optional[Union[str, list[str]]] = None,
    siteTypeName: Optional[Union[str, list[str]]] = None,
    usgsPCode: Optional[Union[str, list[str]]] = None,
    hydrologicUnit: Optional[Union[str, list[str]]] = None,
    monitoringLocationIdentifier: Optional[Union[str, list[str]]] = None,
    organizationIdentifier: Optional[Union[str, list[str]]] = None,
    pointLocationLatitude: Optional[float] = None,
    pointLocationLongitude: Optional[float] = None,
    pointLocationWithinMiles: Optional[float] = None,
    projectIdentifier: Optional[Union[str, list[str]]] = None,
    recordIdentifierUserSupplied: Optional[Union[str, list[str]]] = None,
) -> Tuple[pd.DataFrame, BaseMetadata]:
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
    activityMediaName : string or list of strings, optional
        Name or code indicating environmental medium in which sample was taken.
        Check the `activityMediaName_lookup()` function in this module for all
        possible inputs.
        Example: "Water".
    activityStartDateLower : string, optional
        The start date if using a date range. Takes the format YYYY-MM-DD.
        The logic is inclusive, i.e. it will also return results that
        match the date. If left as None, will pull all data on or before
        activityStartDateUpper, if populated.
    activityStartDateUpper : string, optional
        The end date if using a date range. Takes the format YYYY-MM-DD.
        The logic is inclusive, i.e. it will also return results that
        match the date. If left as None, will pull all data after
        activityStartDateLower up to the most recent available results.
    activityTypeCode : string or list of strings, optional
        Text code that describes type of field activity performed.
        Example: "Sample-Routine, regular".
    characteristicGroup : string or list of strings, optional
        Characteristic group is a broad category of characteristics
        describing one or more results. Check the `characteristicGroup_lookup()`
        function in this module for all possible inputs.
        Example: "Organics, PFAS"
    characteristic : string or list of strings, optional
        Characteristic is a specific category describing one or more results.
        Check the `characteristic_lookup()` function in this module for all
        possible inputs.
        Example: "Suspended Sediment Discharge"
    characteristicUserSupplied : string or list of strings, optional
        A user supplied characteristic name describing one or more results.
    boundingBox: list of four floats, optional
        Filters on the the associated monitoring location's point location
        by checking if it is located within the specified geographic area.
        The logic is inclusive, i.e. it will include locations that overlap
        with the edge of the bounding box. Values are separated by commas,
        expressed in decimal degrees, NAD83, and longitudes west of Greenwich
        are negative. The format is a string consisting of:

            * Western-most longitude
            * Southern-most latitude
            * Eastern-most longitude
            * Northern-most longitude

        Example: [-92.8,44.2,-88.9,46.0]
    countryFips : string or list of strings, optional
        Example: "US" (United States)
    stateFips : string or list of strings, optional
        Check the `stateFips_lookup()` function in this module for all
        possible inputs.
        Example: "US:15" (United States: Hawaii)
    countyFips : string or list of strings, optional
        Check the `countyFips_lookup()` function in this module for all
        possible inputs.
        Example: "US:15:001" (United States: Hawaii, Hawaii County)
    siteTypeCode : string or list of strings, optional
        An abbreviation for a certain site type. Check the `siteType_lookup()`
        function in this module for all possible inputs.
        Example: "GW" (Groundwater site)
    siteTypeName : string or list of strings, optional
        A full name for a certain site type. Check the `siteType_lookup()`
        function in this module for all possible inputs.
        Example: "Well"
    usgsPCode : string or list of strings, optional
        5-digit number used in the US Geological Survey computerized
        data system, National Water Information System (NWIS), to
        uniquely identify a specific constituent. Check the
        `characteristic_lookup()` function in this module for all possible
        inputs.
        Example: "00060" (Discharge, cubic feet per second)
    hydrologicUnit : string or list of strings, optional
        Max 12-digit number used to describe a hydrologic unit.
        Example: "070900020502"
    monitoringLocationIdentifier : string or list of strings, optional
        A monitoring location identifier has two parts: the agency code
        and the location number, separated by a dash (-).
        Example: "USGS-040851385"
    organizationIdentifier : string or list of strings, optional
        Designator used to uniquely identify a specific organization.
        Currently only accepting the organization "USGS".
    pointLocationLatitude : float, optional
        Latitude for a point/radius query (decimal degrees). Must be used
        with pointLocationLongitude and pointLocationWithinMiles.
    pointLocationLongitude : float, optional
        Longitude for a point/radius query (decimal degrees). Must be used
        with pointLocationLatitude and pointLocationWithinMiles.
    pointLocationWithinMiles : float, optional
        Radius for a point/radius query. Must be used with
        pointLocationLatitude and pointLocationLongitude
    projectIdentifier : string or list of strings, optional
        Designator used to uniquely identify a data collection project. Project
        identifiers are specific to an organization (e.g. USGS).
        Example: "ZH003QW03"
    recordIdentifierUserSupplied : string or list of strings, optional
        Internal AQS record identifier that returns 1 entry. Only available
        for the "results" service.

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.Metadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get PFAS results within a bounding box
        >>> df, md = dataretrieval.waterdata.get_samples(
        ...     boundingBox=[-90.2, 42.6, -88.7, 43.2],
        ...     characteristicGroup="Organics, PFAS",
        ... )

        >>> # Get all activities for the Commonwealth of Virginia over a date range
        >>> df, md = dataretrieval.waterdata.get_samples(
        ...     service="activities",
        ...     profile="sampact",
        ...     activityStartDateLower="2023-10-01",
        ...     activityStartDateUpper="2024-01-01",
        ...     stateFips="US:51",
        ... )

        >>> # Get all pH samples for two sites in Utah
        >>> df, md = dataretrieval.waterdata.get_samples(
        ...     monitoringLocationIdentifier=[
        ...         "USGS-393147111462301",
        ...         "USGS-393343111454101",
        ...     ],
        ...     usgsPCode="00400",
        ... )

    """

    _check_profiles(service, profile)

    params = {
        k: v
        for k, v in locals().items()
        if k not in ["ssl_check", "service", "profile"] and v is not None
    }

    params.update({"mimeType": "text/csv"})

    if "boundingBox" in params:
        params["boundingBox"] = to_str(params["boundingBox"])

    url = f"{SAMPLES_URL}/{service}/{profile}"

    req = PreparedRequest()
    req.prepare_url(url, params=params)
    logger.info("Request: %s", req.url)

    response = requests.get(url, params=params, verify=ssl_check)

    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text), delimiter=",")

    return df, BaseMetadata(response)

