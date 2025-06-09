"""Functions for downloading data from the USGS Aquarius Samples database
(https://waterdata.usgs.gov/download-samples/).

See https://api.waterdata.usgs.gov/samples-data/docs#/ for API reference
"""

from __future__ import annotations

import json
from io import StringIO
from typing import TYPE_CHECKING, Literal, get_args

import pandas as pd
import warnings
import requests
from requests.models import PreparedRequest

from dataretrieval.utils import BaseMetadata, to_str
from dataretrieval.waterdata import get_codes, get_args, _check_profiles, _BASE_URL, _CODE_SERVICES, _PROFILES, _SERVICES, _PROFILE_LOOKUP

if TYPE_CHECKING:
    from typing import Optional, Tuple, Union

    from pandas import DataFrame

def get_usgs_samples(
    ssl_check: bool = True,
    service: _SERVICES = "results",
    profile: _PROFILES = "fullphyschem",
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
) -> Tuple[DataFrame, BaseMetadata]:
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
        are negative.
        The format is a string consisting of:
        - Western-most longitude
        - Southern-most latitude
        - Eastern-most longitude
        - Northern-most longitude 
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
        >>> df, md = dataretrieval.samples.get_usgs_samples(
        ...     boundingBox=[-90.2,42.6,-88.7,43.2],
        ...     characteristicGroup="Organics, PFAS"
        ... )

        >>> # Get all activities for the Commonwealth of Virginia over a date range
        >>> df, md = dataretrieval.samples.get_usgs_samples(
        ...     service="activities",
        ...     profile="sampact",
        ...     activityStartDateLower="2023-10-01",
        ...     activityStartDateUpper="2024-01-01",
        ...     stateFips="US:51")

        >>> # Get all pH samples for two sites in Utah
        >>> df, md = dataretrieval.samples.get_usgs_samples(
        ...     monitoringLocationIdentifier=['USGS-393147111462301', 'USGS-393343111454101'],
        ...     usgsPCode='00400')

    """

    warnings.warn("The `get_usgs_samples` function is moving from" \
    " the samples module to the new waterdata module, where" \
    " it will be called simply `get_samples`. All of the same" \
    " functionality will be retained. The samples module is" \
    " deprecated and will eventually be removed. Switch to the" \
    " waterdata module as soon as possible, thank you.")

    _check_profiles(service, profile)

    params = {
        k: v for k, v in locals().items()
        if k not in ["ssl_check", "service", "profile"]
        and v is not None
        }


    params.update({"mimeType": "text/csv"})

    if "boundingBox" in params:
        params["boundingBox"] = to_str(params["boundingBox"])

    url = f"{_BASE_URL}/{service}/{profile}"

    req = PreparedRequest()
    req.prepare_url(url, params=params)
    print(f"Request: {req.url}")

    response = requests.get(url, params=params, verify=ssl_check)

    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text), delimiter=",")

    return df, BaseMetadata(response)


