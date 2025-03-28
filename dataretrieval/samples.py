"""
Tool for downloading data from the USGS Aquarius Samples database (https://waterqualitydata.us)

See https://api.waterdata.usgs.gov/samples-data/docs#/ for API reference

"""

from __future__ import annotations

import warnings
import requests
from requests.models import PreparedRequest
from typing import List, Optional, Tuple, Union
from io import StringIO
from typing import TYPE_CHECKING

import pandas as pd

from utils import BaseMetadata, query, to_str

if TYPE_CHECKING:
    from pandas import DataFrame

BASE_URL = "https://api.waterdata.usgs.gov/samples-data/"

services_dict = {
    "results" : ["fullphyschem", "basicphyschem",
                    "fullbio", "basicbio", "narrow",
                    "resultdetectionquantitationlimit",
                    "labsampleprep", "count"],
    "locations" : ["site", "count"],
    "activities" : ["sampact", "actmetric",
                       "actgroup", "count"],
    "projects" : ["project", "projectmonitoringlocationweight"],
    "organizations" : ["organization", "count"]
}


def _check_profiles(
        service,
        profile
):
    """Check that services are paired correctly with profile in
    a service call.

    Parameters
    ----------
    service : string
        One of the service names from the "services" list.
    profile : string
        One of the profile names from "results_profiles",
        "locations_profiles", "activities_profiles",
        "projects_profiles" or "organizations_profiles". 
    """

    if service not in services_dict.keys():
        raise TypeError(
            f"{service} is not a Samples service. "
            f"Valid options are {list(services_dict.keys())}."
            )
    if profile not in services_dict[service]:
        raise TypeError(
            f"{profile} is not a profile associated with "
            f"the {service} service. Valid options are " 
            f"{services_dict[service]}."
        )

def get_USGS_samples(
        ssl_check=True,
        service="results",
        profile="fullphyschem",
        activityMediaName=None,
        activityStartDateLower=None,
        activityStartDateUpper=None,
        activityTypeCode=None,
        characteristicGroup=None,
        characteristc=None,
        characteristicUserSupplied=None,
        boundingBox=None,
        countryFips=None,
        stateFips=None,
        countyFips=None,
        siteTypeCode=None,
        siteTypeName=None,
        usgsPCode=None,
        hydrologicUnit=None,
        monitoringLocationIdentifier=None,
        organizationIdentifier=None,
        pointLocationLatitude=None,
        pointLocationLongitude=None,
        pointLocationWithinMiles=None,
        projectIdentifier=None,
        recordIdentifierUserSupplied=None
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
        Name or code indicating environmental medium sample was taken.
        Example: "Water".
    activityStartDateLower : string, optional
        The start date if using a date range. Takes the format YYYY-MM-DD.
        The logic is inclusive, i.e. it will also return results that
        match the date. 
    activityStartDateUpper : string, optional
        The end date if using a date range. Takes the format YYYY-MM-DD.
        The logic is inclusive, i.e. it will also return results that
        match the date. If left as None, will pull all data before
        activityStartDateLower up to the most recent available results.
    activityTypeCode : string or list of strings, optional
        Text code that describes type of field activity performed.
        Example: "Sample-Routine, regular".
    characteristicGroup : string or list of strings, optional
        Characteristic group is a broad category describing one or more
        of results.
        Example: "Organics, PFAS"
    characteristc : string or list of strings, optional
        Characteristic is a specific category describing one or more results.
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
        Check out the code service for FIPS codes:
        https://api.waterdata.usgs.gov/samples-data/codeservice/docs#/
        Example: "US:15" (United States: Hawaii)
    countyFips : string or list of strings, optional
        Check out the code service for FIPS codes:
        https://api.waterdata.usgs.gov/samples-data/codeservice/docs#/
        Example: "US:15:001" (United States: Hawaii, Hawaii County)
    siteTypeCode : string or list of strings, optional
        An abbreviation for a certain site type. 
        Example: "GW" (Groundwater site)
    siteTypeName : string or list of strings, optional
        A full name for a certain site type.
        Example: "Well"
    usgsPCode : string or list of strings, optional
        5-digit number used in the US Geological Survey computerized
        data system, National Water Information System (NWIS), to
        uniquely identify a specific constituent
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
        Designator used to uniquely id a data collection project in
        organization context. 
    recordIdentifierUserSupplied : string or list of strings, optional
        Internal AQS record identifier that returns 1 entry. Only available
        for the "results" service.
    mimeType : string, optional
    
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
        >>> df, md = dataretrieval.samples.get_USGS_samples(
        ...     boundingBox=[-90.2,42.6,-88.7,43.2],
        ...     characteristicGroup="Organics, PFAS"
        ... )

        >>> # Get all activities for the Commonwealth of Virginia over a date range
        >>> df, md = dataretrieval.samples.get_USGS_samples(
        ...     service="activities",
        ...     profile="sampact",
        ...     activityStartDateLower="2023-10-01",
        ...     activityStartDateUpper="2024-01-01",
        ...     stateFips="US:51")

    """
    _check_profiles(service, profile)

    # Get all not-None inputs
    params = {key: value for key, value in locals().items() if value is not None and key not in ['service', 'profile', 'ssl_check']}

    if len(params) == 0:
        raise TypeError("No filter parameters provided. You must add at least " 
                        "one filter parameter beyond a service, profile, and format argument.")
    
    # Add in file format (could be an input, too, though not sure about other formats)
    params['mimeType'] = "text/csv"

    # Convert bounding box to a string
    if "boundingBox" in params:
        params['boundingBox'] = to_str(params['boundingBox'])

    # Build URL with service and profile
    url = BASE_URL + service + "/" + profile

    # Print URL
    req = PreparedRequest()
    req.prepare_url(url, params=params)
    print(f"Request: {req.url}")

    # Make a GET request with the filtered parameters
    response = requests.get(url, params=params, verify=ssl_check)

    response.raise_for_status

    df = pd.read_csv(StringIO(response.text), delimiter=",")

    #return response

    return df, BaseMetadata(response)
