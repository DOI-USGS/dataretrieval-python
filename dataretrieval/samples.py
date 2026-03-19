"""Functions for downloading data from the USGS Aquarius Samples database
(https://waterdata.usgs.gov/download-samples/).

See https://api.waterdata.usgs.gov/samples-data/docs#/ for API reference
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from dataretrieval.utils import BaseMetadata

if TYPE_CHECKING:
    from pandas import DataFrame

    from dataretrieval.waterdata import PROFILES, SERVICES


def get_usgs_samples(
    ssl_check: bool = True,
    service: SERVICES = "results",
    profile: PROFILES = "fullphyschem",
    activityMediaName: str | list[str] | None = None,
    activityStartDateLower: str | None = None,
    activityStartDateUpper: str | None = None,
    activityTypeCode: str | list[str] | None = None,
    characteristicGroup: str | list[str] | None = None,
    characteristic: str | list[str] | None = None,
    characteristicUserSupplied: str | list[str] | None = None,
    boundingBox: list[float] | None = None,
    countryFips: str | list[str] | None = None,
    stateFips: str | list[str] | None = None,
    countyFips: str | list[str] | None = None,
    siteTypeCode: str | list[str] | None = None,
    siteTypeName: str | list[str] | None = None,
    usgsPCode: str | list[str] | None = None,
    hydrologicUnit: str | list[str] | None = None,
    monitoringLocationIdentifier: str | list[str] | None = None,
    organizationIdentifier: str | list[str] | None = None,
    pointLocationLatitude: float | None = None,
    pointLocationLongitude: float | None = None,
    pointLocationWithinMiles: float | None = None,
    projectIdentifier: str | list[str] | None = None,
    recordIdentifierUserSupplied: str | list[str] | None = None,
) -> tuple[DataFrame, BaseMetadata]:
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
        ...     boundingBox=[-90.2, 42.6, -88.7, 43.2],
        ...     characteristicGroup="Organics, PFAS",
        ... )

        >>> # Get all activities for the Commonwealth of Virginia over a date range
        >>> df, md = dataretrieval.samples.get_usgs_samples(
        ...     service="activities",
        ...     profile="sampact",
        ...     activityStartDateLower="2023-10-01",
        ...     activityStartDateUpper="2024-01-01",
        ...     stateFips="US:51",
        ... )

        >>> # Get all pH samples for two sites in Utah
        >>> df, md = dataretrieval.samples.get_usgs_samples(
        ...     monitoringLocationIdentifier=[
        ...         "USGS-393147111462301",
        ...         "USGS-393343111454101",
        ...     ],
        ...     usgsPCode="00400",
        ... )

    """

    warnings.warn(
        (
            "`get_usgs_samples` is deprecated and will be removed. "
            "Use `waterdata.get_samples` instead."
        ),
        DeprecationWarning,
        stacklevel=2,
    )

    from dataretrieval.waterdata import get_samples

    result = get_samples(
        ssl_check=ssl_check,
        service=service,
        profile=profile,
        activityMediaName=activityMediaName,
        activityStartDateLower=activityStartDateLower,
        activityStartDateUpper=activityStartDateUpper,
        activityTypeCode=activityTypeCode,
        characteristicGroup=characteristicGroup,
        characteristic=characteristic,
        characteristicUserSupplied=characteristicUserSupplied,
        boundingBox=boundingBox,
        countryFips=countryFips,
        stateFips=stateFips,
        countyFips=countyFips,
        siteTypeCode=siteTypeCode,
        siteTypeName=siteTypeName,
        usgsPCode=usgsPCode,
        hydrologicUnit=hydrologicUnit,
        monitoringLocationIdentifier=monitoringLocationIdentifier,
        organizationIdentifier=organizationIdentifier,
        pointLocationLatitude=pointLocationLatitude,
        pointLocationLongitude=pointLocationLongitude,
        pointLocationWithinMiles=pointLocationWithinMiles,
        projectIdentifier=projectIdentifier,
        recordIdentifierUserSupplied=recordIdentifierUserSupplied,
    )

    return result
