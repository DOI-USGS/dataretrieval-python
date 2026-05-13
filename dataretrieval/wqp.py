"""
Tool for downloading data from the Water Quality Portal (https://waterqualitydata.us)

See https://waterqualitydata.us/webservices_documentation for API reference

.. todo::

    - implement other services like Organization, Activity, etc.

"""

from __future__ import annotations

import warnings
from functools import cached_property
from io import StringIO
from typing import TYPE_CHECKING

import pandas as pd

from .utils import BaseMetadata, query

if TYPE_CHECKING:
    from pandas import DataFrame

# WQP-native site key first, then legacy NWIS-style aliases. WQP_Metadata.site_info
# walks these in order and forwards whichever matched as what_sites' `siteid`.
_SITE_KEYS = ("siteid", "sites", "site", "site_no")


result_profiles_wqx3 = ["basicPhysChem", "fullPhysChem", "narrow"]
result_profiles_legacy = ["biological", "narrowResult", "resultPhysChem"]
activity_profiles_legacy = ["activityAll"]
services_wqx3 = ["Activity", "Result", "Station"]
services_legacy = [
    "Activity",
    "ActivityMetric",
    "BiologicalMetric",
    "Organization",
    "Project",
    "ProjectMonitoringLocationWeighting",
    "Result",
    "ResultDetectionQuantitationLimit",
    "Station",
]


def get_results(
    ssl_check=True,
    legacy=True,
    **kwargs,
) -> tuple[DataFrame, WQP_Metadata]:
    """Query the WQP for results.

    Any WQP API parameter can be passed as a keyword argument to this function.
    More information about the API can be found at:
    https://www.waterqualitydata.us/#advanced=true
    or the beta version of the WQX3.0 API at:
    https://www.waterqualitydata.us/beta/#mimeType=csv&providers=NWIS&providers=STORET
    or the Swagger documentation at:
    https://www.waterqualitydata.us/data/swagger-ui/index.html?docExpansion=none&url=/data/v3/api-docs#/

    Parameters
    ----------
    ssl_check : bool, optional
        Check the SSL certificate.
    legacy : bool, optional
        Return the legacy WQX data profile. Default is True.
    dataProfile : string, optional
        Specifies the data fields returned by the query.
        WQX3.0 profiles include 'fullPhysChem', 'narrow', and 'basicPhysChem'.
        Legacy profiles include 'resultPhysChem','biological', and
        'narrowResult'. Default is 'fullPhysChem'.
    siteid : string
        Monitoring location identified by agency code, a hyphen, and
        identification number (Example: "USGS-05586100").
    statecode : string
        US state FIPS code (Example: Illinois is "US:17").
    countycode : string
        US county FIPS code.
    huc : string
        Eight-digit hydrologic unit (HUC), delimited by semicolons.
    bBox : string
        Search bounding box (Example: bBox=-92.8,44.2,-88.9,46.0)
    lat : string
        Radial-search central latitude in WGS84 decimal degrees.
    long : string
        Radial-search central longitude in WGS84 decimal degrees.
    within : string
        Radial-search distance in decimal miles.
    pCode : string
        Five-digit USGS parameter code, delimited by semicolons.
        NWIS only.
    startDateLo : string
        Date of earliest desired data-collection activity,
        expressed as 'MM-DD-YYYY'
    startDateHi : string
        Date of last desired data-collection activity,
        expressed as 'MM-DD-YYYY'
    characteristicName : string
        One or more case-sensitive characteristic names, separated by
        semicolons (https://www.waterqualitydata.us/public_srsnames/).
    mimeType : string
        Output format. Only 'csv' is supported at this time.

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.Metadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get results within a radial distance of a point
        >>> df, md = dataretrieval.wqp.get_results(
        ...     lat="44.2", long="-88.9", within="0.5"
        ... )

        >>> # Get results within a bounding box
        >>> df, md = dataretrieval.wqp.get_results(bBox="-92.8,44.2,-88.9,46.0")

        >>> # Get results using a new WQX3.0 profile
        >>> df, md = dataretrieval.wqp.get_results(
        ...     legacy=False, siteid="UTAHDWQ_WQX-4993795", dataProfile="narrow"
        ... )

    """

    kwargs = _check_kwargs(kwargs)

    if legacy is True:
        valid_profiles = result_profiles_legacy
        kind = "legacy"
        url = wqp_url("Result")
    else:
        valid_profiles = result_profiles_wqx3
        kind = "WQX3.0"
        url = wqx3_url("Result")

    profile = kwargs.get("dataProfile")
    if profile is not None and profile not in valid_profiles:
        raise ValueError(
            f"dataProfile {profile!r} is not a valid {kind} profile. "
            f"Valid options are {valid_profiles}."
        )
    if legacy is not True and profile is None:
        kwargs["dataProfile"] = "fullPhysChem"

    response = query(url, kwargs, delimiter=";", ssl_check=ssl_check)

    df = pd.read_csv(StringIO(response.text), delimiter=",", low_memory=False)
    return df, WQP_Metadata(response, legacy=legacy, ssl_check=ssl_check, **kwargs)


def what_sites(
    ssl_check=True,
    legacy=True,
    **kwargs,
) -> tuple[DataFrame, WQP_Metadata]:
    """Search WQP for sites within a region with specific data.

    Any WQP API parameter can be passed as a keyword argument to this function.
    More information about the API can be found at:
    https://www.waterqualitydata.us/#advanced=true
    or the beta version of the WQX3.0 API at:
    https://www.waterqualitydata.us/beta/#mimeType=csv&providers=NWIS&providers=STORET
    or the Swagger documentation at:
    https://www.waterqualitydata.us/data/swagger-ui/index.html?docExpansion=none&url=/data/v3/api-docs#/

    Parameters
    ----------
    ssl_check : bool, optional
        Check the SSL certificate. Default is True.
    legacy : bool, optional
        If True, returns the legacy WQX data profile and warns the user of
        the issues associated with it. If False, returns the new WQX3.0
        profile, if available. Defaults to True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get sites within a radial distance of a point
        >>> df, md = dataretrieval.wqp.what_sites(
        ...     lat="44.2", long="-88.9", within="2.5"
        ... )

    """

    kwargs = _check_kwargs(kwargs)

    url = wqp_url("Station") if legacy is True else wqx3_url("Station")

    response = query(url, payload=kwargs, delimiter=";", ssl_check=ssl_check)

    df = pd.read_csv(StringIO(response.text), delimiter=",", low_memory=False)

    return df, WQP_Metadata(response, legacy=legacy, ssl_check=ssl_check, **kwargs)


def what_organizations(
    ssl_check=True,
    legacy=True,
    **kwargs,
) -> tuple[DataFrame, WQP_Metadata]:
    """Search WQP for organizations within a region with specific data.

    Any WQP API parameter can be passed as a keyword argument to this function.
    More information about the API can be found at:
    https://www.waterqualitydata.us/#advanced=true
    or the beta version of the WQX3.0 API at:
    https://www.waterqualitydata.us/beta/#mimeType=csv&providers=NWIS&providers=STORET
    or the Swagger documentation at:
    https://www.waterqualitydata.us/data/swagger-ui/index.html?docExpansion=none&url=/data/v3/api-docs#/

    Parameters
    ----------
    ssl_check : bool, optional
        Check the SSL certificate. Default is True.
    legacy : bool, optional
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get all organizations in the WQP
        >>> df, md = dataretrieval.wqp.what_organizations()

    """

    kwargs = _check_kwargs(kwargs)

    url = _legacy_only_url("Organization", legacy=legacy)

    response = query(url, payload=kwargs, delimiter=";", ssl_check=ssl_check)

    df = pd.read_csv(StringIO(response.text), delimiter=",", low_memory=False)

    return df, WQP_Metadata(response, legacy=legacy, ssl_check=ssl_check, **kwargs)


def what_projects(ssl_check=True, legacy=True, **kwargs):
    """Search WQP for projects within a region with specific data.

    Any WQP API parameter can be passed as a keyword argument to this function.
    More information about the API can be found at:
    https://www.waterqualitydata.us/#advanced=true
    or the beta version of the WQX3.0 API at:
    https://www.waterqualitydata.us/beta/#mimeType=csv&providers=NWIS&providers=STORET
    or the Swagger documentation at:
    https://www.waterqualitydata.us/data/swagger-ui/index.html?docExpansion=none&url=/data/v3/api-docs#/

    Parameters
    ----------
    ssl_check : bool, optional
        Check the SSL certificate. Default is True.
    legacy : bool, optional
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get projects within a HUC region
        >>> df, md = dataretrieval.wqp.what_projects(huc="19")

    """

    kwargs = _check_kwargs(kwargs)

    url = _legacy_only_url("Project", legacy=legacy)

    response = query(url, payload=kwargs, delimiter=";", ssl_check=ssl_check)

    df = pd.read_csv(StringIO(response.text), delimiter=",", low_memory=False)

    return df, WQP_Metadata(response, legacy=legacy, ssl_check=ssl_check, **kwargs)


def what_activities(
    ssl_check=True,
    legacy=True,
    **kwargs,
) -> tuple[DataFrame, WQP_Metadata]:
    """Search WQP for activities within a region with specific data.

    Any WQP API parameter can be passed as a keyword argument to this function.
    More information about the API can be found at:
    https://www.waterqualitydata.us/#advanced=true
    or the beta version of the WQX3.0 API at:
    https://www.waterqualitydata.us/beta/#mimeType=csv&providers=NWIS&providers=STORET
    or the Swagger documentation at:
    https://www.waterqualitydata.us/data/swagger-ui/index.html?docExpansion=none&url=/data/v3/api-docs#/

    Parameters
    ----------
    ssl_check : bool, optional
        Check the SSL certificate. Default is True.
    legacy : bool, optional
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get activities within Washington D.C.
        >>> # during a specific time period
        >>> df, md = dataretrieval.wqp.what_activities(
        ...     statecode="US:11",
        ...     startDateLo="12-30-2019",
        ...     startDateHi="01-01-2020",
        ... )

        >>> # Get activities within Washington D.C.
        >>> # using the WQX3.0 profile during a specific time period
        >>> df, md = dataretrieval.wqp.what_activities(
        ...     legacy=False,
        ...     statecode="US:11",
        ...     startDateLo="12-30-2019",
        ...     startDateHi="01-01-2020",
        ... )
    """

    kwargs = _check_kwargs(kwargs)

    url = wqp_url("Activity") if legacy is True else wqx3_url("Activity")

    response = query(url, payload=kwargs, delimiter=";", ssl_check=ssl_check)

    df = pd.read_csv(StringIO(response.text), delimiter=",", low_memory=False)

    return df, WQP_Metadata(response, legacy=legacy, ssl_check=ssl_check, **kwargs)


def what_detection_limits(
    ssl_check=True,
    legacy=True,
    **kwargs,
) -> tuple[DataFrame, WQP_Metadata]:
    """Search WQP for result detection limits within a region with specific
    data.

    Any WQP API parameter can be passed as a keyword argument to this function.
    More information about the API can be found at:
    https://www.waterqualitydata.us/#advanced=true
    or the beta version of the WQX3.0 API at:
    https://www.waterqualitydata.us/beta/#mimeType=csv&providers=NWIS&providers=STORET
    or the Swagger documentation at:
    https://www.waterqualitydata.us/data/swagger-ui/index.html?docExpansion=none&url=/data/v3/api-docs#/

    Parameters
    ----------
    ssl_check : bool
        Check the SSL certificate. Default is True.
    legacy : bool
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get detection limits for Nitrite measurements in Rhode Island
        >>> # between specific dates
        >>> df, md = dataretrieval.wqp.what_detection_limits(
        ...     statecode="US:44",
        ...     characteristicName="Nitrite",
        ...     startDateLo="01-01-2021",
        ...     startDateHi="02-20-2021",
        ... )

    """

    kwargs = _check_kwargs(kwargs)

    url = _legacy_only_url("ResultDetectionQuantitationLimit", legacy=legacy)

    response = query(url, payload=kwargs, delimiter=";", ssl_check=ssl_check)

    df = pd.read_csv(StringIO(response.text), delimiter=",", low_memory=False)

    return df, WQP_Metadata(response, legacy=legacy, ssl_check=ssl_check, **kwargs)


def what_habitat_metrics(
    ssl_check=True,
    legacy=True,
    **kwargs,
) -> tuple[DataFrame, WQP_Metadata]:
    """Search WQP for habitat metrics within a region with specific data.

    Any WQP API parameter can be passed as a keyword argument to this function.
    More information about the API can be found at:
    https://www.waterqualitydata.us/#advanced=true
    or the beta version of the WQX3.0 API at:
    https://www.waterqualitydata.us/beta/#mimeType=csv&providers=NWIS&providers=STORET
    or the Swagger documentation at:
    https://www.waterqualitydata.us/data/swagger-ui/index.html?docExpansion=none&url=/data/v3/api-docs#/

    Parameters
    ----------
    ssl_check : bool
        Check the SSL certificate. Default is True.
    legacy : bool
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get habitat metrics for a state (Rhode Island in this case)
        >>> df, md = dataretrieval.wqp.what_habitat_metrics(statecode="US:44")

    """

    kwargs = _check_kwargs(kwargs)

    url = _legacy_only_url("BiologicalMetric", legacy=legacy)

    response = query(url, payload=kwargs, delimiter=";", ssl_check=ssl_check)

    df = pd.read_csv(StringIO(response.text), delimiter=",", low_memory=False)

    return df, WQP_Metadata(response, legacy=legacy, ssl_check=ssl_check, **kwargs)


def what_project_weights(ssl_check=True, legacy=True, **kwargs):
    """Search WQP for project weights within a region with specific data.

    Any WQP API parameter can be passed as a keyword argument to this function.
    More information about the API can be found at:
    https://www.waterqualitydata.us/#advanced=true
    or the beta version of the WQX3.0 API at:
    https://www.waterqualitydata.us/beta/#mimeType=csv&providers=NWIS&providers=STORET
    or the Swagger documentation at:
    https://www.waterqualitydata.us/data/swagger-ui/index.html?docExpansion=none&url=/data/v3/api-docs#/

    Parameters
    ----------
    ssl_check : bool
        Check the SSL certificate. Default is True.
    legacy : bool
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get project weights for a state (North Dakota in this case)
        >>> # within a set time period
        >>> df, md = dataretrieval.wqp.what_project_weights(
        ...     statecode="US:38",
        ...     startDateLo="01-01-2006",
        ...     startDateHi="01-01-2009",
        ... )

    """

    kwargs = _check_kwargs(kwargs)

    url = _legacy_only_url("ProjectMonitoringLocationWeighting", legacy=legacy)

    response = query(url, payload=kwargs, delimiter=";", ssl_check=ssl_check)

    df = pd.read_csv(StringIO(response.text), delimiter=",", low_memory=False)

    return df, WQP_Metadata(response, legacy=legacy, ssl_check=ssl_check, **kwargs)


def what_activity_metrics(ssl_check=True, legacy=True, **kwargs):
    """Search WQP for activity metrics within a region with specific data.

    Any WQP API parameter can be passed as a keyword argument to this function.
    More information about the API can be found at:
    https://www.waterqualitydata.us/#advanced=true
    or the beta version of the WQX3.0 API at:
    https://www.waterqualitydata.us/beta/#mimeType=csv&providers=NWIS&providers=STORET
    or the Swagger documentation at:
    https://www.waterqualitydata.us/data/swagger-ui/index.html?docExpansion=none&url=/data/v3/api-docs#/

    Parameters
    ----------
    ssl_check : bool
        Check the SSL certificate. Default is True.
    legacy : bool
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get activity metrics for a state (North Dakota in this case)
        >>> # within a set time period
        >>> df, md = dataretrieval.wqp.what_activity_metrics(
        ...     statecode="US:38",
        ...     startDateLo="07-01-2006",
        ...     startDateHi="12-01-2006",
        ... )

    """

    kwargs = _check_kwargs(kwargs)

    url = _legacy_only_url("ActivityMetric", legacy=legacy)

    response = query(url, payload=kwargs, delimiter=";", ssl_check=ssl_check)

    df = pd.read_csv(StringIO(response.text), delimiter=",", low_memory=False)

    return df, WQP_Metadata(response, legacy=legacy, ssl_check=ssl_check, **kwargs)


def wqp_url(service):
    """Construct the WQP URL for a given service."""

    base_url = "https://www.waterqualitydata.us/data/"
    _warn_legacy_use()

    if service not in services_legacy:
        raise ValueError(
            f"Legacy service not recognized. Valid options are {services_legacy}."
        )

    return f"{base_url}{service}/Search?"


def wqx3_url(service):
    """Construct the WQP URL for a given WQX 3.0 service."""

    base_url = "https://www.waterqualitydata.us/wqx3/"
    _warn_wqx3_use()

    if service not in services_wqx3:
        raise ValueError(
            f"WQX3.0 service not recognized. Valid options are {services_wqx3}."
        )

    return f"{base_url}{service}/search?"


class WQP_Metadata(BaseMetadata):
    """Metadata class for WQP service, derived from BaseMetadata.

    Attributes
    ----------
    url : str
        Response url
    query_time : datetime.timedelta
        Response elapsed time
    header : requests.structures.CaseInsensitiveDict
        Response headers
    comment : None
        Metadata comment. WQP does not return comments.
    site_info : tuple[pd.DataFrame, WQP_Metadata] | None
        Site information if the query included a site filter (`siteid`,
        `sites`, `site`, or `site_no`).
    """

    def __init__(
        self, response, legacy: bool = True, ssl_check: bool = True, **parameters
    ) -> None:
        """Generates a standard set of metadata informed by the response with specific
        metadata for WQP data.

        Parameters
        ----------
        response : Response
            Response object from requests module
        legacy : bool
            Whether the originating request used the legacy WQX endpoint.
            Forwarded to ``what_sites`` when ``site_info`` is accessed so the
            resolved station metadata uses the same profile as the original
            query.
        ssl_check : bool
            Whether the originating request verified SSL. Forwarded to
            ``what_sites`` for consistency.
        parameters : dict
            Unpacked dictionary of the remaining parameters supplied in the
            request.

        Returns
        -------
        md : :obj:`dataretrieval.wqp.WQP_Metadata`
            A ``dataretrieval`` custom :obj:`dataretrieval.wqp.WQP_Metadata` object.

        """

        super().__init__(response)

        self._parameters = parameters
        self._legacy = legacy
        self._ssl_check = ssl_check

    @cached_property
    def site_info(self):
        for key in _SITE_KEYS:
            if key in self._parameters:
                return what_sites(
                    siteid=self._parameters[key],
                    legacy=self._legacy,
                    ssl_check=self._ssl_check,
                )
        return None


def _check_kwargs(kwargs):
    """Private function to check kwargs for unsupported parameters."""
    mimetype = kwargs.get("mimeType")
    if mimetype == "geojson":
        raise NotImplementedError("GeoJSON not yet supported. Set 'mimeType=csv'.")
    elif mimetype != "csv" and mimetype is not None:
        raise ValueError("Invalid mimeType. Set 'mimeType=csv'.")
    else:
        kwargs["mimeType"] = "csv"

    return kwargs


def _warn_wqx3_use():
    message = (
        "Support for the WQX3.0 profiles is experimental. "
        "Queries may be slow or fail intermittently."
    )
    warnings.warn(message, UserWarning, stacklevel=2)


def _warn_legacy_use():
    message = (
        "This function call will return the legacy WQX format, "
        "which means USGS data have not been updated since March 2024. "
        "Please review the dataretrieval-python documentation for more "
        "information on updated WQX3.0 profiles. Setting `legacy=False` "
        "will remove this warning."
    )
    warnings.warn(message, DeprecationWarning, stacklevel=2)


def _warn_wqx3_unavailable():
    # stacklevel=3: warn -> _warn_wqx3_unavailable -> _legacy_only_url -> what_*
    warnings.warn(
        "WQX3.0 profile not available, returning legacy profile.",
        UserWarning,
        stacklevel=3,
    )


def _legacy_only_url(service: str, legacy: bool) -> str:
    """URL builder for WQP services that have no WQX3.0 equivalent.

    When ``legacy=False`` is passed to one of these helpers we emit a
    ``UserWarning`` explaining the fallback and *also* suppress the legacy
    ``DeprecationWarning`` that ``wqp_url`` would otherwise raise — its
    message claims setting ``legacy=False`` removes the warning, which is
    a lie for endpoints that have no WQX3.0 alternative.
    """
    with warnings.catch_warnings():
        if not legacy:
            _warn_wqx3_unavailable()
            warnings.simplefilter("ignore", DeprecationWarning)
        return wqp_url(service)
