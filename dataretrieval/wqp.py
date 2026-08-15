"""Download data from the Water Quality Portal (https://waterqualitydata.us).

See https://waterqualitydata.us/webservices_documentation for the API reference.

.. todo::

    - implement other services like Organization, Activity, etc.

"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from io import StringIO
from typing import TYPE_CHECKING, Any, ClassVar

import pandas as pd

from dataretrieval import configuration as _configuration
from dataretrieval._response_metadata import BaseMetadata
from dataretrieval._validation import require_one_of
from dataretrieval.configuration import (
    BaseConfiguration,
    _Redirectable,
    _register,
    _Retrying,
)
from dataretrieval.credentials import refuse_credential_keywords
from dataretrieval.exceptions import DataCurrencyWarning

from ._querying import _query_with_retry
from ._wqx import _attach_datetime_columns

__all__ = [
    "WqpConfiguration",
    "get_results",
    "what_sites",
    "what_organizations",
    "what_projects",
    "what_activities",
    "what_detection_limits",
    "what_habitat_metrics",
    "what_project_weights",
    "what_activity_metrics",
    "wqp_url",
    "wqx3_url",
    "WQP_Metadata",
]


if TYPE_CHECKING:
    import httpx
    from pandas import DataFrame


#: Root the Water Quality Portal serves both its interfaces from. Private
#: because the two builders below are the documented way to name a WQP URL;
#: this is only the piece they share, and the piece a redirect replaces.
_WQP_BASE_URL = "https://www.waterqualitydata.us"

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


def _is_code_column(name: str) -> bool:
    """Report whether a WQP column name denotes a code or identifier.

    Such columns (HUCs, parameter codes, FIPS codes) have leading zeros that
    are significant and must be preserved as ``str``. A name qualifies if it
    ends with "code" or contains "identifier", "huc", or "fips".
    """
    lname = name.lower()
    return lname.endswith("code") or any(
        token in lname for token in ("identifier", "huc", "fips")
    )


def _read_wqp_csv(text: str) -> DataFrame:
    """Read a WQP CSV, forcing code/identifier columns to ``str``.

    WQP returns codes with significant leading zeros — HUCs, parameter codes
    (``USGSpcode``), FIPS state/county codes. A bare ``read_csv`` infers those
    as int/float and silently drops the zeros (``"00060"`` -> ``60``, HUC8
    ``"07090002"`` -> ``7090002``). Read the header first, then re-read with
    ``dtype=str`` for every column that :func:`_is_code_column` flags, so the
    zeros survive.
    """
    columns = pd.read_csv(StringIO(text), delimiter=",", nrows=0).columns
    str_cols = {col: str for col in columns if _is_code_column(col)}
    return pd.read_csv(StringIO(text), delimiter=",", low_memory=False, dtype=str_cols)


def get_results(
    ssl_check: bool = True,
    legacy: bool = True,
    **kwargs: Any,
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
        Whether to check the SSL certificate. Default is True.
    legacy : bool, optional
        Return the legacy WQX data profile. Default is True.
    dataProfile : string, optional
        Data fields returned by the query.
        WQX3.0 profiles include 'fullPhysChem', 'narrow', and 'basicPhysChem'.
        Legacy profiles include 'resultPhysChem', 'biological', and
        'narrowResult'. For WQX3.0 queries (``legacy=False``), defaults to
        'fullPhysChem'; legacy queries have no default profile.
    siteid : string
        Monitoring location identifier: an agency code, a hyphen, and an
        identification number (Example: "USGS-05586100").
    statecode : string
        US state FIPS code (Example: Illinois is "US:17").
    countycode : string
        US county FIPS code.
    huc : string
        Eight-digit hydrologic unit (HUC), delimited by semicolons.
    bBox : string
        Search bounding box (Example: bBox=-92.8,44.2,-88.9,46.0).
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
        Date of the earliest desired data-collection activity,
        expressed as 'MM-DD-YYYY'.
    startDateHi : string
        Date of the last desired data-collection activity,
        expressed as 'MM-DD-YYYY'.
    characteristicName : string
        One or more case-sensitive characteristic names, separated by
        semicolons (https://www.waterqualitydata.us/public_srsnames/).
    mimeType : string
        Output format. Only 'csv' is supported at this time.

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query. For each
        ``<prefix>Date`` / ``<prefix>Time`` / ``<prefix>TimeZone`` triplet in
        the response (legacy WQP uses ``<prefix>Time/Time`` and
        ``<prefix>Time/TimeZoneCode``), an additional ``<prefix>DateTime``
        column is appended holding a UTC ``Timestamp``. Original triplet
        columns are preserved; unrecognized timezone codes yield ``NaT``.
        Rows are sorted by ``ActivityStartDateTime`` (or ``Activity_StartDateTime``
        for WQX3 responses) when present.
    md : :obj:`dataretrieval.wqp.WQP_Metadata`
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
    if profile is not None:
        require_one_of(
            profile, valid_profiles, name="dataProfile", context=f"{kind} results"
        )
    if legacy is not True and profile is None:
        kwargs["dataProfile"] = "fullPhysChem"

    response = _query_with_retry(
        url, kwargs, delimiter=";", ssl_check=ssl_check, adapter="wqp"
    )

    df = _read_wqp_csv(response.text)
    df = _attach_datetime_columns(df)
    return df, WQP_Metadata(response, **kwargs)


def _what(
    service: str,
    *,
    ssl_check: bool,
    legacy: bool,
    **kwargs: Any,
) -> tuple[DataFrame, WQP_Metadata]:
    """Shared implementation for the ``what_*`` metadata search functions.

    ``service`` is the WQP service name (e.g. ``"Station"``). Services with a
    WQX3.0 equivalent (those in :data:`services_wqx3`) use :func:`wqx3_url`
    when ``legacy=False`` and :func:`wqp_url` otherwise. Legacy-only services
    route through :func:`_legacy_only_url`, which warns and falls back to the
    legacy profile. The CSV response is parsed via :func:`_read_wqp_csv`.
    """
    kwargs = _check_kwargs(kwargs)

    if service in services_wqx3:
        url = wqp_url(service) if legacy else wqx3_url(service)
    else:
        url = _legacy_only_url(service, legacy=legacy)

    response = _query_with_retry(
        url, payload=kwargs, delimiter=";", ssl_check=ssl_check, adapter="wqp"
    )
    df = _read_wqp_csv(response.text)
    return df, WQP_Metadata(response, **kwargs)


def what_sites(
    ssl_check: bool = True,
    legacy: bool = True,
    **kwargs: Any,
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
        Whether to check the SSL certificate. Default is True.
    legacy : bool, optional
        If True, return the legacy WQX data profile and warn the user about
        the issues associated with it. If False, return the new WQX3.0
        profile when one is available. Defaults to True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.wqp.WQP_Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get sites within a radial distance of a point
        >>> df, md = dataretrieval.wqp.what_sites(
        ...     lat="44.2", long="-88.9", within="2.5"
        ... )

    """

    return _what("Station", ssl_check=ssl_check, legacy=legacy, **kwargs)


def what_organizations(
    ssl_check: bool = True,
    legacy: bool = True,
    **kwargs: Any,
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
        Whether to check the SSL certificate. Default is True.
    legacy : bool, optional
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.wqp.WQP_Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get all organizations in the WQP
        >>> df, md = dataretrieval.wqp.what_organizations()

    """

    return _what("Organization", ssl_check=ssl_check, legacy=legacy, **kwargs)


def what_projects(
    ssl_check: bool = True,
    legacy: bool = True,
    **kwargs: Any,
) -> tuple[DataFrame, WQP_Metadata]:
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
        Whether to check the SSL certificate. Default is True.
    legacy : bool, optional
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.wqp.WQP_Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get projects within a HUC region
        >>> df, md = dataretrieval.wqp.what_projects(huc="19")

    """

    return _what("Project", ssl_check=ssl_check, legacy=legacy, **kwargs)


def what_activities(
    ssl_check: bool = True,
    legacy: bool = True,
    **kwargs: Any,
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
        Whether to check the SSL certificate. Default is True.
    legacy : bool, optional
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.wqp.WQP_Metadata`
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

    return _what("Activity", ssl_check=ssl_check, legacy=legacy, **kwargs)


def what_detection_limits(
    ssl_check: bool = True,
    legacy: bool = True,
    **kwargs: Any,
) -> tuple[DataFrame, WQP_Metadata]:
    """Search WQP for result detection limits within a region with specific data.

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
        Whether to check the SSL certificate. Default is True.
    legacy : bool
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.wqp.WQP_Metadata`
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

    return _what(
        "ResultDetectionQuantitationLimit",
        ssl_check=ssl_check,
        legacy=legacy,
        **kwargs,
    )


def what_habitat_metrics(
    ssl_check: bool = True,
    legacy: bool = True,
    **kwargs: Any,
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
        Whether to check the SSL certificate. Default is True.
    legacy : bool
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.wqp.WQP_Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get habitat metrics for a state (Rhode Island in this case)
        >>> df, md = dataretrieval.wqp.what_habitat_metrics(statecode="US:44")

    """

    return _what("BiologicalMetric", ssl_check=ssl_check, legacy=legacy, **kwargs)


def what_project_weights(
    ssl_check: bool = True,
    legacy: bool = True,
    **kwargs: Any,
) -> tuple[DataFrame, WQP_Metadata]:
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
        Whether to check the SSL certificate. Default is True.
    legacy : bool
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.wqp.WQP_Metadata`
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

    return _what(
        "ProjectMonitoringLocationWeighting",
        ssl_check=ssl_check,
        legacy=legacy,
        **kwargs,
    )


def what_activity_metrics(
    ssl_check: bool = True,
    legacy: bool = True,
    **kwargs: Any,
) -> tuple[DataFrame, WQP_Metadata]:
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
        Whether to check the SSL certificate. Default is True.
    legacy : bool
        Return the legacy WQX data profile. Default is True.
    **kwargs : optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df : ``pandas.DataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.wqp.WQP_Metadata`
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

    return _what("ActivityMetric", ssl_check=ssl_check, legacy=legacy, **kwargs)


def _validate_service(service: str, valid_services: list[str], profile: str) -> None:
    """Validate a service against one WQP profile's supported endpoints."""
    require_one_of(service, valid_services, name="service", context=profile)


def _service_base() -> str:
    """The WQP root this call targets: a block's redirect, or the portal's own.

    The portal serves the legacy and WQX3 interfaces from one root under
    different paths, so a ``WqpConfiguration(base_url=...)`` names that root and
    both follow it. Redirecting only the interface a caller happened to use
    first would leave the other pointed at the service they were trying not to
    talk to. Resolved per call, because a ``configure`` block is scoped to a
    ``with`` statement.
    """
    return _configuration.base_url(adapter="wqp", default=_WQP_BASE_URL)


def wqp_url(service: str) -> str:
    """Construct the WQP URL for a given service."""

    _warn_legacy_use()
    _validate_service(service, services_legacy, "Legacy")
    return f"{_service_base()}/data/{service}/Search?"


def wqx3_url(service: str) -> str:
    """Construct the WQP URL for a given WQX 3.0 service."""

    _warn_wqx3_use()
    _validate_service(service, services_wqx3, "WQX3.0")
    return f"{_service_base()}/wqx3/{service}/search?"


class WQP_Metadata(BaseMetadata):
    """Metadata class for WQP service, derived from BaseMetadata.

    Attributes
    ----------
    url : str
        Response url
    query_time : datetime.timedelta
        Response elapsed time
    header : httpx.Headers
        Response headers
    comment : None
        WQP does not return comments.
    site_info : tuple[pd.DataFrame, WQP_Metadata] | None
        Site information (via ``what_sites``) if the query included a ``siteid``.
    """

    def __init__(self, response: httpx.Response, **parameters: Any) -> None:
        """Generate the standard metadata set, plus WQP-specific metadata.

        Parameters
        ----------
        response : ``httpx.Response``
            Response object from the ``httpx`` module.

        parameters : dict
            Unpacked dictionary of the parameters supplied in the request.

        """

        super().__init__(response)

        self._parameters = parameters

    @property
    def site_info(self) -> tuple[DataFrame, WQP_Metadata] | None:
        """Site information for the query.

        Populated (via :func:`dataretrieval.wqp.what_sites`) when the query
        included a ``siteid`` (the WQP site identifier, e.g.
        ``"USGS-05586100"``); ``None`` otherwise.

        Returns
        -------
        df : ``pandas.DataFrame``
            Site data returned by ``wqp.what_sites``.
        md : :obj:`dataretrieval.wqp.WQP_Metadata`
            A WQP_Metadata object.
        """
        siteid = self._parameters.get("siteid")
        if siteid is None:
            return None
        return what_sites(siteid=siteid)


def _check_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Check kwargs for unsupported parameters.

    Every WQP getter's ``**kwargs`` funnels through here on its way to the
    query payload, so this is the choke point where a credential-shaped name is
    refused. The predicate is the credentials leaf's, shared with Water Data's
    ``**queryables`` passthrough: ``api_key=`` is a plausible guess on any
    getter now that ``configure(Configuration(api_key=...))`` is the spelling,
    and this is the adapter with the widest passthrough -- ten getters, whose
    filter names the portal rather than this package defines.
    """
    refuse_credential_keywords(kwargs)

    mimetype = kwargs.get("mimeType")
    if mimetype == "geojson":
        raise NotImplementedError("GeoJSON not yet supported. Set 'mimeType=csv'.")
    elif mimetype != "csv" and mimetype is not None:
        raise ValueError("Invalid mimeType. Set 'mimeType=csv'.")
    else:
        kwargs["mimeType"] = "csv"

    return kwargs


def _warn_wqx3_use() -> None:
    message = (
        "Support for the WQX3.0 profiles is experimental. "
        "Queries may be slow or fail intermittently."
    )
    warnings.warn(message, UserWarning, stacklevel=2)


def _warn_legacy_use() -> None:
    message = (
        "This function call will return the legacy WQX format, "
        "which means USGS data have not been updated since March 2024. "
        "Please review the dataretrieval-python documentation for more "
        "information on updated WQX3.0 profiles. Setting `legacy=False` "
        "will remove this warning."
    )
    warnings.warn(message, DataCurrencyWarning, stacklevel=2)


def _warn_wqx3_unavailable() -> None:
    # stacklevel=4: warn -> _warn_wqx3_unavailable -> _legacy_only_url -> _what
    # -> what_*, so the warning is attributed to the public ``what_*`` call.
    warnings.warn(
        "WQX3.0 profile not available, returning legacy profile.",
        UserWarning,
        stacklevel=4,
    )


def _legacy_only_url(service: str, legacy: bool) -> str:
    """URL builder for WQP services that have no WQX3.0 equivalent.

    Passing ``legacy=False`` to one of these helpers emits a ``UserWarning``
    explaining the fallback and *also* suppresses the legacy
    :class:`~dataretrieval.exceptions.DataCurrencyWarning` that ``wqp_url``
    would otherwise raise. That warning's message claims setting
    ``legacy=False`` removes it, which is a lie for endpoints that have no
    WQX3.0 alternative.
    """
    with warnings.catch_warnings():
        if not legacy:
            _warn_wqx3_unavailable()
            warnings.simplefilter("ignore", DataCurrencyWarning)
        return wqp_url(service)


@dataclass(frozen=True)
class WqpConfiguration(_Redirectable, _Retrying, BaseConfiguration):
    """Settings for Water Quality Portal calls alone.

    No fan-out dials: a WQP query is answered by a single request, so a
    concurrency cap could only report a number nothing honours.

    Lives here rather than in :mod:`dataretrieval.configuration` because
    *which* settings a service reads is the service's own knowledge (ADR
    0011); what each of them means is shared, so the fields come from the
    setting groups declared beside their grammar.

    Parameters
    ----------
    retries : int, optional
        Retries attempted after a transient failure; ``0`` disables retrying.
    stall_timeout : float, optional
        Seconds a call may go without receiving any data before retrying
        stops.
    base_url : str, optional
        Root to send WQP requests to, instead of the portal's own. Both
        interfaces hang off it, so one value moves the legacy ``/data/``
        and the WQX3 ``/wqx3/`` paths together. Code only: the file and
        the environment refuse it.
    """

    # One request per call, so this service reads the retry dials and a
    # redirectable base and no fan-out dial. Each setting is declared once,
    # in :mod:`dataretrieval.configuration`, beside its grammar.
    adapter: ClassVar[str] = "wqp"


_register(WqpConfiguration)
