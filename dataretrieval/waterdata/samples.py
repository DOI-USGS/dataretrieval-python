"""Getters for the Aquarius Samples API, and its wire-parameter policy.

Discrete water-quality results, which come from a different upstream service
than the rest of Water Data -- with its own parameter spellings and its own
error envelope. The translation between this package's argument names and that
service's wire names lives here, next to the getters that need it.
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

from dataretrieval._querying import to_str
from dataretrieval._response_metadata import BaseMetadata
from dataretrieval._wqx import _attach_datetime_columns
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.transport.http import (
    HTTPX_DEFAULTS,
)
from dataretrieval.transport.http import (
    default_headers as _default_headers,
)
from dataretrieval.transport.http import (
    get as _get,
)
from dataretrieval.waterdata.endpoints import redirected
from dataretrieval.waterdata.types import (
    CODE_SERVICES,
    PROFILES,
    SERVICES,
    _check_profiles,
)
from dataretrieval.waterdata.utils import (
    SAMPLES_URL,
    _accept_legacy_kwargs,
    _get_args,
)

logger = logging.getLogger(__name__)


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

    # ``redirected`` applies a ``WaterdataSettings(base_url=...)`` from an
    # enclosing block; the Samples database is one of the four families that
    # move together when a caller redirects the adapter.
    url = (
        f"{redirected(SAMPLES_URL)}/codeservice/{code_service}"
        "?mimeType=application%2Fjson"
    )

    response = _get(url, headers=_default_headers(url), **HTTPX_DEFAULTS)

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
        headers=_default_headers(url),
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
    """Search the USGS Samples database for discrete water-quality results.

    Every available filter is exposed as an argument, but leave as many as
    feasible at their default of ``None``. An overcomplicated query can bog
    down the database's ability to assemble a result before it times out, so
    filtering narrowly is faster than filtering exhaustively.

    The web GUI for the Samples database is at
    https://waterdata.usgs.gov/download-samples/#dataProfile=site

    For more details on feasible query parameters, complete with examples, see
    the Samples database swagger docs at
    https://api.waterdata.usgs.gov/samples-data/docs#/

    Parameters
    ----------
    ssl_check : bool, optional
        Verify the server's SSL certificate.
    service : string
        One of the available Samples services: "results", "locations", "activities",
        "projects", or "organizations". Defaults to "results".
    profile : string
        One of the available profiles associated with a service. Options for
        each service are:

            * results - "fullphyschem", "basicphyschem", "fullbio", "basicbio",
              "narrow", "resultdetectionquantitationlimit", "labsampleprep",
              "count"
            * locations - "site", "count"
            * activities - "sampact", "actmetric", "actgroup", "count"
            * projects - "project", "projectmonitoringlocationweight"
            * organizations - "organization", "count"

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

    url = f"{redirected(SAMPLES_URL)}/{service}/{profile}"

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
        Verify the server's SSL certificate. Default is True.

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

    url = f"{redirected(SAMPLES_URL)}/summary/{quote(monitoring_location_id, safe='')}"
    params = {"mimeType": "text/csv"}

    df, response = _get_samples_csv(url, params, ssl_check)

    return df, BaseMetadata(response)


__all__ = ["get_codes", "get_samples", "get_samples_summary"]
