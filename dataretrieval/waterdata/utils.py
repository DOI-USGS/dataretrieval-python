from __future__ import annotations

import copy
import functools
import json
import logging
import numbers
import os
import re
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    Mapping,
)
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta
from typing import Any, TypeVar, get_args
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
from anyio.from_thread import start_blocking_portal

from dataretrieval import __version__
from dataretrieval.utils import HTTPX_DEFAULTS, BaseMetadata
from dataretrieval.waterdata import _progress, chunking
from dataretrieval.waterdata.chunking import (
    _QUOTA_HEADER,
    RateLimited,
    ServiceUnavailable,
    _safe_elapsed,
    get_active_client,
)
from dataretrieval.waterdata.types import (
    PROFILE_LOOKUP,
    PROFILES,
    SERVICES,
)

try:
    import geopandas as gpd

    GEOPANDAS = True
except ImportError:
    GEOPANDAS = False

# Set up logger for this module
logger = logging.getLogger(__name__)

# Whether geopandas is present is a static, environment-level fact, so warn once
# here at import time rather than per query/chunk. That avoids the warning
# repeating on every call and avoids it interleaving with the progress line's
# carriage-return rewrites.
if not GEOPANDAS:
    logger.warning(
        "Geopandas not installed. Geometries will be flattened into pandas DataFrames."
    )

BASE_URL = "https://api.waterdata.usgs.gov"
OGC_API_VERSION = "v0"
OGC_API_URL = f"{BASE_URL}/ogcapi/{OGC_API_VERSION}"
SAMPLES_URL = f"{BASE_URL}/samples-data"
STATISTICS_API_VERSION = "v0"
STATISTICS_API_URL = f"{BASE_URL}/statistics/{STATISTICS_API_VERSION}"


def _switch_arg_id(ls: dict[str, Any], id_name: str, service: str):
    """
    Switch argument id from its package-specific identifier to the standardized "id" key
    that the API recognizes.

    Sets the "id" key in the provided dictionary `ls`
    with the value from either the service name or the expected id column name.
    If neither key exists, "id" will be set to None.

    Parameters
    ----------
    ls : Dict[str, Any]
        The dictionary containing identifier keys to be standardized.
    id_name : str
        The name of the specific identifier key to look for.
    service : str
        The service name.

    Returns
    -------
    Dict[str, Any]
        The modified dictionary with the "id" key set appropriately.

    Examples
    --------
    For service "time-series-metadata", the function will look for either
    "time_series_metadata_id" or "time_series_id" and change the key to simply
    "id".
    """

    service_id = service.replace("-", "_") + "_id"

    if "id" not in ls:
        if service_id in ls:
            ls["id"] = ls[service_id]
        elif id_name in ls:
            ls["id"] = ls[id_name]

    # Remove the original keys regardless of whether they were used
    ls.pop(service_id, None)
    ls.pop(id_name, None)

    return ls


def _switch_properties_id(properties: list[str] | None, id_name: str, service: str):
    """
    Switch properties id from its package-specific identifier to the
    standardized "id" key that the API recognizes.

    Sets the "id" key in the provided dictionary `ls` with the value from either
    the service name or the expected id column name. If neither key exists, "id"
    will be set to None.

    Parameters
    ----------
    properties : Optional[List[str]]
        A list containing the properties or column names to be pulled from the
        service, or None.
    id_name : str
        The name of the specific identifier key to look for.
    service : str
        The service name.

    Returns
    -------
    List[str]
        The modified list with the "id" key set appropriately.

    Examples
    --------
    For service "monitoring-locations", it will look for
    "monitoring_location_id" and change
    it to "id".
    """
    if not properties:
        return []
    service_id = service.replace("-", "_") + "_id"
    last_letter = service[-1]
    service_id_singular = ""
    if last_letter == "s":
        service_singular = service[:-1]
        service_id_singular = service_singular.replace("-", "_") + "_id"
    # Replace id fields with "id"
    id_fields = [service_id, service_id_singular, id_name]
    properties = ["id" if p in id_fields else p.replace("-", "_") for p in properties]
    # Remove unwanted fields
    return [p for p in properties if p not in ["geometry", service_id]]


_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)

# Anchored to ``[Pp]\d`` so a normal word containing ``p`` (e.g. ``"Apr"``)
# doesn't get mis-classified as an ISO 8601 duration; the optional ``T``
# admits time-only forms like ``PT36H``.
_DURATION_RE = re.compile(r"^[Pp]T?\d")

# OGC API parameters that carry a date/datetime value (single string,
# two-element range, or interval/duration string) rather than a multi-value
# string list. Used by ``_construct_api_requests`` to keep them out of the
# POST/CQL2 multi-value path and to route them through ``_format_api_dates``,
# and by ``_NO_NORMALIZE_PARAMS`` to bypass string-iterable normalization.
_DATE_RANGE_PARAMS = frozenset(
    {"datetime", "last_modified", "begin", "begin_utc", "end", "end_utc", "time"}
)

# Services that don't support comma-separated values for multi-value GET
# parameters and require POST with CQL2 JSON instead.
_CQL2_REQUIRED_SERVICES = frozenset({"monitoring-locations"})


def _parse_datetime(value: str) -> datetime | None:
    """Parse a single datetime string against the supported formats.

    Returns a ``datetime`` (tz-aware iff the input carried a UTC offset),
    or ``None`` if no format matched.
    """
    # ``datetime.strptime`` accepts a numeric offset like ``+00:00`` but not
    # the ``Z`` shorthand, so normalize trailing ``Z`` first.
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(candidate, fmt)
        except ValueError:
            continue
    return None


def _format_one(dt, *, date: bool, local_tz) -> str | None:
    """Format a single datetime element for inclusion in the API time arg."""
    if pd.isna(dt) or dt == "" or dt is None:
        return ".."
    parsed = _parse_datetime(dt)
    if parsed is None:
        return None
    if date:
        return parsed.strftime("%Y-%m-%d")
    aware = parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=local_tz)
    return aware.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_api_dates(
    datetime_input: str | list[str | None] | None, date: bool = False
) -> str | None:
    """
    Formats date or datetime input(s) for use with an API.

    Handles single values or ranges, and converting to ISO 8601 or date-only
    formats as needed.

    Parameters
    ----------
    datetime_input : Union[str, List[Optional[str]], None]
        A single date/datetime string or a list of one or two date/datetime
        strings. Accepts formats like "%Y-%m-%d %H:%M:%S", ISO 8601 (with or
        without ``Z``/numeric offset), or relative periods (e.g., "P7D" /
        "PT36H"). Range endpoints may be ``None``/``NaN``/empty to denote a
        half-bounded range.
    date : bool, optional
        If True, uses only the date portion ("YYYY-MM-DD"). If False (default),
        returns full datetime in UTC ISO 8601 format ("YYYY-MM-DDTHH:MM:SSZ").

    Returns
    -------
    Union[str, None]
        - If input is a single value, returns the formatted date/datetime string
        or None if parsing fails.
        - If input is a list of two values, returns a date/datetime range string
        separated by "/" (e.g., "YYYY-MM-DD/YYYY-MM-DD" or
        "YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ").
        - Returns None if input is empty, all NA, or cannot be parsed.

    Raises
    ------
    ValueError
        If `datetime_input` contains more than two values.

    Notes
    -----
    - A single blank/NA value returns None. In a two-value range, a blank/NA
    endpoint is rendered as ``".."`` to denote an open bound (e.g.
    ``"2024-01-01/.."``); the range is only None when *every* element is
    blank/NA or any non-NA element fails to parse.
    - Supports ISO 8601 durations such as "P7D" and "PT36H" and pre-formatted
    intervals containing ``"/"``; both are passed through unchanged.
    - Converts datetimes to UTC and formats as ISO 8601 with 'Z' suffix when
    `date` is False. Inputs with an explicit offset (``Z`` or ``+HH:MM``) are
    converted from that offset to UTC; naive inputs are interpreted in the
    local time zone for backwards compatibility.
    """
    if datetime_input is None:
        return None
    # Get timezone
    local_timezone = datetime.now().astimezone().tzinfo

    # Convert single string to list for uniform processing
    if isinstance(datetime_input, str):
        datetime_input = [datetime_input]
    elif isinstance(datetime_input, Mapping):
        # `list(mapping)` returns keys, which silently accepts the wrong shape.
        raise TypeError(
            f"date input must be a string or sequence of strings, "
            f"not {type(datetime_input).__name__}."
        )
    elif not isinstance(datetime_input, (list, tuple)):
        # Materialize any other iterable (pandas.Series, numpy.ndarray,
        # generator, ...) so the len()/subscript operations below work.
        datetime_input = list(datetime_input)

    # Check for null or all NA and return None
    if all(pd.isna(dt) or dt == "" or dt is None for dt in datetime_input):
        return None

    if len(datetime_input) > 2:
        raise ValueError("datetime_input should only include 1-2 values")

    # Pass through duration ("P7D", "PT36H") and pre-formatted interval ("a/b")
    # strings untouched.
    if len(datetime_input) == 1 and isinstance(datetime_input[0], str):
        single = datetime_input[0]
        if _DURATION_RE.match(single) or "/" in single:
            return single

    # Half-bounded ranges: NA endpoints render as ".."; any unparseable non-NA
    # element invalidates the range.
    formatted = [
        _format_one(dt, date=date, local_tz=local_timezone) for dt in datetime_input
    ]
    if any(f is None for f in formatted):
        return None
    return "/".join(formatted)


def _cql2_param(args: dict[str, Any]) -> str:
    """
    Convert query parameters to CQL2 JSON format for POST requests.

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of query parameters to convert to CQL2 format.

    Returns
    -------
    str
        Compact JSON string representation of the CQL2 query.

    Notes
    -----
    Serialized with the tightest separators (no indentation or
    whitespace). The body counts against the server's ~8 KB request-size
    limit and against :func:`chunking._request_bytes` when planning
    chunks, so every saved byte fits more values per POST: compact
    encoding roughly halves the per-value cost versus pretty-printing,
    which roughly doubles how many monitoring-location ids fit in one
    sub-request and so halves the chunk count for large id lists.
    """
    filters = []
    for key, values in args.items():
        filters.append({"op": "in", "args": [{"property": key}, values]})

    query = {"op": "and", "args": filters}

    return json.dumps(query, separators=(",", ":"))


def _default_headers():
    """
    Generate default HTTP headers for API requests.

    Returns
    -------
    dict
        A dictionary containing default headers including 'Accept-Encoding',
        'Accept', 'User-Agent', and 'lang'. If the environment variable
        'API_USGS_PAT' is set, its value is included as the 'X-Api-Key' header.
    """
    headers = {
        "Accept-Encoding": "compress, gzip",
        "Accept": "application/json",
        "User-Agent": f"python-dataretrieval/{__version__}",
        "lang": "en-US",
    }
    token = os.getenv("API_USGS_PAT")
    if token:
        headers["X-Api-Key"] = token
    return headers


def _check_ogc_requests(endpoint: str = "daily", req_type: str = "queryables"):
    """
    Sends an HTTP GET request to the specified OGC endpoint and request type,
    returning the JSON response.

    Parameters
    ----------
    endpoint : str, optional
        The OGC collection endpoint to query (default is "daily").
    req_type : str, optional
        The type of request to make. Must be either "queryables" or "schema"
        (default is "queryables").

    Returns
    -------
    dict
        The JSON response from the OGC endpoint.

    Raises
    ------
    ValueError
        If req_type is not "queryables" or "schema".
    RateLimited, ServiceUnavailable, RuntimeError
        From :func:`_raise_for_non_200` on any non-200 — same typed
        contract as the main data path so callers can use one
        ``except`` clause everywhere.
    """
    if req_type not in ("queryables", "schema"):
        raise ValueError(f"req_type must be 'queryables' or 'schema', got {req_type!r}")
    url = f"{OGC_API_URL}/collections/{endpoint}/{req_type}"
    resp = httpx.get(url, headers=_default_headers(), **HTTPX_DEFAULTS)
    _raise_for_non_200(resp)
    return resp.json()


def _error_body(resp: httpx.Response):
    """
    Build an informative error message from an HTTP response.

    Parameters
    ----------
    resp : httpx.Response
        The HTTP response object to extract the error message from.

    Returns
    -------
    str
        An error message string assembled per status code:

        * **429** — predefined message describing the rate-limit and pointing
          at the API-token path; the response body is not consulted.
        * **403** — predefined message describing the most common cause
          (query exceeding server limits); the response body is not
          consulted.
        * **other statuses** — attempts ``resp.json()`` and renders
          ``"<status>: <code>. <description>."`` from the JSON error
          envelope. If the body is not JSON (e.g. an HTML 502 from a
          gateway), falls back to ``"<status>: <reason>. <snippet>"`` with
          the first 200 characters of ``resp.text``; an empty body
          degrades to ``"<status>: <reason>."``.
    """
    status = resp.status_code
    if status == 429:
        return (
            "429: Too many requests made. Please obtain an API token "
            "or try again later."
        )
    elif status == 403:
        return (
            "403: Query request denied. Possible reasons include "
            "query exceeding server limits."
        )
    try:
        j_txt = resp.json()
    except ValueError:
        snippet = (resp.text or "").strip()[:200]
        reason = resp.reason_phrase or "Error"
        if snippet:
            return f"{status}: {reason}. {snippet}"
        return f"{status}: {reason}."
    return (
        f"{status}: {j_txt.get('code', 'Unknown type')}. "
        f"{j_txt.get('description', 'No description provided')}."
    )


def _parse_retry_after(value: str | None) -> float | None:
    """
    Parse a USGS ``Retry-After`` header into seconds.

    Parameters
    ----------
    value : str or None
        The raw header value, or ``None`` if absent.

    Returns
    -------
    float or None
        Non-negative delta-seconds, clamped at zero. ``None`` when the
        header is absent or unparseable; ``ChunkedCall`` treats
        ``None`` as "fall back to my own retry policy".

    Notes
    -----
    USGS sends ``Retry-After`` as integer delta-seconds (empirically
    verified — e.g. ``Retry-After: 2619``). The HTTP spec also allows
    HTTP-date form, but USGS doesn't use it, so this function doesn't
    bother parsing it.
    """
    if not value:
        return None
    try:
        return max(0.0, float(value.strip()))
    except ValueError:
        return None


def _raise_for_non_200(resp: httpx.Response) -> None:
    """
    Raise a typed exception for any non-200 response.

    Routes through :func:`_error_body` (USGS-API-aware: handles
    429/403 specially, extracts ``code``/``description`` from JSON
    error bodies) rather than ``Response.raise_for_status``, which
    raises ``HTTPStatusError`` with a generic message.

    Parameters
    ----------
    resp : httpx.Response
        The HTTP response to inspect.

    Raises
    ------
    RateLimited
        On HTTP 429 — typed so ``ChunkedCall`` can wrap as a resumable
        :class:`~dataretrieval.waterdata.chunking.QuotaExhausted`.
    ServiceUnavailable
        On HTTP 5xx — typed so ``ChunkedCall`` can wrap as a resumable
        :class:`~dataretrieval.waterdata.chunking.ServiceInterrupted`.
    RuntimeError
        On any other non-200 (4xx other than 429) — these are
        programmer errors that retry won't fix.
    """
    status = resp.status_code
    if status == 200:
        return
    body = _error_body(resp)
    retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
    if status == 429:
        raise RateLimited(body, retry_after=retry_after)
    if 500 <= status < 600:
        raise ServiceUnavailable(body, retry_after=retry_after)
    raise RuntimeError(body)


def _paginated_failure_message(pages_collected: int, cause: BaseException) -> str:
    """
    Build a user-facing message for a mid-pagination failure.

    The API exposes no resume cursor, so the caller's only recovery is
    to retry the whole call — the message lists the practical knobs,
    tailored to whether the failure was rate-limit (429) or something
    else.

    Parameters
    ----------
    pages_collected : int
        Number of pages successfully fetched before the failure.
    cause : BaseException
        The underlying exception that interrupted pagination.

    Returns
    -------
    str
        A message suitable for the ``RuntimeError`` that
        ``_walk_pages`` and ``get_stats_data`` raise from the
        original exception.
    """
    cause_str = str(cause).removesuffix(".")
    # Some ``httpx`` exceptions (e.g. ``TimeoutException()`` with no args)
    # stringify to empty; fall back to the class name so the
    # returned message is always informative.
    if not cause_str.strip():
        cause_str = type(cause).__name__
    if isinstance(cause, RateLimited):
        action = "wait for the rate-limit window to reset and retry"
    else:
        action = "retry the request (possibly after a short backoff)"
    return (
        f"Paginated request failed after collecting {pages_collected} "
        f"page(s): {cause_str}. To recover: {action}, reduce the "
        f"request size (e.g. fewer locations, a shorter time range, or "
        f"a smaller ``limit``), or obtain an API token."
    )


def _construct_api_requests(
    service: str,
    properties: list[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    skip_geometry: bool = False,
    **kwargs,
) -> httpx.Request:
    """
    Constructs an HTTP request object for the specified water data API service.

    For most services, list parameters are comma-joined and sent as a single
    GET request (e.g. ``parameter_code=["00060","00010"]`` becomes
    ``parameter_code=00060,00010`` in the URL). For services that do not
    support comma-separated values (currently only ``monitoring-locations``),
    a POST request with CQL2 JSON is used instead.

    Parameters
    ----------
    service : str
        The name of the API service to query (e.g., "daily").
    properties : Optional[List[str]], optional
        List of property names to include in the request.
    bbox : Optional[List[float]], optional
        Bounding box coordinates as a list of floats.
    limit : Optional[int], optional
        Maximum number of results to return per request.
    skip_geometry : bool, optional
        Whether to exclude geometry from the response (default is False).
    **kwargs
        Additional query parameters, including date/time filters and other
        API-specific options.

    Returns
    -------
    httpx.Request
        The constructed HTTP request object ready to be sent.

    Notes
    -----
    - Date/time parameters are automatically formatted to ISO8601.
    """
    service_url = f"{OGC_API_URL}/collections/{service}/items"

    # Format date/time parameters to ISO8601 first — both routing paths need it.
    for key in _DATE_RANGE_PARAMS:
        if key in kwargs:
            kwargs[key] = _format_api_dates(
                kwargs[key],
                date=(service == "daily" and key != "last_modified"),
            )

    if service in _CQL2_REQUIRED_SERVICES:
        # POST with CQL2 JSON: multi-value params go in the request body.
        # The date-range loop above has already collapsed any _DATE_RANGE_PARAMS
        # value to a string, so the list/tuple check below cannot match them.
        post_params = {
            k: v
            for k, v in kwargs.items()
            if isinstance(v, (list, tuple)) and len(v) > 1
        }
        params = {k: v for k, v in kwargs.items() if k not in post_params}
    else:
        # GET with comma-separated values: join list/tuple values into one string.
        post_params = {}
        params = {
            k: ",".join(str(x) for x in v) if isinstance(v, (list, tuple)) else v
            for k, v in kwargs.items()
        }

    params["skipGeometry"] = skip_geometry
    params["limit"] = 50000 if limit is None or limit > 50000 else limit

    # `len()` instead of truthiness: a numpy ndarray would raise on `if bbox:`.
    if bbox is not None and len(bbox) > 0:
        params["bbox"] = ",".join(map(str, bbox))
    if properties:
        params["properties"] = ",".join(properties)

    # Translate CQL filter Python names to the hyphenated URL parameter that
    # the OGC API expects. The Python kwarg is `filter_lang` because hyphens
    # aren't valid in Python identifiers.
    if "filter_lang" in params:
        params["filter-lang"] = params.pop("filter_lang")

    headers = _default_headers()

    if post_params:
        headers["Content-Type"] = "application/query-cql-json"
        return httpx.Request(
            method="POST",
            url=service_url,
            headers=headers,
            content=_cql2_param(post_params),
            params=params,
        )
    return httpx.Request(
        method="GET",
        url=service_url,
        headers=headers,
        params=params,
    )


def _next_req_url(
    resp: httpx.Response, *, body: dict[str, Any] | None = None
) -> str | None:
    """
    Extracts the URL for the next page of results from an HTTP response from a
    water data endpoint.

    Parameters
    ----------
    resp : httpx.Response
        The HTTP response object containing JSON data and headers.
    body : dict, optional
        Pre-parsed JSON body for ``resp``. When provided, skips the
        ``resp.json()`` call — useful when the caller has already
        decoded the body for its own use (avoids a second parse pass).

    Returns
    -------
    Optional[str]
        The URL for the next page of results if available, otherwise None.

    Notes
    -----
    - Returns None when the response carries no features.
    - Expects the response JSON to contain a "links" list with objects having
    "rel" and "href" keys.
    - Checks for the "next" relation in the "links" to determine the next URL.
    """
    if body is None:
        body = resp.json()
    if not body.get("numberReturned"):
        return None
    for link in body.get("links", []):
        if link.get("rel") != "next":
            continue
        href = link.get("href")
        if not href:
            return href
        # Refuse to follow a next-page link to a different host —
        # the request's headers/auth were minted for the original
        # host and shouldn't leak to whatever a poisoned response
        # body might supply. Guarded against mock-shaped ``resp.url``
        # attributes (tests sometimes set strings or ``MagicMock``)
        # by falling open when host extraction isn't reliable.
        try:
            next_host = httpx.URL(href).host
            resp_url = (
                resp.url
                if isinstance(resp.url, httpx.URL)
                else httpx.URL(str(resp.url))
            )
            cur_host = resp_url.host
        except (httpx.InvalidURL, TypeError):
            next_host = cur_host = None
        if next_host and cur_host and next_host != cur_host:
            raise RuntimeError(
                f"Refusing to follow cross-host next-page URL: "
                f"{next_host} != {cur_host}"
            )
        return href
    return None


def _get_resp_data(
    resp: httpx.Response,
    geopd: bool,
    *,
    body: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Extracts and normalizes data from an HTTP response containing GeoJSON features.

    Parameters
    ----------
    resp : httpx.Response
        The HTTP response object expected to contain a JSON body
        with a "features" key.
    geopd : bool
        Indicates whether geopandas is installed and should be used to
        handle geometries.
    body : dict, optional
        Pre-parsed JSON body for ``resp``. When provided, skips the
        ``resp.json()`` call — useful when the caller has already
        decoded the body for its own use (avoids a second parse pass).

    Returns
    -------
    gpd.GeoDataFrame or pd.DataFrame
        A ``GeoDataFrame`` when ``geopd`` is True; otherwise a plain
        ``DataFrame`` carrying the feature properties plus an ``id``
        column and a ``geometry`` column (coordinates list) where the
        response includes them. Returns an empty ``DataFrame`` when no
        features are returned.

    Notes
    -----
    The non-geopandas branch builds the frame directly from each
    feature's ``properties`` dict, plus the top-level ``id`` and
    ``geometry.coordinates`` columns — but adds the ``id`` and
    ``geometry`` columns only when at least one feature actually
    carries them. This skips the GeoJSON envelope entirely, so
    newly-added Feature-level fields (e.g. ``geometry.type`` after
    USGS migrated to full GeoJSON geometry objects) can't leak into
    the result frame; no reactive drop-list needs maintenance every
    time the upstream schema grows.
    """
    if body is None:
        body = resp.json()
    if not body.get("numberReturned"):
        # Preserve the GeoDataFrame type on empty short-circuit so a
        # downstream ``pd.concat([empty_page, geo_page])`` doesn't
        # downgrade the geopd-installed user's result to a plain
        # DataFrame (stripping geometry/CRS).
        return gpd.GeoDataFrame() if geopd else pd.DataFrame()

    # Defensive: a 200 with ``numberReturned > 0`` but missing
    # ``features`` is a real schema-drift shape (mirrors the guard in
    # ``_handle_stats_nesting``). Treat as empty rather than crash with
    # ``KeyError`` — the wrapped failure would otherwise look like a
    # transient transport error to ``_paginate``'s exception handler.
    features = body.get("features") or []
    if not features:
        return gpd.GeoDataFrame() if geopd else pd.DataFrame()

    if not geopd:
        df = pd.json_normalize([f.get("properties") or {} for f in features], sep="_")
        # Always materialize the ``id`` column (may be all-None) so
        # ``_arrange_cols``'s ``df.rename(columns={"id": output_id})``
        # produces the documented service-specific output_id column
        # (daily_id, channel_measurements_id, …) even if the upstream
        # response carried no feature-level id.
        df["id"] = [f.get("id") for f in features]
        geoms = [(f.get("geometry") or {}).get("coordinates") for f in features]
        if any(g is not None for g in geoms):
            df["geometry"] = geoms
        return df

    # Organize json into geodataframe and make sure id column comes along.
    df = gpd.GeoDataFrame.from_features(features)
    df["id"] = pd.json_normalize(features)["id"].values
    df = df[["id"] + [col for col in df.columns if col != "id"]]

    # If no geometry present, then return pandas dataframe. A geodataframe
    # is not needed.
    if df["geometry"].isnull().all():
        df = pd.DataFrame(df.drop(columns="geometry"))

    return df


@asynccontextmanager
async def _client_for(
    client: httpx.AsyncClient | None,
) -> AsyncIterator[httpx.AsyncClient]:
    """
    Yield a usable async client, picking the best available source.

    Resolution order:

    1. ``client`` if the caller supplied one (borrowed; not closed
       here — the caller owns its lifecycle).
    2. The chunker's shared async client if we're inside a
       :class:`~dataretrieval.waterdata.chunking.ChunkedCall` run (per
       :func:`chunking.get_active_client`). Borrowed; the chunker
       closes it on exit.
    3. A fresh short-lived ``httpx.AsyncClient`` opened here and closed
       on context exit.

    Parameters
    ----------
    client : httpx.AsyncClient or None
        A caller-owned client to borrow, or ``None`` to defer to the
        chunker's shared client or a temporary one.

    Yields
    ------
    httpx.AsyncClient
        The chosen client.
    """
    if client is not None:
        yield client
        return
    shared = get_active_client()
    if shared is not None:
        yield shared
        return
    async with httpx.AsyncClient(**HTTPX_DEFAULTS) as new:
        yield new


def _aggregate_paginated_response(
    initial: httpx.Response,
    last: httpx.Response,
    total_elapsed: timedelta,
) -> httpx.Response:
    """
    Build a single response covering a paginated call.

    Returns a shallow copy of ``initial`` with ``.headers`` set to the
    LAST page's (so downstream sees current ``x-ratelimit-remaining``)
    and ``.elapsed`` set to total wall-clock. The canonical
    ``initial.url`` is preserved (it's the user's original query).
    Both ``initial`` and ``last`` are left unmutated, mirroring the
    convention of
    :func:`dataretrieval.waterdata.chunking._combine_chunk_responses`.

    Parameters
    ----------
    initial : httpx.Response
        First-page response (the canonical one for ``md.url``).
    last : httpx.Response
        Last-page response — supplies the headers to copy over.
    total_elapsed : datetime.timedelta
        Cumulative wall-clock across every page, including ``initial``.

    Returns
    -------
    httpx.Response
        A shallow copy of ``initial`` with ``.headers`` set to a fresh
        ``httpx.Headers`` and ``.elapsed`` set to the cumulative
        wall-clock. ``initial.headers`` / ``initial.elapsed`` are
        never mutated, so callers holding a pre-pagination reference
        still see the original first-page values.
    """
    final = copy.copy(initial)
    final.headers = httpx.Headers(last.headers)
    final.elapsed = total_elapsed
    return final


_Cursor = TypeVar("_Cursor")

# Optional cap on the total rows a single paginated call accumulates before it
# stops following ``next`` links. ``None`` (the default the data getters use)
# means "no cap — fetch the whole series". Set via :func:`_row_cap` so the deep
# ``_paginate`` loop can honor it without threading the value through the
# generic chunker; this mirrors the ``_progress`` ambient-reporter pattern.
_row_cap_var: ContextVar[int | None] = ContextVar("waterdata_row_cap", default=None)


@contextmanager
def _row_cap(max_rows: int | None) -> Iterator[None]:
    """Cap the rows any :func:`_paginate` under this context will
    accumulate (``None`` = uncapped). Used by :func:`get_reference_table`
    to preview large tables without downloading every page."""
    token = _row_cap_var.set(max_rows)
    try:
        yield
    finally:
        _row_cap_var.reset(token)


async def _paginate(
    initial_req: httpx.Request,
    *,
    parse_response: Callable[[httpx.Response], tuple[pd.DataFrame, _Cursor | None]],
    follow_up: Callable[[_Cursor, httpx.AsyncClient], Awaitable[httpx.Response]],
    client: httpx.AsyncClient | None = None,
) -> tuple[pd.DataFrame, httpx.Response]:
    """
    Drive a paginated request to completion over an
    :class:`httpx.AsyncClient`.

    The common shape behind :func:`_walk_pages` and
    :func:`get_stats_data`: send the initial request, then loop calling
    ``follow_up`` until ``parse_response`` reports a ``None`` cursor,
    accumulating frames and elapsed time. Any mid-pagination failure
    raises ``RuntimeError`` wrapping the cause — the API exposes no
    resume cursor, so the caller's only recovery is to retry the whole
    call. Issuing HTTP asynchronously lets the multiple sub-requests of a
    chunked call run concurrently under
    :meth:`~dataretrieval.waterdata.chunking.ChunkedCall._run`.

    Parameters
    ----------
    initial_req : httpx.Request
        First-page request to send.
    parse_response : callable
        ``resp -> (df, next_cursor_or_None)``. Returns the page's
        DataFrame and the cursor (URL, token, …) used to drive
        ``follow_up`` for the next page; ``None`` terminates the loop.
    follow_up : callable
        ``(cursor, client) -> Awaitable[httpx.Response]``. Builds and
        sends the next-page request.
    client : httpx.AsyncClient, optional
        Caller-borrowed client. ``None`` (default) means use the
        chunker's shared client (if inside a chunked call) or open
        a temporary one.

    Returns
    -------
    df : pandas.DataFrame
        Concatenation of every page's parsed frame.
    response : httpx.Response
        A shallow copy of the first-page response, with ``.headers``
        rebuilt as a fresh ``httpx.Headers`` reflecting the last page and
        ``.elapsed`` set to cumulative wall-clock. The canonical URL is
        preserved from the first page. The original first-page response
        is not mutated.

    Raises
    ------
    RuntimeError
        On a non-200 initial response (typed
        :class:`~dataretrieval.waterdata.chunking.RateLimited` /
        :class:`~dataretrieval.waterdata.chunking.ServiceUnavailable`
        for 429/5xx, otherwise plain ``RuntimeError`` from
        :func:`_error_body`), on an initial-page parse failure
        (wrapped via :func:`_paginated_failure_message` with the
        original exception on ``__cause__``), or any failure on a
        subsequent page (same wrapping).
    httpx.HTTPError
        Network-level failures on the *initial* request (e.g.
        ``ConnectError``, ``TimeoutException``) propagate unmodified
        so callers can branch on the specific type; equivalent
        failures on subsequent pages are wrapped per above.
    """
    logger.debug("Requesting: %s", initial_req.url)
    reporter = _progress.current()
    async with _client_for(client) as sess:
        resp = await sess.send(initial_req)
        _raise_for_non_200(resp)
        initial_response = resp
        total_elapsed = _safe_elapsed(resp)

        try:
            df, cursor = parse_response(resp)
        except Exception as e:  # noqa: BLE001
            # Initial-page parse failures (malformed JSON, missing
            # ``features``, schema drift) get the same wrapped-message
            # treatment as follow-up failures so callers see a consistent
            # diagnostic regardless of which page broke.
            logger.warning("Initial response parse failed.")
            raise RuntimeError(_paginated_failure_message(0, e)) from e
        dfs = [df]
        # Stop following ``next`` links once the optional row cap is reached
        # (see :func:`_row_cap`); ``None`` means uncapped. The concatenation
        # is sliced to the cap below so a final over-budget page can't exceed it.
        cap = _row_cap_var.get()
        nrows = len(df)
        if reporter is not None:
            reporter.set_rate_remaining(
                resp.headers.get(_QUOTA_HEADER),
                limit=resp.headers.get("x-ratelimit-limit"),
            )
            reporter.add_page(rows=len(df))
        while cursor is not None and (cap is None or nrows < cap):
            try:
                resp = await follow_up(cursor, sess)
                _raise_for_non_200(resp)
                df, cursor = parse_response(resp)
                dfs.append(df)
                nrows += len(df)
                total_elapsed += _safe_elapsed(resp)
                if reporter is not None:
                    reporter.set_rate_remaining(
                        resp.headers.get(_QUOTA_HEADER),
                        limit=resp.headers.get("x-ratelimit-limit"),
                    )
                    reporter.add_page(rows=len(df))
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "Request failed at cursor %r. Data download interrupted.",
                    cursor,
                )
                raise RuntimeError(_paginated_failure_message(len(dfs), e)) from e

        # Aggregate headers / elapsed onto a COPY of the initial
        # response so the user's caller never sees an in-place
        # mutation of the response object they may have inspected
        # mid-pagination via a hook or test fixture.
        final_response = _aggregate_paginated_response(
            initial_response, resp, total_elapsed
        )
        result = pd.concat(dfs, ignore_index=True)
        if cap is not None:
            result = result.head(cap)
        return result, final_response


def _ogc_parse_response(
    resp: httpx.Response, *, geopd: bool
) -> tuple[pd.DataFrame, str | None]:
    """Parse one OGC API page: extract the DataFrame and the next-page URL.

    The parse strategy :func:`_walk_pages` hands to
    :func:`_paginate`. Coerces falsy cursors (empty href, etc.) to
    ``None`` so the paginate loop's ``while cursor is not None``
    terminates instead of spinning on a meaningless value.
    """
    body = resp.json()
    return (
        _get_resp_data(resp, geopd=geopd, body=body),
        _next_req_url(resp, body=body) or None,
    )


async def _walk_pages(
    geopd: bool,
    req: httpx.Request,
    client: httpx.AsyncClient | None = None,
) -> tuple[pd.DataFrame, httpx.Response]:
    """
    Iterate paginated OGC API responses asynchronously and aggregate
    them into one DataFrame.

    Thin wrapper that hands off to :func:`_paginate` with
    OGC-specific strategies: pages are parsed via :func:`_get_resp_data`
    (through :func:`_ogc_parse_response`) and the next-page cursor is the
    URL from the response's ``links`` array (per :func:`_next_req_url`).

    Parameters
    ----------
    geopd : bool
        Whether geopandas is installed (drives geometry handling).
    req : httpx.Request
        The initial HTTP request to send.
    client : httpx.AsyncClient, optional
        Caller-borrowed client; ``None`` defers client management to
        :func:`_paginate`.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the aggregated results from all pages.
    httpx.Response
        Aggregated response — initial-request URL (for query identity),
        final page's headers (so downstream sees current rate-limit
        state), and cumulative ``elapsed`` summed across pages.

    Raises
    ------
    RuntimeError
        See :func:`_paginate`.
    httpx.HTTPError
        See :func:`_paginate`.
    """
    method = req.method  # ``httpx.Request.method`` is already upper-cased.
    headers = req.headers
    content = req.content if method == "POST" else None

    async def follow_up(cursor: str, sess: httpx.AsyncClient) -> httpx.Response:
        return await sess.request(method, cursor, headers=headers, content=content)

    return await _paginate(
        req,
        parse_response=functools.partial(_ogc_parse_response, geopd=geopd),
        follow_up=follow_up,
        client=client,
    )


def _deal_with_empty(
    return_list: pd.DataFrame, properties: list[str] | None, service: str
) -> pd.DataFrame:
    """
    Handles empty DataFrame results by returning a DataFrame with appropriate columns.

    If `return_list` is empty, determines the column names to use:
        - If `properties` is not provided or contains only NaN values,
            retrieves schema properties from the specified service.
    - Otherwise, uses the provided `properties` list as column names.

    Parameters
    ----------
    return_list : pd.DataFrame
        The DataFrame to check for emptiness.
    properties : Optional[List[str]]
        List of property names to use as columns, or None.
    service : str
        The service endpoint to query for schema properties if needed.

    Returns
    -------
    pd.DataFrame
        The original DataFrame if not empty, otherwise an empty
        DataFrame with the appropriate columns.
    """
    if return_list.empty:
        if not properties or all(pd.isna(properties)):
            schema = _check_ogc_requests(endpoint=service, req_type="schema")
            properties = list(schema.get("properties", {}).keys())
        return pd.DataFrame(columns=properties)
    return return_list


def _arrange_cols(
    df: pd.DataFrame, properties: list[str] | None, output_id: str
) -> pd.DataFrame:
    """
    Rearranges and renames columns in a DataFrame based on provided
    properties and the service output id.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame whose columns are to be rearranged or renamed.
    properties : Optional[List[str]]
        A list of column names to possibly rename. If None or contains
        only NaN, the function renames 'id' to output_id.
    output_id : str
        The name to which the 'id' column should be renamed if applicable.

    Returns
    -------
    pd.DataFrame or gpd.GeoDataFrame
        The DataFrame with columns rearranged and/or renamed according
        to the specified properties and output_id.
    """

    # Rename id column to output_id
    df = df.rename(columns={"id": output_id})

    if properties and not all(pd.isna(properties)):
        # Don't alias the caller's list — we mutate below.
        local_properties = list(properties)
        if "geometry" in df.columns and "geometry" not in local_properties:
            local_properties.append("geometry")
        # 'id' is a valid service column, but expose it under the
        # service-specific output_id name instead.
        if "id" in local_properties:
            local_properties[local_properties.index("id")] = output_id
        df = df.loc[:, [col for col in local_properties if col in df.columns]]

    # Move meaningless-to-user, extra id columns to the end
    # of the dataframe, if they exist
    extra_id_col = set(df.columns).intersection(
        {
            "latest_continuous_id",
            "latest_daily_id",
            "daily_id",
            "continuous_id",
            "field_measurement_id",
        }
    )

    # If the arbitrary id column is returned (either due to properties
    # being none or NaN), then move it to the end of the dataframe, but
    # if part of properties, keep in requested order
    if extra_id_col and (properties is None or all(pd.isna(properties))):
        id_col_order = [col for col in df.columns if col not in extra_id_col] + list(
            extra_id_col
        )
        df = df.loc[:, id_col_order]

    return df


def _type_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Casts columns into appropriate types.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing water data.

    Returns
    -------
    pd.DataFrame
        The DataFrame with columns cast to appropriate types.

    """
    cols = set(df.columns)
    numerical_cols = [
        "altitude",
        "altitude_accuracy",
        "contributing_drainage_area",
        "drainage_area",
        "hole_constructed_depth",
        "value",
        "well_constructed_depth",
    ]
    time_cols = [
        "begin",
        "begin_utc",
        "construction_date",
        "end",
        "end_utc",
        "last_modified",
        "time",
    ]

    for col in cols.intersection(time_cols):
        df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in cols.intersection(numerical_cols):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _sort_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts rows by 'time' and 'monitoring_location_id' columns if they
    exist.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing water data.

    Returns
    -------
    pd.DataFrame
        The DataFrame with rows ordered by time and site.

    """
    if "time" in df.columns and "monitoring_location_id" in df.columns:
        df = df.sort_values(by=["time", "monitoring_location_id"], ignore_index=True)
    elif "time" in df.columns:
        df = df.sort_values(by="time", ignore_index=True)

    return df


def _finalize_ogc(
    frame: pd.DataFrame,
    response: httpx.Response,
    *,
    properties: list[str] | None,
    output_id: str,
    convert_type: bool,
    service: str,
    max_rows: int | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Shape a combined OGC result into the user-facing ``(df, md)``.

    The single home for the OGC getters' result shaping: empties
    normalized, types coerced (when ``convert_type``), the wire ``id``
    renamed and columns ordered, rows sorted, optionally truncated to
    ``max_rows``, and the response wrapped as
    :class:`~dataretrieval.utils.BaseMetadata`.

    Injected into the chunker as its ``finalize`` hook (see
    :data:`~dataretrieval.waterdata.chunking._Finalize`) so the
    un-interrupted return *and* a resumed ``ChunkInterrupted.call.resume()``
    produce the same shape — closing the gap where resume used to hand back
    the chunker's raw frame and bare ``httpx.Response``.

    ``max_rows`` is applied here (after dedup/sort, on the *combined* frame)
    rather than only per-sub-request, so a chunked call's total is bounded
    to exactly ``max_rows`` and a resumed call honors the cap too — the
    per-``_paginate`` ``_row_cap`` is only an early-stop download bound.
    """
    frame = _deal_with_empty(frame, properties, service)
    if convert_type:
        frame = _type_cols(frame)
    frame = _arrange_cols(frame, properties, output_id)
    frame = _sort_rows(frame)
    if max_rows is not None:
        frame = frame.head(max_rows)
    return frame, BaseMetadata(response)


def get_ogc_data(
    args: dict[str, Any],
    output_id: str,
    service: str,
    max_rows: int | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Retrieves OGC (Open Geospatial Consortium) data from a specified
    endpoint and returns it as a pandas DataFrame with metadata.

    This function prepares request arguments, constructs API requests,
    handles pagination, processes the results, and formats output
    according to the specified parameters.

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of request arguments for the OGC service.
    output_id : str
        The name of the output identifier to use in the request.
    service : str
        The OGC API collection name (e.g., ``"daily"``,
        ``"monitoring-locations"``, ``"continuous"``).
    max_rows : int, optional
        Stop paginating once this many rows have been collected and
        truncate the result to exactly ``max_rows``. ``None`` (default)
        fetches the full result. Intended for cheap previews of large,
        un-chunked tables (e.g. :func:`get_reference_table`).

    Returns
    -------
    pd.DataFrame or gpd.GeoDataFrame
        A DataFrame containing the retrieved and processed OGC data.
    BaseMetadata
        A metadata object containing request information including URL and query time.

    Notes
    -----
    - The function does not mutate the input `args` dictionary.
    - Handles optional arguments such as `convert_type`.
    - Applies column cleanup and reordering based on service and properties.
    """
    # Enforce a genuine positive integer: a float (even ``10.0``) or ``bool``
    # would pass a bare ``< 1`` check and then crash deep in
    # ``pd.DataFrame.head`` with an opaque ``TypeError`` after HTTP I/O has
    # already fired. ``numbers.Integral`` (not ``int``) so numpy integers —
    # e.g. ``max_rows`` derived from a numpy/pandas computation — are accepted;
    # ``bool`` is an ``Integral`` subtype, so exclude it explicitly.
    if max_rows is not None and (
        not isinstance(max_rows, numbers.Integral)
        or isinstance(max_rows, bool)
        or max_rows < 1
    ):
        raise ValueError(f"max_rows must be a positive integer (got {max_rows!r}).")

    args = args.copy()
    args["service"] = service
    args = _switch_arg_id(args, id_name=output_id, service=service)
    # Capture `properties` before the id-switch so post-processing sees
    # the user-facing names, not the wire-format ones.
    properties = args.get("properties")
    args["properties"] = _switch_properties_id(
        properties, id_name=output_id, service=service
    )
    convert_type = args.pop("convert_type", False)
    args = {k: v for k, v in args.items() if v is not None}

    # Post-processing is injected into the chunker rather than applied here,
    # so it runs on *every* exit: the normal return AND a later
    # ``exc.call.resume()`` after a ChunkInterrupted (which never re-enters
    # this function). ``_finalize_ogc`` is the single source of result shape;
    # it also applies ``max_rows`` to the *combined* frame so the cap is the
    # exact total even when the plan chunks or the call is resumed, while
    # ``_row_cap`` below only early-stops each sub-request's pagination.
    finalize = functools.partial(
        _finalize_ogc,
        properties=properties,
        output_id=output_id,
        convert_type=convert_type,
        service=service,
        max_rows=max_rows,
    )
    with _progress.progress_context(service=service), _row_cap(max_rows):
        return _fetch_once(args, finalize=finalize)


@chunking.multi_value_chunked(build_request=_construct_api_requests)
async def _fetch_once(
    args: dict[str, Any],
) -> tuple[pd.DataFrame, httpx.Response]:
    """Send one prepared-args OGC request asynchronously; return the
    frame + response.

    ``@chunking.multi_value_chunked`` models every multi-value list
    parameter and the cql-text filter as a chunkable axis, greedy-halves
    the biggest chunk across all axes until each sub-request URL fits,
    and iterates the cartesian product. With no chunkable inputs the
    decorator passes args through unchanged. The decorator gathers every
    sub-request over one shared :class:`httpx.AsyncClient` (concurrency
    bounded by the connection pool, sized from ``API_USGS_CONCURRENT``)
    and returns a *synchronous* wrapper, so ``get_ogc_data`` keeps calling
    ``_fetch_once(args, finalize=...)`` synchronously. The return shape is
    ``(frame, response)``.
    """
    req = _construct_api_requests(**args)
    return await _walk_pages(geopd=GEOPANDAS, req=req)


def _handle_stats_nesting(
    body: dict[str, Any],
    geopd: bool = False,
) -> pd.DataFrame:
    """
    Takes nested json from stats service and flattens into a dataframe with
    one row per monitoring location, parameter, and statistic.

    Parameters
    ----------
    body : Dict[str, Any]
        The JSON response body from the statistics service containing nested data.
    geopd : bool, optional
        Whether ``geopandas`` is available — when ``True`` the returned
        frame is a ``GeoDataFrame``; when ``False`` (default) a plain
        ``pd.DataFrame`` is returned with geometry flattened.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the flattened statistical data.

    Notes
    -----
    The non-geopandas branch uses the same schema-aware extraction as
    :func:`_get_resp_data`: it builds the per-feature outer frame
    directly from each feature's ``properties`` (minus the nested
    ``data`` field, which is unrolled separately below via the
    ``record_path`` json_normalize), then adds ``id`` and ``geometry``
    only when present. Skipping the GeoJSON envelope keeps newly-added
    fields like ``geometry.type`` from leaking into the result.
    """
    if body is None:
        return gpd.GeoDataFrame() if geopd else pd.DataFrame()

    # An empty (or missing) features list — a real mid-pagination
    # shape — would otherwise crash the downstream merge with
    # ``KeyError: 'monitoring_location_id'`` because neither df nor
    # dat would carry the merge key. Bail out with an empty frame —
    # ``GeoDataFrame`` when geopd is available so the eventual
    # ``pd.concat`` with non-empty geo pages doesn't downgrade to a
    # plain DataFrame and strip geometry/CRS.
    features = body.get("features") or []
    if not features:
        return gpd.GeoDataFrame() if geopd else pd.DataFrame()

    # The geopd-missing warning is emitted once at import (see top of module);
    # doing it here would log per page.
    if not geopd:
        outer_props = [
            {k: v for k, v in (f.get("properties") or {}).items() if k != "data"}
            for f in features
        ]
        df = pd.json_normalize(outer_props, sep=".")
        df.columns = df.columns.str.split(".").str[-1]
        # Stats features don't carry a top-level ``id`` field — the
        # geopandas branch (``GeoDataFrame.from_features``) doesn't
        # surface one either, so the non-geopd branch stays
        # consistent by NOT adding an id column.
        geoms = [(f.get("geometry") or {}).get("coordinates") for f in features]
        if any(g is not None for g in geoms):
            df["geometry"] = geoms
    else:
        df = gpd.GeoDataFrame.from_features(features).drop(
            columns=["data"], errors="ignore"
        )

    # Unnest json features, properties, data, and values while retaining necessary
    # metadata to merge with main dataframe.
    dat = pd.json_normalize(
        body,
        record_path=["features", "properties", "data", "values"],
        meta=[
            ["features", "properties", "monitoring_location_id"],
            ["features", "properties", "data", "parameter_code"],
            ["features", "properties", "data", "unit_of_measure"],
            ["features", "properties", "data", "parent_time_series_id"],
        ],
        meta_prefix="",
        errors="ignore",
    )
    dat.columns = dat.columns.str.split(".").str[-1]

    return df.merge(dat, on="monitoring_location_id", how="left")


def _expand_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes percentile value and thresholds columns containing lists
    of values and turns each list element into its own row in the
    original dataframe. 'nan's are removed from the dataframe. If
    no percentile data exist, it adds a percentile column and
    populates column with percentile assigned to min, max, and
    median.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe returned from using one of the statistics services.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the flattened percentile data.
    """
    if len(df) > 0:
        if "percentile" in df["computation"].unique():
            # Explode percentile lists into rows called "value" and "percentile"
            percentiles = df.loc[df["computation"] == "percentile"]
            percentiles_explode = percentiles[
                ["computation_id", "values", "percentiles"]
            ].explode(["values", "percentiles"], ignore_index=True)
            percentiles_explode = percentiles_explode.loc[
                percentiles_explode["values"] != "nan"
            ]
            percentiles_explode["value"] = pd.to_numeric(percentiles_explode["values"])
            percentiles_explode["percentile"] = pd.to_numeric(
                percentiles_explode["percentiles"]
            )
            percentiles_explode = percentiles_explode.drop(
                columns=["values", "percentiles"]
            )

            # Merge exploded values back to other metadata/geometry
            percentiles = percentiles.drop(
                columns=["values", "percentiles", "value"], errors="ignore"
            ).merge(percentiles_explode, on="computation_id", how="left")

            # Concatenate back to original
            dfs = pd.concat(
                [df.loc[df["computation"] != "percentile"], percentiles]
            ).drop(columns=["values", "percentiles"])
        else:
            dfs = df
            dfs["percentile"] = pd.NA

        # Give min, max, median a percentile value
        dfs.loc[dfs["computation"] == "maximum", "percentile"] = 100
        dfs.loc[dfs["computation"] == "minimum", "percentile"] = 0
        dfs.loc[dfs["computation"] == "median", "percentile"] = 50

        # Make sure numeric
        dfs["percentile"] = pd.to_numeric(dfs["percentile"])

        # Move percentile column
        cols = dfs.columns.tolist()
        cols.remove("percentile")
        col_index = cols.index("value") + 1
        cols.insert(col_index, "percentile")

        return dfs[cols]

    else:
        return df


def get_stats_data(
    args: dict[str, Any],
    service: str,
    expand_percentiles: bool,
    client: httpx.AsyncClient | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Retrieves statistical data from a specified endpoint and returns it
    as a pandas DataFrame with metadata.

    This function prepares request arguments, constructs API requests,
    handles pagination, processes results, and formats output according
    to the specified parameters.

    The stats path doesn't go through ``multi_value_chunked`` (its query
    shape has no chunkable list axes), so it drives :func:`_paginate`
    directly through an ``anyio`` blocking portal. The portal runs the
    pagination loop in a short-lived worker thread, so this works whether
    or not the caller is already inside an event loop.

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of request arguments for the statistics service.
    service : str
        The statistics service type (for example,
        "observationNormals" or "observationIntervals").
    expand_percentiles : bool
        Determines whether the percentiles column is expanded so that
        each percentile gets its own row in the returned dataframe. If
        True and user requests a computation_type other than
        percentiles, a percentile column is still returned.
    client : httpx.AsyncClient, optional
        Caller-borrowed async client. ``None`` (default) opens a
        temporary one inside the portal. Primarily a test seam.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the retrieved and processed statistical data.
    BaseMetadata
        A metadata object containing request information including URL and query time.
    """

    url = f"{STATISTICS_API_URL}/{service}"
    req = httpx.Request(
        method="GET",
        url=url,
        headers=_default_headers(),
        params=args,
    )
    method = req.method
    headers = req.headers

    def parse_response(resp: httpx.Response) -> tuple[pd.DataFrame, str | None]:
        body = resp.json()
        # Coerce falsy cursors ("", 0) to None so _paginate terminates.
        # USGS uses "next": null at end-of-stream, but defensive coerce
        # protects against any "" sentinel a future schema might use.
        return _handle_stats_nesting(body, geopd=GEOPANDAS), body.get("next") or None

    async def follow_up(cursor: str, sess: httpx.AsyncClient) -> httpx.Response:
        # Build a fresh params dict per page so the caller's ``args``
        # is never mutated.
        return await sess.request(
            method, url=url, params={**args, "next_token": cursor}, headers=headers
        )

    async def _run() -> tuple[pd.DataFrame, httpx.Response]:
        return await _paginate(
            req,
            parse_response=parse_response,
            follow_up=follow_up,
            client=client,
        )

    # The stats path opens its own progress context (it doesn't go through
    # ``multi_value_chunked``); ``_paginate`` reports pages/rate-limit
    # into it. The portal copies the calling context, so the reporter still
    # reaches the worker thread.
    with _progress.progress_context(service=service):
        with start_blocking_portal() as portal:
            df, response = portal.call(_run)

    if expand_percentiles:
        df = _expand_percentiles(df)
    return df, BaseMetadata(response)


def _check_profiles(
    service: SERVICES,
    profile: PROFILES,
) -> None:
    """Check whether a service profile is valid.

    Parameters
    ----------
    service : string
        One of the service names from the "services" list.
    profile : string
        One of the profile names from "results_profiles",
        "locations_profiles", "activities_profiles",
        "projects_profiles" or "organizations_profiles".
    """
    valid_services = get_args(SERVICES)
    if service not in valid_services:
        raise ValueError(
            f"Invalid service: '{service}'. Valid options are: {valid_services}."
        )

    valid_profiles = PROFILE_LOOKUP[service]
    if profile not in valid_profiles:
        raise ValueError(
            f"Invalid profile: '{profile}' for service '{service}'. "
            f"Valid options are: {valid_profiles}."
        )


_MONITORING_LOCATION_ID_RE = re.compile(r"[^-\s]+-[^-\s]+")


# Iterable-shaped params that ``_get_args`` must NOT push through
# ``_normalize_str_iterable`` (scalar non-string knobs are caught by runtime
# type, so only iterables with special handling need to be named here):
#   - date-range params may contain ``pd.NaT``/None or interval strings
#   - ``bbox``/``boundingBox`` are ``list[float]``, sometimes ``numpy.ndarray``
#   - ``get_peaks``'s int-valued filters (``water_year`` etc.) are ``list[int]``
#   - ``get_combined_metadata``'s ``thresholds`` is ``list[float]``
_NO_NORMALIZE_PARAMS = _DATE_RANGE_PARAMS | {
    "bbox",
    "boundingBox",
    "water_year",
    "year",
    "month",
    "day",
    "peak_since",
    "thresholds",
}


def _normalize_str_iterable(
    value: str | Iterable[str] | None,
    param_name: str = "value",
) -> str | list[str] | None:
    """Validate that ``value`` is None, a string, or an iterable of strings.

    Non-string iterables (``list``, ``tuple``, ``pandas.Series``,
    ``pandas.Index``, ``numpy.ndarray``, generators) are materialized to a
    ``list`` so downstream code that branches on ``isinstance(v, (list,
    tuple))`` keeps working. ``Mapping`` types are rejected because
    iterating a mapping yields keys, not values.

    Parameters
    ----------
    value : None, str, or iterable of str
    param_name : str, optional
        Used in error messages. Defaults to ``"value"``.

    Returns
    -------
    None, str, or list of str

    Raises
    ------
    TypeError
        If the input isn't ``None``, ``str``, or a non-``Mapping``
        iterable; or if any iterable element isn't a string.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping) or not isinstance(value, Iterable):
        raise TypeError(
            f"{param_name} must be a string or iterable of strings, "
            f"not {type(value).__name__} (got {value!r})."
        )
    values: list[str] = []
    for v in value:
        if not isinstance(v, str):
            raise TypeError(
                f"{param_name} elements must be strings, "
                f"not {type(v).__name__} (got {v!r})."
            )
        values.append(v)
    return values


def _check_monitoring_location_id(
    monitoring_location_id: str | Iterable[str] | None,
) -> str | list[str] | None:
    """Validate and normalize a ``monitoring_location_id`` value.

    Combines :func:`_normalize_str_iterable` with the AGENCY-ID format
    check that is unique to ``monitoring_location_id`` (the OGC spec
    requires a hyphen separator, e.g. ``USGS-01646500``).

    Parameters
    ----------
    monitoring_location_id : None, str, or iterable of str
        See :func:`_normalize_str_iterable`. Each string is additionally
        required to match the AGENCY-ID hyphen-separated format.

    Returns
    -------
    None, str, or list of str

    Raises
    ------
    TypeError
        If the input isn't ``None``, ``str``, or a non-``Mapping``
        iterable; or if any iterable element isn't a string.
    ValueError
        If any identifier doesn't contain a hyphen separator
        (per the OGC API spec: AGENCY-ID format, e.g. ``USGS-01646500``).
    """
    try:
        value = _normalize_str_iterable(
            monitoring_location_id, "monitoring_location_id"
        )
    except TypeError as exc:
        # Re-raise with the AGENCY-ID hint the generic helper doesn't carry.
        raise TypeError(
            f"{exc} Expected 'AGENCY-ID' format, e.g., 'USGS-01646500'."
        ) from None
    if value is None:
        return None
    for item in (value,) if isinstance(value, str) else value:
        _check_id_format(item)
    return value


def _check_id_format(value: str) -> None:
    """Raise ``ValueError`` if ``value`` is not in ``AGENCY-ID`` format."""
    if not _MONITORING_LOCATION_ID_RE.fullmatch(value):
        raise ValueError(
            f"Invalid monitoring_location_id: {value!r}. "
            f"Expected 'AGENCY-ID' format, e.g., 'USGS-01646500'."
        )


def _get_args(
    local_vars: dict[str, Any], exclude: set[str] | None = None
) -> dict[str, Any]:
    """
    Build the API-request kwargs dict from a getter's ``locals()``.

    Drops bookkeeping keys (``service``, ``output_id``, anything in
    ``exclude``) and ``None``-valued kwargs, then normalizes the
    remaining values:

    - ``monitoring_location_id`` is validated against the AGENCY-ID
      format (per :func:`_check_monitoring_location_id`).
    - ``properties`` is materialized to ``list[str]`` (a bare string
      gets wrapped in a single-element list so downstream
      ``",".join(properties)`` doesn't iterate per character).
    - Any other ``Iterable[str]`` that isn't in ``_NO_NORMALIZE_PARAMS``
      is materialized to ``list[str]`` via
      :func:`_normalize_str_iterable` so downstream code that branches
      on ``isinstance(v, (list, tuple))`` works for ``pandas.Series``,
      ``numpy.ndarray``, generators, etc.
    - Scalars, strings, and ``_NO_NORMALIZE_PARAMS`` values pass through
      unchanged.

    Parameters
    ----------
    local_vars : dict[str, Any]
        Dictionary of local variables, typically from ``locals()``.
    exclude : set[str], optional
        Additional keys to exclude from the resulting dictionary.

    Returns
    -------
    dict[str, Any]
        Filtered and normalized arguments for API requests.
    """
    to_exclude = {"service", "output_id"}
    if exclude:
        to_exclude.update(exclude)

    args: dict[str, Any] = {}
    for k, v in local_vars.items():
        if k in to_exclude or v is None:
            continue
        if k == "monitoring_location_id":
            args[k] = _check_monitoring_location_id(v)
        elif k == "properties":
            # `",".join(properties)` would iterate a bare string as characters.
            args[k] = [v] if isinstance(v, str) else _normalize_str_iterable(v, k)
        elif (
            k in _NO_NORMALIZE_PARAMS
            or isinstance(v, str)
            or not isinstance(v, Iterable)
        ):
            args[k] = v
        else:
            args[k] = _normalize_str_iterable(v, k)
    return args
