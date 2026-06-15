"""OGC API request construction.

Date/datetime formatting, CQL2 JSON serialization, the shared query-knob
builder, the typed GET/POST request builder (``_construct_api_requests``), the
verbatim-CQL2 builder (``_construct_cql_request``), next-page URL extraction,
and the queryables/schema probe (``_check_ogc_requests``).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, cast
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

from dataretrieval.ogc._constants import (
    _DATE_RANGE_PARAMS,
    _DATETIME_FORMATS,
    _DURATION_RE,
)
from dataretrieval.ogc._context import _dialect_var, _ogc_base_url_var
from dataretrieval.ogc._http import _default_headers, _raise_for_non_200
from dataretrieval.utils import HTTPX_DEFAULTS, _get


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


def _format_one(dt: str | None, *, date: bool) -> str | None:
    """Format a single datetime element for inclusion in the API time arg."""
    if pd.isna(dt) or dt == "" or dt is None:
        return ".."
    parsed = _parse_datetime(dt)
    if parsed is None:
        return None
    if date:
        return parsed.strftime("%Y-%m-%d")
    # Naive inputs are interpreted in the system local zone (for backwards
    # compatibility). Use ``.astimezone()`` rather than a fixed offset so each
    # value is resolved against the DST rules for ITS OWN date — a frozen
    # ``datetime.now()`` offset shifted off-season inputs by an hour.
    aware = parsed if parsed.tzinfo is not None else parsed.astimezone()
    return aware.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_api_dates(
    datetime_input: str | Sequence[str | None] | None, date: bool = False
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

    # element invalidates the range.
    formatted: list[str] = []
    for dt in datetime_input:
        one = _format_one(dt, date=date)
        if one is None:
            return None
        formatted.append(one)
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


def _ogc_query_params(
    params: dict[str, Any],
    *,
    properties: list[str] | None,
    bbox: list[float] | None,
    limit: int | None,
    skip_geometry: bool | None,
) -> dict[str, Any]:
    """Add the shared OGC query knobs to ``params`` (mutated in place).

    Factors out the ``skipGeometry``/``limit``/``bbox``/``properties`` block
    common to every OGC request so the typed getters
    (:func:`_construct_api_requests`) and the generalized CQL2 path
    (:func:`_construct_cql_request`) build identical URL parameters.

    ``skip_geometry=None`` leaves ``skipGeometry`` unset (the server defaults to
    including geometry); the typed getters always pass a bool, so their behavior
    is unchanged.
    """
    if skip_geometry is not None:
        params["skipGeometry"] = skip_geometry
    params["limit"] = 50000 if limit is None or limit > 50000 else limit
    # `len()` instead of truthiness: a numpy ndarray would raise on `if bbox:`.
    if bbox is not None and len(bbox) > 0:
        params["bbox"] = ",".join(map(str, bbox))
    if properties:
        params["properties"] = ",".join(properties)
    return params


def _construct_api_requests(
    service: str,
    properties: list[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    skip_geometry: bool = False,
    **kwargs: Any,
) -> httpx.Request:
    """
    Constructs an HTTP request object for the specified water data API service.

    For most services, list parameters are comma-joined and sent as a single
    GET request (e.g. ``parameter_code=["00060","00010"]`` becomes
    ``parameter_code=00060,00010`` in the URL). For services the active dialect
    flags as CQL2-only (``dialect.cql2_services``, e.g. the Water Data API's
    ``monitoring-locations``), a POST request with CQL2 JSON is used instead.

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
    service_url = f"{_ogc_base_url_var.get()}/collections/{service}/items"
    dialect = _dialect_var.get()

    # Format date/time parameters to ISO8601 first — both routing paths need it.
    for key in _DATE_RANGE_PARAMS:
        if key in kwargs:
            kwargs[key] = _format_api_dates(
                kwargs[key],
                date=(service in dialect.date_only_services and key != "last_modified"),
            )

    if service in dialect.cql2_services:
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
        # Skip empty lists/tuples so they're omitted rather than emitted as a
        # filterless ``&param=`` (which the server reads as "match empty").
        post_params = {}
        params = {
            k: ",".join(str(x) for x in v) if isinstance(v, (list, tuple)) else v
            for k, v in kwargs.items()
            if not (isinstance(v, (list, tuple)) and len(v) == 0)
        }

    _ogc_query_params(
        params,
        properties=properties,
        bbox=bbox,
        limit=limit,
        skip_geometry=skip_geometry,
    )

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


def _construct_cql_request(
    service: str,
    cql_body: str,
    *,
    properties: list[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    skip_geometry: bool | None = None,
) -> httpx.Request:
    """Build a POST/CQL2 request from a verbatim CQL2 body.

    The OGC-API counterpart to :func:`_construct_api_requests` for the
    generalized :func:`~dataretrieval.waterdata.api.get_cql` path: the
    caller supplies an already-serialized CQL2 JSON document (any predicate the
    grammar allows), sent unchanged as the request body, while
    ``properties``/``bbox``/``limit``/``skip_geometry`` go on the URL via the
    shared :func:`_ogc_query_params` — so a generalized query and an equivalent
    typed getter produce the same URL parameters.

    Parameters
    ----------
    service : str
        OGC collection name (e.g. ``"daily"``).
    cql_body : str
        Serialized CQL2 JSON document, sent as the POST body verbatim.
    properties, bbox, limit, skip_geometry
        See :func:`_ogc_query_params`. ``properties`` are wire-format
        (``id``-translated) names.

    Returns
    -------
    httpx.Request
        A POST request with ``Content-Type: application/query-cql-json``.
    """
    service_url = f"{_ogc_base_url_var.get()}/collections/{service}/items"
    params = _ogc_query_params(
        {},
        properties=properties,
        bbox=bbox,
        limit=limit,
        skip_geometry=skip_geometry,
    )
    headers = _default_headers()
    headers["Content-Type"] = "application/query-cql-json"
    return httpx.Request(
        method="POST",
        url=service_url,
        headers=headers,
        content=cql_body,
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
    # Stop paging when the response carries no features. Key off ``features``
    # rather than ``numberReturned``: the main Water Data API reports
    # ``numberReturned`` but the NGWMN OGC API omits it, so trusting it would
    # refuse to follow a ``next`` link on a page that actually carries
    # features (mirrors the same guard in :func:`_get_resp_data`).
    if not (body.get("features") or []):
        return None
    for link in body.get("links", []):
        if link.get("rel") != "next":
            continue
        href = link.get("href")
        if not href:
            return None
        # Refuse to follow a next-page link to a different host —
        # the request's headers/auth were minted for the original
        # host and shouldn't leak to whatever a poisoned response
        # body might supply. Guarded against mock-shaped ``resp.url``
        # attributes (tests sometimes set strings or ``MagicMock``)
        # by falling open when host extraction isn't reliable.
        next_host: str | None
        cur_host: str | None
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
        # ``href`` comes from the JSON ``links`` array (typed ``Any``); the
        # ``not href`` guard above already excluded empty/None, and it is a
        # URL string (passed to ``httpx.URL`` above).
        return cast("str", href)
    return None


def _check_ogc_requests(endpoint: str, req_type: str = "queryables") -> dict[str, Any]:
    """
    Sends an HTTP GET request to the specified OGC endpoint and request type,
    returning the JSON response.

    Parameters
    ----------
    endpoint : str
        The OGC collection endpoint to query (e.g. the service/collection id).
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
    DataRetrievalError
        From :func:`_raise_for_non_200` on any non-200 (the typed subclass for
        the status) — same typed contract as the main data path so callers can
        use one ``except`` clause everywhere.
    """
    if req_type not in ("queryables", "schema"):
        raise ValueError(f"req_type must be 'queryables' or 'schema', got {req_type!r}")
    url = f"{_ogc_base_url_var.get()}/collections/{endpoint}/{req_type}"
    resp = _get(url, headers=_default_headers(), **HTTPX_DEFAULTS)
    _raise_for_non_200(resp)
    # ``Response.json`` is typed ``Any``; the OGC queryables/schema endpoints
    # return a JSON object, and callers index it as a dict.
    return cast("dict[str, Any]", resp.json())
