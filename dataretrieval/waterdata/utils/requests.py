"""Request construction for the Water Data internals.

Translates package-specific id arguments to the wire ``id`` name, formats
date/datetime arguments to ISO 8601, serializes CQL2 JSON bodies, and builds
the GET/POST :class:`httpx.Request` objects for both the typed getters
(:func:`_construct_api_requests`) and the generalized CQL2 path
(:func:`_construct_cql_request`). Depends on
:mod:`dataretrieval.waterdata.utils.constants` and
:mod:`dataretrieval.waterdata.utils.http`.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

from dataretrieval.waterdata.utils.constants import (
    _CQL2_REQUIRED_SERVICES,
    _DATE_RANGE_PARAMS,
    _DATETIME_FORMATS,
    _DURATION_RE,
    OGC_API_URL,
)
from dataretrieval.waterdata.utils.http import _default_headers


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


def _format_one(dt, *, date: bool) -> str | None:
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
    formatted = [_format_one(dt, date=date) for dt in datetime_input]
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
    service_url = f"{OGC_API_URL}/collections/{service}/items"
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
