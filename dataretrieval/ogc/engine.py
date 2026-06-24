"""Generic OGC API engine shared by the Water Data and NGWMN getters.

This module holds the API-agnostic core for talking to an OGC API Features
service — request construction (GET comma-joined or POST/CQL2), async
pagination, and the chunked fetch entry point :func:`get_ogc_data` that
orchestrates them. The surrounding concerns live in sibling modules it
composes, each with its own reason to change:
:mod:`~dataretrieval.ogc.dates` (time-parameter marshalling),
:mod:`~dataretrieval.ogc.errors` (HTTP error mapping), and
:mod:`~dataretrieval.ogc.shaping` (GeoJSON features to DataFrame and result
finalization). It is deliberately free of any Water-Data-specific constants
so a sibling package (e.g. NGWMN) can drive it without importing
``dataretrieval.waterdata``.

API-specific behavior is supplied by the caller:

* ``output_id`` — the user-facing column the wire ``id`` is renamed to,
  passed explicitly (no service map lives here).
* ``base_url`` — the OGC API base to target.
* ``extra_id_cols`` — synthetic id columns to push to the end of a result.
* ``dialect`` — an :class:`OgcDialect` describing which services need
  POST/CQL2 and which use date-only (vs. full datetime) time arguments.
"""

from __future__ import annotations

import functools
import json
import logging
import numbers
import re
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
)
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, TypeVar, cast

import httpx
import pandas as pd
from anyio.from_thread import start_blocking_portal

from dataretrieval.exceptions import DataRetrievalError
from dataretrieval.ogc import chunking
from dataretrieval.ogc import progress as _progress
from dataretrieval.ogc.chunking import get_active_client
from dataretrieval.ogc.dates import _DATE_RANGE_PARAMS, _format_api_dates
from dataretrieval.ogc.errors import _paginated_failure_message, _raise_for_non_200
from dataretrieval.ogc.planning import _QUOTA_HEADER, _merge_response, _safe_elapsed
from dataretrieval.ogc.shaping import GEOPANDAS, _finalize_ogc, _get_resp_data
from dataretrieval.utils import (
    HTTPX_DEFAULTS,
    Ambient,
    BaseMetadata,
    _default_headers,
    _get,
    _network_error,
)

# Set up logger for this module
logger = logging.getLogger(__name__)

BASE_URL = "https://api.waterdata.usgs.gov"
OGC_API_VERSION = "v0"
OGC_API_URL = f"{BASE_URL}/ogcapi/{OGC_API_VERSION}"


@dataclass(frozen=True)
class OgcDialect:
    """Per-API quirks the generic request builder needs to know about.

    Attributes
    ----------
    cql2_services : frozenset[str]
        Collections that don't accept comma-separated multi-value GET
        parameters and so must be queried via POST with a CQL2 JSON body.
    date_only_services : frozenset[str]
        Collections whose time arguments are rendered date-only
        (``YYYY-MM-DD``) rather than as a full UTC datetime. The
        ``last_modified`` parameter is always rendered as a full datetime
        regardless of this set.
    time_cols : frozenset[str]
        Result columns to coerce to datetime when ``convert_type`` is set.
        Empty by default, so the generic engine carries no API-specific
        column knowledge; each API supplies its own.
    numerical_cols : frozenset[str]
        Result columns to coerce to numeric when ``convert_type`` is set.
    sort_cols : tuple[str, ...]
        Columns to sort the combined result by, in priority order. Sorting
        is applied only when the first (primary) column is present; any
        later columns also present are added as secondary keys.
    """

    cql2_services: frozenset[str] = field(default_factory=frozenset)
    date_only_services: frozenset[str] = field(default_factory=frozenset)
    time_cols: frozenset[str] = field(default_factory=frozenset)
    numerical_cols: frozenset[str] = field(default_factory=frozenset)
    sort_cols: tuple[str, ...] = field(default_factory=tuple)


# Default dialect: a plain OGC API with no CQL2-only collections and no
# date-only collections (every time argument rendered as a full UTC datetime).
_DEFAULT_DIALECT = OgcDialect()


def _switch_arg_id(ls: dict[str, Any], id_name: str, service: str) -> dict[str, Any]:
    """
    Switch argument id from its package-specific identifier to the standardized "id" key
    that the API recognizes.

    If `ls` does not already have an "id" key, sets it from either the
    service-derived id key or the expected id column name. If neither key
    exists, "id" is left unset. The original service-specific id keys are
    removed regardless.

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


def _switch_properties_id(
    properties: list[str] | None, id_name: str, service: str
) -> list[str]:
    """
    Build the wire ``properties`` list, dropping every id alias and
    ``geometry``.

    The feature ``id`` is always returned and is renamed to the
    service-specific id column (e.g. ``daily_id``) in post-processing, so
    it must not be requested as a property: several collections (e.g.
    ``daily``, ``continuous``) reject ``id`` in ``properties`` with an
    HTTP 400. ``geometry`` is likewise excluded because it is controlled
    by ``skip_geometry``. Any service-specific id name (``daily_id``,
    ``monitoring_location_id``, …) and the bare ``id`` are dropped, and
    remaining hyphens are normalized to underscores. Returns an empty
    list when `properties` is empty or None — the URL then omits the
    ``properties`` filter and the result is shaped by :func:`_arrange_cols`.

    Parameters
    ----------
    properties : Optional[List[str]]
        A list containing the properties or column names to be pulled from the
        service, or None.
    id_name : str
        The service-specific id column name to drop (e.g. ``daily_id``).
    service : str
        The service name.

    Returns
    -------
    List[str]
        The wire ``properties`` with id aliases and ``geometry`` removed
        and hyphens normalized.

    Examples
    --------
    For service "daily" with ``properties=["daily_id", "value", "geometry"]``,
    returns ``["value"]`` — ``daily_id`` and ``geometry`` are dropped, while
    the ``daily_id`` column still appears in the result, renamed from the
    always-returned feature ``id``.
    """
    if not properties:
        return []
    service_id = service.replace("-", "_") + "_id"
    # The feature ``id`` always comes back (renamed to the service id
    # downstream) and several collections reject it as a selectable
    # property; ``geometry`` is controlled by ``skip_geometry``. Drop both,
    # plus the service-specific id column (``id_name``) and the name derived
    # straight from the service (``service_id``).
    drop = {"id", "geometry", id_name, service_id}
    normalized = (p.replace("-", "_") for p in properties)
    return [p for p in normalized if p not in drop]


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
    limit and against :func:`planning._request_bytes` when planning
    chunks, so every saved byte fits more values per POST: compact
    encoding roughly halves the per-value cost versus pretty-printing,
    which roughly doubles how many monitoring-location ids fit in one
    sub-request and so halves the chunk count for large id lists.
    """
    query = {
        "op": "and",
        "args": [
            {"op": "in", "args": [{"property": key}, values]}
            for key, values in args.items()
        ],
    }
    return json.dumps(query, separators=(",", ":"))


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
    url = f"{_ogc_base_url.get()}/collections/{endpoint}/{req_type}"
    resp = _get(url, headers=_default_headers(), **HTTPX_DEFAULTS)
    _raise_for_non_200(resp)
    # ``Response.json`` is typed ``Any``; the OGC queryables/schema endpoints
    # return a JSON object, and callers index it as a dict.
    return cast("dict[str, Any]", resp.json())


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
    skip_geometry: bool | None = None,
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
    service_url = f"{_ogc_base_url.get()}/collections/{service}/items"
    dialect = _dialect.get()

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
    service_url = f"{_ogc_base_url.get()}/collections/{service}/items"
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
       :class:`~dataretrieval.ogc.chunking.ChunkedCall` run (per
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


_Cursor = TypeVar("_Cursor")

# Ambient per-call state the generic chunker would otherwise have to thread
# through to the deep request builder / paginate loop. Each is read with
# ``.get()`` and scoped with ``with _x(value):``; the defaults leave every
# existing getter unaffected. (Mirrors the ``_progress`` ambient-reporter.)

# Optional cap on the rows one paginated call accumulates before it stops
# following ``next`` links (``None`` = uncapped). Set by :func:`get_reference_table`
# to preview large tables without downloading every page.
_row_cap: Ambient[int | None] = Ambient("ogc_row_cap", None)

# OGC base URL the shared request builder (:func:`_construct_api_requests`)
# targets — the main Water Data API or, for NGWMN collections, their own base.
_ogc_base_url: Ambient[str] = Ambient("ogc_base_url", OGC_API_URL)

# Per-call OGC dialect the request builder reads for CQL2-vs-GET routing and
# date-only formatting (default: a plain OGC API).
_dialect: Ambient[OgcDialect] = Ambient("ogc_dialect", _DEFAULT_DIALECT)


async def _paginate(
    initial_req: httpx.Request,
    *,
    parse_response: Callable[[httpx.Response], tuple[pd.DataFrame, _Cursor | None]],
    follow_up: Callable[[_Cursor, httpx.AsyncClient], Awaitable[httpx.Response]],
    client: httpx.AsyncClient | None = None,
    raise_for_status: Callable[[httpx.Response], None] = _raise_for_non_200,
) -> tuple[pd.DataFrame, httpx.Response]:
    """
    Drive a paginated request to completion over an
    :class:`httpx.AsyncClient`.

    The common shape behind the paginated fetch paths (e.g.
    :func:`_walk_pages`): send the initial request, then loop calling
    ``follow_up`` until ``parse_response`` reports a ``None`` cursor,
    accumulating frames and elapsed time. Any mid-pagination failure
    raises ``DataRetrievalError`` wrapping the cause — the API exposes no
    resume cursor, so the caller's only recovery is to retry the whole
    call. Issuing HTTP asynchronously lets the multiple sub-requests of a
    chunked call run concurrently under
    :meth:`~dataretrieval.ogc.chunking.ChunkedCall._run`.

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
    raise_for_status : callable, optional
        ``resp -> None``; raises the typed error for a non-OK response.
        Defaults to :func:`_raise_for_non_200` (the OGC ``{code, description}``
        envelope); wateruse passes its own to surface the NWDC ``detail``.

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
    DataRetrievalError
        On a non-200 initial response, the typed subclass for the status from
        :func:`_raise_for_non_200` (a
        :class:`~dataretrieval.exceptions.TransientError` for a retryable
        429 / 5xx, otherwise a fatal :class:`~dataretrieval.exceptions.HTTPError`);
        or, on an initial-page parse failure or any subsequent-page failure, a
        base ``DataRetrievalError`` wrapping the cause (built by
        :func:`_paginated_failure_message`, original exception on ``__cause__``).
    httpx.HTTPError
        Network-level failures on the *initial* request (e.g.
        ``ConnectError``, ``TimeoutException``) propagate unmodified
        so callers can branch on the specific type; equivalent
        failures on subsequent pages are wrapped per above.
    """
    logger.debug("Requesting: %s", initial_req.url)
    reporter = _progress.current()

    def report_page(page: httpx.Response, frame: pd.DataFrame) -> None:
        """Tick the ambient progress reporter (a no-op when unset) for one page."""
        if reporter is not None:
            reporter.set_rate_remaining(
                page.headers.get(_QUOTA_HEADER),
                limit=page.headers.get("x-ratelimit-limit"),
            )
            reporter.add_page(rows=len(frame))

    async with _client_for(client) as sess:
        resp = await sess.send(initial_req)
        raise_for_status(resp)
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
            raise DataRetrievalError(_paginated_failure_message(0, e)) from e
        dfs = [df]
        # Stop following ``next`` links once the optional row cap is reached
        # (see :func:`_row_cap`); ``None`` means uncapped. The concatenation
        # is sliced to the cap below so a final over-budget page can't exceed it.
        cap = _row_cap.get()
        nrows = len(df)
        # Guard a non-advancing or cyclic cursor (a server bug that would
        # otherwise loop forever). OGC's next-URLs are unique, so this never
        # fires for them; the Link-header pagers (e.g. wateruse) rely on it.
        seen: set[Any] = set()
        report_page(resp, df)
        while (
            cursor is not None and cursor not in seen and (cap is None or nrows < cap)
        ):
            seen.add(cursor)
            try:
                resp = await follow_up(cursor, sess)
                raise_for_status(resp)
                df, cursor = parse_response(resp)
                dfs.append(df)
                nrows += len(df)
                total_elapsed += _safe_elapsed(resp)
                report_page(resp, df)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "Request failed at cursor %r. Data download interrupted.",
                    cursor,
                )
                raise DataRetrievalError(_paginated_failure_message(len(dfs), e)) from e

        # Fold the pages onto a COPY of the initial response so a caller that
        # inspected it mid-pagination (a hook, a test fixture) never sees an
        # in-place mutation. ``resp`` is the last page, whose headers carry the
        # current ``x-ratelimit-remaining`` (monotonic, so the last page is the
        # most depleted) — the same low-level merge the fan-out aggregation uses.
        final_response = _merge_response(
            initial_response, headers_from=resp, elapsed=total_elapsed
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
    DataRetrievalError
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


def get_ogc_data(
    args: dict[str, Any],
    service: str,
    output_id: str,
    *,
    max_rows: int | None = None,
    base_url: str = OGC_API_URL,
    extra_id_cols: frozenset[str] | set[str] = frozenset(),
    dialect: OgcDialect | None = None,
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
    service : str
        The OGC API collection name (e.g., ``"daily"``,
        ``"monitoring-locations"``, ``"continuous"``).
    output_id : str
        The user-facing id column the wire ``id`` is renamed to. Required —
        the per-API service-to-id map lives in the caller, not here.
    max_rows : int, optional
        Stop paginating once this many rows have been collected and
        truncate the result to exactly ``max_rows``. ``None`` (default)
        fetches the full result. Intended for cheap previews of large,
        un-chunked tables (e.g. :func:`get_reference_table`).
    base_url : str, optional
        OGC API base URL to target. Defaults to the main Water Data API.
    extra_id_cols : set or frozenset, optional
        Synthetic id columns to push to the end of a result frame (see
        :func:`_arrange_cols`). Defaults to an empty set.
    dialect : OgcDialect, optional
        Per-API request quirks (CQL2-only services, date-only services).
        Defaults to a plain OGC API with neither.

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

    if dialect is None:
        dialect = _DEFAULT_DIALECT

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
        extra_id_cols=extra_id_cols,
        dialect=dialect,
    )
    with _progress.progress_context(service=service), _row_cap(max_rows):
        with _ogc_base_url(base_url), _dialect(dialect):
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
    bounded by a semaphore, sized from ``API_USGS_CONCURRENT``)
    and returns a *synchronous* wrapper, so ``get_ogc_data`` keeps calling
    ``_fetch_once(args, finalize=...)`` synchronously. The return shape is
    ``(frame, response)``.
    """
    req = _construct_api_requests(**args)
    return await _walk_pages(geopd=GEOPANDAS, req=req)


def _run_sync(
    make_coro: Callable[[], Awaitable[tuple[pd.DataFrame, httpx.Response]]],
    *,
    service: str,
    error_url: str | httpx.URL | None = None,
) -> tuple[pd.DataFrame, httpx.Response]:
    """Drive an async OGC fetch to completion from synchronous code.

    Opens the service progress context and runs ``make_coro()`` through a
    short-lived ``anyio`` blocking portal (a worker thread), so the
    non-chunked getters work whether or not the caller is already inside an
    event loop (Jupyter/async apps). The portal copies the calling context,
    so the active progress reporter still reaches the sub-requests.

    Shared by the non-chunked fetch paths; the chunked OGC getters
    drive their own portal
    inside :meth:`chunking.ChunkedCall.resume`.

    A connection failure on the initial request is surfaced as a typed
    ``NetworkError`` against ``error_url`` when given (callers that build their
    own requests, e.g. ``wateruse``), else the request-builder base the caller
    scoped via ``_ogc_base_url`` (the OGC / NGWMN getters).
    """
    with _progress.progress_context(service=service):
        with start_blocking_portal() as portal:
            try:
                # ``portal.call`` is ``Any`` (anyio is skipped by mypy — its
                # source uses 3.10 syntax our 3.9 target can't parse), so cast
                # to the declared return type, as ``ChunkedCall`` does too.
                return cast(
                    "tuple[pd.DataFrame, httpx.Response]", portal.call(make_coro)
                )
            except httpx.TransportError as exc:
                # The initial-request connection failure ``_paginate`` lets
                # through raw; mid-pagination failures are already typed.
                # Report the base URL actually targeted: callers that build
                # their own requests (``wateruse``) pass ``error_url``; the OGC
                # getters leave it unset and fall back to the request-builder
                # base they scoped via ``_ogc_base_url`` (NGWMN/sibling APIs set
                # their own), not a hardcoded host.
                raise _network_error(
                    error_url if error_url is not None else _ogc_base_url.get(),
                    exc,
                ) from exc


# ``AGENCY-ID``: a hyphen-separated agency prefix and local id. The local id
# may itself contain hyphens (``\S+`` after the first separator) — NGWMN
# aggregates many non-USGS agencies whose local ids aren't bare digits, so
# only the agency prefix is constrained to be hyphen/space-free.
_MONITORING_LOCATION_ID_RE = re.compile(r"[^-\s]+-\S+")

# Default set of iterable-shaped params that ``_get_args`` must NOT push
# through ``_normalize_str_iterable`` (date-range params may carry
# ``pd.NaT``/None or interval strings; ``bbox`` is ``list[float]``). Callers
# with extra numeric params (e.g. the Water Data API's ``water_year``,
# ``thresholds``) pass their own superset.
_NO_NORMALIZE_PARAMS = _DATE_RANGE_PARAMS | {"bbox"}


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


def _as_str_list(
    value: str | Iterable[str] | None,
    param_name: str = "value",
) -> list[str] | None:
    """Normalize ``value`` to ``list[str]`` (``None`` passes through).

    Wraps a bare ``str`` in a single-element list — so a later
    ``",".join(...)`` doesn't iterate it character-by-character — and
    materializes any other iterable via :func:`_normalize_str_iterable`.
    """
    normalized = _normalize_str_iterable(value, param_name)
    if isinstance(normalized, str):
        return [normalized]
    return normalized


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
        if not _MONITORING_LOCATION_ID_RE.fullmatch(item):
            raise ValueError(
                f"Invalid monitoring_location_id: {item!r}. "
                f"Expected 'AGENCY-ID' format, e.g., 'USGS-01646500'."
            )
    return value


def _get_args(
    local_vars: dict[str, Any],
    exclude: set[str] | None = None,
    *,
    no_normalize: frozenset[str] | set[str] = _NO_NORMALIZE_PARAMS,
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
    - A non-string iterable in ``no_normalize`` (numeric params
      such as ``water_year``, ``bbox``, ``thresholds``) is materialized
      to a ``list`` with its element types preserved (no string
      normalization), so the GET comma-join and the chunker — which test
      ``list``/``tuple`` — handle it instead of ``str()``-ing the whole
      array.
    - Any other ``Iterable[str]`` (i.e. not in ``no_normalize``)
      is materialized to ``list[str]`` via
      :func:`_normalize_str_iterable` so downstream code that branches
      on ``isinstance(v, (list, tuple))`` works for ``pandas.Series``,
      ``numpy.ndarray``, generators, etc.
    - Scalars and strings pass through unchanged.

    Parameters
    ----------
    local_vars : dict[str, Any]
        Dictionary of local variables, typically from ``locals()``.
    exclude : set[str], optional
        Additional keys to exclude from the resulting dictionary.
    no_normalize : set[str], optional
        Iterable-shaped params whose element types must be preserved
        (no string normalization). Defaults to the generic date-range +
        ``bbox`` set; callers with extra numeric params pass a superset.

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
            args[k] = _as_str_list(v, k)
        elif k in no_normalize and isinstance(v, Iterable) and not isinstance(v, str):
            # Numeric params (water_year, bbox, thresholds, …) keep their
            # element types — no string-normalization — but a non-string
            # iterable (numpy array, pandas Series, generator) is materialized
            # to a list so the GET comma-join and the chunker, which test
            # ``list``/``tuple``, handle it instead of str()-ing the whole
            # array. ``.tolist()`` yields native int/float; ``list()`` covers
            # generators and other iterables. Scalars/strings fall through.
            args[k] = v.tolist() if hasattr(v, "tolist") else list(v)
        elif isinstance(v, str) or not isinstance(v, Iterable):
            args[k] = v
        else:
            args[k] = _normalize_str_iterable(v, k)
    return args
