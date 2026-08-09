"""Generic OGC API engine shared by the Water Data and NGWMN getters.

This module holds OGC API Features orchestration — OGC cursor/response
strategies and the chunked fetch entry point :func:`get_ogc_data`. Generic
pagination and sync dispatch live in :mod:`dataretrieval.transport`; request
construction lives in :mod:`~dataretrieval.ogc.requests`. The surrounding
concerns live in sibling modules this one composes, each with its own reason
to change: :mod:`~dataretrieval.ogc.dates` (time-parameter marshalling),
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
import logging
from collections.abc import (
    Awaitable,
    Callable,
)
from typing import Any, TypeVar, cast

import httpx
import pandas as pd

import dataretrieval.ogc.chunking as chunking
import dataretrieval.progress as _progress
from dataretrieval.credentials import without_embedded_credentials
from dataretrieval.ogc.chunking import get_active_client
from dataretrieval.ogc.context import _row_cap
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.ogc.policy import (
    BASE_URL,  # noqa: F401  — compatibility alias
    DEFAULT_DIALECT,
    OGC_API_URL,
    OgcDialect,
)

# Frozen legacy compatibility surface; tests prevent new request-side re-exports.
from dataretrieval.ogc.requests import (  # noqa: F401
    _NO_NORMALIZE_PARAMS,
    _as_str_list,
    _check_monitoring_location_id,
    _construct_api_requests,
    _construct_cql_request,
    _cql2_param,
    _dialect,
    _get_args,
    _normalize_str_iterable,
    _ogc_base_url,
    _ogc_query_params,
    _switch_arg_id,
    _switch_properties_id,
    prepare_request_args,
)
from dataretrieval.ogc.shaping import GEOPANDAS, _finalize_ogc, _get_resp_data
from dataretrieval.response_metadata import BaseMetadata
from dataretrieval.transport.pagination import paginate
from dataretrieval.transport.sync import run_sync
from dataretrieval.utils import (
    _default_headers,  # noqa: F401  — compatibility re-export for tests
    _require_positive_int,
)

# Set up logger for this module
logger = logging.getLogger(__name__)

# Compatibility alias: the old name used internally and in tests.
_DEFAULT_DIALECT = DEFAULT_DIALECT


def _next_req_url(
    resp: httpx.Response, *, body: dict[str, Any] | None = None
) -> str | None:
    """
    Extracts the next-page URL from a water data endpoint's HTTP response.

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
        next_url: httpx.URL | None
        try:
            next_url = httpx.URL(href)
            next_host = next_url.host
            resp_url = (
                resp.url
                if isinstance(resp.url, httpx.URL)
                else httpx.URL(str(resp.url))
            )
            cur_host = resp_url.host
        except (httpx.InvalidURL, TypeError):
            next_url = None
            next_host = cur_host = None
        if next_host and cur_host and next_host != cur_host:
            raise RuntimeError(
                f"Refusing to follow cross-host next-page URL: "
                f"{next_host} != {cur_host}"
            )
        # Matching hosts is not enough: a link may also carry ``user:pass@``,
        # which httpx turns into an ``Authorization: Basic`` header on the
        # follow-up request. The host check above passes in exactly that case,
        # so strip it here rather than trusting the link we were handed.
        if next_url is not None:
            return str(without_embedded_credentials(next_url))
        # ``href`` comes from the JSON ``links`` array (typed ``Any``); the
        # ``not href`` guard above already excluded empty/None, and it is a
        # URL string (passed to ``httpx.URL`` above).
        return cast("str", href)
    return None


_Cursor = TypeVar("_Cursor")


async def _paginate(
    initial_req: httpx.Request,
    *,
    parse_response: Callable[[httpx.Response], tuple[pd.DataFrame, _Cursor | None]],
    follow_up: Callable[[_Cursor, httpx.AsyncClient], Awaitable[httpx.Response]],
    client: httpx.AsyncClient | None = None,
    raise_for_status: Callable[[httpx.Response], None] = _raise_for_non_200,
) -> tuple[pd.DataFrame, httpx.Response]:
    """Compatibility wrapper around service-neutral cursor pagination."""
    active_client = client if client is not None else get_active_client()
    return await paginate(
        initial_req,
        parse_response=parse_response,
        follow_up=follow_up,
        client=active_client,
        raise_for_status=raise_for_status,
        row_cap=_row_cap.get(),
    )


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
    Iterate paginated OGC API responses and aggregate them into one DataFrame.

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
    Retrieves OGC (Open Geospatial Consortium) data as a DataFrame with metadata.

    Prepares request arguments, constructs API requests, handles pagination,
    processes the results, and formats output according to the specified
    parameters.

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
    # Enforce a genuine positive integer up front: a float (even ``10.0``) or
    # ``bool`` would pass a bare ``< 1`` check and then crash deep in
    # ``pd.DataFrame.head`` with an opaque ``TypeError`` after HTTP I/O has
    # already fired. Shared with ``parallel_chunks(n)`` via the helper.
    if max_rows is not None:
        _require_positive_int(max_rows, "max_rows")

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
    with (
        _progress.progress_context(service=service, target_url=base_url),
        _row_cap(max_rows),
    ):
        with _ogc_base_url(base_url), _dialect(dialect):
            return _fetch_once(args, finalize=finalize)


@chunking.multi_value_chunked(build_request=_construct_api_requests)
async def _fetch_once(
    args: dict[str, Any],
) -> tuple[pd.DataFrame, httpx.Response]:
    """Send one prepared-args OGC request asynchronously; return (frame, response).

    ``@chunking.multi_value_chunked`` models every multi-value list
    parameter and the cql-text filter as a chunkable axis, greedy-halves
    the biggest chunk across all axes until each sub-request URL fits,
    and iterates the cartesian product. With no chunkable inputs the
    decorator passes args through unchanged. The decorator gathers every
    sub-request over one shared :class:`httpx.AsyncClient` (concurrency
    bounded by a semaphore, sized from ``API_USGS_CONCURRENT``). It also
    returns a *synchronous* wrapper, so ``get_ogc_data`` keeps calling
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
    """Compatibility wrapper around the service-neutral sync bridge."""
    return run_sync(
        make_coro,
        service=service,
        error_url=error_url if error_url is not None else _ogc_base_url.get(),
    )


def fetch_ogc_request(
    request: httpx.Request,
    *,
    service: str,
) -> tuple[pd.DataFrame, httpx.Response]:
    """Execute a prepared OGC request with pagination, returning (df, response).

    This is the facade-level entry point for generalized CQL requests: the
    caller builds its own :class:`httpx.Request` (e.g. via
    :func:`~dataretrieval.ogc.requests._construct_cql_request`) and hands it
    here. Pagination, progress reporting, and error handling are identical to
    the typed getters' path through :func:`_walk_pages`.

    Parameters
    ----------
    request : httpx.Request
        A fully-constructed OGC API request (typically a POST/CQL2).
    service : str
        Collection name, used only for progress-context labelling.

    Returns
    -------
    pd.DataFrame
        Concatenated page results.
    httpx.Response
        Aggregated response metadata.
    """

    async def _coro() -> tuple[pd.DataFrame, httpx.Response]:
        return await _walk_pages(geopd=GEOPANDAS, req=request)

    return _run_sync(_coro, service=service)
