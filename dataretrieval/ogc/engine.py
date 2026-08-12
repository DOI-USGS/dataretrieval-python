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
  passed explicitly (no collection map lives here).
* ``base_url`` — the OGC API base to target.
* ``extra_id_cols`` — synthetic id columns to push to the end of a result.
* ``dialect`` — an :class:`OgcDialect` describing which collections need
  POST/CQL2 and which use date-only (vs. full datetime) time arguments.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import (
    Awaitable,
    Callable,
)
from typing import TYPE_CHECKING, Any, TypeVar

import httpx
import pandas as pd

import dataretrieval.ogc.chunking as chunking
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.ogc.policy import (
    DEFAULT_DIALECT,
    OgcDialect,
    _require_positive_int,
)

# Request construction stays in its canonical module; the engine imports only
# the symbols its orchestration uses.
from dataretrieval.ogc.requests import (
    _construct_api_requests,
    _switch_arg_id,
    _switch_properties_id,
)
from dataretrieval.ogc.shaping import GEOPANDAS, _finalize_ogc, _get_resp_data
from dataretrieval.transport.fanout import FanOut, active_client
from dataretrieval.transport.links import resolve_next_url
from dataretrieval.transport.pagination import paginate
from dataretrieval.transport.retry import RetryPolicy

if TYPE_CHECKING:
    from dataretrieval._response_metadata import BaseMetadata

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
        # The link is response data: parsing it, resolving a relative
        # reference, refusing a foreign host and stripping embedded
        # credentials is one shared policy, so this walk cannot drift from
        # the other two. ``RuntimeError`` rather than the taxonomy's
        # ``DataRetrievalError`` because that is what this walk has always
        # raised; retyping it is a released behavior change to make
        # deliberately, not a side effect of sharing the check.
        return resolve_next_url(href, resp, service="OGC", error=RuntimeError)
    return None


_Cursor = TypeVar("_Cursor")


async def _paginate(
    initial_req: httpx.Request,
    *,
    parse_response: Callable[[httpx.Response], tuple[pd.DataFrame, _Cursor | None]],
    follow_up: Callable[[_Cursor, httpx.AsyncClient], Awaitable[httpx.Response]],
    client: httpx.AsyncClient | None = None,
    raise_for_status: Callable[[httpx.Response], None] = _raise_for_non_200,
    row_cap: int | None = None,
) -> tuple[pd.DataFrame, httpx.Response]:
    """Compatibility wrapper around collection-neutral cursor pagination."""
    session = client if client is not None else active_client()
    return await paginate(
        initial_req,
        parse_response=parse_response,
        follow_up=follow_up,
        client=session,
        raise_for_status=raise_for_status,
        row_cap=row_cap,
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
    *,
    row_cap: int | None = None,
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
    row_cap : int, optional
        Stop following pages once this many rows have accumulated and
        truncate to exactly this many. ``None`` (default) walks every page.
        An early-stop download bound only — the combined-result cap is
        applied in :func:`~dataretrieval.ogc.shaping._finalize_ogc`.

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
        row_cap=row_cap,
    )


def get_ogc_data(
    args: dict[str, Any],
    collection: str,
    output_id: str,
    *,
    base_url: str,
    max_rows: int | None = None,
    extra_id_cols: frozenset[str] | set[str] = frozenset(),
    dialect: OgcDialect | None = None,
    adapter: str | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Retrieves OGC (Open Geospatial Consortium) data as a DataFrame with metadata.

    Prepares request arguments, constructs API requests, handles pagination,
    processes the results, and formats output according to the specified
    parameters.

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of request arguments for the OGC collection.
    collection : str
        The OGC API collection name (e.g., ``"daily"``,
        ``"monitoring-locations"``, ``"continuous"``).
    output_id : str
        The user-facing id column the wire ``id`` is renamed to. Required —
        the per-API collection-to-id map lives in the caller, not here.
    max_rows : int, optional
        Stop paginating once this many rows have been collected and
        truncate the result to exactly ``max_rows``. ``None`` (default)
        fetches the full result. Intended for cheap previews of large,
        un-chunked tables (e.g. :func:`get_reference_table`).
    base_url : str
        OGC API base URL to target. Required: this package is API-neutral and
        names no API of its own, so each adapter passes its own base (e.g.
        ``waterdata.utils.OGC_API_URL``, ``ngwmn.NGWMN_OGC_API_URL``). It was
        once optional, falling back to whatever was in ambient scope -- which
        defaults to the empty string, so omitting it built a *relative*
        ``/collections/{id}/items`` that planning accepted and only httpx
        rejected at send time, surfacing as a NetworkError about an unknown
        service. Requiring it moves that mistake to the call site, where mypy
        catches it.
    extra_id_cols : set or frozenset, optional
        Synthetic id columns to push to the end of a result frame (see
        :func:`_arrange_cols`). Defaults to an empty set.
    dialect : OgcDialect, optional
        Per-API request quirks (CQL2-only collections, date-only collections).
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
    - Applies column cleanup and reordering based on collection and properties.
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
    args["collection"] = collection
    args = _switch_arg_id(args, id_name=output_id, collection=collection)
    # Capture `properties` before the id-switch so post-processing sees
    # the user-facing names, not the wire-format ones.
    properties = args.get("properties")
    args["properties"] = _switch_properties_id(
        properties, id_name=output_id, collection=collection
    )
    convert_type = args.pop("convert_type", False)
    args = {k: v for k, v in args.items() if v is not None}

    # Post-processing is injected into the chunker rather than applied here,
    # so it runs on *every* exit: the normal return AND a later
    # ``exc.call.resume()`` after a ChunkInterrupted (which never re-enters
    # this function). ``_finalize_ogc`` is the single source of result shape;
    # it also applies ``max_rows`` to the *combined* frame so the cap is the
    # exact total even when the plan chunks or the call is resumed, while
    # the per-chunk ``row_cap`` bound below only early-stops each chunk's
    # pagination.
    finalize = functools.partial(
        _finalize_ogc,
        properties=properties,
        output_id=output_id,
        convert_type=convert_type,
        collection=collection,
        max_rows=max_rows,
        extra_id_cols=extra_id_cols,
        dialect=dialect,
        base_url=base_url,
    )
    # Bind the API target and quirks into the request builder and fetcher the
    # same way ``finalize`` binds its own state: with ``functools.partial``.
    # The plan sizes candidate chunks and a later ``exc.call.resume()``
    # rebuilds them through these same bound callables, so the values the
    # call was created with reach every chunk — even a resume fired long
    # after this function returned — without any ambient state to snapshot.
    build_request = functools.partial(
        _construct_api_requests, base_url=base_url, dialect=dialect
    )
    fetch = functools.partial(
        _fetch_once, build_request=build_request, row_cap=max_rows
    )
    run = chunking.multi_value_chunked(build_request=build_request, adapter=adapter)(
        fetch
    )
    # No progress block here: the executor that emits the events owns the line
    # (see :meth:`~dataretrieval.transport.fanout.FanOut.resume`).
    return run(args, finalize=finalize)


async def _fetch_once(
    args: dict[str, Any],
    *,
    build_request: Callable[..., httpx.Request],
    row_cap: int | None = None,
) -> tuple[pd.DataFrame, httpx.Response]:
    """Send one prepared-args OGC request asynchronously; return (frame, response).

    The undecorated per-chunk fetcher: ``get_ogc_data`` binds
    ``build_request`` (the target API's request builder) and ``row_cap``,
    then wraps the result in ``chunking.multi_value_chunked``, which models
    every multi-value list parameter and the cql-text filter as a chunkable
    axis, greedy-halves the biggest chunk across all axes until each chunk
    URL fits, and iterates the cartesian product. With no chunkable inputs
    the decorator passes args through unchanged. The decorator gathers every
    chunk over one shared :class:`httpx.AsyncClient` (concurrency
    bounded by a semaphore, sized from the effective ``concurrency``
    setting) and returns a *synchronous* wrapper, so ``get_ogc_data`` drives
    it synchronously. The return shape is ``(frame, response)``.
    """
    req = build_request(**args)
    return await _walk_pages(geopd=GEOPANDAS, req=req, row_cap=row_cap)


def fetch_ogc_request(
    request: httpx.Request,
    *,
    collection: str,
    adapter: str | None = None,
) -> tuple[pd.DataFrame, httpx.Response]:
    """Execute a prepared OGC request with pagination, returning (df, response).

    This is the facade-level entry point for generalized CQL requests: the
    caller builds its own :class:`httpx.Request` (e.g. via
    :func:`~dataretrieval.ogc.requests._construct_cql_request`) and hands it
    here. The request is driven as a one-item
    :class:`~dataretrieval.transport.fanout.FanOut` -- the same executor the
    typed getters use -- so pagination, retry, progress reporting, and error
    handling are identical to their path through :func:`_walk_pages`.

    Parameters
    ----------
    request : httpx.Request
        A fully-constructed OGC API request (typically a POST/CQL2).
    collection : str
        Collection name, used only for progress-context labelling.

    Returns
    -------
    pd.DataFrame
        Concatenated page results.
    httpx.Response
        Aggregated response metadata.
    """

    async def _fetch(req: httpx.Request) -> tuple[pd.DataFrame, httpx.Response]:
        return await _walk_pages(geopd=GEOPANDAS, req=req)

    return FanOut(
        [request],
        _fetch,
        RetryPolicy.from_settings(adapter=adapter),
        canonical_url=str(request.url),
        service=collection,
        adapter=adapter,
    ).resume()
