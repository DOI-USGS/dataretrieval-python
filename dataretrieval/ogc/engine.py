"""Generic OGC API engine shared by the Water Data and NGWMN getters.

This module holds the API-agnostic orchestration core for talking to an OGC
API Features service — async pagination, the sync bridge, and the chunked
fetch entry point :func:`get_ogc_data` that orchestrates them. Request
construction lives in :mod:`~dataretrieval.ogc.requests`. The surrounding
concerns live in sibling modules it composes, each with its own reason to
change: :mod:`~dataretrieval.ogc.dates` (time-parameter marshalling),
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
    AsyncIterator,
    Awaitable,
    Callable,
)
from contextlib import asynccontextmanager
from typing import Any, TypeVar, cast

import httpx
import pandas as pd
from anyio.from_thread import start_blocking_portal

import dataretrieval.ogc.chunking as chunking
import dataretrieval.ogc.progress as _progress
from dataretrieval.exceptions import DataRetrievalError
from dataretrieval.ogc.chunking import get_active_client
from dataretrieval.ogc.combining import _QUOTA_HEADER, _merge_response, _safe_elapsed
from dataretrieval.ogc.errors import _paginated_failure_message, _raise_for_non_200
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
    _check_ogc_requests,
    _construct_api_requests,
    _construct_cql_request,
    _cql2_param,
    _dialect,
    _get_args,
    _normalize_str_iterable,
    _ogc_base_url,
    _ogc_query_params,
    _row_cap,
    _switch_arg_id,
    _switch_properties_id,
    prepare_request_args,
)
from dataretrieval.ogc.shaping import GEOPANDAS, _finalize_ogc, _get_resp_data
from dataretrieval.utils import (
    HTTPX_ASYNC_DEFAULTS,
    BaseMetadata,
    _default_headers,  # noqa: F401  — compatibility re-export for tests
    _network_error,
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
    async with httpx.AsyncClient(**HTTPX_ASYNC_DEFAULTS) as new:
        yield new


_Cursor = TypeVar("_Cursor")


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
        ``.elapsed`` set to the sum of the per-page response durations. The
        canonical URL is preserved from the first page. The original first-page
        response is not mutated.

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
