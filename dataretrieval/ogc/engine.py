"""Generic OGC API engine shared by the Water Data and NGWMN getters.

This module holds the API-agnostic machinery for talking to an OGC API
Features service: request construction (GET comma-joined or POST/CQL2),
async pagination, response shaping, and the chunked fetch entry point
:func:`get_ogc_data`. It is deliberately free of any Water-Data-specific
constants so a sibling package (e.g. NGWMN) can drive it without importing
``dataretrieval.waterdata``.

API-specific behavior is supplied by the caller:

* ``output_id`` — the user-facing column the wire ``id`` is renamed to,
  passed explicitly (no service map lives here).
* ``base_url`` — the OGC API base to target.
* ``extra_id_cols`` — synthetic id columns to push to the end of a result.
* ``dialect`` — an :class:`OgcDialect` describing which services need
  POST/CQL2 and which use date-only (vs. full datetime) time arguments.

The implementation is split across cohesive sibling modules; this module is the
facade + async driver. It KEEPS only the pagination driver, and RE-EXPORTS every
other symbol so existing ``from dataretrieval.ogc.engine import <name>`` sites
keep working unchanged. The whole "wire response → DataFrame" parse chain
(including the geopandas-touching ``_empty_feature_frame`` / ``_get_resp_data``)
now lives in :mod:`dataretrieval.ogc._responses`, so the geopandas seam is
patched there (``mock.patch("...ogc._responses.gpd")``), not on this module.
``_construct_api_requests``, ``_walk_pages``, ``get_ogc_data`` are patched on
this module's namespace and so must be looked up via this module's globals.
"""

from __future__ import annotations

import functools
import numbers
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pandas as pd
from anyio.from_thread import start_blocking_portal

from dataretrieval.exceptions import DataRetrievalError
from dataretrieval.ogc import chunking
from dataretrieval.ogc import progress as _progress

# Shared engine state re-exported so ``engine.<name>`` stays patchable here.
from dataretrieval.ogc._constants import (  # noqa: F401  (re-exported facade names)
    _CAMEL_BOUNDARY_RE,
    _DATE_RANGE_PARAMS,
    _DATETIME_FORMATS,
    _DEFAULT_DIALECT,
    _DURATION_RE,
    _MONITORING_LOCATION_ID_RE,
    _NO_NORMALIZE_PARAMS,
    BASE_URL,
    GEOPANDAS,
    OGC_API_URL,
    OGC_API_VERSION,
    OgcDialect,
    _Cursor,
    logger,
)
from dataretrieval.ogc._context import (  # noqa: F401  (re-exported facade names)
    _dialect,
    _dialect_var,
    _ogc_base_url,
    _ogc_base_url_var,
    _row_cap,
    _row_cap_var,
)
from dataretrieval.ogc._http import (  # noqa: F401  (re-exported facade names)
    _default_headers,
    _error_body,
    _paginated_failure_message,
    _parse_retry_after,
    _raise_for_non_200,
)
from dataretrieval.ogc._requests import (  # noqa: F401  (re-exported facade names)
    _check_ogc_requests,
    _construct_api_requests,
    _construct_cql_request,
    _cql2_param,
    _format_api_dates,
    _format_one,
    _next_req_url,
    _ogc_query_params,
    _parse_datetime,
)
from dataretrieval.ogc._responses import (  # noqa: F401  (re-exported facade names)
    _aggregate_paginated_response,
    _arrange_cols,
    _attach_coordinates,
    _deal_with_empty,
    _empty_feature_frame,
    _finalize_ogc,
    _get_resp_data,
    _ogc_parse_response,
    _sort_rows,
    _to_snake_case,
    _type_cols,
)
from dataretrieval.ogc._validate import (  # noqa: F401  (re-exported facade names)
    _as_str_list,
    _check_id_format,
    _check_monitoring_location_id,
    _get_args,
    _normalize_str_iterable,
    _switch_arg_id,
    _switch_properties_id,
)
from dataretrieval.ogc.chunking import (
    _QUOTA_HEADER,
    _safe_elapsed,
    get_active_client,
)
from dataretrieval.utils import HTTPX_DEFAULTS, BaseMetadata, _network_error

# Explicit re-export list (also marks these names public for ``mypy --strict``).
__all__ = [
    # constants / shared state
    "BASE_URL",
    "GEOPANDAS",
    "OGC_API_URL",
    "OGC_API_VERSION",
    "OgcDialect",
    "_CAMEL_BOUNDARY_RE",
    "_Cursor",
    "_DATE_RANGE_PARAMS",
    "_DATETIME_FORMATS",
    "_DEFAULT_DIALECT",
    "_DURATION_RE",
    "_MONITORING_LOCATION_ID_RE",
    "_NO_NORMALIZE_PARAMS",
    "_dialect",
    "_dialect_var",
    "_ogc_base_url",
    "_ogc_base_url_var",
    "_row_cap",
    "_row_cap_var",
    "logger",
    # http helpers
    "_default_headers",
    "_error_body",
    "_paginated_failure_message",
    "_parse_retry_after",
    "_raise_for_non_200",
    # request construction
    "_construct_api_requests",
    "_construct_cql_request",
    "_cql2_param",
    "_format_api_dates",
    "_format_one",
    "_next_req_url",
    "_ogc_query_params",
    "_parse_datetime",
    # response shaping (wire response -> DataFrame, incl. the gpd seam)
    "_aggregate_paginated_response",
    "_arrange_cols",
    "_attach_coordinates",
    "_deal_with_empty",
    "_empty_feature_frame",
    "_finalize_ogc",
    "_get_resp_data",
    "_ogc_parse_response",
    "_sort_rows",
    "_to_snake_case",
    "_type_cols",
    # validation / normalization
    "_as_str_list",
    "_check_id_format",
    "_check_monitoring_location_id",
    "_check_ogc_requests",
    "_get_args",
    "_normalize_str_iterable",
    "_switch_arg_id",
    "_switch_properties_id",
    # pagination driver (defined here)
    "_client_for",
    "_fetch_once",
    "_paginate",
    "_run_sync",
    "_walk_pages",
    "get_ogc_data",
]


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
            raise DataRetrievalError(_paginated_failure_message(0, e)) from e
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
                raise DataRetrievalError(_paginated_failure_message(len(dfs), e)) from e

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
    """
    with _progress.progress_context(service=service):
        with start_blocking_portal() as portal:
            try:
                return portal.call(make_coro)
            except httpx.TransportError as exc:
                # The initial-request connection failure ``_paginate`` lets
                # through raw; mid-pagination failures are already typed.
                # Report the base URL actually targeted (NGWMN/sibling APIs
                # set their own via ``_ogc_base_url``), not a hardcoded host.
                raise _network_error(_ogc_base_url_var.get(), exc) from exc
