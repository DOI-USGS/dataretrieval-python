"""Pagination and async-execution engine for the Water Data internals.

The client-resolution context manager, the paginated-response aggregator, the
optional row-cap context, the generic async pagination driver
(:func:`_paginate`), and the sync-from-async bridge (:func:`_run_sync`).
Depends on :mod:`dataretrieval.waterdata.utils.http`,
:mod:`dataretrieval.waterdata._progress`, and
:mod:`dataretrieval.waterdata.chunking`.

The module logger is named ``"dataretrieval.waterdata.utils"`` (not
``__name__``) so the pagination ``logger.warning`` records are captured by the
test suite's ``caplog`` assertions, which target that historical logger name.
"""

from __future__ import annotations

import copy
import logging
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
)
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from datetime import timedelta
from typing import TypeVar

import httpx
import pandas as pd
from anyio.from_thread import start_blocking_portal

from dataretrieval.utils import HTTPX_DEFAULTS
from dataretrieval.waterdata import _progress
from dataretrieval.waterdata.chunking import (
    _QUOTA_HEADER,
    _safe_elapsed,
    get_active_client,
)
from dataretrieval.waterdata.utils.http import (
    _paginated_failure_message,
    _raise_for_non_200,
)

# Set up logger for this module. Use the literal historical name (not
# ``__name__``) so ``caplog`` assertions in the test suite — which capture
# under ``dataretrieval.waterdata.utils`` — still match the pagination
# warning records emitted below.
logger = logging.getLogger("dataretrieval.waterdata.utils")


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

    Shared by the non-chunked fetch paths (:func:`get_stats_data`,
    :func:`get_cql`); the chunked OGC getters drive their own portal
    inside :meth:`chunking.ChunkedCall.resume`.
    """
    with _progress.progress_context(service=service):
        with start_blocking_portal() as portal:
            return portal.call(make_coro)
