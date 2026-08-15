"""Callback-driven cursor pagination independent of any service protocol.

:func:`paginate` is the page walk itself; :func:`run_paginated` composes
it with the shared executor, so an adapter supplies only its strategies.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, TypeVar

import httpx
import pandas as pd

from dataretrieval import configuration as _configuration
from dataretrieval import progress as _progress
from dataretrieval.combining import (
    _QUOTA_HEADER,
    _merge_response,
    _safe_elapsed,
)
from dataretrieval.exceptions import DataRetrievalError, RateLimited

# One-way: ``fanout`` does not import this module, so this edge cannot cycle.
from dataretrieval.transport.fanout import (
    FanOut,
    _Finalize,
    _passthrough_result,
    active_client,
)
from dataretrieval.transport.http import open_async_client
from dataretrieval.transport.liveness import note_progress
from dataretrieval.transport.retry import RetryPolicy

logger = logging.getLogger(__name__)
_Cursor = TypeVar("_Cursor")


@asynccontextmanager
async def _client_for(
    client: httpx.AsyncClient | None,
) -> AsyncIterator[httpx.AsyncClient]:
    """Borrow a client: the caller's, else the running drive's, else a new one.

    Preferring the executor's published client over a fresh one keeps every
    page of every request on one connection pool. Both callers wanted that and
    each spelled it itself before it moved here.
    """
    borrowed = client if client is not None else active_client()
    if borrowed is not None:
        yield borrowed
        return
    async with open_async_client() as new:
        yield new


def paginated_failure_message(pages_collected: int, cause: BaseException) -> str:
    """Build a recovery-oriented message for an interrupted page walk."""
    cause_str = str(cause).removesuffix(".")
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


async def paginate(
    initial_req: httpx.Request,
    *,
    parse_response: Callable[[httpx.Response], tuple[pd.DataFrame, _Cursor | None]],
    follow_up: Callable[[_Cursor, httpx.AsyncClient], Awaitable[httpx.Response]],
    raise_for_status: Callable[[httpx.Response], None],
    client: httpx.AsyncClient | None = None,
    row_cap: int | None = None,
) -> tuple[pd.DataFrame, httpx.Response]:
    """Fetch and combine pages until the injected parser returns no cursor.

    The service adapter supplies response parsing, cursor following, and status
    mapping. This loop owns client lifecycle, repeated-cursor protection,
    optional row capping, progress updates, failure wrapping, and response
    metadata aggregation.
    """
    logger.debug("Requesting: %s", initial_req.url)
    reporter = _progress.current()

    def report_page(page: httpx.Response, frame: pd.DataFrame) -> None:
        note_progress()  # a walk still delivering pages is not stalled
        if reporter is not None:
            reporter.set_rate_remaining(
                page.headers.get(_QUOTA_HEADER),
                limit=page.headers.get("x-ratelimit-limit"),
            )
            reporter.add_page(rows=len(frame))

    async with _client_for(client) as session:
        response = await session.send(initial_req)
        raise_for_status(response)
        initial_response = response
        total_elapsed = _safe_elapsed(response)

        try:
            frame, cursor = parse_response(response)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Initial response parse failed.")
            raise DataRetrievalError(paginated_failure_message(0, exc)) from exc

        frames = [frame]
        nrows = len(frame)
        seen: set[Any] = set()
        report_page(response, frame)

        while (
            cursor is not None
            and cursor not in seen
            and (row_cap is None or nrows < row_cap)
        ):
            seen.add(cursor)
            try:
                response = await follow_up(cursor, session)
                raise_for_status(response)
                frame, cursor = parse_response(response)
                frames.append(frame)
                nrows += len(frame)
                total_elapsed += _safe_elapsed(response)
                report_page(response, frame)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Request failed at cursor %r. Data download interrupted.", cursor
                )
                raise DataRetrievalError(
                    paginated_failure_message(len(frames), exc)
                ) from exc

        final_response = _merge_response(
            initial_response,
            headers_from=response,
            elapsed=total_elapsed,
        )
        result = pd.concat(frames, ignore_index=True)
        if row_cap is not None:
            result = result.head(row_cap)
        return result, final_response


def run_paginated(
    requests: list[httpx.Request],
    *,
    parse_response: Callable[[httpx.Response], tuple[pd.DataFrame, Any]],
    follow_up: Callable[[Any, httpx.AsyncClient], Awaitable[httpx.Response]],
    raise_for_status: Callable[[httpx.Response], None],
    service: str,
    finalize: _Finalize = _passthrough_result,
    client: httpx.AsyncClient | None = None,
    client_options: dict[str, Any] | None = None,
    default_concurrent: int = _configuration.DEFAULT_CONCURRENCY,
    canonical_url: str | None = None,
    adapter: str | None = None,
) -> tuple[pd.DataFrame, Any]:
    """Drive one full page walk per request through the shared executor.

    The adapter supplies its strategies (``parse_response``, ``follow_up``,
    ``raise_for_status``, and optionally ``finalize``); this driver owns the
    composition three adapters used to copy -- each request paginated on the
    client the executor publishes unless ``client`` is injected, the retry
    policy, bounded concurrency, and the canonical URL the aggregate reports
    (the first request's, unless overridden).

    Raw transport errors need no mapping in the strategies: the executor
    retries them and normalizes a deterministic one into the typed
    :class:`~dataretrieval.exceptions.NetworkError`. ``adapter`` names the
    adapter for the settings chain, so a ``[waterdata] retries = 2`` table
    scopes to that adapter alone.
    """

    async def fetch(request: httpx.Request) -> tuple[pd.DataFrame, httpx.Response]:
        return await paginate(
            request,
            parse_response=parse_response,
            follow_up=follow_up,
            client=client,
            raise_for_status=raise_for_status,
        )

    if canonical_url is None and requests:
        canonical_url = str(requests[0].url)
    return FanOut(
        requests,
        fetch,
        RetryPolicy.from_configuration(adapter=adapter),
        finalize=finalize,
        client_options=client_options,
        default_concurrent=default_concurrent,
        canonical_url=canonical_url,
        service=service,
        adapter=adapter,
    ).resume()
