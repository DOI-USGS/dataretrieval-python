"""Callback-driven cursor pagination independent of any service protocol.

:func:`paginate` is the page walk itself; :func:`run_paginated` composes
it with the shared executor, so an adapter supplies only its strategies.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, TypeVar, overload

import httpx
import pandas as pd

from dataretrieval import progress as _progress
from dataretrieval.combining import (
    _QUOTA_HEADER,
    _merge_response,
    _safe_elapsed,
)
from dataretrieval.exceptions import DataRetrievalError, RateLimited

# One-way: ``fanout`` does not import this module, so this edge cannot cycle.
from dataretrieval.transport.fanout import (
    _CONCURRENCY_DEFAULT,
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
    """Borrow a caller client or open a guarded short-lived client."""
    if client is not None:
        yield client
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


def _combine_frame_pages(
    pages: list[pd.DataFrame], row_cap: int | None
) -> pd.DataFrame:
    """Combine DataFrame pages using the paginator's established behavior."""
    result = pd.concat(pages, ignore_index=True)
    return result if row_cap is None else result.head(row_cap)


_Page = TypeVar("_Page")
_Result = TypeVar("_Result")


@overload
async def paginate(
    initial_req: httpx.Request,
    *,
    parse_response: Callable[[httpx.Response], tuple[pd.DataFrame, _Cursor | None]],
    follow_up: Callable[[_Cursor, httpx.AsyncClient], Awaitable[httpx.Response]],
    raise_for_status: Callable[[httpx.Response], None],
    client: httpx.AsyncClient | None = None,
    row_cap: int | None = None,
    combine_pages: None = None,
) -> tuple[pd.DataFrame, httpx.Response]: ...


@overload
async def paginate(
    initial_req: httpx.Request,
    *,
    parse_response: Callable[[httpx.Response], tuple[_Page, _Cursor | None]],
    follow_up: Callable[[_Cursor, httpx.AsyncClient], Awaitable[httpx.Response]],
    raise_for_status: Callable[[httpx.Response], None],
    combine_pages: Callable[[list[_Page], int | None], _Result],
    client: httpx.AsyncClient | None = None,
    row_cap: int | None = None,
) -> tuple[_Result, httpx.Response]: ...


async def paginate(
    initial_req: httpx.Request,
    *,
    parse_response: Callable[[httpx.Response], tuple[Any, Any | None]],
    follow_up: Callable[[Any, httpx.AsyncClient], Awaitable[httpx.Response]],
    raise_for_status: Callable[[httpx.Response], None],
    client: httpx.AsyncClient | None = None,
    row_cap: int | None = None,
    combine_pages: Callable[[list[Any], int | None], Any] | None = None,
) -> tuple[Any, httpx.Response]:
    """Fetch and combine pages until the injected parser returns no cursor.

    The service adapter supplies response parsing, cursor following, status
    mapping, and optionally how its natural page payloads combine. DataFrame
    pages retain the established concatenation default. This loop owns client
    lifecycle, repeated-cursor protection, optional row capping, progress
    updates, failure wrapping, and response metadata aggregation.
    """
    logger.debug("Requesting: %s", initial_req.url)
    reporter = _progress.current()

    def report_page(page: httpx.Response, payload: Any) -> None:
        note_progress()  # a walk still delivering pages is not stalled
        if reporter is not None:
            reporter.set_rate_remaining(
                page.headers.get(_QUOTA_HEADER),
                limit=page.headers.get("x-ratelimit-limit"),
            )
            reporter.add_page(rows=len(payload))

    async with _client_for(client) as session:
        response = await session.send(initial_req)
        raise_for_status(response)
        initial_response = response
        total_elapsed = _safe_elapsed(response)

        try:
            payload, cursor = parse_response(response)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Initial response parse failed.")
            raise DataRetrievalError(paginated_failure_message(0, exc)) from exc

        pages = [payload]
        nrows = len(payload)
        seen: set[Any] = set()
        report_page(response, payload)

        while (
            cursor is not None
            and cursor not in seen
            and (row_cap is None or nrows < row_cap)
        ):
            seen.add(cursor)
            try:
                response = await follow_up(cursor, session)
                raise_for_status(response)
                payload, cursor = parse_response(response)
                pages.append(payload)
                nrows += len(payload)
                total_elapsed += _safe_elapsed(response)
                report_page(response, payload)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Request failed at cursor %r. Data download interrupted.", cursor
                )
                raise DataRetrievalError(
                    paginated_failure_message(len(pages), exc)
                ) from exc

        final_response = _merge_response(
            initial_response,
            headers_from=response,
            elapsed=total_elapsed,
        )
        combine = _combine_frame_pages if combine_pages is None else combine_pages
        return combine(pages, row_cap), final_response


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
    default_concurrent: int = _CONCURRENCY_DEFAULT,
    canonical_url: str | None = None,
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
    :class:`~dataretrieval.exceptions.NetworkError`.
    """

    async def fetch(request: httpx.Request) -> tuple[pd.DataFrame, httpx.Response]:
        return await paginate(
            request,
            parse_response=parse_response,
            follow_up=follow_up,
            client=client if client is not None else active_client(),
            raise_for_status=raise_for_status,
        )

    if canonical_url is None and requests:
        canonical_url = str(requests[0].url)
    return FanOut(
        requests,
        fetch,
        RetryPolicy.from_env(),
        finalize=finalize,
        client_options=client_options,
        default_concurrent=default_concurrent,
        canonical_url=canonical_url,
        service=service,
    ).resume()
