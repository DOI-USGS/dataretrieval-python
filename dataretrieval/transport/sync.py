"""Synchronous dispatch over asynchronous retrieval internals."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar, cast

import httpx
from anyio.from_thread import start_blocking_portal

from dataretrieval import progress as _progress
from dataretrieval.transport.http import network_error

_T = TypeVar("_T")


def run_sync(
    make_coro: Callable[[], Awaitable[_T]],
    *,
    service: str,
    error_url: str | httpx.URL,
) -> _T:
    """Run an async retrieval from synchronous code in a blocking portal."""
    with _progress.progress_context(service=service, target_url=error_url):
        with start_blocking_portal() as portal:
            try:
                return cast("_T", portal.call(make_coro))
            except httpx.TransportError as exc:
                raise network_error(error_url, exc) from exc
