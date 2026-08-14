"""URL-byte chunk planning and dispatch for the OGC getters.

An OGC query has several chunkable axes: every multi-value list
parameter (sites, parameter codes, …) plus the cql-text ``filter``,
which splits along its top-level OR clauses. Any of them can fan the
URL past the server's ~8 KB byte limit. ``ChunkPlan`` picks a fan-out
for each axis that minimizes total chunks while keeping every
chunk URL under the budget. Requests that already fit get a
trivial single-step plan — the executor has one code path either way.

This module owns the OGC-specific half: the byte budget and the
``multi_value_chunked`` decorator that ties a plan to a fetcher. The planner
splits only as far as the byte limit forces. Parallelism within each chunk is
the offset page walk's job, which overlaps pages without manufacturing extra
chunks. Driving chunks to completion — bounded concurrency, retry, failure
precedence, resume — is API-neutral and belongs to
:class:`dataretrieval.transport.fanout.FanOut`, which this module hands its plan
to. :class:`~dataretrieval.ogc.planning.ChunkPlan` satisfies
:class:`~dataretrieval.transport.fanout.FanOutPlan` structurally.

Concurrency, retries, and interruption semantics are documented on
:mod:`dataretrieval.transport.fanout`; ``API_USGS_CONCURRENT`` and
``API_USGS_RETRIES`` are read there.

Dedup: list-axis chunks don't overlap; filter-axis chunks can, so
``_combine_chunk_frames`` dedupes by feature ``id``. ``properties``,
``bbox``, date intervals, ``limit``, ``skip_geometry``, and
``filter``/``filter_lang`` themselves are never sliced as list axes
(the filter is partitioned along its top-level OR axis instead).
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

import httpx
import pandas as pd

from dataretrieval.transport.fanout import (
    _CONCURRENCY_DEFAULT,
    FanOut,
    _active_client,
    _Fetch,
    _Finalize,
    _passthrough_result,
    _resolve_concurrency,
    active_client,
)
from dataretrieval.transport.retry import RetryPolicy

from .planning import ChunkPlan

# Compatibility aliases. ``ChunkedCall`` was this module's executor before it
# moved down to transport as the API-neutral ``FanOut``; ``get_active_client``
# and ``_chunked_client`` named its shared per-call client. Only the
# chunking/progress test modules still use these names, and the rename is not
# worth churning them over -- package code imports the canonical spellings from
# :mod:`dataretrieval.transport.fanout`. They are aliases, not copies: the
# ambient in particular must be the *same* object transport publishes, or a
# test reading it here would never see the running client.
ChunkedCall = FanOut
get_active_client = active_client
_chunked_client = _active_client

# Empirically the API replies HTTP 414 above ~8200 bytes of full URL —
# matches nginx's default ``large_client_header_buffers`` of 8 KB. 8000
# leaves ~200 bytes for request-line framing and proxy variance. The decorator
# resolves this module-level default at call time when ``url_limit`` is None,
# so a test can ``monkeypatch.setattr`` it on this module.
_OGC_URL_BYTE_LIMIT = 8000


def page_concurrency() -> int:
    """Return the bounded page-wave width for offset pagination.

    ``API_USGS_CONCURRENT`` governs both chunk fan-out and page waves. The
    ``unbounded`` chunk setting is clamped here because a speculative page wave
    must remain finite.
    """
    resolved = _resolve_concurrency(_CONCURRENCY_DEFAULT)
    return _CONCURRENCY_DEFAULT if resolved is None else resolved


def multi_value_chunked(
    *,
    build_request: Callable[..., httpx.Request],
    url_limit: int | None = None,
) -> Callable[[_Fetch[dict[str, Any]]], Callable[..., tuple[pd.DataFrame, Any]]]:
    """
    Decorate an async fetcher to transparently chunk over-budget requests.

    Returns a callable that builds a :class:`ChunkPlan` from ``args``,
    constructs a :class:`ChunkedCall` over the decorated
    ``async def fetch(args) -> (df, response)``, and drives it to
    completion via :meth:`ChunkedCall.resume`. The plan splits multi-value
    list params and the cql-text filter so each chunk URL fits the
    byte limit. An already-fitting request is a one-step plan. Each chunk
    then fetches its pages through the strategy selected by the OGC engine.
    See the module docstring for the concurrency model.

    Parameters
    ----------
    build_request : Callable[..., httpx.Request]
        Factory that turns a kwargs dict into a sized httpx request,
        e.g. ``_construct_api_requests``. Called during planning to
        measure each candidate plan.
    url_limit : int, optional
        Byte budget for the request (URL + body). When ``None``
        (default), the module-level ``_OGC_URL_BYTE_LIMIT`` is
        resolved at call time so test patches via
        ``monkeypatch.setattr`` take effect.

    Returns
    -------
    Callable
        A *synchronous* wrapper ``wrapper(args, *, finalize=...) ->
        (df, response)`` that executes the underlying plan transparently
        over the decorated async fetcher.

    Raises
    ------
    Unchunkable
        If no plan can fit ``url_limit``.
    ChunkInterrupted
        On a mid-execution transient — 429, 5xx, or a bare transport
        error: :class:`QuotaExhausted` for 429, :class:`ServiceInterrupted`
        for the rest. See :class:`ChunkedCall` for the resume semantics.

    See Also
    --------
    ChunkPlan : Planning shape (axes, partitioning, passthrough).
    ChunkedCall : Per-chunk execution and resume semantics.
    """

    def decorator(
        fetch: _Fetch[dict[str, Any]],
    ) -> Callable[..., tuple[pd.DataFrame, Any]]:
        @functools.wraps(fetch)
        def wrapper(
            args: dict[str, Any],
            *,
            finalize: _Finalize = _passthrough_result,
        ) -> tuple[pd.DataFrame, Any]:
            limit = _OGC_URL_BYTE_LIMIT if url_limit is None else url_limit
            plan = ChunkPlan(args, build_request, limit)
            retry_policy = RetryPolicy.from_env()
            # The concurrency cap is resolved inside ``resume()`` from
            # ``API_USGS_CONCURRENT``; ``1`` is a sequential gather,
            # ``total <= 1`` a one-element gather — no special branch.
            return ChunkedCall(
                plan,
                fetch,
                retry_policy,
                finalize,
                canonical_url=plan.canonical_url,
                # The collection name, for the progress line the executor
                # opens. ``get_ogc_data`` puts it in ``args``.
                service=args.get("collection"),
                # One chunk can fan out a page wave independently of the chunk
                # semaphore, so the shared pool must cover both dimensions.
                connection_multiplier=page_concurrency,
            ).resume()

        return wrapper

    return decorator
