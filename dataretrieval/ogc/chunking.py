"""Joint URL-byte chunking for the OGC getters.

An OGC query has several chunkable axes: every multi-value list
parameter (sites, parameter codes, …) plus the cql-text ``filter``,
which splits along its top-level OR clauses. Any of them can fan the
URL past the server's ~8 KB byte limit. ``ChunkPlan`` picks a fan-out
for each axis that minimizes total sub-requests while keeping every
sub-request URL under the budget; ``ChunkedCall`` fetches the resulting
cartesian product of chunks. Requests that already fit get a trivial
single-step plan — ``ChunkedCall`` has one code path either way.

This module owns the *execution* half — the event loop and bounded
concurrency that drive a plan to completion (``ChunkedCall``) plus the
public ``multi_value_chunked`` decorator. The neighboring concerns live in
sibling modules it imports, each with its own reason to change:
:mod:`~dataretrieval.ogc.planning` builds the
:class:`~dataretrieval.ogc.planning.ChunkPlan` and recombines per-chunk
frames and responses (pure, no I/O); :mod:`~dataretrieval.ogc.retry` holds
the transient-classification and exponential-backoff policy; and
:mod:`~dataretrieval.ogc.interruptions` defines the resumable
:class:`~dataretrieval.ogc.interruptions.ChunkInterrupted` exception
contract.

Concurrency: ``multi_value_chunked`` fans every pending sub-request out
under one ``asyncio.gather`` sharing a single ``httpx.AsyncClient``. An
``asyncio.Semaphore`` — not the client's connection pool, which is
merely sized to match — caps the sub-requests in flight at ``N``; see
:meth:`ChunkedCall._run` for why the gate must be the semaphore rather
than the pool. ``API_USGS_CONCURRENT`` resolves ``N``: an integer N > 1
allows N sub-requests in flight; ``1`` forces sequential dispatch (one
request at a time); the literal ``unbounded`` lifts the cap. ``N``
bounds only how many of a chunked query's sub-requests are in flight at
once — a client-side trade-off between open connections and fan-out
latency. It does not affect the API rate limit: a chunked call issues
the same number of sub-requests regardless of ``N``, so ``N`` changes
their timing, not the total request volume. The USGS API rate-limits by
volume over time (HTTP 429), not by simultaneity; set ``API_USGS_PAT``
to raise that quota. The default of 32 is a conservative cap that keeps
connection use modest. The fan-out runs in a short-lived worker thread
(an ``anyio`` blocking portal), so it works whether or not the caller is
already inside an event loop (Jupyter / IPython / async apps).

Retries: each sub-request is retried on a transient failure (429,
5xx, connect/read timeout) with exponential backoff + full jitter,
honoring a server ``Retry-After`` when present. ``API_USGS_RETRIES``
sets the cap (default 4; ``0`` disables). A ``Retry-After`` longer
than the per-call ceiling escalates to a resumable interruption.

Interruption: any mid-stream transient failure — 429, 5xx, or a bare
transport error (connect/read timeout, oversize follow-up URL) — surfaces
as a ``ChunkInterrupted`` subclass: ``QuotaExhausted`` for 429,
``ServiceInterrupted`` for the rest. The exception carries ``.call``, a
``ChunkedCall`` handle that owns the already-completed sub-request
state (sparse-indexed, since gathered sub-requests complete out of
order). Call ``.call.resume()`` once the underlying condition clears;
only the still-pending sub-requests are re-issued. ``Retry-After`` (when
the server sets it) is surfaced on the exception as ``.retry_after``.

Dedup: list-axis chunks don't overlap; filter-axis chunks can, so
``_combine_chunk_frames`` dedupes by feature ``id``. ``properties``,
``bbox``, date intervals, ``limit``, ``skip_geometry``, and
``filter``/``filter_lang`` themselves are never sliced as list axes
(the filter is partitioned along its top-level OR axis instead).
"""

from __future__ import annotations

import asyncio
import functools
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from typing import Any, cast

import httpx
import pandas as pd
from anyio.from_thread import start_blocking_portal

from dataretrieval.utils import HTTPX_DEFAULTS

from . import progress as _progress
from .interruptions import (
    ChunkInterrupted,
    _Fetch,
    _Finalize,
    _passthrough_result,
)
from .planning import (
    ChunkPlan,
    _combine_chunk_frames,
    _combine_chunk_responses,
)
from .retry import (
    _NO_RETRY,
    RetryPolicy,
    _classify_chunk_error,
    _retry,
)

# Empirically the API replies HTTP 414 above ~8200 bytes of full URL —
# matches nginx's default ``large_client_header_buffers`` of 8 KB. 8000
# leaves ~200 bytes for request-line framing and proxy variance. The decorator
# resolves this module-level default at call time when ``url_limit`` is None,
# so a test can ``monkeypatch.setattr`` it on this module.
_OGC_URL_BYTE_LIMIT = 8000


# Response header USGS uses to advertise remaining hourly quota.
_QUOTA_HEADER = "x-ratelimit-remaining"

# Fan-out concurrency cap, read at call time (not import) so test
# ``monkeypatch.setenv`` applies. Value grammar in :func:`_read_concurrency_env`;
# the concurrency model is in the module docstring.
_CONCURRENCY_ENV = "API_USGS_CONCURRENT"
_CONCURRENCY_DEFAULT = 32
_CONCURRENCY_UNBOUNDED = "unbounded"


def _read_concurrency_env() -> int | None:
    """
    Resolve the ``API_USGS_CONCURRENT`` env var to a parallelism cap.

    Returns
    -------
    int or None
        ``1`` for sequential dispatch (one sub-request at a time); an
        integer >1 for bounded concurrency; ``None`` to disable the
        per-call cap entirely (``unbounded`` keyword). Unset → default
        of ``_CONCURRENCY_DEFAULT``.
    """
    raw = os.environ.get(_CONCURRENCY_ENV)
    if raw is None:
        return _CONCURRENCY_DEFAULT
    raw = raw.strip()
    if raw == "":
        return _CONCURRENCY_DEFAULT
    if raw.lower() == _CONCURRENCY_UNBOUNDED:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{_CONCURRENCY_ENV} must be a positive integer or "
            f"'{_CONCURRENCY_UNBOUNDED}'; got {raw!r}."
        ) from exc
    if value < 1:
        raise ValueError(
            f"{_CONCURRENCY_ENV} must be >= 1 (got {value}); use "
            f"'{_CONCURRENCY_UNBOUNDED}' to disable the cap."
        )
    return value


# Shared per-call ``httpx.AsyncClient``, published via :func:`_publish`
# during ``ChunkedCall._run`` so paginated-loop helpers (``_walk_pages``)
# reuse the same connection pool across every sub-request. ``None``
# outside a chunked call — paginated helpers then open their own
# short-lived client.
_chunked_client: ContextVar[httpx.AsyncClient | None] = ContextVar(
    "_chunked_client", default=None
)


@contextmanager
def _publish(client: httpx.AsyncClient) -> Iterator[None]:
    """
    Publish ``client`` on the ``_chunked_client`` ContextVar so the
    paginated-loop helpers can borrow it via :func:`get_active_client`
    for the duration of the ``with`` block.

    Parameters
    ----------
    client : httpx.AsyncClient
        The client to publish.

    Yields
    ------
    None
        Yields once, for the duration of the bind.
    """
    token = _chunked_client.set(client)
    try:
        yield
    finally:
        _chunked_client.reset(token)


def get_active_client() -> httpx.AsyncClient | None:
    """
    Return the chunker's currently-published client, or ``None``.

    Used by the paginated-loop helpers (e.g.
    :func:`dataretrieval.ogc.engine._client_for`) to reuse the
    per-call connection pool.

    Returns
    -------
    httpx.AsyncClient or None
        The client published via :func:`_publish` if currently inside a
        :class:`ChunkedCall` run; ``None`` otherwise.
    """
    return _chunked_client.get()


class ChunkedCall:
    """
    Stateful handle for a chunked call.

    Holds the in-flight state (per-sub-request frames and responses)
    and the async fetcher. A single :meth:`resume` entry point drives
    the call from wherever it is to completion — used both for the
    first invocation (from :meth:`ChunkPlan.execute`) and for subsequent
    retries after a :class:`ChunkInterrupted`.

    :meth:`_run` gathers every pending sub-request over one shared
    :class:`httpx.AsyncClient`, applies the failure-precedence rules, and
    combines; :meth:`resume` drives it through an ``anyio`` blocking
    portal so it works whether or not the caller is already inside an
    event loop. Concurrency is bounded by a per-run ``asyncio.Semaphore``
    (see :meth:`_run`), so sequential dispatch
    (``API_USGS_CONCURRENT=1``) is just a degenerate gather.

    A ``ChunkedCall`` is created internally when a :class:`ChunkPlan`
    executes; callers reach it via :attr:`ChunkInterrupted.call` on
    the exception raised by a mid-stream failure.

    :meth:`resume` is idempotent: :meth:`_run` iterates
    :meth:`ChunkPlan.iter_sub_args` (deterministic order) and skips
    any index whose result is already in ``self._chunks``. The
    completion set is a sparse ``dict[int, (df, response)]`` so the
    gather can record scattered completions (e.g. indices [0, 2, 5]
    after siblings [1, 3, 4] failed) and a subsequent ``resume`` only
    re-issues the missing indices.

    Parameters
    ----------
    plan : ChunkPlan
        The chunking plan to execute.
    fetch : Callable
        ``async def`` that issues a single sub-request, given the
        substituted args dict, and returns ``(frame, response)``.

    Attributes
    ----------
    plan : ChunkPlan
        The plan being driven (read-only after construction).
    fetch : Callable
        The async per-sub-request fetch function.
    finalize : Callable
        Transform applied to the combined result (see :data:`_Finalize`) at
        the terminal :meth:`_run` return, so a completed call yields the
        caller's finished shape. The ``partial_*`` accessors deliberately
        skip it and stay raw.
    partial_frame : pandas.DataFrame
        Raw combined frame of completed sub-requests (live; recomputed per
        access). Not finalized — call :meth:`resume` for the finished shape.
    partial_response : httpx.Response or None
        Raw aggregate response (canonical URL restored), or ``None`` when
        nothing has completed yet (live; recomputed per access).
    """

    def __init__(
        self,
        plan: ChunkPlan,
        fetch: _Fetch,
        retry_policy: RetryPolicy = _NO_RETRY,
        finalize: _Finalize = _passthrough_result,
    ) -> None:
        self.plan = plan
        self.fetch = fetch
        self.retry_policy = retry_policy
        self.finalize = finalize
        # Snapshot the ambient context at construction time — i.e. inside the
        # caller's ``with`` blocks (base URL, dialect, row cap, progress
        # reporter). :meth:`resume` runs every drive inside this snapshot, so
        # a *later* ``exc.call.resume()`` — which fires after those ``with``
        # blocks have exited and reset their ContextVars — still rebuilds
        # sub-requests against the original API's base URL/dialect rather than
        # the process defaults. ``build_request`` reads those ContextVars when
        # it reconstructs each sub-request, so the snapshot must outlive them.
        self._ctx = copy_context()
        # Completed (frame, response) pairs keyed by sub-args index; sparse
        # (gathered sub-requests complete out of order — see class docstring).
        # ``_run``'s ``track`` closure is the only writer, so ``dict`` insertion
        # order is completion order (relied on by :meth:`_combine_raw`).
        self._chunks: dict[int, tuple[pd.DataFrame, httpx.Response]] = {}

    def wrap_failure(self, exc: BaseException) -> ChunkInterrupted | None:
        """
        Build the matching :class:`ChunkInterrupted` carrying this
        call when ``exc`` is a recognized transient transport failure;
        return ``None`` for unrecognized failures so the caller can
        re-raise. Encapsulates the
        ``classify → instantiate-with-call-state`` recipe so
        :class:`ChunkedCall`'s private fields stay private.

        Parameters
        ----------
        exc : BaseException
            The exception raised by a sub-request.

        Returns
        -------
        ChunkInterrupted or None
            The matching :class:`ChunkInterrupted` subclass carrying this
            call for a recognized transient failure; ``None`` otherwise.
        """
        classification = _classify_chunk_error(exc)
        if classification is None:
            return None
        interrupted_class, retry_after = classification
        return interrupted_class(
            completed_chunks=self.completed_chunks,
            total_chunks=self.plan.total,
            call=self,
            retry_after=retry_after,
            cause=exc,
        )

    @property
    def completed_chunks(self) -> int:
        """Number of sub-requests completed so far."""
        return len(self._chunks)

    def _combine_raw(self) -> tuple[pd.DataFrame, httpx.Response]:
        """Assemble the raw ``(frame, response)`` from completed sub-requests,
        before :attr:`finalize` runs.

        Frames concatenate in sub-args *index* order (``sorted`` keys —
        deterministic, independent of parallel completion order). The
        aggregated response takes its headers from the most-recently-
        *completed* sub-request: the ``track`` closure in :meth:`_run`
        is the only writer of ``self._chunks`` and ``dict`` preserves
        insertion order, so the chunks' natural order is completion
        order and the last one carries the freshest
        ``x-ratelimit-remaining``.

        Returns
        -------
        tuple of (pandas.DataFrame, httpx.Response)
            The concatenated frame and the aggregated response, before
            :attr:`finalize` is applied.
        """
        frames = [self._chunks[i][0] for i in sorted(self._chunks)]
        responses = [response for _, response in self._chunks.values()]
        return (
            _combine_chunk_frames(frames),
            _combine_chunk_responses(responses, self.plan.canonical_url),
        )

    @property
    def partial_frame(self) -> pd.DataFrame:
        """
        Raw combined frame of sub-requests that have completed so far.

        Live — recomputed on each access so it reflects current state
        across resume attempts. Deliberately the *raw* combined frame
        (``_combine_raw``), NOT the finalized result: this is a cheap,
        side-effect-free snapshot for inspecting partial progress, so
        reading it (or building a :class:`ChunkInterrupted` around it)
        never triggers ``finalize`` work — which for OGC getters includes
        a schema network fetch on an empty frame. Use ``call.resume()``
        for the finalized result.

        Returns
        -------
        pandas.DataFrame
            Combined frame of completed sub-requests, or an empty
            ``DataFrame`` when nothing has completed.
        """
        if not self._chunks:
            return pd.DataFrame()
        return self._combine_raw()[0]

    @property
    def partial_response(self) -> httpx.Response | None:
        """
        Raw aggregate response with the canonical URL restored to the
        user's full original query.

        Live — recomputed on each access. Like :attr:`partial_frame`, this
        is the *raw* aggregate (an :class:`httpx.Response`), not the
        finalized result, so inspecting it is side-effect-free.

        Returns
        -------
        httpx.Response or None
            Aggregated response when at least one sub-request has
            completed, ``None`` otherwise.
        """
        if not self._chunks:
            return None
        return self._combine_raw()[1]

    def _pending(self) -> Iterator[tuple[int, dict[str, Any]]]:
        """
        Yield ``(index, sub_args)`` for sub-requests not yet completed.

        Walks :meth:`ChunkPlan.iter_sub_args` in deterministic order
        and skips any index already in ``self._chunks``. :meth:`_run`
        uses this to pick up exactly the sub-requests it still owes —
        first run and every resume alike.

        Yields
        ------
        tuple of (int, dict)
            The sub-args ``index`` and its ``sub_args`` dict for each
            sub-request not yet completed.
        """
        for index, sub_args in enumerate(self.plan.iter_sub_args()):
            if index not in self._chunks:
                yield index, sub_args

    def resume(self) -> tuple[pd.DataFrame, Any]:
        """
        Drive the chunked call to completion and return the combined result.

        Runs :meth:`_run` through an ``anyio`` blocking portal (a
        short-lived worker thread), so it works whether or not the caller
        is already inside an event loop (Jupyter / IPython / async apps).
        The portal copies the calling context, so the active progress
        reporter still reaches the sub-requests.

        Idempotent: only sub-requests whose index isn't already in
        ``self._chunks`` are re-issued. Sub-args order matches
        :meth:`ChunkPlan.iter_sub_args` and is deterministic, so a
        partial completion (sparse indices) resumes correctly.

        Returns
        -------
        df : pandas.DataFrame
            Combined data from every successful sub-request.
        response
            The finalized aggregate — a raw :class:`httpx.Response`
            (canonical URL, most-recently-completed sub-request's headers,
            cumulative elapsed time) by default, or whatever
            :attr:`finalize` produces (e.g. ``BaseMetadata`` for the OGC
            getters).

        Raises
        ------
        ChunkInterrupted
            On a mid-stream transient failure — 429, 5xx, or a bare
            transport error: :class:`QuotaExhausted` for 429,
            :class:`ServiceInterrupted` for the rest. The resumable
            handle is on ``exc.call`` — wait for the underlying
            condition to clear and call ``exc.call.resume()`` again.
        """
        # Drive inside the snapshot taken at construction (see ``__init__``).
        # ``start_blocking_portal`` copies the *calling* context into its
        # worker thread, and running here means that calling context is the
        # snapshot — so the base URL / dialect / row cap / progress reporter
        # active when the call was created reach the rebuilt sub-requests,
        # even when this is a resume fired long after the original ``with``
        # blocks exited.
        return self._ctx.run(self._resume_in_context)

    def _resume_in_context(self) -> tuple[pd.DataFrame, Any]:
        """Body of :meth:`resume`, run inside the captured context."""
        concurrency = _read_concurrency_env()
        with start_blocking_portal() as portal:
            # ``portal.call`` returns ``Any`` because ``functools.partial``
            # erases ``_run``'s return type; restore the declared tuple.
            return cast(
                "tuple[pd.DataFrame, Any]",
                portal.call(functools.partial(self._run, concurrency)),
            )

    async def _run(self, max_concurrent: int | None) -> tuple[pd.DataFrame, Any]:
        """
        Gather every pending sub-request over one shared
        :class:`httpx.AsyncClient` and return the combined, finalized result.

        Pending sub-requests (:meth:`_pending`) fan out under
        ``asyncio.gather`` with ``return_exceptions=True`` so completed
        sub-requests survive a sibling's transient failure. On a
        recognized transient (:class:`RateLimited`, :class:`ServiceUnavailable`,
        or a bare ``httpx.HTTPError`` / ``httpx.InvalidURL``) a
        :class:`ChunkInterrupted` subclass is raised carrying ``self`` on
        ``.call``; ``exc.call.resume()`` then re-issues only the unfinished
        indices through this same runner.

        The gather dispatches *every* pending sub-request at once, but an
        ``asyncio.Semaphore`` caps the number of concurrent fetches at
        ``N = max_concurrent`` — ``None`` lifts the cap, ``N=1`` runs them
        one at a time. The connection pool is sized to the same ``N``
        (``httpx.Limits(max_connections=N, max_keepalive_connections=N)``)
        so the in-flight fetches reuse keepalive connections.

        The semaphore, not the pool, is deliberately the throttle. If the
        pool throttled instead, the excess sub-requests would queue
        *inside* httpx waiting for a connection, and that wait counts
        against the pool-acquire timeout (60 s, from ``HTTPX_DEFAULTS``).
        A batch of slow pages that keeps every connection busy past that
        window would then trip ``httpx.PoolTimeout`` on the queued tail —
        a purely client-side failure that consumes the retry budget and
        surfaces as a spurious resumable ``ServiceInterrupted``. Holding
        sub-requests at the semaphore keeps them out of the pool until a
        slot frees, so the pool timeout only fires for a genuinely stuck
        connection.

        The shared client is published on :data:`_chunked_client` so
        the paginated-loop helpers reuse its connection pool.

        Parameters
        ----------
        max_concurrent : int or None
            Maximum sub-requests in flight (the semaphore value, and the
            connection-pool size). ``None`` lifts the cap entirely.

        Returns
        -------
        df : pandas.DataFrame
            Combined data from every sub-request.
        response
            The finalized aggregate — a raw :class:`httpx.Response`
            (canonical URL, most-recently-completed sub-request's headers,
            cumulative elapsed time) by default, or whatever
            :attr:`finalize` produces (e.g. ``BaseMetadata`` for OGC getters).

        Raises
        ------
        ChunkInterrupted
            On a transient sub-request failure. ``.call`` is ``self``,
            holding the sparse completed sub-requests; ``.call.resume()``
            re-issues the unfinished ones.
        """
        # The semaphore is the throttle; the pool is merely sized to match
        # it. Left at httpx's default client limits (``max_connections=100``,
        # keepalive 20) the pool would bottleneck a wider cap or churn
        # connections by keeping too few alive. See the method docstring for
        # why the gate can't be the pool itself. ``unbounded``
        # (``max_concurrent=None``) is a degenerate cap at the plan total — a
        # semaphore that can never block — so gated is the only code path.
        limits = httpx.Limits(
            max_connections=max_concurrent, max_keepalive_connections=max_concurrent
        )
        semaphore = asyncio.Semaphore(
            self.plan.total if max_concurrent is None else max_concurrent
        )

        async with httpx.AsyncClient(limits=limits, **HTTPX_DEFAULTS) as client:
            with _publish(client):
                reporter = _progress.current()
                if reporter is not None:
                    reporter.set_chunks(self.plan.total)

                async def fetch_gated(
                    args: dict[str, Any],
                ) -> tuple[pd.DataFrame, httpx.Response]:
                    """One fetch attempt under the concurrency gate.

                    The slot is held for the attempt's full duration —
                    every page of a paginated sub-request — but acquired
                    per *attempt* (this is what ``_retry`` re-invokes), so
                    a sub-request sleeping off a retry backoff isn't
                    holding a slot while it isn't touching the server.
                    """
                    async with semaphore:
                        return await self.fetch(args)

                async def track(
                    index: int, args: dict[str, Any]
                ) -> tuple[pd.DataFrame, httpx.Response]:
                    """One sub-request (with retry) + result-store + progress tick."""
                    result = await _retry(lambda: fetch_gated(args), self.retry_policy)
                    self._chunks[index] = result
                    if reporter is not None:
                        # Chunks finish out of order under gather, so tick the
                        # completed *count* rather than a positional index.
                        reporter.start_chunk(self.completed_chunks)
                    return result

                # Dispatch every pending sub-request concurrently; the
                # semaphore (via ``fetch_gated``) is the only throttle.
                # ``return_exceptions`` keeps completed pairs after a sibling
                # fails, so partial state stays recoverable via :meth:`resume`.
                # Failure precedence, in order:
                #   1. Cancellation / interrupt signals (CancelledError,
                #      KeyboardInterrupt, SystemExit — non-Exception) propagate
                #      unmodified; wrapping them as a transient would swallow
                #      the user's stop signal.
                #   2. A non-transient failure (a real bug — unrecognized by
                #      ``wrap_failure``) surfaces raw, so it isn't masked behind
                #      a resumable handle for a transient sibling that landed
                #      later.
                #   3. Only when every failure is a recognized transient do we
                #      raise the first as a resumable ``ChunkInterrupted``.
                results = await asyncio.gather(
                    *(track(index, args) for index, args in self._pending()),
                    return_exceptions=True,
                )
                failures = [r for r in results if isinstance(r, BaseException)]
                for exc in failures:
                    if not isinstance(exc, Exception):
                        raise exc
                first_transient: tuple[ChunkInterrupted, BaseException] | None = None
                for exc in failures:
                    interrupted = self.wrap_failure(exc)
                    if interrupted is None:
                        raise exc
                    if first_transient is None:
                        first_transient = (interrupted, exc)
                if first_transient is not None:
                    interrupted, exc = first_transient
                    raise interrupted from exc

        return self.finalize(*self._combine_raw())


def multi_value_chunked(
    *,
    build_request: Callable[..., httpx.Request],
    url_limit: int | None = None,
) -> Callable[[_Fetch], Callable[..., tuple[pd.DataFrame, Any]]]:
    """
    Decorate an async fetcher to transparently chunk over-budget requests.

    Returns a callable that builds a :class:`ChunkPlan` from ``args``,
    constructs a :class:`ChunkedCall` over the decorated
    ``async def fetch(args) -> (df, response)``, and drives it to
    completion via :meth:`ChunkedCall.resume`. The plan splits multi-value
    list params and the cql-text filter so each sub-request URL fits the
    byte limit; an already-fitting request is a one-step plan. See the
    module docstring for the concurrency model.

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
    ChunkedCall : Per-sub-request execution and resume semantics.
    """

    def decorator(fetch: _Fetch) -> Callable[..., tuple[pd.DataFrame, Any]]:
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
            return ChunkedCall(plan, fetch, retry_policy, finalize).resume()

        return wrapper

    return decorator
