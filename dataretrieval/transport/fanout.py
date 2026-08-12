"""Bounded, resumable fan-out execution over a plan of chunks.

A fan-out is one logical query the service forces into several requests. Two
unrelated reasons produce one:

- a Water Data / NGWMN query whose URL exceeds the server's byte limit, split
  along its multi-value axes by :class:`dataretrieval.ogc.planning.ChunkPlan`;
- a Water Use query naming several locations, which the NWDC accepts only one
  at a time.

Chunking is how you divide the data structurally; fan-out is how you distribute
the work operationally. The two are orthogonal, and only the first is protocol
knowledge: dividing a query needs the byte budget, the CQL2 grammar, and which
parameters are list-valued, while distributing the pieces needs none of it. Only
the Water Data / NGWMN case above involves chunking at all — Water Use fans out
without dividing anything, because the caller's locations were never one body to
split.

So this module owns distribution and nothing else: concurrency bounded by a
semaphore, per-attempt retry, deterministic failure precedence, sparse
completion tracking, and resume. It names no protocol concept — an adapter
supplies a :class:`FanOutPlan` (whatever structure it divided into, if any) and
an ``async def fetch(item) -> (df, response)``.

Concurrency: :meth:`FanOut._run` dispatches every pending chunk under one
``asyncio.gather`` sharing a single ``httpx.AsyncClient``. An
``asyncio.Semaphore`` -- not the client's connection pool, which is merely sized
to match -- caps the chunks in flight at ``N``; see :meth:`FanOut._run`
for why the gate must be the semaphore rather than the pool.
The ``concurrency`` setting resolves ``N`` -- a ``configure()`` block, then
``API_USGS_CONCURRENT``, then the config file, and per adapter as well as
package-wide: an integer N > 1 allows N chunks in flight; ``1`` forces
sequential dispatch; the literal ``unbounded`` lifts the cap. ``N`` bounds only
how many of a query's chunks are in flight at once
-- a client-side trade-off between open connections and fan-out latency. It does
not affect the API rate limit: a fanned-out call issues the same number of
chunks regardless of ``N``, so ``N`` changes their timing, not the total
request volume. The USGS API rate-limits by volume over time (HTTP 429), not by
simultaneity; set ``API_USGS_PAT`` to raise that quota. The default of 32 is a
conservative cap that keeps connection use modest. The fan-out runs in a
short-lived worker thread (an ``anyio`` blocking portal), so it works whether or
not the caller is already inside an event loop (Jupyter / IPython / async apps).

Retries: each chunk is retried on a transient failure (429, 5xx,
connect/read timeout) with exponential backoff + full jitter, honoring a server
``Retry-After`` when present. The ``retries`` setting caps them (default 4;
``0`` disables), resolved through the same chain and scopable per adapter. A
``Retry-After`` longer than the per-call ceiling escalates to a resumable
interruption.

Interruption: any mid-stream transient failure surfaces as a
:class:`~dataretrieval.interruptions.FanOutInterrupted` subclass carrying
``.call``, a :class:`FanOut` handle owning the already-completed chunk
state. Call ``.call.resume()`` once the underlying condition clears; only the
still-pending chunks are re-issued.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable, Iterator
from typing import Any, Generic, Protocol, TypeVar, cast

import httpx
import pandas as pd
from anyio.from_thread import start_blocking_portal

from dataretrieval import progress as _progress
from dataretrieval import settings as _settings
from dataretrieval._ambient import Ambient
from dataretrieval.combining import (
    _combine_chunk_frames,
    _combine_chunk_responses,
)
from dataretrieval.interruptions import (
    FanOutInterrupted,
    _classify_chunk_error,
    _walk_causes,
)
from dataretrieval.transport.http import network_error, open_async_client
from dataretrieval.transport.retry import _NO_RETRY, RetryPolicy
from dataretrieval.transport.retry import retry_async as _retry

#: One chunk's description, as the adapter's ``fetch`` wants it. The
#: executor never inspects it — see :class:`FanOutPlan`.
_Chunk = TypeVar("_Chunk")
#: The same thing in :class:`FanOutPlan`, where it only ever comes *out* of the
#: plan. Covariant so a ``list[httpx.Request]`` satisfies a plan of any
#: supertype, the way ``Iterable`` is covariant for the same reason.
_ChunkCo = TypeVar("_ChunkCo", covariant=True)

# The fan-out concurrency cap resolves through
# :func:`dataretrieval.settings.concurrency`, which owns the setting's name, its
# grammar (``1`` sequential, >1 bounded, ``unbounded`` uncapped) and its
# built-in default. Naming any of those here too would let this module and the
# chain disagree about what a value means. The concurrency model -- why the cap
# is a semaphore rather than the connection pool -- is in the module docstring.


# ---------------------------------------------------------------------------
# The plan contract
# ---------------------------------------------------------------------------


class FanOutPlan(Protocol[_ChunkCo]):
    """
    The contract a plan satisfies for a fan-out to execute it.

    A **plan** is defined in ``CONTEXT.md``. This protocol is that enumeration
    and nothing more, which is why it is named for the role it plays here
    rather than for its contents:
    :class:`~dataretrieval.ogc.planning.ChunkPlan` is *a* plan, and so is a
    plain list of requests.

    Deliberately the two standard protocols rather than bespoke members, since
    an enumeration of chunks is exactly ``__len__`` + ``__iter__`` -- so a
    plain ``list`` of pre-built
    requests satisfies this with no adapter class, and a real planner
    satisfies it by delegating (see
    :class:`~dataretrieval.ogc.planning.ChunkPlan`, whose domain vocabulary is
    ``total`` / ``iter_chunk_args``). Naming them ``total`` and
    ``iter_chunk_args`` here would mean two names for ``len`` that could report
    different counts, and a shim class for every adapter whose chunks
    are already a list.

    The item type is whatever an adapter's own ``fetch`` accepts: this executor
    passes each item through untouched and never inspects it, so the OGC
    getters yield kwargs dicts while Water Use yields ready
    :class:`httpx.Request` objects.

    Iteration order is load-bearing: :meth:`FanOut.resume` keys completed work
    by position, so a plan that yielded a different order on a second pass
    would resume the wrong chunks. ``len`` must agree with the number of
    items iteration yields — the usual contract for a sized collection.

    The identity of the query as a whole is *not* here: it is a value stamped
    on the combined response, not a property of how the work divides, so it is
    the ``canonical_url`` argument to :class:`FanOut`.
    """

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[_ChunkCo]: ...


# ---------------------------------------------------------------------------
# Shared per-call client
# ---------------------------------------------------------------------------

# The per-call ``httpx.AsyncClient``, published for the duration of
# ``FanOut._run`` so paginated-loop helpers reuse the same connection pool
# across every chunk. ``None`` outside a fan-out — paginated helpers then
# open their own short-lived client. Deliberately a plain ContextVar-backed
# ambient rather than a parameter: the fetch closure an adapter injects is often
# several frames below the client's owner.
_active_client: Ambient[httpx.AsyncClient | None] = Ambient("_fanout_client", None)


def active_client() -> httpx.AsyncClient | None:
    """
    Return the fan-out's currently-published client, or ``None``.

    Used by paginated-loop helpers to reuse the per-call connection pool.

    Returns
    -------
    httpx.AsyncClient or None
        The client published for the duration of a :meth:`FanOut._run`;
        ``None`` outside one.
    """
    return _active_client.get()


# ---------------------------------------------------------------------------
# Type aliases for the FanOut contract
# ---------------------------------------------------------------------------

# The per-chunk fetcher an adapter injects and ``FanOut`` drives: an
# ``async def fetch(item) -> (df, response)``, where ``item`` is whatever the
# adapter's plan yields.
_Fetch = Callable[[_Chunk], Awaitable[tuple[pd.DataFrame, httpx.Response]]]

# Caller-supplied transform applied to the combined result, so a resumed call
# returns the same shape as an un-interrupted one rather than the executor's raw
# ``(frame, httpx.Response)``. This keeps the executor generic: the OGC getters
# inject their post-processing (type coercion, column arrangement,
# ``BaseMetadata``) through ``_finalize_ogc``. The default is identity.
_Finalize = Callable[[pd.DataFrame, httpx.Response], tuple[pd.DataFrame, Any]]


def _passthrough_result(
    frame: pd.DataFrame, response: httpx.Response
) -> tuple[pd.DataFrame, Any]:
    """Default :data:`_Finalize`: return the raw combined pair unchanged."""
    return frame, response


class FanOut(Generic[_Chunk]):
    """
    Stateful handle for a fanned-out call.

    Holds the in-flight state (per-chunk frames and responses)
    and the async fetcher. A single :meth:`resume` entry point drives
    the call from wherever it is to completion — used both for the
    first invocation and for subsequent retries after a
    :class:`~dataretrieval.interruptions.FanOutInterrupted`.

    :meth:`_run` gathers every pending chunk over one shared
    :class:`httpx.AsyncClient`, applies the failure-precedence rules, and
    combines; :meth:`resume` drives it through an ``anyio`` blocking
    portal so it works whether or not the caller is already inside an
    event loop. Concurrency is bounded by a per-run ``asyncio.Semaphore``
    (see :meth:`_run`), so sequential dispatch
    (``API_USGS_CONCURRENT=1``) is just a degenerate gather.

    A ``FanOut`` is created internally when an adapter executes a plan;
    callers reach it via ``FanOutInterrupted.call`` on the exception raised
    by a mid-stream failure.

    :meth:`resume` is idempotent: :meth:`_run` iterates the plan
    (deterministic order) and skips
    any index whose result is already in ``self._chunks``. The
    completion set is a sparse ``dict[int, (df, response)]`` so the
    gather can record scattered completions (e.g. indices [0, 2, 5]
    after siblings [1, 3, 4] failed) and a subsequent ``resume`` only
    re-issues the missing indices.

    Parameters
    ----------
    plan : FanOutPlan
        The chunks to execute: anything sized and iterable, from a
        :class:`~dataretrieval.ogc.planning.ChunkPlan` to a plain ``list`` of
        pre-built requests.
    fetch : Callable
        ``async def`` that issues a single chunk, given one item from
        ``plan``, and returns ``(frame, response)``.
    client_options : dict, optional
        Extra ``httpx.AsyncClient`` options for the shared client this run
        opens (e.g. ``{"verify": False}``).
    default_concurrent : int, optional
        This adapter's preferred in-flight cap for when nothing is
        configured. Any resolved ``concurrency`` outranks it. Defaults to 32.
    canonical_url : str or None, optional
        URL identifying the query as a whole, restored onto the combined
        response so the caller sees the request they made rather than
        whichever chunk happened to land last. Also the destination
        :meth:`resume` labels its progress line with.
    service : str or None, optional
        Human-facing name of what is being retrieved (e.g. ``"daily"``,
        ``"nwdc"``), used to label the progress line :meth:`resume`
        opens. ``None`` leaves the line unlabelled.

    Attributes
    ----------
    plan : FanOutPlan
        The plan being driven (read-only after construction).
    fetch : Callable
        The async per-chunk fetch function.
    finalize : Callable
        Transform applied to the combined result (see :data:`_Finalize`) at
        the terminal :meth:`_run` return, so a completed call yields the
        caller's finished shape. The ``partial_*`` accessors deliberately
        skip it and stay raw.
    partial_frame : pandas.DataFrame
        Raw combined frame of completed chunks (live; recomputed per
        access). Not finalized — call :meth:`resume` for the finished shape.
    partial_response : httpx.Response or None
        Raw aggregate response (canonical URL restored), or ``None`` when
        nothing has completed yet (live; recomputed per access).
    """

    def __init__(
        self,
        plan: FanOutPlan[_Chunk],
        fetch: _Fetch[_Chunk],
        retry_policy: RetryPolicy = _NO_RETRY,
        finalize: _Finalize = _passthrough_result,
        client_options: dict[str, Any] | None = None,
        default_concurrent: int = _settings.DEFAULT_CONCURRENCY,
        *,
        canonical_url: str | None = None,
        service: str | None = None,
        adapter: str | None = None,
    ) -> None:
        self.plan = plan
        self.fetch = fetch
        self.retry_policy = retry_policy
        self.finalize = finalize
        self.canonical_url = canonical_url
        # Label for the progress line :meth:`resume` opens. It lives here, next
        # to ``canonical_url``, because this class is what emits the progress
        # events — see :meth:`resume`.
        self.service = service
        # Which adapter's settings this drive resolves, so a ``[ngwmn]`` table
        # or an ``NgwmnSettings`` reaches only NGWMN calls. Distinct from
        # ``service`` above, which is a *display label* for the progress line
        # and is variously a collection name or prose. ``None`` resolves
        # package-wide. See ADR 0010.
        self.adapter = adapter
        # This service's preferred cap for when nothing is configured. Resolved
        # at resume time, not here, so a setting that arrives after this call
        # was built still applies. Anything the chain resolves outranks it --
        # see :func:`dataretrieval.settings.concurrency` for why a service
        # preference must not override an explicit setting.
        self.default_concurrent = default_concurrent
        # Extra ``httpx.AsyncClient`` options merged into the shared client this
        # run opens (``verify`` for the Water Use ``ssl_check`` flag, say). The
        # executor owns client lifecycle, so an adapter with a per-call client
        # requirement has to hand it down rather than open its own — opening its
        # own would defeat the shared connection pool. Empty for OGC, which
        # exposes no such flag.
        self.client_options = client_options or {}
        # No ambient state is snapshotted here: everything a chunk rebuild
        # needs (base URL, dialect, row cap for the OGC getters) is closed
        # over by the adapter's ``fetch``/plan, so a *later*
        # ``exc.call.resume()`` — fired after the originating call
        # returned — rebuilds chunks against the values the call was
        # created with without this executor carrying adapter state.
        # Completed (frame, response) pairs keyed by sub-args index; sparse
        # (gathered chunks complete out of order — see class docstring).
        # ``_run``'s ``track`` closure is the only writer, so ``dict`` insertion
        # order is completion order (relied on by :meth:`_combine_raw`).
        self._chunks: dict[int, tuple[pd.DataFrame, httpx.Response]] = {}

    def wrap_failure(self, exc: BaseException) -> FanOutInterrupted | None:
        """
        Build the matching :class:`FanOutInterrupted` carrying this
        call when ``exc`` is a recognized transient transport failure;
        return ``None`` for unrecognized failures so the caller can
        re-raise. Encapsulates the
        ``classify → instantiate-with-call-state`` recipe so
        :class:`FanOut`'s private fields stay private.

        Parameters
        ----------
        exc : BaseException
            The exception raised by a chunk.

        Returns
        -------
        FanOutInterrupted or None
            The matching :class:`FanOutInterrupted` subclass carrying this
            call for a recognized transient failure; ``None`` otherwise.
        """
        classification = _classify_chunk_error(exc)
        if classification is None:
            return None
        interrupted_class, retry_after = classification
        return interrupted_class(
            completed_chunks=self.completed_chunks,
            total_chunks=len(self.plan),
            call=self,
            retry_after=retry_after,
            cause=exc,
        )

    def _normalize_failure(self, exc: BaseException) -> BaseException:
        """Map an explicitly caused transport failure into the public taxonomy."""
        for current in _walk_causes(exc):
            if isinstance(current, httpx.TransportError):
                wrapped = network_error(
                    self.canonical_url or "unknown service", current
                )
                wrapped.__cause__ = current
                return wrapped
        return exc

    @property
    def completed_chunks(self) -> int:
        """Number of chunks completed so far."""
        return len(self._chunks)

    def _combine_raw(self) -> tuple[pd.DataFrame, httpx.Response]:
        """Assemble the raw ``(frame, response)`` from completed chunks,
        before :attr:`finalize` runs.

        Frames concatenate in sub-args *index* order (``sorted`` keys —
        deterministic, independent of parallel completion order). The
        aggregated response takes its headers from the response with the
        lowest reported ``x-ratelimit-remaining`` value. If no response
        reports that header, it falls back to the last completed response;
        ``self._chunks`` preserves completion order because the ``track``
        closure in :meth:`_run` is its only writer.

        Returns
        -------
        tuple of (pandas.DataFrame, httpx.Response)
            The concatenated frame and the aggregated response, before
            :attr:`finalize` is applied.
        """
        return self._combine_frames(), self._combine_responses()

    def _combine_frames(self) -> pd.DataFrame:
        """Combine completed frames in deterministic chunk order."""
        return _combine_chunk_frames([self._chunks[i][0] for i in sorted(self._chunks)])

    def _combine_responses(self) -> httpx.Response:
        """Aggregate completed responses under the canonical request URL."""
        responses = [response for _, response in self._chunks.values()]
        return _combine_chunk_responses(responses, self.canonical_url)

    @property
    def partial_frame(self) -> pd.DataFrame:
        """
        Raw combined frame of chunks that have completed so far.

        Live — recomputed on each access so it reflects current state
        across resume attempts. Deliberately the *raw* combined frame
        (``_combine_frames``), NOT the finalized result: this is a cheap,
        side-effect-free snapshot for inspecting partial progress, so
        reading it (or building a :class:`FanOutInterrupted` around it)
        never triggers ``finalize`` work — which for OGC getters includes
        a schema network fetch on an empty frame. Use ``call.resume()``
        for the finalized result.

        Returns
        -------
        pandas.DataFrame
            Combined frame of completed chunks, or an empty
            ``DataFrame`` when nothing has completed.
        """
        return self._combine_frames() if self._chunks else pd.DataFrame()

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
            Aggregated response when at least one chunk has
            completed, ``None`` otherwise.
        """
        return self._combine_responses() if self._chunks else None

    def _pending(self) -> Iterator[tuple[int, _Chunk]]:
        """
        Yield ``(index, item)`` for chunks not yet completed.

        Iterates the plan in its deterministic order and skips any index
        already in ``self._chunks``. :meth:`_run` uses this to pick up
        exactly the chunks it still owes — the mechanism behind
        idempotent resume.
        """
        for index, item in enumerate(self.plan):
            if index not in self._chunks:
                yield index, item

    def resume(self) -> tuple[pd.DataFrame, Any]:
        """
        Drive the call to completion and return the combined result.

        Opens the progress line for the drive and runs :meth:`_run` through
        an ``anyio`` blocking portal (a short-lived worker thread), so it
        works whether or not the caller is already inside an event loop
        (Jupyter / IPython / async apps). The portal copies the calling
        context, so the active progress reporter still reaches the
        chunks.

        This executor is what emits progress events, so it is also what owns
        the reporter's lifetime: an adapter that drives a ``FanOut`` gets the
        line for free instead of having to remember a separate
        ``with progress_context(...)`` block. A reporter already active
        (a nested getter, or a caller's own context) is reused unchanged.

        Idempotent: only chunks whose index isn't already in
        ``self._chunks`` are re-issued. Item order is the plan's own and
        is deterministic, so a partial completion (sparse indices)
        resumes correctly.

        Returns
        -------
        df : pandas.DataFrame
            Combined data from every successful chunk.
        response
            The finalized aggregate — a raw :class:`httpx.Response`
            (canonical URL, headers from the response with the lowest reported
            remaining quota, and summed response elapsed durations) by default,
            or whatever :attr:`finalize` produces (e.g. ``BaseMetadata`` for
            the OGC getters).

        Raises
        ------
        FanOutInterrupted
            On a mid-stream transient failure — 429, 5xx, or a bare
            transport error: :class:`~dataretrieval.interruptions.QuotaExhausted`
            for 429, :class:`~dataretrieval.interruptions.ServiceInterrupted`
            for the rest. The resumable handle is on ``exc.call`` — wait for
            the underlying condition to clear and call ``exc.call.resume()``
            again.
        """
        # Open the line here, in the *calling* context, so an outer
        # reporter (a nested getter, or the caller's own ``progress_context``)
        # is the one found and reused; a drive that finds none gets a fresh
        # line, which is what makes a resume long after the interruption
        # report progress at all. ``start_blocking_portal`` copies this
        # calling context into its worker thread, so the active reporter
        # reaches the chunks. Chunk-rebuild state (base URL, dialect, row
        # cap) travels in the adapter's ``fetch`` closure, not in ambient
        # state, so no construction-time snapshot is needed for a resume
        # fired after the originating call returned (see ``__init__``).
        with _progress.progress_context(
            service=self.service, target_url=self.canonical_url
        ):
            # Resolve concurrency here, per drive, rather than at construction.
            # It is the one dial a caller adjusts precisely *while* retrying --
            # the documented recovery from QuotaExhausted is to wait and
            # re-issue more gently -- so a ``configure()`` block entered
            # between the interruption and the resume has to win.
            concurrency = _settings.concurrency(
                self.default_concurrent, adapter=self.adapter
            )
            with start_blocking_portal() as portal:
                # ``portal.call`` returns ``Any`` because ``functools.partial``
                # erases ``_run``'s return type; restore the declared tuple.
                return cast(
                    "tuple[pd.DataFrame, Any]",
                    portal.call(functools.partial(self._run, concurrency)),
                )

    async def _run(self, max_concurrent: int | None) -> tuple[pd.DataFrame, Any]:
        """
        Gather every pending chunk over one shared
        :class:`httpx.AsyncClient` and return the combined, finalized result.

        Pending chunks (:meth:`_pending`) fan out under
        ``asyncio.gather`` with ``return_exceptions=True`` so completed
        chunks survive a sibling's transient failure. On a
        recognized transient (:class:`~dataretrieval.exceptions.RateLimited`,
        :class:`~dataretrieval.exceptions.ServiceUnavailable`, or a bare
        ``httpx.HTTPError`` / ``httpx.InvalidURL``) a
        :class:`FanOutInterrupted` subclass is raised carrying ``self`` on
        ``.call``; ``exc.call.resume()`` then re-issues only the unfinished
        indices through this same runner.

        The gather dispatches *every* pending chunk at once, but an
        ``asyncio.Semaphore`` caps the number of concurrent fetches at
        ``N = max_concurrent`` — ``None`` lifts the cap, ``N=1`` runs them
        one at a time. The connection pool is sized to the same ``N``
        (``httpx.Limits(max_connections=N, max_keepalive_connections=N)``)
        so the in-flight fetches reuse keepalive connections.

        The semaphore, not the pool, is deliberately the throttle. If the
        pool throttled instead, the excess chunks would queue
        *inside* httpx waiting for a connection, and that wait counts
        against the pool-acquire timeout (60 s, from ``HTTPX_ASYNC_DEFAULTS``).
        A batch of slow pages that keeps every connection busy past that
        window would then trip ``httpx.PoolTimeout`` on the queued tail —
        a purely client-side failure that consumes the retry budget and
        surfaces as a spurious resumable ``ServiceInterrupted``. Holding
        chunks at the semaphore keeps them out of the pool until a
        slot frees, so the pool timeout only fires for a genuinely stuck
        connection.

        The shared client is published on :data:`_active_client` so
        the paginated-loop helpers reuse its connection pool.

        Parameters
        ----------
        max_concurrent : int or None
            Maximum chunks in flight (the semaphore value, and the
            connection-pool size). ``None`` lifts the cap entirely.

        Returns
        -------
        df : pandas.DataFrame
            Combined data from every chunk.
        response
            The finalized aggregate — a raw :class:`httpx.Response`
            (canonical URL, headers from the response with the lowest reported
            remaining quota, and summed response elapsed durations) by default,
            or whatever :attr:`finalize` produces.

        Raises
        ------
        FanOutInterrupted
            On a transient chunk failure. ``.call`` is ``self``,
            holding the sparse completed chunks; ``.call.resume()``
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
            len(self.plan) if max_concurrent is None else max_concurrent
        )

        async with open_async_client(limits=limits, **self.client_options) as client:
            with _active_client(client):
                reporter = _progress.current()
                if reporter is not None:
                    reporter.set_chunks(len(self.plan))

                async def track(
                    index: int, item: _Chunk
                ) -> tuple[pd.DataFrame, httpx.Response]:
                    """One chunk (with retry) + result-store + progress tick."""
                    result = await _retry(
                        lambda: self.fetch(item), self.retry_policy, gate=semaphore
                    )
                    self._chunks[index] = result
                    if reporter is not None:
                        # Chunks finish out of order under gather, so tick the
                        # completed *count* rather than a positional index.
                        reporter.start_chunk(self.completed_chunks)
                    return result

                # Dispatch every pending chunk concurrently; the
                # semaphore (held by ``_retry`` per attempt) is the only throttle.
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
                #      raise the first as a resumable ``FanOutInterrupted``.
                results = await asyncio.gather(
                    *(track(index, item) for index, item in self._pending()),
                    return_exceptions=True,
                )
                failures = [r for r in results if isinstance(r, BaseException)]
                for exc in failures:
                    if not isinstance(exc, Exception):
                        raise exc
                # Classify first, build once. Every failure has to be
                # examined -- a non-transient sibling must surface raw -- but
                # only the first transient is ever raised. Asking
                # ``wrap_failure`` per failure would snapshot the combined
                # frame N times (a full concat over every completed
                # chunk) and discard all but one, which a batch of
                # chunks failing together makes routine.
                first_transient: BaseException | None = None
                for exc in failures:
                    if _classify_chunk_error(exc) is None:
                        raise self._normalize_failure(exc)
                    if first_transient is None:
                        first_transient = exc
                if first_transient is not None:
                    interrupted = self.wrap_failure(first_transient)
                    if interrupted is None:
                        # Unreachable: classified as transient just above.
                        raise self._normalize_failure(first_transient)
                    raise interrupted from first_transient

        return self.finalize(*self._combine_raw())


__all__ = [
    "FanOut",
    "FanOutPlan",
    "active_client",
]
