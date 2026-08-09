"""Bounded, resumable fan-out execution over a plan of sub-requests.

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
an ``async def fetch(args) -> (df, response)``.

Concurrency: :meth:`FanOut._run` dispatches every pending sub-request under one
``asyncio.gather`` sharing a single ``httpx.AsyncClient``. An
``asyncio.Semaphore`` -- not the client's connection pool, which is merely sized
to match -- caps the sub-requests in flight at ``N``; see :meth:`FanOut._run`
for why the gate must be the semaphore rather than the pool.
``API_USGS_CONCURRENT`` resolves ``N``: an integer N > 1 allows N sub-requests
in flight; ``1`` forces sequential dispatch; the literal ``unbounded`` lifts the
cap. ``N`` bounds only how many of a query's sub-requests are in flight at once
-- a client-side trade-off between open connections and fan-out latency. It does
not affect the API rate limit: a fanned-out call issues the same number of
sub-requests regardless of ``N``, so ``N`` changes their timing, not the total
request volume. The USGS API rate-limits by volume over time (HTTP 429), not by
simultaneity; set ``API_USGS_PAT`` to raise that quota. The default of 32 is a
conservative cap that keeps connection use modest. The fan-out runs in a
short-lived worker thread (an ``anyio`` blocking portal), so it works whether or
not the caller is already inside an event loop (Jupyter / IPython / async apps).

Retries: each sub-request is retried on a transient failure (429, 5xx,
connect/read timeout) with exponential backoff + full jitter, honoring a server
``Retry-After`` when present. ``API_USGS_RETRIES`` sets the cap (default 4;
``0`` disables). A ``Retry-After`` longer than the per-call ceiling escalates to
a resumable interruption.

Interruption: any mid-stream transient failure surfaces as a
:class:`~dataretrieval.interruptions.FanOutInterrupted` subclass carrying
``.call``, a :class:`FanOut` handle owning the already-completed sub-request
state. Call ``.call.resume()`` once the underlying condition clears; only the
still-pending sub-requests are re-issued.
"""

from __future__ import annotations

import asyncio
import functools
import os
from collections.abc import Awaitable, Callable, Iterator
from contextvars import copy_context
from typing import Any, Protocol, cast

import httpx
import pandas as pd
from anyio.from_thread import start_blocking_portal

from dataretrieval import progress as _progress
from dataretrieval._ambient import Ambient
from dataretrieval.combining import (
    _combine_chunk_frames,
    _combine_chunk_responses,
)
from dataretrieval.exceptions import ConfigurationError
from dataretrieval.interruptions import FanOutInterrupted, _classify_chunk_error
from dataretrieval.transport.http import network_error, open_async_client
from dataretrieval.transport.retry import _NO_RETRY, RetryPolicy
from dataretrieval.transport.retry import retry_async as _retry

# Fan-out concurrency cap, read at call time (not import) so test
# ``monkeypatch.setenv`` applies. Value grammar in :func:`_read_concurrency_env`;
# the concurrency model is in the module docstring.
_CONCURRENCY_ENV = "API_USGS_CONCURRENT"
_CONCURRENCY_DEFAULT = 32
_CONCURRENCY_UNBOUNDED = "unbounded"


def _resolve_concurrency(default: int = _CONCURRENCY_DEFAULT) -> int | None:
    """
    Resolve the parallelism cap: the general setting, or a module's default.

    ``API_USGS_CONCURRENT`` is the general knob and applies to every fanned-out
    call in the package. A module may pass a different ``default`` when its
    service warrants one — Water Use ships a lower figure than the OGC getters,
    because the NWDC is only stress-tested to that level.

    The ordering is deliberate: an explicitly set environment variable wins over
    a module's default, never the reverse. A module that could override the
    general setting would make ``API_USGS_CONCURRENT=1`` a lie — the user
    dialing concurrency down to be polite to the service would find one adapter
    quietly ignoring them, which is precisely the defect this consolidates away.
    Module defaults express "absent instruction, this service prefers N"; they
    do not express "this service knows better than you".

    Parameters
    ----------
    default : int
        Cap to use when ``API_USGS_CONCURRENT`` is unset or empty.

    Returns
    -------
    int or None
        ``1`` for sequential dispatch (one sub-request at a time); an
        integer >1 for bounded concurrency; ``None`` to disable the
        per-call cap entirely (the ``unbounded`` keyword).
    """
    raw = os.environ.get(_CONCURRENCY_ENV)
    if raw is None:
        return default
    raw = raw.strip()
    if raw == "":
        return default
    if raw.lower() == _CONCURRENCY_UNBOUNDED:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(
            f"{_CONCURRENCY_ENV} must be a positive integer or "
            f"'{_CONCURRENCY_UNBOUNDED}'; got {raw!r}."
        ) from exc
    if value < 1:
        raise ConfigurationError(
            f"{_CONCURRENCY_ENV} must be >= 1 (got {value}); use "
            f"'{_CONCURRENCY_UNBOUNDED}' to disable the cap."
        )
    return value


# ---------------------------------------------------------------------------
# The plan contract
# ---------------------------------------------------------------------------


class FanOutPlan(Protocol):
    """
    A fan-out's shape: how many sub-requests, their arguments, and the
    identity of the whole query.

    Structural, not nominal: an implementation satisfies this by having the
    three members, not by inheriting. That is the right relationship here
    because the two implementations share an interface and no implementation
    at all. :class:`~dataretrieval.ogc.planning.ChunkPlan` derives its
    sub-requests from a URL byte budget over multi-value axes; a Water Use
    plan simply lists the locations the caller named. Neither has anything
    the other could inherit.

    Attributes
    ----------
    total : int
        Number of sub-requests in the plan. Bounds progress reporting and
        sizes the degenerate semaphore when concurrency is unbounded.
    canonical_url : str or None
        URL identifying the query as a whole, restored onto the combined
        response so the caller sees the request they made rather than
        whichever sub-request happened to land last.
    """

    @property
    def total(self) -> int: ...

    @property
    def canonical_url(self) -> str | None: ...

    def iter_sub_args(self) -> Iterator[dict[str, Any]]:
        """
        Yield each sub-request's arguments, in a deterministic order.

        Order is load-bearing: :meth:`FanOut.resume` keys completed work by
        position, so a plan that yielded a different order on a second pass
        would resume the wrong sub-requests.
        """
        ...


# ---------------------------------------------------------------------------
# Shared per-call client
# ---------------------------------------------------------------------------

# The per-call ``httpx.AsyncClient``, published for the duration of
# ``FanOut._run`` so paginated-loop helpers reuse the same connection pool
# across every sub-request. ``None`` outside a fan-out — paginated helpers then
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

# The per-sub-request fetcher an adapter injects and ``FanOut`` drives:
# an ``async def fetch(args) -> (df, response)``.
_Fetch = Callable[[dict[str, Any]], Awaitable[tuple[pd.DataFrame, httpx.Response]]]

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


class FanOut:
    """
    Stateful handle for a fanned-out call.

    Holds the in-flight state (per-sub-request frames and responses)
    and the async fetcher. A single :meth:`resume` entry point drives
    the call from wherever it is to completion — used both for the
    first invocation and for subsequent retries after a
    :class:`~dataretrieval.interruptions.FanOutInterrupted`.

    :meth:`_run` gathers every pending sub-request over one shared
    :class:`httpx.AsyncClient`, applies the failure-precedence rules, and
    combines; :meth:`resume` drives it through an ``anyio`` blocking
    portal so it works whether or not the caller is already inside an
    event loop. Concurrency is bounded by a per-run ``asyncio.Semaphore``
    (see :meth:`_run`), so sequential dispatch
    (``API_USGS_CONCURRENT=1``) is just a degenerate gather.

    A ``FanOut`` is created internally when an adapter executes a plan;
    callers reach it via ``FanOutInterrupted.call`` on the exception raised
    by a mid-stream failure.

    :meth:`resume` is idempotent: :meth:`_run` iterates
    :meth:`FanOutPlan.iter_sub_args` (deterministic order) and skips
    any index whose result is already in ``self._chunks``. The
    completion set is a sparse ``dict[int, (df, response)]`` so the
    gather can record scattered completions (e.g. indices [0, 2, 5]
    after siblings [1, 3, 4] failed) and a subsequent ``resume`` only
    re-issues the missing indices.

    Parameters
    ----------
    plan : FanOutPlan
        The plan to execute.
    fetch : Callable
        ``async def`` that issues a single sub-request, given the
        substituted args dict, and returns ``(frame, response)``.
    client_options : dict, optional
        Extra ``httpx.AsyncClient`` options for the shared client this run
        opens (e.g. ``{"verify": False}``).
    default_concurrent : int, optional
        This service's preferred in-flight cap when ``API_USGS_CONCURRENT``
        is unset. Defaults to 32.

    Attributes
    ----------
    plan : FanOutPlan
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
        plan: FanOutPlan,
        fetch: _Fetch,
        retry_policy: RetryPolicy = _NO_RETRY,
        finalize: _Finalize = _passthrough_result,
        client_options: dict[str, Any] | None = None,
        default_concurrent: int = _CONCURRENCY_DEFAULT,
    ) -> None:
        self.plan = plan
        self.fetch = fetch
        self.retry_policy = retry_policy
        self.finalize = finalize
        # This service's preferred cap when the user has not set
        # ``API_USGS_CONCURRENT``. Resolved at resume time, not here, so a
        # test's ``monkeypatch.setenv`` still applies. See
        # :func:`_resolve_concurrency` for why the env var outranks it.
        self.default_concurrent = default_concurrent
        # Extra ``httpx.AsyncClient`` options merged into the shared client this
        # run opens (``verify`` for the Water Use ``ssl_check`` flag, say). The
        # executor owns client lifecycle, so an adapter with a per-call client
        # requirement has to hand it down rather than open its own — opening its
        # own would defeat the shared connection pool. Empty for OGC, which
        # exposes no such flag.
        self.client_options = client_options or {}
        # Snapshot the ambient context at construction time — i.e. inside the
        # caller's ``with`` blocks (base URL, dialect, row cap, progress
        # reporter). :meth:`resume` runs every drive inside this snapshot, so
        # a *later* ``exc.call.resume()`` — which fires after those ``with``
        # blocks have exited and reset their ContextVars — still rebuilds
        # sub-requests against the original API's base URL/dialect rather than
        # the process defaults. The adapter's request builder reads those
        # ContextVars when it reconstructs each sub-request, so the snapshot
        # must outlive them. The mechanism is generic; which ambients matter is
        # the adapter's business.
        self._ctx = copy_context()
        # Completed (frame, response) pairs keyed by sub-args index; sparse
        # (gathered sub-requests complete out of order — see class docstring).
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
            The exception raised by a sub-request.

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
            total_chunks=self.plan.total,
            call=self,
            retry_after=retry_after,
            cause=exc,
        )

    def _normalize_failure(self, exc: BaseException) -> BaseException:
        """Map an explicitly caused transport failure into the public taxonomy."""
        seen: set[int] = set()
        current: BaseException | None = exc
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if isinstance(current, httpx.TransportError):
                wrapped = network_error(
                    self.plan.canonical_url or "unknown service", current
                )
                wrapped.__cause__ = current
                return wrapped
            current = current.__cause__
        return exc

    @property
    def completed_chunks(self) -> int:
        """Number of sub-requests completed so far."""
        return len(self._chunks)

    def _combine_raw(self) -> tuple[pd.DataFrame, httpx.Response]:
        """Assemble the raw ``(frame, response)`` from completed sub-requests,
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
        """Combine completed frames in deterministic sub-request order."""
        return _combine_chunk_frames([self._chunks[i][0] for i in sorted(self._chunks)])

    def _combine_responses(self) -> httpx.Response:
        """Aggregate completed responses under the canonical request URL."""
        responses = [response for _, response in self._chunks.values()]
        return _combine_chunk_responses(responses, self.plan.canonical_url)

    @property
    def partial_frame(self) -> pd.DataFrame:
        """
        Raw combined frame of sub-requests that have completed so far.

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
            Combined frame of completed sub-requests, or an empty
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
            Aggregated response when at least one sub-request has
            completed, ``None`` otherwise.
        """
        return self._combine_responses() if self._chunks else None

    def _pending(self) -> Iterator[tuple[int, dict[str, Any]]]:
        """
        Yield ``(index, sub_args)`` for sub-requests not yet completed.

        Walks :meth:`FanOutPlan.iter_sub_args` in deterministic order
        and skips any index already in ``self._chunks``. :meth:`_run`
        uses this to pick up exactly the sub-requests it still owes —
        the mechanism behind idempotent resume.
        """
        for index, args in enumerate(self.plan.iter_sub_args()):
            if index not in self._chunks:
                yield index, args

    def resume(self) -> tuple[pd.DataFrame, Any]:
        """
        Drive the call to completion and return the combined result.

        Runs :meth:`_run` through an ``anyio`` blocking portal (a
        short-lived worker thread), so it works whether or not the caller
        is already inside an event loop (Jupyter / IPython / async apps).
        The portal copies the calling context, so the active progress
        reporter still reaches the sub-requests.

        Idempotent: only sub-requests whose index isn't already in
        ``self._chunks`` are re-issued. Sub-args order matches
        :meth:`FanOutPlan.iter_sub_args` and is deterministic, so a
        partial completion (sparse indices) resumes correctly.

        Returns
        -------
        df : pandas.DataFrame
            Combined data from every successful sub-request.
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
        # Drive inside the snapshot taken at construction (see ``__init__``).
        # ``start_blocking_portal`` copies the *calling* context into its
        # worker thread, and running here means that calling context is the
        # snapshot — so the base URL / dialect / row cap / progress reporter
        # active when the call was created reach the rebuilt sub-requests,
        # even when this is a resume fired long after the original ``with``
        # blocks exited.
        # Do not resurrect the reporter captured with the adapter context: the
        # originating progress context closes it when an interruption escapes.
        # A later resume uses whichever reporter is active now (or none), while
        # retaining every other captured ambient needed to rebuild requests.
        return self._ctx.run(self._resume_in_context, _progress.current())

    def _resume_in_context(
        self, reporter: _progress.ProgressReporter | None
    ) -> tuple[pd.DataFrame, Any]:
        """Body of :meth:`resume`, run inside the captured context."""
        concurrency = _resolve_concurrency(self.default_concurrent)
        with _progress._use_reporter(reporter), start_blocking_portal() as portal:
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
        recognized transient (:class:`~dataretrieval.exceptions.RateLimited`,
        :class:`~dataretrieval.exceptions.ServiceUnavailable`, or a bare
        ``httpx.HTTPError`` / ``httpx.InvalidURL``) a
        :class:`FanOutInterrupted` subclass is raised carrying ``self`` on
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
        against the pool-acquire timeout (60 s, from ``HTTPX_ASYNC_DEFAULTS``).
        A batch of slow pages that keeps every connection busy past that
        window would then trip ``httpx.PoolTimeout`` on the queued tail —
        a purely client-side failure that consumes the retry budget and
        surfaces as a spurious resumable ``ServiceInterrupted``. Holding
        sub-requests at the semaphore keeps them out of the pool until a
        slot frees, so the pool timeout only fires for a genuinely stuck
        connection.

        The shared client is published on :data:`_active_client` so
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
            (canonical URL, headers from the response with the lowest reported
            remaining quota, and summed response elapsed durations) by default,
            or whatever :attr:`finalize` produces.

        Raises
        ------
        FanOutInterrupted
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

        async with open_async_client(limits=limits, **self.client_options) as client:
            with _active_client(client):
                reporter = _progress.current()
                if reporter is not None:
                    reporter.set_chunks(self.plan.total)

                async def track(
                    index: int, args: dict[str, Any]
                ) -> tuple[pd.DataFrame, httpx.Response]:
                    """One sub-request (with retry) + result-store + progress tick."""
                    result = await _retry(
                        lambda: self.fetch(args), self.retry_policy, gate=semaphore
                    )
                    self._chunks[index] = result
                    if reporter is not None:
                        # Chunks finish out of order under gather, so tick the
                        # completed *count* rather than a positional index.
                        reporter.start_chunk(self.completed_chunks)
                    return result

                # Dispatch every pending sub-request concurrently; the
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
                    *(track(index, args) for index, args in self._pending()),
                    return_exceptions=True,
                )
                failures = [r for r in results if isinstance(r, BaseException)]
                for exc in failures:
                    if not isinstance(exc, Exception):
                        raise exc
                first_transient: tuple[FanOutInterrupted, BaseException] | None = None
                for exc in failures:
                    interrupted = self.wrap_failure(exc)
                    if interrupted is None:
                        raise self._normalize_failure(exc)
                    if first_transient is None:
                        first_transient = (interrupted, exc)
                if first_transient is not None:
                    interrupted, exc = first_transient
                    raise interrupted from exc

        return self.finalize(*self._combine_raw())


__all__ = [
    "FanOut",
    "FanOutPlan",
    "active_client",
]
