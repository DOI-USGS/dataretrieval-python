"""Bounded retry policy and transient-failure classification."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import NamedTuple, TypeVar

import httpx

from dataretrieval import progress as _progress
from dataretrieval import settings as _settings
from dataretrieval.exceptions import (
    ConfigurationError,
    NetworkError,
    TransientError,
)
from dataretrieval.interruptions import _deterministic_failure
from dataretrieval.transport.liveness import (
    credit_wait,
    elapsed_since_progress,
    note_progress,
)

# Which error statuses a request may be re-sent for. Both are narrower than
# :attr:`~dataretrieval.exceptions.DataRetrievalError.retryable`, deliberately:
# that field tells a caller re-issuing *might* work, while spending someone's
# quota unasked needs a stricter bar.
#
# The default keeps every 5xx, because for a query interface like the Water Data
# OGC API a 500 is an upstream hiccup and re-sending is how a chunked call rides
# one out. The gateway-only set is for the single-shot adapters whose services
# answer a *bad query* with a 500 -- WQP does that for an over-large request,
# StreamStats for out-of-network coordinates -- where re-sending multiplies load
# on a request that can never succeed and delays the caller's error.
_RETRYABLE_STATUSES = frozenset({429, *range(500, 600)})
_GATEWAY_STATUSES = frozenset({429, 502, 503, 504})
_RETRY_BASE_BACKOFF = 0.5
_RETRY_MAX_BACKOFF = 30.0
_RETRY_AFTER_CAP = 60.0
# Most a server-named delay is nudged by, to keep chunks handed the same
# hint from waking together. Small on purpose: the server named the wait, so
# jitter here decorrelates rather than extends it.
_RETRY_AFTER_JITTER = 1.0
# Attempts the no-progress budget never withholds; see RetryPolicy.allows_wait.
_STALL_EXEMPT_ATTEMPTS = 1

_T = TypeVar("_T")


@dataclass(frozen=True)
class RetryPolicy:
    """Immutable bounded exponential-backoff-with-full-jitter policy.

    Two independent bounds decide when to stop: :attr:`max_retries` caps *how
    many* attempts a failure gets, and :attr:`stall_timeout` caps *how long* a
    call may go on receiving nothing.
    """

    #: Attempts after the first. ``0`` disables retry entirely. The default is
    #: ``config``'s, not a second copy of it: a directly-constructed policy and
    #: one built by :meth:`from_settings` must agree on the retry budget.
    max_retries: int = _settings.DEFAULT_RETRIES
    #: First backoff ceiling; doubles per attempt up to :attr:`max_backoff`.
    base_backoff: float = _RETRY_BASE_BACKOFF
    #: Ceiling for our own exponential backoff.
    max_backoff: float = _RETRY_MAX_BACKOFF
    #: Longest server-named ``Retry-After`` we are willing to wait out inline.
    #: A longer one stops the retry and surfaces a resumable transient, so the
    #: caller decides whether to wait rather than blocking inside the request.
    retry_after_cap: float = _RETRY_AFTER_CAP
    #: Error statuses this policy will re-send for. Defaults to 429 and every
    #: 5xx; single-shot adapters whose service reports a rejected query as a 500
    #: pass :data:`_GATEWAY_STATUSES` instead.
    retryable_statuses: frozenset[int] = _RETRYABLE_STATUSES
    #: Longest a call may go *without receiving any data* before retrying stops
    #: and the failure surfaces -- the total of every silent attempt and every
    #: unsanctioned wait since the last page arrived (a server-named
    #: ``Retry-After`` and time queued behind the concurrency gate are excused;
    #: see :meth:`allows_wait`). Bounds the wall-clock cost of a dead
    #: connection or a service that keeps refusing, which :attr:`max_retries`
    #: alone does not: it counts attempts, not seconds, so four retries of a
    #: request that times out after a minute is four silent minutes. Progress
    #: resets the clock (see
    #: :func:`~dataretrieval.transport.liveness.note_progress`), so a slow but
    #: productive download is never cut short, and an attempt already in flight
    #: is never interrupted. ``0`` disables the bound. See :meth:`allows_wait`
    #: for how it is applied.
    stall_timeout: float = _settings.DEFAULT_STALL_TIMEOUT

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ConfigurationError(
                f"max_retries must be >= 0 (got {self.max_retries})."
            )
        if (
            self.base_backoff < 0
            or self.max_backoff < 0
            or self.retry_after_cap < 0
            or self.stall_timeout < 0
        ):
            raise ConfigurationError("retry backoff settings must be non-negative.")

    @classmethod
    def from_settings(
        cls,
        retryable_statuses: frozenset[int] | None = None,
        *,
        adapter: str | None = None,
    ) -> RetryPolicy:
        """Build a policy from the effective configuration and module defaults.

        ``max_retries`` and ``stall_timeout`` both resolve through
        :mod:`dataretrieval.settings` -- a ``configure()`` block, then the
        environment variable, then the config file. ``adapter`` names the
        adapter this policy is for, so a ``[wqp] retries = 2`` table applies to
        WQP calls and nothing else; ``None`` resolves package-wide. The pure
        timing knobs stay module constants read at call time so a test's
        ``monkeypatch.setattr`` still applies.
        """
        statuses = (
            _RETRYABLE_STATUSES if retryable_statuses is None else retryable_statuses
        )
        return cls(
            retryable_statuses=statuses,
            max_retries=_settings.retries(adapter=adapter),
            base_backoff=_RETRY_BASE_BACKOFF,
            max_backoff=_RETRY_MAX_BACKOFF,
            retry_after_cap=_RETRY_AFTER_CAP,
            stall_timeout=_settings.stall_timeout(adapter=adapter),
        )

    def should_retry(self, attempt: int, retry_after: float | None) -> bool:
        """Whether a just-failed 1-based attempt warrants another try."""
        if attempt > self.max_retries:
            return False
        return retry_after is None or retry_after <= self.retry_after_cap

    def allows_wait(
        self,
        attempt: int,
        delay: float,
        elapsed: float | None,
        retry_after: float | None = None,
    ) -> bool:
        """Whether waiting ``delay`` more fits the no-progress budget.

        ``elapsed`` is the silence so far (see
        :func:`~dataretrieval.transport.liveness.elapsed_since_progress`), passed
        in rather than read here so the policy stays a pure value object.

        The first retry is always allowed. One slow attempt can spend the whole
        budget on its own -- a heavy page against a loaded service, or any
        attempt that runs to the read timeout -- and letting that suppress retry
        entirely would turn a recoverable transient into an immediate failure
        for exactly the large queries that most need retrying. So the budget
        bounds *repeated* silence: with the defaults a dead connection costs
        about two read timeouts rather than five attempts' worth.

        A delay the *server* named -- ``retry_after`` is not ``None``, the same
        hint :meth:`should_retry` and :meth:`backoff` take -- costs the budget
        nothing. Charging for it would mean a service that answers 429 with
        ``Retry-After: 30`` gets fewer retries than one that says nothing at all
        -- with the shipped defaults (a 60 s budget, a 60 s
        :attr:`retry_after_cap`) any honored hint of half the budget or more
        would allow exactly one retry no matter what
        :attr:`max_retries` says. Waiting because we were told to is not the
        service going quiet on us; it is the service telling us when to come
        back. The driver credits the same wait back afterwards (see
        :func:`~dataretrieval.transport.liveness.credit_wait`) so it doesn't
        accumulate into the *next* attempt's silence either.
        """
        if attempt <= _STALL_EXEMPT_ATTEMPTS:
            return True
        if self.stall_timeout <= 0 or elapsed is None:
            return True
        return (
            elapsed + (0.0 if retry_after is not None else delay) <= self.stall_timeout
        )

    def backoff(self, attempt: int, retry_after: float | None) -> float:
        """Seconds to wait before a 1-based retry attempt.

        A jittered component is always included, even when the server named a
        delay: a hint of ``0`` -- or a ``Retry-After`` date that has already
        passed -- would otherwise become a zero-delay re-send against a service
        that just asked us to slow down, and chunks handed the same hint
        would all wake at the same instant and burst together.

        On a server hint that jitter is a small decorrelating nudge rather than
        a second backoff, and the total is held to :attr:`retry_after_cap`:
        full jitter on top of a hint already at the cap would sleep half again
        as long as any bound this policy declares. It is bounded by
        :attr:`max_backoff` rather than by this attempt's exponential ceiling,
        so it survives a :attr:`base_backoff` of zero -- the case where the
        ceiling collapses and a hint of ``0`` would otherwise become exactly the
        zero-delay re-send this prevents. A policy that declares no backoff at
        all still gets none.
        """
        ceiling = min(self.max_backoff, self.base_backoff * 2 ** (attempt - 1))
        if retry_after is None:
            return random.uniform(0.0, ceiling)
        nudge = random.uniform(0.0, min(self.max_backoff, _RETRY_AFTER_JITTER))
        return min(retry_after + nudge, self.retry_after_cap)


_NO_RETRY = RetryPolicy(max_retries=0)


def _retryable(
    exc: BaseException, statuses: frozenset[int] = _RETRYABLE_STATUSES
) -> tuple[bool, float | None]:
    """Return whether ``exc`` is safe to retry and any server delay hint."""
    if isinstance(exc, TransientError):
        if exc.status_code is not None and exc.status_code not in statuses:
            return False, None
        return True, exc.retry_after
    if isinstance(exc, (NetworkError, httpx.TransportError)):
        return not _deterministic_failure(exc), None
    return False, None


class _Wait(NamedTuple):
    """How long to hold off before a retry, and whether the server asked for it.

    ``sanctioned`` travels with the delay because only the driver knows when the
    sleep finished, and a server-named wait has to be credited back to the
    no-progress budget once it has been served (see
    :meth:`RetryPolicy.allows_wait`).
    """

    delay: float
    sanctioned: bool

    def settle(self) -> None:
        """Credit a served server-named wait back to the no-progress budget.

        Paired with the sleep rather than left to each driver: a wait that
        :meth:`RetryPolicy.allows_wait` excused going in has to be excused coming
        out too, or it accumulates into the *next* attempt's silence and caps the
        retries anyway. Both drivers sleep differently but settle identically.
        """
        if self.sanctioned:
            credit_wait(self.delay)


def _retry_delay(exc: BaseException, attempt: int, policy: RetryPolicy) -> _Wait | None:
    """Return the bounded wait for a failed attempt, or ``None`` to stop."""
    retryable, retry_after = _retryable(exc, policy.retryable_statuses)
    if not retryable or not policy.should_retry(attempt, retry_after):
        return None
    delay = policy.backoff(attempt, retry_after)
    if not policy.allows_wait(attempt, delay, elapsed_since_progress(), retry_after):
        return None
    reporter = _progress.current()
    if reporter is not None:
        reporter.note_retry(attempt=attempt, wait=delay)
    return _Wait(delay, retry_after is not None)


async def retry_async(
    afn: Callable[[], Awaitable[_T]],
    policy: RetryPolicy | None = None,
    *,
    gate: asyncio.Semaphore | None = None,
) -> _T:
    """Call an awaitable with bounded retry on typed transient failures.

    ``gate`` bounds how many attempts run concurrently. Owning it here rather
    than letting each caller wrap its own body keeps two rules in one place: the
    slot is acquired per *attempt*, so a call sleeping off a backoff isn't
    holding one while it isn't touching the server, and the time spent waiting
    for it is credited back to the no-progress budget rather than counted as
    silence. A caller that gated its own body would have to rediscover both, and
    nothing would catch it getting them wrong.
    """
    policy = RetryPolicy.from_settings() if policy is None else policy
    attempt = 0
    note_progress()

    async def attempt_once() -> _T:
        if gate is None:
            return await afn()
        started = time.monotonic()
        async with gate:
            credit_wait(time.monotonic() - started)
            return await afn()

    while True:
        try:
            return await attempt_once()
        except Exception as exc:  # noqa: BLE001 - re-raised unless retryable
            attempt += 1
            wait = _retry_delay(exc, attempt, policy)
            if wait is None:
                raise
            await asyncio.sleep(wait.delay)
            wait.settle()


def retry_sync(fn: Callable[[], _T], policy: RetryPolicy | None = None) -> _T:
    """Call a synchronous operation with bounded retry on typed transients.

    ``KeyboardInterrupt``, ``SystemExit``, and other cancellation signals are
    not caught because the loop handles ``Exception`` rather than
    ``BaseException``.
    """
    policy = RetryPolicy.from_settings() if policy is None else policy
    attempt = 0
    note_progress()
    while True:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - re-raised unless retryable
            attempt += 1
            wait = _retry_delay(exc, attempt, policy)
            if wait is None:
                raise
            time.sleep(wait.delay)
            wait.settle()
