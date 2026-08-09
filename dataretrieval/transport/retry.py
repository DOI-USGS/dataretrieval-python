"""Bounded retry policy and transient-failure classification."""

from __future__ import annotations

import asyncio
import math
import os
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import NamedTuple, TypeVar

import httpx

from dataretrieval import progress as _progress
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
_RETRIES_ENV = "API_USGS_RETRIES"
_RETRIES_DEFAULT = 4
_RETRY_BASE_BACKOFF = 0.5
_RETRY_MAX_BACKOFF = 30.0
_RETRY_AFTER_CAP = 60.0
# Most a server-named delay is nudged by, to keep sub-requests handed the same
# hint from waking together. Small on purpose: the server named the wait, so
# jitter here decorrelates rather than extends it.
_RETRY_AFTER_JITTER = 1.0
# Attempts the no-progress budget never withholds; see RetryPolicy.allows_wait.
_STALL_EXEMPT_ATTEMPTS = 1
_STALL_TIMEOUT_ENV = "API_USGS_STALL_TIMEOUT"
_STALL_TIMEOUT_DEFAULT = 60.0

_T = TypeVar("_T")
_Number = TypeVar("_Number", int, float)


def parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header into seconds, or ``None`` for no usable hint.

    Both header forms mean the same thing and are treated the same way: the
    seconds are returned as given, however large. A value past what a caller will
    wait out inline stops the retry and surfaces a transient carrying the hint on
    ``.retry_after``, so a long wait becomes the caller's decision (and, for a
    chunked call, a resumable interruption) instead of being ignored.

    An over-long hint is honored rather than discarded. Dropping it would make
    the client retry *harder* against a service that just asked for a long
    pause, and would deny the caller the number it needs on ``.retry_after``.
    Clock skew can inflate a date-form hint, but trusting one costs a
    recoverable escalation while ignoring it costs hammering a service that is
    already asking for room.

    A date that has *already* passed yields no hint at all rather than ``0.0``.
    Read literally it says "retry now", but the likelier reading is that our
    clock runs ahead of the server's -- and acting on it would re-send almost
    immediately against a service that just asked for a pause. Falling back to
    our own bounded backoff is right under either reading. (Delta-seconds is
    clock-independent, so a literal ``Retry-After: 0`` is still honored as the
    instruction it is, floored by :meth:`RetryPolicy.backoff`'s jitter.)
    """
    if not value:
        return None
    raw = value.strip()
    try:
        seconds = float(raw)
    except ValueError:
        pass
    else:
        # ``inf``/``nan`` parse cleanly but poison every later comparison: an
        # infinite hint would refuse retry forever and travel to the caller on
        # ``.retry_after``. Treat them as no hint at all.
        return max(0.0, seconds) if math.isfinite(seconds) else None
    try:
        retry_at = parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
    return delay if delay > 0 else None


def _read_env_number(
    name: str, default: _Number, cast: Callable[[str], _Number], expected: str
) -> _Number:
    """Read a non-negative number from the environment, or ``default`` if unset.

    Raises :class:`~dataretrieval.exceptions.ConfigurationError` -- a
    ``DataRetrievalError`` *and* a ``ValueError`` -- for an unusable value, so a
    typo in the environment doesn't escape a request path as a bare
    ``ValueError`` that ``except DataRetrievalError`` misses.
    """
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = cast(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be {expected} (got {raw!r}).") from exc
    # ``nan`` passes every ordering test, so a bare ``< 0`` guard lets it through
    # and then silently makes each budget comparison false.
    if not math.isfinite(value):
        raise ConfigurationError(f"{name} must be {expected} (got {raw!r}).")
    if value < 0:
        raise ConfigurationError(f"{name} must be >= 0 (got {value}).")
    return value


@dataclass(frozen=True)
class RetryPolicy:
    """Immutable bounded exponential-backoff-with-full-jitter policy.

    Two independent bounds decide when to stop: :attr:`max_retries` caps *how
    many* attempts a failure gets, and :attr:`stall_timeout` caps *how long* a
    call may go on receiving nothing.
    """

    #: Attempts after the first. ``0`` disables retry entirely.
    max_retries: int = _RETRIES_DEFAULT
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
    stall_timeout: float = _STALL_TIMEOUT_DEFAULT

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
    def from_env(cls, retryable_statuses: frozenset[int] | None = None) -> RetryPolicy:
        """Build a policy from current environment and module defaults."""
        statuses = (
            _RETRYABLE_STATUSES if retryable_statuses is None else retryable_statuses
        )
        return cls(
            retryable_statuses=statuses,
            max_retries=_read_env_number(
                _RETRIES_ENV, _RETRIES_DEFAULT, int, "a non-negative integer"
            ),
            base_backoff=_RETRY_BASE_BACKOFF,
            max_backoff=_RETRY_MAX_BACKOFF,
            retry_after_cap=_RETRY_AFTER_CAP,
            stall_timeout=_read_env_number(
                _STALL_TIMEOUT_ENV,
                _STALL_TIMEOUT_DEFAULT,
                float,
                "a non-negative number of seconds",
            ),
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
        that just asked us to slow down, and sub-requests handed the same hint
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
    policy = RetryPolicy.from_env() if policy is None else policy
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
    policy = RetryPolicy.from_env() if policy is None else policy
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
