"""Transient-failure retry policy for chunked sub-requests.

Defines what counts as a retryable transient (:func:`_classify_chunk_error`,
:func:`_retryable`), the bounded exponential-backoff-with-jitter policy
(:class:`RetryPolicy`), and the driver that applies it (:func:`_retry`). Kept
separate from the execution engine in :mod:`dataretrieval.ogc.chunking` so the
retry/backoff behavior is one cohesive unit that changes independently of the
concurrency model.
"""

from __future__ import annotations

import asyncio
import os
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx
import pandas as pd

from dataretrieval.exceptions import RateLimited, ServiceUnavailable, TransientError
from dataretrieval.ogc import progress as _progress
from dataretrieval.ogc.interruptions import (
    ChunkInterrupted,
    QuotaExhausted,
    ServiceInterrupted,
)

# Retry-with-backoff defaults for transient sub-request failures (429 /
# 5xx / connect-read timeouts): exponential backoff with full jitter, and
# honor a server ``Retry-After`` up to the cap below before escalating
# to a resumable interruption instead.
_RETRIES_ENV = "API_USGS_RETRIES"


_RETRIES_DEFAULT = 4


_RETRY_BASE_BACKOFF = 0.5


_RETRY_MAX_BACKOFF = 30.0


_RETRY_AFTER_CAP = 60.0


def _read_retries_env() -> int:
    """
    Resolve the ``API_USGS_RETRIES`` env var to a max-retry count.

    Returns
    -------
    int
        Number of retries after the first attempt; ``0`` disables
        retrying. Unset/blank → ``_RETRIES_DEFAULT``.
    """
    raw = os.environ.get(_RETRIES_ENV)
    if raw is None or raw.strip() == "":
        return _RETRIES_DEFAULT
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise ValueError(
            f"{_RETRIES_ENV} must be a non-negative integer (got {raw!r})."
        ) from exc
    if value < 0:
        raise ValueError(f"{_RETRIES_ENV} must be >= 0 (got {value}).")
    return value


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded retry-with-backoff config for transient sub-request failures.

    An immutable value object that owns the *timing* decisions; the
    exception taxonomy (which failures are retryable) lives in
    :func:`_retryable`. Backoff is exponential with **full jitter**
    (:func:`random.uniform` over ``[0, ceiling]``) so the concurrent
    fan-out's retries don't re-burst in lockstep. A server ``Retry-After``
    hint, when present, overrides the computed backoff — unless it exceeds
    :attr:`retry_after_cap`, in which case retrying stops and the failure
    surfaces as a resumable :class:`ChunkInterrupted` (a multi-minute
    quota-window reset shouldn't block the call inline).

    Attributes
    ----------
    max_retries : int
        Retries attempted after the first try; ``0`` disables retrying.
    base_backoff : float
        Seconds; the jitter ceiling for the first retry, doubled each
        subsequent attempt.
    max_backoff : float
        Upper bound on any single attempt's backoff ceiling.
    retry_after_cap : float
        Largest ``Retry-After`` (seconds) honored inline; longer hints
        escalate to a resumable interruption.
    """

    max_retries: int = _RETRIES_DEFAULT
    base_backoff: float = _RETRY_BASE_BACKOFF
    max_backoff: float = _RETRY_MAX_BACKOFF
    retry_after_cap: float = _RETRY_AFTER_CAP

    def __post_init__(self) -> None:
        # Catch invalid timing knobs here so a misconfiguration fails at
        # construction, not deep in a later ``time.sleep`` (ValueError on
        # a negative delay) or silently in ``asyncio.sleep`` (which
        # treats negative as zero).
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0 (got {self.max_retries}).")
        if self.base_backoff < 0 or self.max_backoff < 0 or self.retry_after_cap < 0:
            raise ValueError("retry backoff settings must be non-negative.")

    @classmethod
    def from_env(cls) -> RetryPolicy:
        """
        Build a policy from the module-level defaults, resolved now.

        Reads ``max_retries`` from ``API_USGS_RETRIES`` and the timing
        knobs from the ``_RETRY_*`` module constants at call time — not
        the dataclass field defaults (which freeze at class definition)
        — so test ``monkeypatch.setattr`` on the constants takes effect.

        Returns
        -------
        RetryPolicy
            A policy built from the module-level defaults resolved at
            call time.
        """
        return cls(
            max_retries=_read_retries_env(),
            base_backoff=_RETRY_BASE_BACKOFF,
            max_backoff=_RETRY_MAX_BACKOFF,
            retry_after_cap=_RETRY_AFTER_CAP,
        )

    def should_retry(self, attempt: int, retry_after: float | None) -> bool:
        """
        Whether a just-failed ``attempt`` (1-based) warrants another try.

        A ``Retry-After`` longer than ``retry_after_cap`` is *not* slept
        off inline — it returns ``False`` so the failure escalates to a
        resumable interruption instead of blocking the call for minutes.

        Parameters
        ----------
        attempt : int
            The just-failed attempt number (1-based).
        retry_after : float or None
            Seconds the server suggested waiting (``Retry-After`` hint),
            or ``None`` when no hint was given.

        Returns
        -------
        bool
            ``True`` if another try is warranted, ``False`` otherwise.
        """
        if attempt > self.max_retries:
            return False
        return retry_after is None or retry_after <= self.retry_after_cap

    def backoff(self, attempt: int, retry_after: float | None) -> float:
        """
        Seconds to wait before retry ``attempt`` (1-based).

        Parameters
        ----------
        attempt : int
            The retry attempt number (1-based).
        retry_after : float or None
            Seconds the server suggested waiting (``Retry-After`` hint),
            or ``None`` to use the computed exponential backoff instead.

        Returns
        -------
        float
            Seconds to wait before the retry.
        """
        if retry_after is not None:
            return retry_after
        ceiling = min(self.max_backoff, self.base_backoff * 2 ** (attempt - 1))
        return random.uniform(0.0, ceiling)


# Default for direct ``ChunkedCall`` / ``ChunkPlan.execute`` construction
# (and tests): no retrying. The production decorator path explicitly passes
# ``RetryPolicy.from_env()`` so retries are on by default there.
_NO_RETRY = RetryPolicy(max_retries=0)


def _classify_chunk_error(
    exc: BaseException,
) -> tuple[type[ChunkInterrupted], float | None] | None:
    """
    Classify a fetch error as a known transient (resumable) failure.

    Walks the ``__cause__`` chain of ``exc`` looking for a known typed
    transport failure. Returns the matching ``ChunkInterrupted``
    subclass and any ``Retry-After`` hint, or ``None`` if the error is
    not a recognized transient — in which case ``ChunkedCall``
    re-raises rather than wrapping (programmer errors and unknown
    failures shouldn't masquerade as resumable).

    Parameters
    ----------
    exc : BaseException
        The exception raised by a sub-request.

    Returns
    -------
    tuple[type[ChunkInterrupted], float or None] or None
        ``(interrupted_class, retry_after)`` for recognized transient
        failures; ``None`` otherwise.

    Notes
    -----
    ``_walk_pages`` re-wraps mid-pagination failures as a base
    ``DataRetrievalError`` with the typed transport exception linked as
    ``__cause__``, so this function must walk the chain rather than
    just ``isinstance`` the top-level exception.

    Bare ``httpx.HTTPError`` (``ConnectError``, ``TimeoutException``,
    etc.) and ``httpx.InvalidURL`` (server-supplied cursor URL too
    long, oversize follow-up) are also treated as transport failures
    and wrapped as :class:`ServiceInterrupted` — they aren't one of the
    typed status errors above (and ``InvalidURL`` doesn't even inherit
    from ``httpx.HTTPError``), so without explicit handling they would
    escape classification with no resumable handle.
    """
    cur: BaseException | None = exc
    while cur is not None:
        if isinstance(cur, RateLimited):
            return QuotaExhausted, cur.retry_after
        if isinstance(cur, ServiceUnavailable):
            return ServiceInterrupted, cur.retry_after
        if isinstance(cur, (httpx.HTTPError, httpx.InvalidURL)):
            return ServiceInterrupted, None
        cur = cur.__cause__
    return None


def _retryable(exc: BaseException) -> tuple[bool, float | None]:
    """
    Decide whether ``exc`` is a transient worth an automatic retry.

    Only the *top-level* exception is inspected — unlike
    :func:`_classify_chunk_error`, which walks the ``__cause__`` chain.
    The distinction matters because ``_paginate`` raises an
    initial-request transient (429 / 5xx / :class:`httpx.TransportError`)
    *raw*, but wraps a mid-pagination failure as a base ``DataRetrievalError``.
    So a raw transient means a sub-request that made no progress and is cheap to
    re-issue, whereas a mid-pagination failure is left to escalate to a
    resumable :class:`ChunkInterrupted` rather than re-walked from page 1
    (which would re-spend the quota just exhausted). ``httpx.InvalidURL``
    is never retried — a too-long cursor won't fix on a retry.

    Returns
    -------
    tuple[bool, float or None]
        ``(retryable, retry_after)`` — the server ``Retry-After`` hint
        (seconds) when the transient carried one, else ``None``.
    """
    if isinstance(exc, TransientError):
        return True, exc.retry_after
    if isinstance(exc, httpx.TransportError):
        return True, None
    return False, None


def _retry_delay(exc: BaseException, attempt: int, policy: RetryPolicy) -> float | None:
    """
    Decide the backoff for a just-failed ``attempt`` (1-based), or ``None``
    to give up and re-raise.

    Returns ``None`` in three cases — the error isn't a retryable
    transient, the policy is exhausted, or the server's ``Retry-After``
    exceeds the cap (escalates to a resumable :class:`ChunkInterrupted`
    instead). Otherwise returns the seconds to wait and emits the
    progress-bar retry note.

    Parameters
    ----------
    exc : BaseException
        The exception raised by the just-failed attempt.
    attempt : int
        The just-failed attempt number (1-based).
    policy : RetryPolicy
        The retry-with-backoff policy governing the decision.

    Returns
    -------
    float or None
        Seconds to wait before retrying, or ``None`` to give up and
        re-raise.
    """
    retryable, retry_after = _retryable(exc)
    if not retryable or not policy.should_retry(attempt, retry_after):
        return None
    delay = policy.backoff(attempt, retry_after)
    # Surface the imminent retry on the active progress reporter, if any.
    reporter = _progress.current()
    if reporter is not None:
        reporter.note_retry(attempt=attempt, wait=delay)
    return delay


async def _retry(
    afn: Callable[[], Awaitable[tuple[pd.DataFrame, httpx.Response]]],
    policy: RetryPolicy,
) -> tuple[pd.DataFrame, httpx.Response]:
    """
    Call ``afn`` with bounded retry-with-backoff on transient failures.

    A non-retryable or policy-exhausted failure (see :func:`_retry_delay`)
    propagates unchanged so the caller's existing handling wraps it as a
    resumable :class:`ChunkInterrupted`. The whole retry *decision* lives
    in :func:`_retry_delay`; this driver only awaits the sleep between
    attempts.

    Parameters
    ----------
    afn : Callable
        Zero-arg awaitable callable that issues a single sub-request and
        returns ``(frame, response)``.
    policy : RetryPolicy
        The retry-with-backoff policy governing the retries.

    Returns
    -------
    tuple of (pandas.DataFrame, httpx.Response)
        The ``(frame, response)`` pair from the first successful call.
    """
    attempt = 0
    while True:
        try:
            return await afn()
        except Exception as exc:  # noqa: BLE001 — re-raised unless retryable
            attempt += 1
            delay = _retry_delay(exc, attempt, policy)
            if delay is None:
                raise
            await asyncio.sleep(delay)
