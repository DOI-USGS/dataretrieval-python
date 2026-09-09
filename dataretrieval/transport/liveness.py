"""When data last arrived, shared by the loops that produce and consume it.

A retrieval can be slow for two reasons: it is downloading a lot (progressing,
however long it takes) or it is receiving nothing at all (and should be
abandoned). Distinguishing them needs one fact -- when data last arrived -- that the
page-walking loop records and the retry loop reads. Keeping it in this leaf lets
both depend on it rather than on each other, and gives any future producer
of liveness (a streaming body reader, a chunk-level fetch) a place to record it.

The timestamp is held in a :class:`~contextvars.ContextVar` so concurrent retrievals --
each chunk of a chunked call, each location of a Water Use fan-out --
measure their own time without data instead of sharing one timestamp.
"""

from __future__ import annotations

import contextvars
import time

_last_progress: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "transport_last_progress", default=None
)


def note_progress() -> None:
    """Restart the no-progress budget: data has arrived."""
    _last_progress.set(time.monotonic())


def elapsed_since_progress() -> float | None:
    """Seconds since data last arrived, or ``None`` if nothing has reported yet."""
    last = _last_progress.get()
    return None if last is None else time.monotonic() - last


def credit_wait(seconds: float) -> None:
    """Subtract ``seconds`` of permitted waiting from the no-progress budget.

    Two kinds of waiting do not count as time without data: queueing behind a
    concurrency cap, and waiting out a delay the server specified (see
    :meth:`~dataretrieval.transport.retry.RetryPolicy.allows_wait` for why a
    server-specified delay is not charged against the budget). The last chunks of a wide
    fan-out can wait past the whole budget and would otherwise start their
    first attempt with no budget left. Neither is progress, and the
    distinction matters: crediting only the measured wait keeps the budget
    cumulative across attempts, where resetting the timestamp to now would also discard
    time without data accumulated by earlier attempts and turn a bound on total
    time without data into a per-attempt latency bound.

    The credit never moves the timestamp past the present. A wait longer than the whole
    budget would otherwise set the timestamp in the *future*, making
    :func:`elapsed_since_progress` negative -- and since nothing ever reduces
    it, that one long queue wait would disable the bound for the rest of the
    call, which is the case the budget exists to bound.
    """
    last = _last_progress.get()
    if last is not None:
        _last_progress.set(min(time.monotonic(), last + seconds))
