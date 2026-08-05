"""When data last arrived, shared by the loops that produce and consume it.

A retrieval can be slow for two very different reasons: it is downloading a lot
(fine, however long it takes) or it is receiving nothing at all (worth giving up
on). Telling those apart needs one fact -- when data last arrived -- that the
page-walking loop knows and the retry loop acts on. Keeping it in this leaf lets
both point *down* at it rather than at each other, and leaves any future producer
of liveness (a streaming body reader, a chunk-level fetch) somewhere to report.

The stamp lives in a :class:`~contextvars.ContextVar` so concurrent retrievals --
each sub-request of a chunked call, each location of a Water Use fan-out --
measure their own silence instead of sharing one clock.
"""

from __future__ import annotations

import contextvars
import time

_last_progress: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "transport_last_progress", default=None
)


def note_progress() -> None:
    """Restart the no-progress budget: data just arrived."""
    _last_progress.set(time.monotonic())


def elapsed_since_progress() -> float | None:
    """Seconds since data last arrived, or ``None`` if nothing has reported yet."""
    last = _last_progress.get()
    return None if last is None else time.monotonic() - last


def credit_wait(seconds: float) -> None:
    """Excuse ``seconds`` of waiting-for-a-turn from the no-progress budget.

    Queueing behind a concurrency cap is not silence -- the deep tail of a wide
    fan-out can wait past the whole budget and would otherwise start its first
    attempt with nothing left to retry with. But neither is it progress, and the
    difference matters: crediting only the measured wait keeps the budget
    cumulative across attempts, where restamping to "now" would also discard
    silence accumulated by earlier attempts and quietly turn a bound on total
    silence into a per-attempt latency bound.
    """
    last = _last_progress.get()
    if last is not None:
        _last_progress.set(last + seconds)
