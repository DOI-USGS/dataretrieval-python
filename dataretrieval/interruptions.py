"""Resumable fan-out interruption exceptions — the public resume contract.

When a fanned-out request fails mid-stream (a 429, a 5xx, or a bare transport
error), the work already completed is preserved and the call is resumable: the
raised exception carries a ``.call`` handle whose ``resume()`` re-issues only
the still-pending chunks. These exception types are that contract,
re-exported at the top level (``from dataretrieval import ChunkInterrupted``).
The execution machinery that raises and resumes them is
:class:`dataretrieval.transport.fanout.FanOut`.

Vocabulary, consistently (see ``CONTEXT.md``): a **chunk** is one of the
requests a query was split into, named for being a piece rather than for why it
became one; **chunking** is how a query is split; and a **fan-out** is the
concurrent execution of a query's chunks. Water Use chunks one request per
location, Water Data chunks to fit a URL byte budget -- different reasons, the
same word. The base class is named :class:`FanOutInterrupted` because the
failure interrupts the *execution*, not the split.

``ChunkInterrupted`` is retained as an alias of that same class, not a
deprecated shim to delete later: it is the name published in the user guide and
caught in user code, and aliasing costs nothing to keep. ``except
ChunkInterrupted`` and ``except FanOutInterrupted`` are the same handler.

This is a top-level leaf rather than a member of ``ogc`` or ``transport``,
for the reason ADR 0006 gives for ``combining``, ``progress``, and
``credentials``: adapters need it whether or not they go through transport, and
an exception taxonomy is not HTTP execution policy. It stays out of
:mod:`dataretrieval.exceptions` because it carries pandas/httpx state, which
would pull heavy dependencies into that lightweight leaf.
"""

from __future__ import annotations

import socket
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, ClassVar

import httpx
import pandas as pd

from dataretrieval.exceptions import DataRetrievalError, RateLimited, TransientError

if TYPE_CHECKING:
    from dataretrieval.transport.fanout import FanOut


class FanOutInterrupted(DataRetrievalError):
    """
    Base class for mid-stream chunk failures whose completed work
    is preserved and resumable.

    A ``FanOutInterrupted`` subclass means: a chunk failed, but
    ``FanOut`` still owns whatever completed successfully before
    the failure. Call ``self.call.resume()`` to pick up where the
    failure stopped you — only still-pending chunks are
    re-issued.

    Subclasses describe *why* ``FanOut`` stopped so callers can
    pick a retry policy: :class:`QuotaExhausted` for 429 (wait for the
    rate-limit window), :class:`ServiceInterrupted` for 5xx (wait for
    the upstream to recover). The ``.call`` handle is the same object
    across every interruption of a single fanned-out call — frames
    accumulate across retries.

    Attributes
    ----------
    call : FanOut or None
        Resumable handle into the ``FanOut`` that raised this
        exception. ``None`` only on hand-constructed exceptions (test
        fixtures), where ``.call``-derived accessors degrade to
        empty/``None``.
    retry_after : float or None
        Seconds the server suggested waiting (``Retry-After`` header).
        ``None`` when the server gave no hint.
    completed_chunks : int
        Number of chunks successfully completed before the failure.
    total_chunks : int
        Total chunks in the plan.
    partial_frame : pandas.DataFrame
        Combined frame of work completed by the moment this exception
        was raised. Snapshot at raise time — does NOT advance on a
        later ``call.resume()`` (use ``exc.call.partial_frame`` for
        the live view).
    partial_response : httpx.Response or None
        Raw aggregate response covering the completed chunks at
        raise time; ``None`` if nothing had completed yet. Same snapshot
        semantics as ``partial_frame``. (Raw, not finalized — use
        ``exc.call.resume()`` for the finalized ``(df, metadata)`` result.)

    Examples
    --------
    Retry on any transient interruption, honoring the server's
    ``Retry-After`` hint when present and falling back to a fixed wait
    otherwise. Each new interruption keeps the already-completed work
    intact — only the still-pending chunks are re-issued.

    .. code-block:: python

        import time
        from dataretrieval import ChunkInterrupted

        # ``getter`` is any chunked OGC getter — e.g.
        # ``waterdata.get_daily`` or ``ngwmn.get_water_level``.
        try:
            df, md = getter(monitoring_location_id=long_list_of_sites)
        except ChunkInterrupted as exc:
            while True:
                time.sleep(exc.retry_after or 5 * 60)
                try:
                    df, md = exc.call.resume()
                    break
                except ChunkInterrupted as next_exc:
                    exc = next_exc
    """

    # Subclasses override with a ``str.format`` template; the format
    # call sees ``completed_chunks`` and ``total_chunks`` as kwargs.
    _MESSAGE_TEMPLATE: ClassVar[str] = (
        "Fan-out interrupted after {completed_chunks}/"
        "{total_chunks} chunks; call .call.resume() to continue."
    )
    retryable: ClassVar[bool] = True

    def __init__(
        self,
        *,
        completed_chunks: int,
        total_chunks: int,
        call: FanOut[Any] | None = None,
        retry_after: float | None = None,
        cause: BaseException | None = None,
    ) -> None:
        message = self._MESSAGE_TEMPLATE.format(
            completed_chunks=completed_chunks, total_chunks=total_chunks
        )
        if cause is not None:
            cause_msg = str(cause) or type(cause).__name__
            message = f"{message} Cause: {type(cause).__name__}: {cause_msg}"
        super().__init__(message)
        self.completed_chunks = completed_chunks
        self.total_chunks = total_chunks
        self.call = call
        self.retry_after = retry_after
        self.status_code = getattr(type(self), "_DEFAULT_STATUS", None)
        if self.status_code is None and cause is not None:
            # The status is usually a few frames down: a typed error raised
            # ``from`` the httpx failure that carried it.
            for current in _walk_causes(cause):
                status = getattr(current, "status_code", None)
                if status is not None:
                    self.status_code = status
                    break
        # Snapshot partial state at raise time so the exception stays a stable
        # record of the failure moment: ``exc.partial_frame`` /
        # ``.partial_response`` do NOT advance on a later ``call.resume()``
        # (that live view is on ``call.partial_frame`` / ``.partial_response``).
        # This keeps each interruption in a resume loop a faithful record of
        # what it saw, rather than every exception aliasing the shared call's
        # advancing state. ``.copy()`` guards the single-chunk fast path, where
        # the combined frame may be returned verbatim.
        if call is None:
            self.partial_frame: pd.DataFrame = pd.DataFrame()
            self.partial_response: httpx.Response | None = None
        else:
            self.partial_frame = call.partial_frame.copy()
            self.partial_response = call.partial_response

    def __getstate__(self) -> dict[str, Any]:
        # Drop the live FanOut before pickling: its ``.fetch`` is an
        # undecorated module function pickle can't reference by name, so the
        # interruption can't cross a process boundary with ``.call`` attached.
        # The degraded ``call=None`` form keeps the counts, retry hint, and the
        # snapshotted partial frame / response — plain instance attributes the
        # base ``__getstate__`` already pickles; only ``.resume()`` is lost
        # (cross-process resume was never possible anyway).
        return {**super().__getstate__(), "call": None}


class QuotaExhausted(FanOutInterrupted):
    """
    A chunk returned HTTP 429 — the per-key rate-limit window
    is exhausted. Subclass of :class:`FanOutInterrupted`.

    The completed chunks are preserved on ``.call``; once the
    rate-limit window resets, ``.call.resume()`` re-issues only the
    still-pending work. ``partial_frame`` holds what completed
    before the 429.
    """

    _MESSAGE_TEMPLATE = (
        "HTTP 429 after {completed_chunks}/{total_chunks} chunks; "
        "catch QuotaExhausted (or FanOutInterrupted) to access "
        ".partial_frame or .call.resume() once the rate-limit "
        "window has rolled over."
    )
    _DEFAULT_STATUS = 429


class ServiceInterrupted(FanOutInterrupted):
    """
    A chunk returned HTTP 5xx — the upstream service failed
    transiently. Subclass of :class:`FanOutInterrupted`.

    The completed chunks are preserved on ``.call``; once the
    upstream recovers, ``.call.resume()`` resumes only the
    still-pending work.
    """

    _MESSAGE_TEMPLATE = (
        "Service error after {completed_chunks}/{total_chunks} "
        "chunks; catch ServiceInterrupted (or FanOutInterrupted) "
        "and call .call.resume() once the upstream service recovers."
    )


# Resolver failures that will not resolve differently on a later attempt. The
# temporary ones (notably EAI_AGAIN -- "try again", raised while a resolver is
# still coming up, on VPN reconnect, or after a laptop wakes) are deliberately
# absent: those are worth another try. Looked up defensively because the EAI_*
# constants are platform-dependent; an unrecognized code stays retryable, since
# spending a few seconds on a retry is cheaper than dropping a recoverable call.
_PERMANENT_DNS_ERRORS = frozenset(
    code
    for code in (
        getattr(socket, name, None) for name in ("EAI_NONAME", "EAI_FAIL", "EAI_NODATA")
    )
    if code is not None
)


def _walk_causes(
    exc: BaseException, *, follow_context: bool = False
) -> Iterator[BaseException]:
    """Yield ``exc`` and the exceptions it chains to, each at most once.

    Every question this module asks about a failure -- is it transient, is it
    deterministic, what status did it carry -- is "find the first exception in
    this chain that satisfies P". One traversal answers all of them, so the
    cycle guard and the choice of links cannot drift between callers.

    ``__cause__`` (explicit ``raise ... from``) is always followed.
    ``__context__`` (implicit chaining, from raising inside an ``except``
    block) is followed only when ``follow_context`` is set, because it can
    lead away from the failure being classified into whatever unrelated error
    happened to be in flight.

    The ``seen`` set keeps a chain that rejoins itself, or points back at an
    ancestor, from looping.
    """
    seen: set[int] = set()
    pending: list[BaseException | None] = [exc]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        yield current
        if follow_context:
            pending += [current.__cause__, current.__context__]
        else:
            pending.append(current.__cause__)


def _deterministic_failure(exc: BaseException) -> bool:
    """Whether a transport failure would fail identically on every retry.

    An unsupported scheme or a request we built wrong is settled before a byte
    goes out, and a hostname the resolver rejects outright won't be accepted on
    the next attempt either -- so retrying only delays the error the caller
    needs. A *temporary* resolver failure is not in that class and stays
    retryable (see :data:`_PERMANENT_DNS_ERRORS`).

    Walks ``__context__`` as well as ``__cause__``, because the original
    failure is several layers down and not always an explicit ``raise ...
    from``: a DNS failure reaches us as ``NetworkError`` ->
    ``httpx.ConnectError`` -> ``httpcore.ConnectError`` -> ``socket.gaierror``,
    linked by implicit chaining. Following only the cause would walk off down
    the explicit branch and miss a ``gaierror`` sitting on the implicit one --
    spending the whole retry budget on a hostname that will never resolve.
    """
    for current in _walk_causes(exc, follow_context=True):
        if isinstance(current, (httpx.UnsupportedProtocol, httpx.LocalProtocolError)):
            return True
        if isinstance(current, socket.gaierror):
            # Return, not continue: the first resolver code found settles the chain.
            return current.errno in _PERMANENT_DNS_ERRORS
    return False


def _classify_transient(
    exc: BaseException,
) -> tuple[type[FanOutInterrupted], float | None] | None:
    """Classify one failure as a resumable interruption."""
    if isinstance(exc, RateLimited):
        return QuotaExhausted, exc.retry_after
    if isinstance(exc, TransientError):
        return ServiceInterrupted, exc.retry_after
    if isinstance(exc, (httpx.HTTPError, httpx.InvalidURL)):
        # Some failures will fail the same way every time -- a bad scheme, a
        # hostname that doesn't resolve. Offering to resume one would just
        # hide the real error behind a retry that can never work.
        if _deterministic_failure(exc):
            return None
        return ServiceInterrupted, None
    return None


def _classify_chunk_error(
    exc: BaseException,
) -> tuple[type[FanOutInterrupted], float | None] | None:
    """Walk a wrapped pagination failure for a resumable transport cause."""
    return next(
        (
            result
            for current in _walk_causes(exc)
            if (result := _classify_transient(current)) is not None
        ),
        None,
    )


#: The name this taxonomy was published under, kept as a permanent alias so
#: ``except ChunkInterrupted`` keeps working. Same class object, not a subclass.
ChunkInterrupted = FanOutInterrupted

__all__ = [
    "ChunkInterrupted",
    "FanOutInterrupted",
    "QuotaExhausted",
    "ServiceInterrupted",
]
