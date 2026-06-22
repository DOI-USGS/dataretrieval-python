"""Resumable chunk-interruption exceptions — the public resume contract.

When a transparently-chunked request fails mid-stream (a 429, a 5xx, or a
bare transport error), the work already completed is preserved and the call
is resumable: the raised exception carries a ``.call`` handle whose
``resume()`` re-issues only the still-pending sub-requests. These exception
types are that contract, re-exported at the top level
(``from dataretrieval import ChunkInterrupted``). The execution machinery
that raises and resumes them lives in :mod:`dataretrieval.ogc.chunking`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, ClassVar

import httpx
import pandas as pd

from dataretrieval.exceptions import DataRetrievalError

if TYPE_CHECKING:
    from dataretrieval.ogc.chunking import ChunkedCall


# ``_Fetch`` is the per-sub-request fetcher the decorator wraps and
# ``ChunkedCall`` drives: an ``async def fetch(args) -> (df, response)``.
_Fetch = Callable[[dict[str, Any]], Awaitable[tuple[pd.DataFrame, httpx.Response]]]


# Caller-supplied transform applied to the combined chunk result, so a
# resumed call returns the same shape as an un-interrupted one rather than
# the chunker's raw ``(frame, httpx.Response)``. This keeps the chunker
# generic: the OGC getters inject their post-processing (type coercion,
# column arrangement, ``BaseMetadata``) through ``utils._finalize_ogc``.
# The default is identity, so direct ``ChunkedCall`` use is unaffected.
_Finalize = Callable[[pd.DataFrame, httpx.Response], tuple[pd.DataFrame, Any]]


def _passthrough_result(
    frame: pd.DataFrame, response: httpx.Response
) -> tuple[pd.DataFrame, Any]:
    """Default :data:`_Finalize`: return the raw combined pair unchanged."""
    return frame, response


class ChunkInterrupted(DataRetrievalError):
    """
    Base class for mid-stream chunk failures whose completed work is
    preserved and resumable.

    A ``ChunkInterrupted`` subclass means: a sub-request failed, but
    ``ChunkedCall`` still owns whatever completed successfully before
    the failure. Call ``self.call.resume()`` to pick up where the
    failure stopped you — only still-pending sub-requests are
    re-issued.

    Subclasses describe *why* ``ChunkedCall`` stopped so callers can
    pick a retry policy: :class:`QuotaExhausted` for 429 (wait for the
    rate-limit window), :class:`ServiceInterrupted` for 5xx (wait for
    the upstream to recover). The ``.call`` handle is the same object
    across every interruption of a single chunked call — frames
    accumulate across retries.

    Attributes
    ----------
    call : ChunkedCall or None
        Resumable handle into the ``ChunkedCall`` that raised this
        exception. ``None`` only on hand-constructed exceptions (test
        fixtures), where ``.call``-derived accessors degrade to
        empty/``None``.
    retry_after : float or None
        Seconds the server suggested waiting (``Retry-After`` header).
        ``None`` when the server gave no hint.
    completed_chunks : int
        Number of sub-requests successfully completed before the failure.
    total_chunks : int
        Total sub-requests in the plan.
    partial_frame : pandas.DataFrame
        Combined frame of work completed by the moment this exception
        was raised. Snapshot at raise time — does NOT advance on a
        later ``call.resume()`` (use ``exc.call.partial_frame`` for
        the live view).
    partial_response : httpx.Response or None
        Raw aggregate response covering the completed sub-requests at
        raise time; ``None`` if nothing had completed yet. Same snapshot
        semantics as ``partial_frame``. (Raw, not finalized — use
        ``exc.call.resume()`` for the finalized ``(df, metadata)`` result.)

    Examples
    --------
    Retry on any transient interruption, honoring the server's
    ``Retry-After`` hint when present and falling back to a fixed wait
    otherwise. Each new interruption keeps the already-completed work
    intact — only the still-pending sub-requests are re-issued.

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
        "Chunked request interrupted after {completed_chunks}/"
        "{total_chunks} sub-requests; call .call.resume() to continue."
    )

    def __init__(
        self,
        *,
        completed_chunks: int,
        total_chunks: int,
        call: ChunkedCall | None = None,
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
        # Snapshot partial state at raise time so the exception's view stays
        # stable across later ``call.resume()`` advances (the live view is on
        # ``call.partial_frame`` / ``.partial_response``). ``.copy()`` guards
        # the single-chunk fast path, where the frame may be returned verbatim.
        if call is None:
            self.partial_frame: pd.DataFrame = pd.DataFrame()
            self.partial_response: httpx.Response | None = None
        else:
            self.partial_frame = call.partial_frame.copy()
            self.partial_response = call.partial_response

    def __getstate__(self) -> dict[str, Any]:
        # Drop the live ChunkedCall before pickling: its ``.fetch`` is an
        # undecorated module function pickle can't reference by name, so the
        # interruption can't cross a process boundary with ``.call`` attached.
        # The degraded ``call=None`` form keeps the counts, retry hint, and
        # partial frame / response; only ``.resume()`` is lost (cross-process
        # resume was never possible anyway).
        return {**super().__getstate__(), "call": None}


class QuotaExhausted(ChunkInterrupted):
    """
    A sub-request returned HTTP 429 — the per-key rate-limit window
    is exhausted. Subclass of :class:`ChunkInterrupted`.

    The completed sub-requests are preserved on ``.call``; once the
    rate-limit window resets, ``.call.resume()`` re-issues only the
    still-pending work. ``partial_frame`` holds what completed
    before the 429.
    """

    _MESSAGE_TEMPLATE = (
        "HTTP 429 after {completed_chunks}/{total_chunks} sub-requests; "
        "catch QuotaExhausted (or ChunkInterrupted) to access "
        ".partial_frame or .call.resume() once the rate-limit "
        "window has rolled over."
    )


class ServiceInterrupted(ChunkInterrupted):
    """
    A sub-request returned HTTP 5xx — the upstream service failed
    transiently. Subclass of :class:`ChunkInterrupted`.

    The completed sub-requests are preserved on ``.call``; once the
    upstream recovers, ``.call.resume()`` resumes only the
    still-pending work.
    """

    _MESSAGE_TEMPLATE = (
        "Service error after {completed_chunks}/{total_chunks} "
        "sub-requests; catch ServiceInterrupted (or ChunkInterrupted) "
        "and call .call.resume() once the upstream service recovers."
    )
