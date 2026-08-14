"""Offset-parallel page fetching: overlap a page walk instead of serializing it.

Cursor pagination is inherently sequential — page ``N+1``'s URL only exists once
page ``N`` has been parsed, so a 10-page result costs 10 round trips end to end.
When a service also honors ``offset``, every page's URL is computable up front
(``offset = i * limit``), so the same pages can be fetched concurrently. The
request *count* is unchanged; only their timing is. That distinction matters
because the USGS quota is volume-based (``x-ratelimit-limit``, default 1000/hr),
so overlapping pages costs no extra quota.

This module owns the generic half of that strategy: given a page-request
builder and a page parser, drive a bounded, speculative, wave-by-wave fetch and
return the concatenated frames. It is service-neutral — no OGC or Water Data
knowledge — mirroring :mod:`dataretrieval.transport.pagination`, which owns the
sequential cursor walk this is an alternative to.

Why waves, and why they ramp
----------------------------
The size of the result is unknown before it is fetched. OGC API - Features
Part 1 makes ``numberMatched`` *optional* ("each page may include information
about the number of selected and returned features"), and the Water Data API
omits it — a page carries ``numberReturned`` but no total. So a client cannot
compute the page count in advance; it must probe.

Probing is where a naive fan-out gets expensive. Issuing ``width`` requests
immediately means a *one-page* result costs ``width`` requests instead of one:
at ``width=32`` a small query would spend 32x the quota to discover it was
already done. Since the quota here is volume-based, that is a straight 32x tax
on exactly the queries that had nothing to gain from parallelism.

So the wave width **ramps**: 1 request, then 2, then 4, doubling up to
``width``. The properties that buys:

- A single-page result costs exactly **one** request — identical to the
  sequential walk, so the common small query pays nothing for this feature.
- Total requests stay under **2x** the pages actually needed (doubling means
  every prior wave summed is less than the current one), and approach ``width``
  overshoot only for results large enough to amortize it.
- Round trips are logarithmic in the page count rather than linear: a 10-page
  result is 4 waves, not 10 round trips.

That is the standard unbounded-search ramp, and it is the reason this walk can
claim to leave the request count essentially unchanged while still overlapping
pages. The ceiling clip in :func:`plan_offsets` is what bounds the final wave.

Stop conditions
---------------
A wave stops the walk when any of these holds — see :func:`_stop_index` for the
precedence, which is the single source of truth:

1. **A short page.** A page with fewer than ``limit`` rows is the last page by
   construction: the server had no more rows to give. This is the normal exit.
2. **An empty page.** Zero rows means the previous page ended exactly on a
   ``limit`` boundary and this offset is past the end.
3. **The row cap.** ``row_cap`` (from ``max_rows``) is reached, so further
   pages would be discarded anyway.
4. **The offset ceiling.** The service refuses offsets beyond ``max_offset``
   (Water Data: 40000). This is *not* an end-of-data signal, so it must not end
   the walk: the caller supplies ``tail_walk``, a sequential continuation that
   picks up where the offsets stop. Offsets have a ceiling; cursors don't, so
   the hybrid is fast over the parallelizable prefix and complete over the rest.

   The seam needs care. The next offset the walk *would* need is by definition
   past the ceiling, so it can't seed the continuation either — that request
   would earn the same rejection. So the walk rewinds one page: it drops the
   last page it fetched and re-seeds the cursor walk at the largest offset the
   service still accepts. One page is re-fetched per deep query, in exchange
   for a seam with neither a gap (missing rows) nor an overlap (duplicates).

Ordering is preserved regardless of completion order: results are indexed by
wave position and concatenated in offset order, so the frame matches what a
sequential walk would have produced.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta

import httpx
import pandas as pd

from dataretrieval import progress as _progress
from dataretrieval.combining import (
    _QUOTA_HEADER,
    _merge_response,
    _safe_elapsed,
)
from dataretrieval.exceptions import DataRetrievalError
from dataretrieval.transport.liveness import note_progress
from dataretrieval.transport.pagination import _client_for, paginated_failure_message

logger = logging.getLogger(__name__)


class OffsetUnsupported(Exception):
    """The service does not honor ``offset``, so this strategy can't be used.

    Internal control-flow signal, not a user-facing error: the caller catches it
    and re-runs the query through the sequential cursor walk, which needs no
    non-standard parameters. Raised *before* any rows are returned, so a fallback
    re-fetch can't produce a partial or double-counted result.
    """


# A page builder maps an absolute row offset to the request that fetches it.
PageRequest = Callable[[int], httpx.Request]

# A page parser maps a response to its frame. Unlike the cursor walk's parser
# it returns no cursor — the offsets *are* the cursor, computed not discovered.
PageParser = Callable[[httpx.Response], pd.DataFrame]

# The sequential continuation used past the offset ceiling:
# ``(resume_offset, rows_so_far, client) -> (frame, response)``.
TailWalk = Callable[
    [int, int, httpx.AsyncClient], "Awaitable[tuple[pd.DataFrame, httpx.Response]]"
]


def plan_offsets(
    *,
    limit: int,
    width: int,
    start: int,
    max_offset: int | None,
) -> list[int]:
    """Offsets for one wave, clipped to the service's offset ceiling.

    Returns up to ``width`` offsets spaced ``limit`` apart beginning at
    ``start``, dropping any that would exceed ``max_offset``. An empty list
    means the ceiling has been reached and the caller must stop (or fall back
    to a cursor walk) rather than issue a request the service will reject.

    Parameters
    ----------
    limit : int
        Page size — the offset stride.
    width : int
        Maximum number of offsets to plan.
    start : int
        First offset in this wave.
    max_offset : int or None
        Largest offset the service accepts, or ``None`` for no ceiling.

    Returns
    -------
    list of int
        The planned offsets, ascending; possibly empty.
    """
    offsets = [start + i * limit for i in range(width)]
    if max_offset is not None:
        offsets = [off for off in offsets if off <= max_offset]
    return offsets


def _stop_index(
    frames: list[pd.DataFrame],
    *,
    limit: int,
    rows_before: int,
    row_cap: int | None,
) -> int | None:
    """Index of the page that ends the walk, or ``None`` to continue.

    Encodes the stop precedence documented in the module docstring. Pages are
    inspected in offset order so the *earliest* terminal page wins: a short
    page at index 2 ends the walk even if index 5 (fetched speculatively past
    the end) also looks terminal. Returning the index — rather than a bool —
    lets the caller discard the pages after it, which is what makes a
    speculative overshoot harmless.

    Parameters
    ----------
    frames : list of pandas.DataFrame
        This wave's page frames, in offset order.
    limit : int
        The page size requested; a frame shorter than this is terminal.
    rows_before : int
        Rows already collected by earlier waves, for the ``row_cap`` test.
    row_cap : int or None
        Stop once this many rows are held, or ``None`` for uncapped.

    Returns
    -------
    int or None
        Index of the last page to keep, or ``None`` if the walk continues.
    """
    running = rows_before
    for i, frame in enumerate(frames):
        n = len(frame)
        running += n
        # An empty page is past the end: keep everything before it. A short
        # page is the genuine last page: keep it, including its rows.
        if n == 0:
            return i - 1 if i else -1
        if n < limit:
            return i
        if row_cap is not None and running >= row_cap:
            return i
    return None


def _offset_ignored(frames: list[pd.DataFrame]) -> bool:
    """Whether the server appears to be ignoring ``offset``.

    ``offset`` is not a standard OGC API - Features parameter, and an
    unrecognized query parameter is conventionally *ignored* rather than
    rejected. A server that ignores it answers every offset with page 1, so the
    walk would happily concatenate the same rows N times and report success —
    silent duplication, the worst failure mode available to this design.

    The check: two full-length pages fetched at different offsets must not be
    identical. Comparing the first two suffices — if the stride is being
    honored at all, page 0 and page 1 hold different rows. This is a cheap
    structural comparison on frames already in memory, run once on the first
    wave, so it costs no extra request.

    False positives are possible in principle (two genuinely identical pages of
    data), which is why the caller treats a positive as "fall back to the cursor
    walk" rather than an error: the safe strategy always remains available.
    """
    if len(frames) < 2:
        return False
    first, second = frames[0], frames[1]
    if first.empty or len(first) != len(second):
        return False
    if list(first.columns) != list(second.columns):
        return False
    return bool(first.equals(second))


async def _fetch_page(
    build_page: PageRequest,
    offset: int,
    client: httpx.AsyncClient,
    raise_for_status: Callable[[httpx.Response], None],
    semaphore: asyncio.Semaphore | None,
) -> httpx.Response:
    """Fetch one page at ``offset``, honoring the concurrency gate."""
    if semaphore is None:
        response = await client.send(build_page(offset))
    else:
        async with semaphore:
            response = await client.send(build_page(offset))
    raise_for_status(response)
    return response


async def _fetch_wave(
    build_page: PageRequest,
    offsets: list[int],
    client: httpx.AsyncClient,
    raise_for_status: Callable[[httpx.Response], None],
) -> list[httpx.Response]:
    """Fetch one wave, cancelling and draining siblings on any failure."""
    tasks = [
        asyncio.create_task(
            _fetch_page(build_page, offset, client, raise_for_status, None)
        )
        for offset in offsets
    ]
    try:
        return list(await asyncio.gather(*tasks))
    except BaseException:
        # ``asyncio.gather`` propagates the first failure but deliberately
        # leaves siblings running. A retry of the whole page walk would then
        # overlap those abandoned requests, double-spend quota, and exceed the
        # declared concurrency bound. Cancel and await them before control can
        # return to retry policy; catching BaseException also cleans up when
        # the caller cancels this walk.
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


@dataclass
class _WalkState:
    """Mutable state shared by the small steps of one offset page walk."""

    frames: list[pd.DataFrame] = field(default_factory=list)
    first_response: httpx.Response | None = None
    last_response: httpx.Response | None = None
    total_elapsed: timedelta = field(default_factory=timedelta)
    offset: int = 0
    wave_width: int = 1
    offset_verified: bool = False


async def _fetch_and_parse_wave(
    *,
    build_page: PageRequest,
    parse_page: PageParser,
    raise_for_status: Callable[[httpx.Response], None],
    session: httpx.AsyncClient,
    offsets: list[int],
    completed_pages: int,
) -> tuple[list[httpx.Response], list[pd.DataFrame]]:
    """Fetch and parse one wave, adding standard pagination guidance."""
    try:
        responses = await _fetch_wave(
            build_page,
            offsets,
            session,
            raise_for_status,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Offset-parallel page fetch failed at offsets %r.", offsets)
        raise DataRetrievalError(
            paginated_failure_message(completed_pages, exc)
        ) from exc

    try:
        return responses, [parse_page(response) for response in responses]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Offset-parallel page parse failed.")
        raise DataRetrievalError(
            paginated_failure_message(completed_pages, exc)
        ) from exc


def _verify_offset(wave: list[pd.DataFrame], prior: list[pd.DataFrame]) -> bool:
    """Verify distinct offsets return distinct pages once two are available."""
    probe = wave if len(wave) >= 2 else [*prior[-1:], *wave]
    if _offset_ignored(probe):
        raise OffsetUnsupported(
            "The service returned identical pages for different `offset` values, "
            "so it appears to ignore `offset`."
        )
    return len(probe) >= 2


def _record_page(
    response: httpx.Response,
    frame: pd.DataFrame,
    reporter: _progress.ProgressReporter | None,
) -> timedelta:
    """Record progress for one kept page and return its elapsed duration."""
    note_progress()
    if reporter is not None:
        reporter.set_rate_remaining(
            response.headers.get(_QUOTA_HEADER),
            limit=response.headers.get("x-ratelimit-limit"),
        )
        reporter.add_page(rows=len(frame))
    return _safe_elapsed(response)


def _accept_wave(
    state: _WalkState,
    *,
    responses: list[httpx.Response],
    wave: list[pd.DataFrame],
    offsets: list[int],
    limit: int,
    width: int,
    row_cap: int | None,
    reporter: _progress.ProgressReporter | None,
) -> bool:
    """Keep the useful prefix of one wave; return whether the walk is done."""
    if not state.offset_verified:
        state.offset_verified = _verify_offset(wave, state.frames)

    if state.first_response is None:
        state.first_response = responses[0]
    if state.last_response is None:
        # An empty first page keeps no frame but is still valid metadata.
        state.last_response = responses[0]

    stop_at = _stop_index(
        wave,
        limit=limit,
        rows_before=sum(len(frame) for frame in state.frames),
        row_cap=row_cap,
    )
    keep = wave if stop_at is None else wave[: stop_at + 1]
    for response, frame in zip(responses[: len(keep)], keep, strict=False):
        state.total_elapsed += _record_page(response, frame, reporter)
        state.last_response = response
    state.frames.extend(keep)

    if stop_at is not None:
        return True
    state.offset = offsets[-1] + limit
    state.wave_width = min(state.wave_width * 2, width)
    return False


async def _walk_offset_waves(
    state: _WalkState,
    *,
    build_page: PageRequest,
    parse_page: PageParser,
    raise_for_status: Callable[[httpx.Response], None],
    session: httpx.AsyncClient,
    limit: int,
    width: int,
    max_offset: int | None,
    row_cap: int | None,
    reporter: _progress.ProgressReporter | None,
) -> bool:
    """Drive ramped waves; return whether the offset ceiling was reached."""
    while True:
        offsets = plan_offsets(
            limit=limit,
            width=state.wave_width,
            start=state.offset,
            max_offset=max_offset,
        )
        if not offsets:
            return True
        responses, wave = await _fetch_and_parse_wave(
            build_page=build_page,
            parse_page=parse_page,
            raise_for_status=raise_for_status,
            session=session,
            offsets=offsets,
            completed_pages=len(state.frames),
        )
        if _accept_wave(
            state,
            responses=responses,
            wave=wave,
            offsets=offsets,
            limit=limit,
            width=width,
            row_cap=row_cap,
            reporter=reporter,
        ):
            return False


async def _continue_after_ceiling(
    state: _WalkState,
    *,
    session: httpx.AsyncClient,
    tail_walk: TailWalk | None,
    limit: int,
    max_offset: int | None,
) -> None:
    """Rewind one legal page and cursor-walk the unbounded tail."""
    if tail_walk is None:
        logger.warning(
            "Stopped at the service's offset ceiling (%s) with %d row(s) "
            "collected; the result may be incomplete because no sequential "
            "continuation was supplied.",
            max_offset,
            sum(len(frame) for frame in state.frames),
        )
        return

    if state.frames:
        state.offset -= limit
        state.frames.pop()
    rows_so_far = sum(len(frame) for frame in state.frames)
    logger.debug(
        "Offset ceiling (%s) reached after %d row(s); continuing sequentially "
        "from offset %d.",
        max_offset,
        rows_so_far,
        state.offset,
    )
    tail_frame, tail_response = await tail_walk(
        state.offset,
        rows_so_far,
        session,
    )
    if len(tail_frame):
        state.frames.append(tail_frame)
    state.total_elapsed += _safe_elapsed(tail_response)
    state.last_response = tail_response


def _finalize_walk(
    state: _WalkState,
    *,
    max_offset: int | None,
    row_cap: int | None,
) -> tuple[pd.DataFrame, httpx.Response]:
    """Build the final frame and aggregate response from a completed walk."""
    if state.first_response is None or state.last_response is None:
        raise DataRetrievalError(
            "Offset-parallel pagination issued no requests; "
            f"max_offset={max_offset!r} leaves no valid page offset."
        )
    result = (
        pd.concat(state.frames, ignore_index=True) if state.frames else pd.DataFrame()
    )
    if row_cap is not None:
        result = result.head(row_cap)
    return result, _merge_response(
        state.first_response,
        headers_from=state.last_response,
        elapsed=state.total_elapsed,
    )


async def paginate_by_offset(
    *,
    build_page: PageRequest,
    parse_page: PageParser,
    raise_for_status: Callable[[httpx.Response], None],
    client: httpx.AsyncClient | None = None,
    limit: int,
    width: int,
    max_offset: int | None = None,
    row_cap: int | None = None,
    tail_walk: TailWalk | None = None,
) -> tuple[pd.DataFrame, httpx.Response]:
    """Fetch pages concurrently in ramped waves until a stop condition.

    This is the offset-parallel counterpart to
    :func:`dataretrieval.transport.pagination.paginate`. ``build_page`` maps an
    absolute row offset to a request; ``parse_page`` maps a response to its
    frame. ``limit`` is both page size and offset stride, while ``width`` caps
    each speculative wave. ``row_cap`` stops and truncates early.

    ``max_offset`` is the largest offset the service accepts. Reaching it is
    not end-of-data: when ``tail_walk`` is supplied, the last legal page is
    rewound and the callback cursor-walks the unbounded remainder. Without a
    callback, the partial result is returned with a warning.

    Any page failure raises :class:`DataRetrievalError` with the same recovery
    guidance as cursor pagination. Sibling requests are cancelled and drained
    before the error returns, so a retry cannot overlap abandoned work.
    """
    async with _client_for(client) as session:
        state = _WalkState()
        ceiling_reached = await _walk_offset_waves(
            state,
            build_page=build_page,
            parse_page=parse_page,
            raise_for_status=raise_for_status,
            session=session,
            limit=limit,
            width=width,
            max_offset=max_offset,
            row_cap=row_cap,
            reporter=_progress.current(),
        )
        if ceiling_reached:
            await _continue_after_ceiling(
                state,
                session=session,
                tail_walk=tail_walk,
                limit=limit,
                max_offset=max_offset,
            )
        return _finalize_walk(state, max_offset=max_offset, row_cap=row_cap)
