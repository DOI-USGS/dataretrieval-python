"""URL-byte chunk planning and dispatch for the OGC getters.

An OGC query has several chunkable axes: every multi-value list
parameter (sites, parameter codes, …) plus the cql-text ``filter``,
which splits along its top-level OR clauses. Any of them can fan the
URL past the server's ~8 KB byte limit. ``ChunkPlan`` picks a fan-out
for each axis that minimizes total chunks while keeping every
chunk URL under the budget. Requests that already fit get a
trivial single-step plan — the executor has one code path either way.

This module owns the public ``parallel_chunks`` dial and preserves compatibility
names from the former combined planner/executor. The OGC engine composes
planning with API-neutral fan-out directly, avoiding an extra dependency hop in
every getter call.

Parallel chunks: the planner is conservative by default — it splits only as
far as the byte limit forces. A caller who knows their result is large can opt
into a finer split via the ``parallel_chunks(n)`` context manager, which fans
the query out into ``n`` parallel chunks. ``n`` drives
:meth:`ChunkPlan._refine`; see ``parallel_chunks`` for the why and the when.

Concurrency, retries, and interruption semantics are documented on
:mod:`dataretrieval.transport.fanout`; ``API_USGS_CONCURRENT`` and
``API_USGS_RETRIES`` are read there.

Dedup: list-axis chunks don't overlap; filter-axis chunks can, so
``_combine_chunk_frames`` dedupes by feature ``id``. ``properties``,
``bbox``, date intervals, ``limit``, ``skip_geometry``, and
``filter``/``filter_lang`` themselves are never sliced as list axes
(the filter is partitioned along its top-level OR axis instead).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from dataretrieval.ogc.engine import (
    _parallel_chunks,
)
from dataretrieval.ogc.engine import (
    multi_value_chunked as multi_value_chunked,
)
from dataretrieval.transport.fanout import FanOut, _active_client, active_client

from .policy import _require_positive_int

# Compatibility aliases. ``ChunkedCall`` was this module's executor before it
# moved down to transport as the API-neutral ``FanOut``; ``get_active_client``
# and ``_chunked_client`` named its shared per-call client. Only the
# chunking/progress test modules still use these names, and the rename is not
# worth churning them over -- package code imports the canonical spellings from
# :mod:`dataretrieval.transport.fanout`. They are aliases, not copies: the
# ambient in particular must be the *same* object transport publishes, or a
# test reading it here would never see the running client.
ChunkedCall = FanOut
get_active_client = active_client
_chunked_client = _active_client


@contextmanager
def parallel_chunks(n: int) -> Iterator[None]:
    """
    Fan the OGC getters' multi-value requests out into ``n`` parallel chunks.

    By default the Water Data / NGWMN getters chunk a request only as much as
    the server's ~8 KB URL-byte limit forces — the fewest chunks that
    fit. That is the safe default, but it can be *needlessly* conservative.
    Because every chunk paginates, splitting a large result further costs
    little or no extra quota *as long as each chunk still spans many
    pages* — rows-per-chunk far exceeding the page size (ten states pulled as
    one request page nearly as many times as ten per-state requests would).
    When a split leaves each chunk only a page or two, its partial final
    page is extra, so finer chunks do add some requests. This context manager
    lets a caller who *knows* their pull is large ask for that finer split. The
    trade is roughly the same pages for more, smaller chunks, which gives
    smoother progress, more even concurrency, and a smaller unit of
    retry/resume.

    This is a *deliberate* per-call knob rather than an automatic behavior or a
    process-wide environment variable, because the library can't tell in
    advance whether a query is large (ten states over a short window might fit
    in a single page, where extra chunks would only burn quota). Scoping it to
    a ``with`` block keeps an aggressive setting from leaking into unrelated
    calls and accidentally spending quota. Outside any block the getters use
    the conservative default. Only the OGC getters (Water Data, NGWMN) read
    this; wrapping a legacy NWIS call in the block is a harmless no-op.

    Parameters
    ----------
    n : int
        The number of chunks to fan the whole call out into — a positive
        integer such as ``2``, ``8``, or ``32``. It caps the plan's *total*
        chunk count (the cartesian product across every multi-value
        argument combined, not per argument), so several multi-value arguments
        cannot multiply past it. The cap is a ceiling, never exceeded: the
        actual count is bounded below by what the ~8 KB URL limit already
        forces and above by ``n``. So an ``n`` larger than the input allows
        simply yields one chunk per value, and with several multi-value
        arguments the total may land somewhat below ``n`` because splits are
        whole (the plan can't always divide evenly onto ``n``). ``n=1`` asks
        for no extra fan-out.

        Each chunk fetches at least one page, so it costs at least one
        request against your hourly rate limit — a larger ``n`` spends more
        quota. How many chunks run *at once* is capped separately by
        ``API_USGS_CONCURRENT`` (default 32), so an ``n`` beyond that adds
        quota without adding parallelism; the useful range is roughly ``2``
        up to ``API_USGS_CONCURRENT``.

    Yields
    ------
    None

    Raises
    ------
    ValueError
        If ``n`` is not a positive integer — raised on ``with`` entry, before
        any request is issued, so a bad value fails loudly rather than silently
        doing nothing.

    Notes
    -----
    Fanning out carries the same consequences as the byte-limit chunking the
    getters already do for oversized requests; opting in just brings them to a
    request that would otherwise be a single call:

    - ``max_rows``: each chunk paginates up to ``max_rows`` rows
      independently, then the combined result is sorted and truncated to
      ``max_rows``. So a call with ``max_rows`` set returns a *different*
      (though still valid and deterministically sorted) row set inside a
      ``parallel_chunks`` block than without one. The cap is drawn from the
      union of the chunks, not a single stream. Don't pair a tight
      ``max_rows`` preview with ``parallel_chunks`` if you need exactly the
      rows the un-fanned call would return.
    - Resumability: a single request either fully succeeds or fully fails,
      but a fanned-out call can fail partway (e.g. a mid-call rate-limit) and
      raise a resumable :class:`~dataretrieval.ogc.interruptions.ChunkInterrupted`
      (or ``QuotaExhausted``) carrying the completed chunks. Finish the
      call with ``exc.call.resume()``.
    - Cross-chunk de-duplication keys on the feature ``id``; features
      with no ``id`` can't be deduped, so overlapping filter clauses split
      across chunks may yield duplicate rows.

    Examples
    --------
    >>> from dataretrieval import waterdata
    >>> with waterdata.parallel_chunks(32):
    ...     df, md = waterdata.get_daily(
    ...         monitoring_location_id=many_sites, parameter_code="00060"
    ...     )  # doctest: +SKIP

    See Also
    --------
    ChunkPlan._refine : the planning-side effect of ``n``.
    """
    # Fail loudly on a bad ``n`` at ``with`` entry, before any request. Shared
    # rules with ``max_rows`` via the helper (accepts numpy ints, rejects bool).
    _require_positive_int(n, "parallel_chunks(n)", examples="2, 8, 32")
    with _parallel_chunks(n):
        yield
