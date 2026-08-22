"""Result recombination: merge per-chunk frames and responses (no I/O).

These utilities assemble the output of a chunked/fan-out call from its
individual per-chunk results.  They have no event-loop, retry, or
network state — they're pure data transforms shared by protocol-specific
chunk execution, service fan-out, and cursor-driven pagination.

Separated from :mod:`dataretrieval.ogc.planning` so that module stays
focused on *what* to split, while this module owns *how* to reassemble.

A top-level leaf rather than part of :mod:`dataretrieval.transport`: these are
pandas transforms over already-fetched results, with no HTTP or event-loop
concern, consumed by chunk planning and service fan-out as well as by pagination.
"""

from __future__ import annotations

import copy
from datetime import timedelta

import httpx
import pandas as pd

# Response header USGS uses to advertise remaining hourly quota.  Lives in this
# module so every layer (the combine helpers below, the engine's per-page
# progress reporter) reads it from one place rather than hard-coding the string.
_QUOTA_HEADER = "x-ratelimit-remaining"


def _safe_elapsed(response: httpx.Response) -> timedelta:
    """
    Read ``response.elapsed``, falling back to ``timedelta(0)`` when
    the attribute hasn't been populated.

    httpx only writes ``.elapsed`` when a response is closed through
    its normal transport path. ``MockTransport`` (used by
    ``pytest-httpx``) and hand-constructed ``httpx.Response`` objects
    leave the attribute unset, so accessing it raises ``RuntimeError``.
    Combining responses across chunks needs a defined duration, so we
    treat the missing attribute as zero elapsed.
    """
    try:
        elapsed: object = response.elapsed
    except RuntimeError:
        return timedelta(0)
    return elapsed if isinstance(elapsed, timedelta) else timedelta(0)


def _set_response_url(response: httpx.Response, url: str | httpx.URL) -> None:
    """
    Overwrite the URL surfaced by a response without back-propagating
    the change into any aliased original.

    Lightweight test doubles expose ``.url`` as a writable attribute. Real
    :class:`httpx.Response` objects resolve it through a bound request, so swap
    in a fresh request carrying the new URL; mutating the existing request would
    leak through any shallow copy that shares it.
    """
    if not isinstance(response, httpx.Response):
        # Lightweight test doubles expose ``url`` as a writable attribute.
        response.url = url
        return

    target = httpx.URL(str(url))
    try:
        old = response.request
    except RuntimeError:
        # No request bound (some hand-built httpx.Response fixtures);
        # synthesize a minimal one to hold the URL.
        response.request = httpx.Request("GET", target)
        return
    response.request = httpx.Request(method=old.method, url=target, headers=old.headers)


def _lowest_remaining(responses: list[httpx.Response]) -> httpx.Response:
    """The response reporting the lowest ``x-ratelimit-remaining``.

    Within a rate-limit window, the counter decreases monotonically, so the
    smallest value observed is the most conservative value to surface. Under
    concurrent fan-out, the last response *by index* need not be the one the
    server processed last. Fall back to the last response when none reports
    the header.
    """
    best: httpx.Response | None = None
    best_remaining: int | None = None
    for response in responses:
        try:
            remaining = int(response.headers[_QUOTA_HEADER])
        except (KeyError, ValueError):
            continue
        if best_remaining is None or remaining < best_remaining:
            best, best_remaining = response, remaining
    return best if best is not None else responses[-1]


def _merge_response(
    base: httpx.Response,
    *,
    headers_from: httpx.Response,
    elapsed: timedelta,
    url: str | httpx.URL | None = None,
) -> httpx.Response:
    """Fold several responses into one shallow copy of ``base``.

    The copy's ``.headers`` are rebuilt as a fresh ``httpx.Headers`` from
    ``headers_from``, ``.elapsed`` is set to ``elapsed``, and ``.url`` is
    overridden when ``url`` is given.  ``base`` and ``headers_from`` are never
    mutated, and the fresh ``httpx.Headers`` means downstream mutations don't
    back-propagate into any underlying response — so callers may re-fold
    idempotently.  This is the one low-level merge behind both pagination
    (:func:`~dataretrieval.transport.pagination.paginate`) and the chunked /
    fan-out aggregation (:func:`_combine_chunk_responses`)."""
    merged = copy.copy(base)
    # Drop the body: an aggregate's content would be one arbitrary page's
    # bytes (the base's), and holding it keeps every chunk's first page
    # resident for the whole call — the frames are the product, not the raw
    # JSON. Cleared on the copy only; ``base`` keeps its body.
    merged._content = b""
    merged.headers = httpx.Headers(headers_from.headers)
    merged.elapsed = elapsed
    if url is not None:
        _set_response_url(merged, url)
    return merged


def _combine_chunk_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate per-chunk frames and deduplicate IDs across chunks.

    Empty frames are ignored before concatenation so an empty plain
    :class:`pandas.DataFrame` cannot downgrade a real ``GeoDataFrame`` and
    strip its geometry or CRS. When every frame is empty, the first frame is
    returned to preserve its concrete type.

    When multiple non-empty frames are combined, non-null feature IDs are
    deduplicated regardless of the plan axis. Filter clauses can match the same
    feature, and list inputs can contain repeated values or otherwise select
    overlapping records. Rows without an ``id`` are preserved verbatim: pandas
    treats null values as duplicates, so deduplicating them would silently lose
    data.
    """
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return frames[0] if frames else pd.DataFrame()
    if len(non_empty) == 1:
        return non_empty[0].copy()

    combined = pd.concat(non_empty, ignore_index=True)
    if "id" not in combined.columns:
        return combined

    has_id = combined["id"].notna()
    if has_id.all():
        return combined.drop_duplicates(subset="id", ignore_index=True)
    if has_id.any():
        id_rows = combined[has_id].drop_duplicates(subset="id")
        no_id_rows = combined[~has_id]
        return pd.concat([id_rows, no_id_rows], ignore_index=True)
    return combined


def _combine_chunk_responses(
    responses: list[httpx.Response], canonical_url: str | None
) -> httpx.Response:
    """
    Fold per-chunk responses into a single aggregated response.

    For a multi-response input, returns a shallow copy of
    ``responses[0]`` with ``.headers`` set to those of the response reporting
    the lowest ``x-ratelimit-remaining`` value (the most conservative quota
    observation; see :func:`_lowest_remaining`), ``.elapsed`` set to the sum of
    the per-response elapsed durations, and ``.url`` set to the
    canonical original-query URL (when supplied) so ``BaseMetadata``
    reflects the user's full request rather than the first chunk.

    For a single-response input with no canonical-URL override,
    ``responses[0]`` is returned unchanged to skip the copy on the
    passthrough hot path.

    Parameters
    ----------
    responses : list[httpx.Response]
        One response per completed chunk, in caller-provided order.
    canonical_url : str or None
        URL of the unchunked original request. ``None`` skips the URL
        override — used by the passthrough path (the fetcher's
        response already carries the original-query URL) and by the
        worst-case overflow path (no buildable canonical URL exists).

    Returns
    -------
    httpx.Response
        A shallow copy of the first response with aggregated
        ``headers``, ``elapsed``, and ``url``.  The function is
        idempotent (the input responses' ``headers`` / ``elapsed`` /
        ``url`` are never mutated), so it's safe to call repeatedly
        via :attr:`ChunkedCall.partial_response` during error
        inspection or resume retries.  ``headers`` on the returned
        object is a fresh ``httpx.Headers``, so mutations there don't
        back-propagate into any chunk's underlying response.
    """
    if len(responses) == 1 and canonical_url is None:
        return responses[0]

    # Headers come from the response with the lowest reported remaining quota
    # (``_lowest_remaining`` returns the lone response as-is for a
    # single-element list).  ``_merge_response`` re-sums elapsed onto a
    # fresh copy, so repeated calls (e.g. via ``ChunkedCall.partial_response``
    # during resume) stay idempotent.
    elapsed = sum((_safe_elapsed(r) for r in responses), start=timedelta())
    return _merge_response(
        responses[0],
        headers_from=_lowest_remaining(responses),
        elapsed=elapsed,
        url=canonical_url,
    )
