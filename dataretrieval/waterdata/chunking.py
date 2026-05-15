"""Multi-value GET-parameter chunking for the Water Data OGC getters.

PR 233 routes most services through GET with comma-separated values
(e.g. ``monitoring_location_id=USGS-A,USGS-B,...``). Long lists can blow
the server's ~8 KB URL byte limit. This module adds a decorator that
sits OUTSIDE ``filters.chunked`` and splits multi-value list params
across multiple sub-requests so each URL fits.

Motivating use case: chained queries where one getter feeds the next:

    >>> # All stream sites in Ohio, then their daily discharge.
    >>> # Without chunking the second call's URL would exceed the
    >>> # server's byte limit for any state with > ~500 stations.
    >>> sites_df, _ = waterdata.get_monitoring_locations(
    ...     state_name="Ohio",
    ...     site_type="Stream",
    ... )
    >>> df, _ = waterdata.get_daily(
    ...     monitoring_location_id=sites_df["monitoring_location_id"].tolist(),
    ...     parameter_code="00060",
    ...     time="P7D",
    ... )

Design (orthogonal to filter chunking):

- N-dimensional cartesian product: for each chunkable list param, the
  values are partitioned into sub-lists; the planner emits the cartesian
  product of those partitions. Sub-chunks of the same dim never overlap,
  so frame concat needs no dedup across multi-value chunks.
- Greedy halving of the largest chunk in any dim until the worst-case
  sub-request URL fits the limit. Minimises total request count.
- Date params, ``bbox``, and ``properties`` are not chunked: dates are
  intervals not enumerable sets; bbox is a coord array; ``properties``
  determines output schema and chunking it would shard columns.

Coordination with ``filters.chunked``:
The planner probes URL length using the LONGEST top-level OR-clause
when a chunkable filter is present, not the full filter. ``filters.
chunked`` (inner) will split the filter per sub-request but bails if
any single clause exceeds its budget, so the longest clause is the
smallest filter size the stack is guaranteed to emit. Without this
coordination, a long OR-filter plus multi-value lists would trigger a
premature ``RequestTooLarge`` even though the combined chunkers would
have made things fit.
"""

from __future__ import annotations

import functools
import itertools
import math
from collections.abc import Callable
from typing import Any, TypeVar

import pandas as pd
import requests

from . import filters
from .filters import (
    _combine_chunk_frames,
    _combine_chunk_responses,
    _is_chunkable,
    _split_top_level_or,
)

# Params that look like lists but must NOT be chunked. ``properties`` is
# excluded because it defines the response schema; chunking it would
# return frames with different columns per sub-request. ``bbox`` is a
# fixed 4-element coord tuple. Date params are intervals not sets. The
# CQL ``filter`` (and its ``filter_lang``) is a string that has its own
# inner chunker (``filters.chunked``); if a caller passes ``filter`` as
# a list, treating it as a multi-value param would emit malformed CQL.
_NEVER_CHUNK = frozenset(
    {
        "properties",
        "bbox",
        "datetime",
        "last_modified",
        "begin",
        "begin_utc",
        "end",
        "end_utc",
        "time",
        "filter",
        "filter_lang",
    }
)

# Default cap on the number of sub-requests a single chunked call may
# emit. The USGS Water Data API rate-limits each HTTP request (including
# pagination), so the true budget is ``hourly_quota / avg_pages_per_chunk``.
# 1000 matches the default hourly quota and is a reasonable upper bound
# for single-page sub-requests; tune lower if your queries paginate.
# Override per-decorator via ``max_chunks=`` or by monkeypatching this
# module attribute (read lazily in the wrapper).
_DEFAULT_MAX_CHUNKS = 1000

# When ``x-ratelimit-remaining`` drops below this between sub-requests,
# the chunker bails with ``QuotaExhausted`` rather than risk a mid-call
# HTTP 429. Carries the partial result so callers can resume from a
# known offset instead of retrying the whole chunked call from scratch.
_DEFAULT_QUOTA_SAFETY_FLOOR = 50

# Sentinel returned by ``_read_remaining`` when the response has no
# parseable ``x-ratelimit-remaining`` header. Large enough to beat any
# plausible safety floor so a missing/malformed header doesn't trigger
# spurious ``QuotaExhausted`` aborts.
_QUOTA_UNKNOWN = 10**9


class RequestTooLarge(ValueError):
    """Raised when a chunked request cannot be issued. Two cases:
    (1) URL exceeds the byte limit even with every multi-value param at
    a singleton chunk and any chunkable filter reduced to its smallest
    top-level OR-clause; (2) the cartesian-product plan would issue more
    than ``max_chunks`` sub-requests."""


class QuotaExhausted(RuntimeError):
    """Raised mid-chunked-call when the API's reported remaining quota
    (``x-ratelimit-remaining`` header) drops below the configured safety
    floor. The chunker stops before issuing the next sub-request to
    avoid a mid-call HTTP 429 that would silently truncate paginated
    results (see PR #273 for the pagination side of that bug).

    The exception carries everything needed to resume: the combined
    partial frame from completed sub-requests, the metadata for the
    last successful sub-request, the number of chunks completed out of
    the plan total, and the last-observed ``remaining`` value.

    Attributes
    ----------
    partial_frame : pd.DataFrame
        Concatenated, deduplicated result of every sub-request that
        completed before the floor was crossed.
    partial_response : requests.Response
        Aggregated response (URL/headers of the first sub-request,
        summed ``elapsed``). Wrap in ``BaseMetadata`` to surface to
        the caller alongside the partial frame.
    completed_chunks : int
        Number of sub-requests successfully completed.
    total_chunks : int
        Total sub-requests in the cartesian-product plan.
    remaining : int
        Last observed ``x-ratelimit-remaining`` value.
    """

    def __init__(
        self,
        *,
        partial_frame: pd.DataFrame,
        partial_response: requests.Response,
        completed_chunks: int,
        total_chunks: int,
        remaining: int,
    ) -> None:
        super().__init__(
            f"x-ratelimit-remaining dropped to {remaining} after "
            f"{completed_chunks}/{total_chunks} chunks; aborting to avoid "
            f"mid-call HTTP 429. Catch QuotaExhausted to access "
            f".partial_frame and resume from chunk {completed_chunks}."
        )
        self.partial_frame = partial_frame
        self.partial_response = partial_response
        self.completed_chunks = completed_chunks
        self.total_chunks = total_chunks
        self.remaining = remaining


def _chunkable_params(args: dict[str, Any]) -> dict[str, list]:
    """Return ``{name: list(values)}`` for every list/tuple kwarg with
    >1 element that is allowed to chunk."""
    return {
        k: list(v)
        for k, v in args.items()
        if k not in _NEVER_CHUNK and isinstance(v, (list, tuple)) and len(v) > 1
    }


def _filter_aware_probe_args(args: dict[str, Any]) -> dict[str, Any]:
    """Substitute the filter with its LONGEST top-level OR-clause if the
    filter is chunkable, otherwise return ``args`` unchanged.

    The inner ``filters.chunked`` decorator splits a filter into chunks
    each ≤ the per-sub-request byte budget, but bails (returns the full
    filter unchanged) when ANY single OR-clause exceeds the budget. So
    the smallest filter the inner chunker is guaranteed to emit per
    sub-request is bounded below by the largest single clause — not the
    smallest. Probing with ``max(parts, key=len)`` models the worst
    achievable per-sub-request URL the decorator stack can produce; if
    that fits, we know the inner chunker won't bail and the actual URL
    will fit too.
    """
    filter_expr = args.get("filter")
    filter_lang = args.get("filter_lang")
    if not _is_chunkable(filter_expr, filter_lang):
        return args
    parts = _split_top_level_or(filter_expr)
    if len(parts) < 2:
        return args  # one-clause filter — inner chunker can't shrink it
    return {**args, "filter": max(parts, key=len)}


def _chunk_bytes(chunk: list) -> int:
    """Byte length of ``chunk`` when comma-joined into a URL param value.

    This is the cost the planner uses to compare chunks across dims; the
    real URL builder also URL-encodes the comma, but the byte counts come
    out the same modulo a constant per-chunk overhead.
    """
    return len(",".join(map(str, chunk)))


def _request_bytes(req: requests.PreparedRequest) -> int:
    """Total bytes of a prepared request: URL + body.

    GET routes have ``body=None`` and reduce to URL length. POST routes
    (CQL2 JSON body) need body bytes — the URL stays short regardless of
    payload, so URL-only sizing would underestimate the request and skip
    chunking when it's needed.
    """
    url_len = len(req.url)
    body = req.body
    if body is None:
        return url_len
    if isinstance(body, (bytes, bytearray)):
        return url_len + len(body)
    return url_len + len(body.encode("utf-8"))


def _worst_case_args(
    probe_args: dict[str, Any], plan: dict[str, list[list]]
) -> dict[str, Any]:
    """Args dict using the LARGEST chunk from each dim — represents the
    most byte-heavy sub-request the plan will issue, with the filter
    already reduced to its filter-chunker floor."""
    out = dict(probe_args)
    for k, chunks in plan.items():
        out[k] = max(chunks, key=_chunk_bytes)
    return out


def _plan_chunks(
    args: dict[str, Any],
    build_request: Callable[..., Any],
    url_limit: int,
    max_chunks: int = _DEFAULT_MAX_CHUNKS,
) -> dict[str, list[list]] | None:
    """Greedy halving until the worst-case sub-request URL fits.

    Returns ``None`` when no chunking is needed (request as-is fits or
    no chunkable lists). Raises ``RequestTooLarge`` when:
    - every multi-value param is already a singleton chunk AND the
      filter (if any) is already at its smallest OR-clause and the URL
      still exceeds ``url_limit`` (irreducible), or
    - the converged cartesian-product plan would issue more than
      ``max_chunks`` sub-requests (hourly API budget).
    """
    chunkable = _chunkable_params(args)
    if not chunkable:
        return None
    probe_args = _filter_aware_probe_args(args)
    if _request_bytes(build_request(**probe_args)) <= url_limit:
        return None

    plan: dict[str, list[list]] = {k: [v] for k, v in chunkable.items()}

    while True:
        worst = _worst_case_args(probe_args, plan)
        if _request_bytes(build_request(**worst)) <= url_limit:
            break

        # Find the single biggest chunk across all dims and halve it.
        best: tuple[str, int, int] | None = None  # (dim, chunk_index, size)
        for dim, dim_chunks in plan.items():
            for idx, chunk in enumerate(dim_chunks):
                if len(chunk) <= 1:
                    continue
                size = _chunk_bytes(chunk)
                if best is None or size > best[2]:
                    best = (dim, idx, size)

        if best is None:
            raise RequestTooLarge(
                f"Request exceeds {url_limit} bytes (URL + body) even "
                f"with every multi-value parameter at a singleton chunk "
                f"and any chunkable filter reduced to one OR-clause. "
                f"Reduce the number of values or split the call manually."
            )
        dim, idx, _ = best
        big = plan[dim][idx]
        mid = len(big) // 2
        plan[dim] = plan[dim][:idx] + [big[:mid], big[mid:]] + plan[dim][idx + 1 :]

    total = math.prod(len(chunks) for chunks in plan.values())
    if total > max_chunks:
        raise RequestTooLarge(
            f"Chunked plan would issue {total} sub-requests, exceeding "
            f"max_chunks={max_chunks} (USGS API's default hourly rate "
            f"limit per key). Reduce input list sizes, narrow the time "
            f"window, or raise max_chunks if you have a higher quota."
        )
    return plan


_FetchOnce = TypeVar(
    "_FetchOnce",
    bound=Callable[[dict[str, Any]], tuple[pd.DataFrame, requests.Response]],
)


def _read_remaining(response: requests.Response) -> int:
    """Parse ``x-ratelimit-remaining`` from a response. Missing or
    malformed header → return ``_QUOTA_UNKNOWN`` so the safety check
    treats it as 'plenty of quota' (don't abort on header glitches)."""
    raw = response.headers.get("x-ratelimit-remaining")
    if raw is None:
        return _QUOTA_UNKNOWN
    try:
        return int(raw)
    except (TypeError, ValueError):
        return _QUOTA_UNKNOWN


def multi_value_chunked(
    *,
    build_request: Callable[..., Any],
    url_limit: int | None = None,
    max_chunks: int | None = None,
    quota_safety_floor: int | None = None,
) -> Callable[[_FetchOnce], _FetchOnce]:
    """Decorator that splits multi-value list params across sub-requests so
    each URL fits ``url_limit`` bytes (defaults to ``filters._WATERDATA_
    URL_BYTE_LIMIT``) and the cartesian-product plan stays ≤ ``max_chunks``
    sub-requests (defaults to ``_DEFAULT_MAX_CHUNKS``). All defaults are
    resolved at call time so tests/users that patch the module constants
    affect this decorator uniformly.

    Between sub-requests the wrapper reads ``x-ratelimit-remaining`` from
    each response. If it drops below ``quota_safety_floor`` (default
    ``_DEFAULT_QUOTA_SAFETY_FLOOR``), the wrapper raises ``QuotaExhausted``
    carrying the combined partial result and the chunk offset so callers
    can resume after the hourly window resets, instead of crashing into
    a mid-pagination HTTP 429 (which the upstream pagination loop in
    ``_walk_pages`` historically truncated silently — see PR #273).

    Sits OUTSIDE ``@filters.chunked``: list-chunking is the outer loop,
    filter-chunking is the inner loop. The wrapped function has the same
    signature as ``filters.chunked`` expects — ``(args: dict) -> (frame,
    response)`` — so the two decorators compose cleanly. The planner is
    filter-aware so it doesn't raise prematurely when the inner filter
    chunker would have shrunk the per-sub-request URL on its own.
    """

    def decorator(fetch_once: _FetchOnce) -> _FetchOnce:
        @functools.wraps(fetch_once)
        def wrapper(
            args: dict[str, Any],
        ) -> tuple[pd.DataFrame, requests.Response]:
            limit = (
                url_limit
                if url_limit is not None
                else filters._WATERDATA_URL_BYTE_LIMIT
            )
            cap = max_chunks if max_chunks is not None else _DEFAULT_MAX_CHUNKS
            floor = (
                quota_safety_floor
                if quota_safety_floor is not None
                else _DEFAULT_QUOTA_SAFETY_FLOOR
            )
            plan = _plan_chunks(args, build_request, limit, cap)
            if plan is None:
                return fetch_once(args)

            keys = list(plan)
            total = math.prod(len(plan[k]) for k in keys)
            frames: list[pd.DataFrame] = []
            responses: list[requests.Response] = []
            for i, combo in enumerate(itertools.product(*(plan[k] for k in keys))):
                sub_args = {**args, **dict(zip(keys, combo))}
                frame, response = fetch_once(sub_args)
                frames.append(frame)
                responses.append(response)
                # Quota check happens BETWEEN sub-requests: skip on the
                # last iteration because there's nothing left to abort.
                if i < total - 1:
                    remaining = _read_remaining(response)
                    if remaining < floor:
                        raise QuotaExhausted(
                            partial_frame=_combine_chunk_frames(frames),
                            partial_response=_combine_chunk_responses(responses),
                            completed_chunks=i + 1,
                            total_chunks=total,
                            remaining=remaining,
                        )

            return (
                _combine_chunk_frames(frames),
                _combine_chunk_responses(responses),
            )

        return wrapper  # type: ignore[return-value]

    return decorator
