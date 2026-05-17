"""Multi-value GET-parameter chunking for the Water Data OGC getters.

PR 233 routes most services through GET with comma-separated values
(e.g. ``monitoring_location_id=USGS-A,USGS-B,...``). Long lists can blow
the server's ~8 KB URL byte limit. This module adds a decorator that
sits OUTSIDE ``filters.chunked`` and splits multi-value list params
across multiple sub-requests so each URL fits. See ``get_daily``'s
docstring for an end-to-end chained-query example.

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
The planner probes the URL with a synthetic clause sized to the inner
chunker's bail floor — ``len(longest_clause) * max(per-clause encoding
ratio)`` — when a chunkable filter is present. The inner chunker bails
(emits the full filter) when any single clause's URL-encoded length
exceeds its per-sub-request budget; mirroring
``filters._effective_filter_budget``, that floor already accounts for
the worst per-call encoding ratio, so a long alphanumeric clause
coexisting with a shorter heavily-encoded clause is sized correctly.
Without this coordination, a long OR-filter plus multi-value lists
would trigger a premature ``RequestTooLarge`` even though the combined
chunkers would have made things fit.
"""

from __future__ import annotations

import functools
import itertools
import math
from collections.abc import Callable
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
import requests

from . import filters
from .filters import (
    _combine_chunk_frames,
    _combine_chunk_responses,
    _FetchOnce,
    _is_chunkable,
    _max_per_clause_encoding_ratio,
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
# module attribute — both the decorator wrapper and ``_plan_chunks``
# read it lazily.
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
    a singleton chunk and any chunkable filter at the inner chunker's
    bail-floor size (the URL contribution of its longest single
    OR-clause, after URL-encoding); (2) the cartesian-product plan
    would issue more than ``max_chunks`` sub-requests."""


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
        Aggregated response: first sub-request's URL (so
        ``BaseMetadata.url`` still reflects the user's original query
        for reproducibility), last completed sub-request's headers
        (so callers inspecting ``x-ratelimit-remaining`` see current
        quota state), and summed ``elapsed`` across completed
        sub-requests. Wrap in ``BaseMetadata`` to surface to the
        caller alongside the partial frame.
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


def _chunkable_params(args: dict[str, Any]) -> dict[str, list[Any]]:
    """Return ``{name: list(values)}`` for every list/tuple kwarg with
    >1 element that is allowed to chunk."""
    return {
        k: list(v)
        for k, v in args.items()
        if k not in _NEVER_CHUNK and isinstance(v, (list, tuple)) and len(v) > 1
    }


def _filter_aware_probe_args(args: dict[str, Any]) -> dict[str, Any]:
    """Substitute the filter with a synthetic ASCII clause sized to the
    inner chunker's bail floor, so the planner's URL probe matches what
    the inner chunker would emit.

    The inner ``filters.chunked`` bails (emits the full filter) when any
    single OR-clause's URL-encoded length exceeds the per-sub-request
    budget. Mirroring ``filters._effective_filter_budget``, that floor
    is ``len(longest_clause) * max(per-clause encoding ratio)``.
    Substituting an ASCII clause of that exact length makes
    ``quote_plus`` a no-op, so the URL builder sees exactly the
    bail-floor byte count.
    """
    filter_expr = args.get("filter")
    filter_lang = args.get("filter_lang")
    if not _is_chunkable(filter_expr, filter_lang):
        return args
    parts = _split_top_level_or(filter_expr)
    if len(parts) < 2:
        return args  # one-clause filter — inner chunker can't shrink it
    longest_raw = max(len(p) for p in parts)
    probe_size = math.ceil(longest_raw * _max_per_clause_encoding_ratio(parts))
    return {**args, "filter": "x" * probe_size}


def _chunk_bytes(chunk: list[Any]) -> int:
    """URL-encoded byte length of ``chunk`` when comma-joined into a
    URL parameter value.

    Used as the planner's biggest-chunk comparator in
    ``_worst_case_args`` and the halving loop. ``quote_plus`` (rather
    than raw ``,``-join length) keeps the comparator faithful to what
    the real URL builder produces, so values containing characters
    that expand under URL encoding (``%``, ``+``, ``/``, ``&``, …)
    can't be mis-ranked. For typical USGS multi-value workloads
    (alphanumeric IDs and codes) raw and encoded lengths are equal,
    but the encoded form is always correct.
    """
    return len(quote_plus(",".join(map(str, chunk))))


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
    probe_args: dict[str, Any], plan: dict[str, list[list[Any]]]
) -> dict[str, Any]:
    """Args representing the worst-case sub-request the plan will issue:
    each dim's largest chunk (by URL-encoded bytes), composed onto
    the ``probe_args`` already returned by ``_filter_aware_probe_args``
    so any chunkable filter sits at the inner chunker's bail-floor
    size. The planner feeds these args through ``_request_bytes`` to
    decide whether the biggest sub-request fits the budget."""
    out = dict(probe_args)
    for k, chunks in plan.items():
        out[k] = max(chunks, key=_chunk_bytes)
    return out


def _plan_chunks(
    args: dict[str, Any],
    build_request: Callable[..., Any],
    url_limit: int,
    max_chunks: int | None = None,
) -> dict[str, list[list[Any]]] | None:
    """Greedy halving until the worst-case sub-request fits ``url_limit``.

    Budget is total request bytes (URL + body, via ``_request_bytes``)
    so POST routes size correctly — see ``multi_value_chunked`` for the
    parameter-name caveat.

    Returns ``None`` when no chunking is needed (request as-is fits or
    no chunkable lists). Raises ``RequestTooLarge`` when:
    - the smallest reducible plan still exceeds ``url_limit`` (every
      multi-value param at a singleton chunk and any chunkable filter
      already at the inner chunker's bail-floor size), or
    - the cartesian-product plan exceeds ``max_chunks`` sub-requests
      (the hourly API budget); checked after each split so we bail
      promptly once the cap is unreachable.

    ``max_chunks`` defaults to ``_DEFAULT_MAX_CHUNKS`` resolved at call
    time, so monkeypatching the module constant takes effect for
    direct callers too.
    """
    if max_chunks is None:
        max_chunks = _DEFAULT_MAX_CHUNKS
    chunkable = _chunkable_params(args)
    if not chunkable:
        return None
    probe_args = _filter_aware_probe_args(args)
    if _request_bytes(build_request(**probe_args)) <= url_limit:
        return None

    plan: dict[str, list[list[Any]]] = {k: [v] for k, v in chunkable.items()}

    while True:
        worst = _worst_case_args(probe_args, plan)
        if _request_bytes(build_request(**worst)) <= url_limit:
            return plan

        # Largest splittable chunk across all dims, by URL-encoded bytes.
        splittable = (
            (dim, idx, chunk)
            for dim, dim_chunks in plan.items()
            for idx, chunk in enumerate(dim_chunks)
            if len(chunk) > 1
        )
        biggest = max(splittable, key=lambda t: _chunk_bytes(t[2]), default=None)
        if biggest is None:
            raise RequestTooLarge(
                f"Request exceeds {url_limit} bytes (URL + body) at the "
                f"smallest reducible plan: every multi-value parameter "
                f"at a singleton chunk and any chunkable filter at the "
                f"inner chunker's bail-floor size. Reduce the number "
                f"of values, shorten the filter, or split the call "
                f"manually."
            )
        dim, idx, chunk = biggest
        mid = len(chunk) // 2
        plan[dim] = plan[dim][:idx] + [chunk[:mid], chunk[mid:]] + plan[dim][idx + 1 :]

        # Each split only grows the cartesian product, so once we
        # cross max_chunks we can never come back under. Bail now
        # rather than keep splitting (the URL probe could still take
        # many more iterations).
        total = math.prod(len(chunks) for chunks in plan.values())
        if total > max_chunks:
            raise RequestTooLarge(
                f"Chunked plan would issue {total} sub-requests, exceeding "
                f"max_chunks={max_chunks} (USGS API's default hourly rate "
                f"limit per key). Reduce input list sizes, narrow the time "
                f"window, or raise max_chunks if you have a higher quota."
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
    """Decorator that splits multi-value list params across sub-requests
    so each sub-request fits ``url_limit`` bytes (defaults to
    ``filters._WATERDATA_URL_BYTE_LIMIT``) and the cartesian-product
    plan stays ≤ ``max_chunks`` sub-requests (defaults to
    ``_DEFAULT_MAX_CHUNKS``). All defaults are resolved at call time so
    tests/users that patch the module constants affect this decorator
    uniformly.

    ``url_limit`` is enforced against total request bytes (URL + body,
    via ``_request_bytes``); the name reflects the dominant GET case
    where body is empty. POST routes (e.g. ``monitoring-locations`` via
    CQL2 JSON) are conservatively sized — never under-chunks, but may
    over-chunk at the body's true ceiling.

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
            floor = (
                quota_safety_floor
                if quota_safety_floor is not None
                else _DEFAULT_QUOTA_SAFETY_FLOOR
            )
            plan = _plan_chunks(args, build_request, limit, max_chunks)
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
