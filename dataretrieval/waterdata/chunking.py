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

import datetime
import functools
import itertools
import math
from collections.abc import Callable
from dataclasses import dataclass
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

# Response header USGS uses to advertise remaining hourly quota.
_QUOTA_HEADER = "x-ratelimit-remaining"

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


# Normalized plan shape: a tuple of ``(dim_name, tuple-of-chunk-tuples)``
# pairs. Used for hashing/equality on ``ChunkManifest`` and to compare a
# resumed call's fresh plan against the saved manifest's plan.
_NormalizedPlan = tuple[tuple[str, tuple[tuple[Any, ...], ...]], ...]


def _normalize_plan(plan: dict[str, list[list[Any]]]) -> _NormalizedPlan:
    """Convert the planner's mutable nested-list plan to an immutable,
    comparable nested-tuple form. Preserves insertion order, which is
    the cartesian-product iteration order (Python 3.7+ dict guarantee)."""
    return tuple(
        (key, tuple(tuple(chunk) for chunk in chunks)) for key, chunks in plan.items()
    )


@dataclass(frozen=True)
class ChunkManifest:
    """Snapshot of a chunked call's progress, sufficient to resume.

    Attached to ``BaseMetadata.chunk_manifest`` on every chunked call
    (successful or failed). On a failed call the manifest records how
    many sub-requests completed; passing the partial metadata back via
    ``resume_from=`` re-runs only the remaining cartesian-product
    combinations. Pinning the normalized plan (not just the input
    args) lets resume detect when a caller has changed their inputs
    between the original call and the retry — same-looking args that
    chunk differently would silently re-fetch wrong sub-ranges.

    Attributes
    ----------
    plan : tuple
        Normalized form of the chunker's cartesian-product plan: a
        tuple of ``(dim_name, tuple-of-chunk-tuples)`` pairs, in the
        order the planner iterates them.
    completed : int
        Number of sub-requests that completed before the call
        terminated. Equal to ``total`` on a fully successful call.
    """

    plan: _NormalizedPlan
    completed: int

    @property
    def total(self) -> int:
        """Total sub-requests in the cartesian-product plan."""
        return math.prod(len(chunks) for _, chunks in self.plan)

    @property
    def is_complete(self) -> bool:
        """``True`` when every sub-request in the plan completed."""
        return self.completed >= self.total

    @property
    def remaining(self) -> int:
        """Number of sub-requests still to fetch on resume."""
        return max(self.total - self.completed, 0)

    def __repr__(self) -> str:
        return (
            f"ChunkManifest(completed={self.completed}/{self.total}, "
            f"dims={len(self.plan)})"
        )


class PartialResult(RuntimeError):
    """Raised mid-chunked-call when any sub-request fails. Carries the
    combined partial frame and a ``ChunkManifest`` recording how many
    sub-requests completed. Catch this exception to access partial
    data, then re-call the original getter with the partial metadata
    via ``resume_from=`` to fetch the remaining chunks.

    ``__cause__`` (set via ``raise ... from exc``) holds the underlying
    exception — typically a ``RuntimeError`` from ``_walk_pages``'s
    mid-pagination failure handler (see PR #279) or a transport-level
    ``requests`` exception.

    Attributes
    ----------
    partial_frame : pd.DataFrame
        Concatenated, deduplicated result of every sub-request that
        completed in this call. Empty if the first attempted
        sub-request failed.
    partial_response : requests.Response
        Aggregated response carrying the canonical URL (the user's
        full original query), last successful sub-request's headers,
        summed ``elapsed``, and ``chunk_manifest`` attribute set so
        ``BaseMetadata(partial_response).chunk_manifest`` exposes the
        manifest.
    manifest : ChunkManifest
        Records the chunk plan and the number of completed
        sub-requests. Pass the wrapping metadata back via
        ``resume_from=`` to resume.
    """

    def __init__(
        self,
        *,
        partial_frame: pd.DataFrame,
        partial_response: requests.Response,
        manifest: ChunkManifest,
        message: str | None = None,
    ) -> None:
        super().__init__(
            message
            or (
                f"Chunked request failed after "
                f"{manifest.completed}/{manifest.total} sub-requests. "
                f"Catch PartialResult to access .partial_frame and "
                f"resume_from=partial_metadata on a retry."
            )
        )
        self.partial_frame = partial_frame
        self.partial_response = partial_response
        self.manifest = manifest

    @property
    def partial_metadata(self):
        """``BaseMetadata`` wrapping ``partial_response``. Lazy so the
        chunker module stays decoupled from ``dataretrieval.utils`` at
        import time (avoids a circular-import-shaped surface)."""
        from dataretrieval.utils import BaseMetadata

        return BaseMetadata(self.partial_response)


class QuotaExhausted(PartialResult):
    """Raised mid-chunked-call when the API's reported remaining quota
    (``x-ratelimit-remaining`` header) drops below the configured
    safety floor. The chunker stops before issuing the next
    sub-request to avoid a mid-call HTTP 429 that would silently
    truncate paginated results (see PR #273).

    Inherits ``partial_frame``, ``partial_response``, and ``manifest``
    from ``PartialResult``. Adds ``remaining`` (the last observed
    header value).
    """

    def __init__(
        self,
        *,
        partial_frame: pd.DataFrame,
        partial_response: requests.Response,
        manifest: ChunkManifest,
        remaining: int,
    ) -> None:
        super().__init__(
            partial_frame=partial_frame,
            partial_response=partial_response,
            manifest=manifest,
            message=(
                f"x-ratelimit-remaining dropped to {remaining} after "
                f"{manifest.completed}/{manifest.total} chunks; aborting to "
                f"avoid mid-call HTTP 429. Catch QuotaExhausted to access "
                f".partial_frame and resume_from=partial_metadata after the "
                f"hourly window resets."
            ),
        )
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

    Raises ``TypeError`` on non-sizable bodies (generators, file-like
    streams). Size-based planning needs a deterministic byte count;
    silently treating an unknown body as zero bytes would under-chunk
    and let the request blow past the server's POST-body limit.
    """
    url_len = len(req.url)
    body = req.body
    if body is None:
        return url_len
    if isinstance(body, (bytes, bytearray)):
        return url_len + len(body)
    if isinstance(body, str):
        return url_len + len(body.encode("utf-8"))
    raise TypeError(
        f"multi_value_chunked cannot size a request body of type "
        f"{type(body).__name__!r}; pass str, bytes, or None. Streaming "
        f"bodies (generators, file-like) are not supported because the "
        f"planner needs a deterministic byte count up front."
    )


def _plan_total(plan: dict[str, list[list[Any]]]) -> int:
    """Sub-request count a plan will issue: the cartesian product of
    per-dim chunk counts. Computed in two places (planner's max_chunks
    early-bail and wrapper's QuotaExhausted payload) — centralized
    here so the two can't drift."""
    return math.prod(len(chunks) for chunks in plan.values())


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
    if max_chunks < 1:
        raise ValueError(
            f"max_chunks must be >= 1; got {max_chunks}. Zero or negative "
            f"values would silently bypass the cap on the no-chunking path."
        )
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
        total = _plan_total(plan)
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
    raw = response.headers.get(_QUOTA_HEADER)
    if raw is None:
        return _QUOTA_UNKNOWN
    try:
        return int(raw)
    except (TypeError, ValueError):
        return _QUOTA_UNKNOWN


def _build_partial_failure(
    *,
    frames: list[pd.DataFrame],
    responses: list[requests.Response],
    canonical_url: str,
    plan_norm: _NormalizedPlan,
    completed: int,
    sub_index: int,
    total: int,
    cause: BaseException,
) -> PartialResult:
    """Assemble a ``PartialResult`` for a sub-request that errored.

    ``completed`` is the count of sub-requests that finished before the
    failure (so a fresh ``ChunkManifest(completed=completed)`` resumes at
    the failed one). ``responses`` may be empty (failure on the very
    first attempted sub-request, including the first chunk of a resume
    call); in that case the synthesized response carries only the
    canonical URL and an empty header set, with the manifest still
    attached so caller-side ``BaseMetadata.chunk_manifest`` works.
    """
    if responses:
        partial = _combine_chunk_responses(responses)
    else:
        partial = requests.Response()
        partial.elapsed = datetime.timedelta(0)
    partial.url = canonical_url
    manifest = ChunkManifest(plan=plan_norm, completed=completed)
    partial.chunk_manifest = manifest
    partial_frame = _combine_chunk_frames(frames) if frames else pd.DataFrame()
    return PartialResult(
        partial_frame=partial_frame,
        partial_response=partial,
        manifest=manifest,
        message=(
            f"Chunked request failed at sub-request "
            f"{sub_index + 1}/{total} ({type(cause).__name__}: {cause}). "
            f"Catch PartialResult to access .partial_frame and resume "
            f"with resume_from=partial_metadata."
        ),
    )


def _resolve_resume(
    resume_from: Any, plan: dict[str, list[list[Any]]] | None
) -> tuple[int, _NormalizedPlan]:
    """Validate a ``resume_from`` metadata against a freshly computed
    plan and return ``(start_index, normalized_plan)``.

    A resume call is only valid when the freshly chunked plan matches
    the saved manifest exactly. Mismatched plans mean the caller's args
    changed between the original call and the retry — silently
    re-fetching with the new plan would produce a frame that
    interleaves data from two incompatible queries.
    """
    manifest = getattr(resume_from, "chunk_manifest", None)
    if manifest is None:
        raise ValueError(
            "resume_from has no chunk_manifest. The original call was "
            "not chunked (or the metadata is from a different source), "
            "so there's nothing to resume."
        )
    if plan is None:
        raise ValueError(
            "resume_from was provided but the current args do not "
            "produce a chunk plan — the request fits in one round-trip. "
            "Pass the same kwargs as the original call to resume."
        )
    fresh = _normalize_plan(plan)
    if fresh != manifest.plan:
        raise ValueError(
            "resume_from manifest does not match the current chunk plan. "
            "The kwargs passed to this call would chunk differently than "
            "the original. Pass identical kwargs (minus resume_from) to "
            "resume; otherwise drop resume_from to issue a fresh query."
        )
    if manifest.is_complete:
        raise ValueError(
            f"resume_from manifest is already complete "
            f"({manifest.completed}/{manifest.total} chunks). There is "
            f"nothing to resume; drop resume_from."
        )
    return manifest.completed, fresh


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
    carrying the combined partial result and a ``ChunkManifest`` so
    callers can resume after the hourly window resets via
    ``resume_from=partial_metadata``, instead of crashing into a
    mid-pagination HTTP 429 (which the upstream pagination loop in
    ``_walk_pages`` historically truncated silently — see PR #273).

    Any other failure inside a sub-request (transport errors, mid-
    pagination ``RuntimeError`` from PR #279, inner-filter
    ``RequestTooLarge``) is re-raised as ``PartialResult`` with the
    same partial-state payload, with the underlying exception preserved
    via ``__cause__``.

    Sits OUTSIDE ``@filters.chunked``: list-chunking is the outer loop,
    filter-chunking is the inner loop. The wrapped function has the same
    signature as ``filters.chunked`` expects — ``(args: dict) -> (frame,
    response)`` — so the two decorators compose cleanly. The planner is
    filter-aware so it doesn't raise prematurely when the inner filter
    chunker would have shrunk the per-sub-request URL on its own.

    Sub-requests run sequentially with no per-call timeout enforced here.
    A hung single sub-request will block the entire chunked call; the
    caller is responsible for configuring an HTTP-layer timeout (e.g.
    via a ``requests.Session`` wrapper) if bounded latency matters.

    Cartesian-product iteration order is deterministic for a given
    ``args`` dict: the wrapper iterates ``plan.values()`` in insertion
    order (Python 3.7+ guarantee), which equals the order in which
    chunkable params appeared in ``args``. For the public waterdata
    getters that order is the function-signature order, so
    ``ChunkManifest.completed`` maps to the same sub-requests across
    repeated calls with the same arguments — resume is well-defined.

    Resume is triggered by passing ``resume_from=partial_metadata`` in
    the caller's kwargs. The wrapper pops it before planning (so it
    never reaches the underlying HTTP request), validates the saved
    plan matches the fresh plan, and skips the already-completed
    cartesian-product combinations. See ``get_daily``'s docstring for
    a worked retry-loop example using a one-hour deadline matched to
    the API's rate-limit window.
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
            # ``resume_from`` is a chunker-only control kwarg; pull it
            # out before any planner/URL probe runs so it can't reach
            # the underlying API as a bogus query parameter.
            resume_from = args.get("resume_from")
            args = {k: v for k, v in args.items() if k != "resume_from"}

            plan = _plan_chunks(args, build_request, limit, max_chunks)

            if resume_from is not None:
                start_index, plan_norm = _resolve_resume(resume_from, plan)
            else:
                start_index = 0
                plan_norm = _normalize_plan(plan) if plan is not None else ()

            if plan is None:
                return fetch_once(args)

            # Pre-build the canonical URL representing the user's full
            # original query. The chunker sends sub-requests with sliced
            # multi-value lists; without this restore, the aggregated
            # response's ``.url`` would only show the first chunk and
            # callers logging ``md.url`` for reproducibility would see a
            # truncated view of their own query.
            canonical_url = build_request(**args).url

            keys = list(plan)
            total = _plan_total(plan)
            frames: list[pd.DataFrame] = []
            responses: list[requests.Response] = []
            combos = itertools.islice(
                itertools.product(*(plan[k] for k in keys)), start_index, None
            )
            for i, combo in enumerate(combos, start=start_index):
                sub_args = {**args, **dict(zip(keys, combo))}
                try:
                    frame, response = fetch_once(sub_args)
                except Exception as exc:
                    raise _build_partial_failure(
                        frames=frames,
                        responses=responses,
                        canonical_url=canonical_url,
                        plan_norm=plan_norm,
                        completed=i,
                        sub_index=i,
                        total=total,
                        cause=exc,
                    ) from exc
                frames.append(frame)
                responses.append(response)
                # Skip the quota check after the last sub-request —
                # nothing left to abort.
                if i < total - 1:
                    remaining = _read_remaining(response)
                    if remaining < floor:
                        partial = _combine_chunk_responses(responses)
                        partial.url = canonical_url
                        manifest = ChunkManifest(plan=plan_norm, completed=i + 1)
                        partial.chunk_manifest = manifest
                        raise QuotaExhausted(
                            partial_frame=_combine_chunk_frames(frames),
                            partial_response=partial,
                            manifest=manifest,
                            remaining=remaining,
                        )

            combined = _combine_chunk_responses(responses)
            combined.url = canonical_url
            combined.chunk_manifest = ChunkManifest(plan=plan_norm, completed=total)
            return _combine_chunk_frames(frames), combined

        return wrapper  # type: ignore[return-value]

    return decorator
