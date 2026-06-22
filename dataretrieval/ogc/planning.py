"""Pure URL-byte chunk planning and result recombination (no I/O).

This module holds the side-effect-free half of the chunker: deciding how
to split one over-budget OGC request into URL-fitting sub-requests
(:class:`ChunkPlan` and the axis/byte-accounting helpers) and reassembling
their per-chunk frames and responses (:func:`_combine_chunk_frames`,
:func:`_combine_chunk_responses`). It has no event loop, retry policy, or
network state — those live in :mod:`dataretrieval.ogc.chunking` (execution)
and :mod:`dataretrieval.ogc.retry` (retry policy), which import the plan and
drive it. Keeping the planning/combination logic here
makes it unit-testable without an HTTP client and gives the two concerns
separate reasons to change.
"""

from __future__ import annotations

import copy
import itertools
import math
from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from urllib.parse import quote_plus

import httpx
import pandas as pd

from dataretrieval.exceptions import Unchunkable
from dataretrieval.ogc.filters import (
    _check_numeric_filter_pitfall,
    _is_chunkable,
    _split_top_level_or,
)

# Any list-shaped kwarg with >1 element is chunked (comma-joined per
# sub-list in the URL); ~90 OGC params qualify, so we denylist the few
# exceptions rather than maintain a growing allowlist. Excluded because:
# ``properties`` defines the column schema; ``bbox`` is a fixed coord
# tuple; date/time params are intervals, not enumerable sets; ``filter``
# is handled as its own OR-axis in ``_extract_axes``; and ``limit`` /
# ``skip_geometry`` / ``filter_lang`` are scalar by contract.
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
        "limit",
        "skip_geometry",
    }
)


# Separators the two axis kinds use to join their atoms back into
# URL text. List axes comma-join values (``site=USGS-A,USGS-B``); the
# filter axis OR-joins clauses (``filter=a='1' OR a='2'``).
_LIST_SEP = ","


_OR_SEP = " OR "


def _request_bytes(req: httpx.Request) -> int:
    """
    Return the total bytes of an httpx request: URL + body.

    GET routes have empty ``.content`` and reduce to URL length. POST
    routes (CQL2 JSON body) need body bytes — the URL stays short
    regardless of payload, so URL-only sizing would underestimate the
    request and skip chunking when it's needed.

    Parameters
    ----------
    req : httpx.Request
        The request to size.

    Returns
    -------
    int
        ``len(str(req.url)) + len(req.content)``. ``httpx.URL`` doesn't
        support ``len()`` directly, so the str-coercion is required.
    """
    return len(str(req.url)) + len(req.content)


def _safe_request_bytes(
    build_request: Callable[..., httpx.Request],
    args: dict[str, Any],
    url_limit: int,
) -> int:
    """
    Size a candidate sub-request, treating ``httpx.InvalidURL`` as
    "still too large".

    ``httpx.URL`` enforces a hard 64 KB cap per URL component
    (``MAX_URL_LENGTH``) and raises ``httpx.InvalidURL`` for anything
    bigger. We report ``url_limit + 1`` on overflow so the greedy
    halving loop in :meth:`ChunkPlan._plan` keeps shrinking the
    largest axis until ``httpx.Request`` can be constructed at all.

    Parameters
    ----------
    build_request : Callable[..., httpx.Request]
        Factory that turns a kwargs dict into a sized request.
    args : dict[str, Any]
        Per-sub-request kwargs to pass through to ``build_request``.
    url_limit : int
        The chunker's byte budget; returned + 1 on overflow.

    Returns
    -------
    int
        Real byte count when the request builds, otherwise
        ``url_limit + 1`` so the planner's "too large" branch keeps
        halving.
    """
    try:
        req = build_request(**args)
    except httpx.InvalidURL:
        return url_limit + 1
    return _request_bytes(req)


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
        return response.elapsed
    except RuntimeError:
        return timedelta(0)


def _set_response_url(response: httpx.Response, url: str | httpx.URL) -> None:
    """
    Overwrite the URL surfaced by a response without back-propagating
    the change into any aliased original.

    Try the direct assignment first: on lightweight test mocks ``.url``
    is a plain writable attribute. On real ``httpx.Response`` it's
    read-only (it resolves through the bound request), so swap in a
    fresh :class:`httpx.Request` carrying the new URL — mutating the
    existing one would leak through any shallow copy that shares the
    same ``.request``.
    """
    try:
        response.url = url  # type: ignore[misc, assignment]
    except AttributeError:
        target = httpx.URL(str(url))
        try:
            old = response.request
        except RuntimeError:
            # No request bound (some hand-built httpx.Response fixtures);
            # synthesize a minimal one to hold the URL.
            response.request = httpx.Request("GET", target)
            return
        response.request = httpx.Request(
            method=old.method, url=target, headers=old.headers
        )


@dataclass(frozen=True)
class _Axis:
    """
    A single chunkable axis of one user-level request — a list of
    atomic units and the separator that joins them in the URL.

    Both multi-value list parameters (``sites=[...]``, joiner ``","``)
    and the cql-text ``filter`` (split on top-level ``OR``, joiner
    ``" OR "``) fit this shape, so a single greedy halving loop in
    ``ChunkPlan._plan`` handles both — no need for two separate
    algorithms.

    Attributes
    ----------
    arg_key : str
        The args-dict key this axis substitutes back into when a
        sub-request is rendered.
    atoms : tuple of str
        The smallest indivisible units along this axis (one site, one
        OR-clause, …). A "chunk" is a contiguous slice of ``atoms``.
    joiner : str
        Separator placed between atoms when they are joined back into
        URL text — ``","`` for list axes, ``" OR "`` for the filter
        axis.
    """

    arg_key: str
    atoms: tuple[str, ...]
    joiner: str

    def chunk_bytes(self, chunk: list[str]) -> int:
        """
        Return the URL-encoded byte count this chunk contributes when
        substituted into the request.

        ``quote_plus`` is faithful to what the real URL builder
        produces, so values containing characters that expand under URL
        encoding (``%``, ``+``, ``/``, ``&``, …) can't be mis-ranked.

        Parameters
        ----------
        chunk : list of str
            A contiguous slice of ``self.atoms``.

        Returns
        -------
        int
            Length of ``quote_plus(self.joiner.join(chunk))``.
        """
        return len(quote_plus(self.joiner.join(map(str, chunk))))

    def render(self, chunk: list[str]) -> Any:
        """
        Convert a chunk into the form the URL builder expects.

        List axes yield a fresh list of atoms (``build_request`` will
        comma-join); the filter axis yields a pre-joined string (CQL
        doesn't take a list).

        Parameters
        ----------
        chunk : list of str
            A contiguous slice of ``self.atoms``.

        Returns
        -------
        list of str or str
            ``list(chunk)`` for list axes, ``self.joiner.join(chunk)``
            for the filter axis.
        """
        return list(chunk) if self.joiner == _LIST_SEP else self.joiner.join(chunk)


def _extract_axes(args: dict[str, Any]) -> list[_Axis]:
    """
    Build the chunkable-axis set from a request's args.

    Multi-value list params with more than one element each become an
    axis. The cql-text filter (when chunkable and split into more than
    one top-level OR-clause) becomes one too. Anything in
    ``_NEVER_CHUNK`` is excluded except ``filter`` itself, which is
    handled separately so its atoms are clauses not characters.

    Parameters
    ----------
    args : dict[str, Any]
        The user-level request kwargs (the same dict that would be
        passed to ``build_request``).

    Returns
    -------
    list[_Axis]
        Zero or more axes in insertion order: list axes first (one
        per eligible kwarg, in ``args`` order), then the filter axis
        if present.
    """
    axes: list[_Axis] = []
    for key, value in args.items():
        if key in _NEVER_CHUNK:
            continue
        if isinstance(value, (list, tuple)) and len(value) > 1:
            axes.append(_Axis(arg_key=key, atoms=tuple(value), joiner=_LIST_SEP))

    filter_expr = args.get("filter")
    if filter_expr is not None and _is_chunkable(filter_expr, args.get("filter_lang")):
        _check_numeric_filter_pitfall(filter_expr)
        clauses = _split_top_level_or(filter_expr)
        if len(clauses) >= 2:
            axes.append(_Axis(arg_key="filter", atoms=tuple(clauses), joiner=_OR_SEP))
    return axes


class ChunkPlan:
    """
    Strategy for issuing one user-level request as a sequence of
    sub-requests whose URLs each fit ``url_limit``.

    Constructing a plan *is* planning:
    ``ChunkPlan(args, build_request, url_limit)`` extracts the
    chunkable axes, runs greedy halving on the biggest chunk across
    all axes, and stores the result.

    Passthrough requests (no chunkable axes, or already fitting) are
    represented as a trivial plan with empty ``axes`` / ``chunks`` and
    ``total == 1``; :meth:`iter_sub_args` yields the original args
    unchanged so the ``ChunkedCall`` loop is the same shape either
    way.

    Parameters
    ----------
    args : dict[str, Any]
        The user-level request kwargs.
    build_request : Callable[..., httpx.Request]
        Factory that turns a kwargs dict into a sized httpx request,
        e.g. ``_construct_api_requests``.
    url_limit : int
        Byte budget for the request (URL + body).

    Attributes
    ----------
    args : dict
        The original user-level args this plan was built for. Bound to
        the plan so :meth:`iter_sub_args` is self-contained.
    axes : list[_Axis]
        The chunkable axes of ``args``: each multi-value list
        parameter, plus the cql-text filter (if any) split on top-level
        OR. Empty in the passthrough case.
    chunks : dict[str, list[list[str]]]
        Per-axis partition: ``chunks[axis.arg_key]`` is the list of
        atom-sublists this axis is split into. Empty in passthrough.
    canonical_url : str or None
        URL of the user's original (un-chunked) request, used to
        overwrite a chunked response's ``.url`` so ``BaseMetadata``
        reflects the full query. ``None`` on the passthrough path
        and when no buildable URL exists.

    Raises
    ------
    Unchunkable
        If the request needs chunking but even the singleton plan
        doesn't fit ``url_limit``.
    """

    def __init__(
        self,
        args: dict[str, Any],
        build_request: Callable[..., httpx.Request],
        url_limit: int,
    ) -> None:
        self.args = args
        self.axes: list[_Axis] = []
        self.chunks: dict[str, list[list[str]]] = {}
        self.canonical_url: str | None = None

        axes = _extract_axes(args)
        if not axes:
            # No chunkable axis: nothing to split. If the single request fits,
            # run it verbatim (the common passthrough). ``_safe_request_bytes``
            # treats an un-constructable URL (httpx.InvalidURL, > 64 KB) as over
            # budget.
            if _safe_request_bytes(build_request, args, url_limit) <= url_limit:
                return
            # Over budget. A filter the chunker doesn't manage — cql-json — is
            # passed through unchanged (chunking applies only to cql-text); the
            # server, not us, judges it. Otherwise this is an in-domain shape we
            # would normally chunk but can't (a single large CQL ``IN`` clause
            # with no top-level ``OR``, or one oversized value), so raise an
            # actionable error instead of shipping it for an opaque HTTP 414.
            filter_expr = args.get("filter")
            if filter_expr is not None and not _is_chunkable(
                filter_expr, args.get("filter_lang")
            ):
                return
            raise Unchunkable(
                f"Request exceeds {url_limit} bytes (URL + body) and has no "
                f"chunkable multi-value argument to split (e.g. a single large "
                f"CQL `IN` clause, or one oversized value). Narrow the query, "
                f"simplify the filter, or split the call manually."
            )

        # Constructing the initial request can itself trip
        # ``httpx.InvalidURL`` (URL > 64 KB) — that's the canonical
        # "needs chunking" signal, so swallow it and proceed to plan.
        # When the unchunked URL does build, preserve it as
        # ``canonical_url`` so ``BaseMetadata.url`` echoes the user's
        # original query verbatim; only fall back to a worst-case
        # sub-request URL when the URL itself can't be constructed.
        try:
            initial_request = build_request(**args)
        except httpx.InvalidURL:
            initial_request = None

        if initial_request is not None:
            self.canonical_url = str(initial_request.url)
            if _request_bytes(initial_request) <= url_limit:
                return

        self.axes = axes
        self.chunks = {axis.arg_key: [list(axis.atoms)] for axis in axes}
        self._plan(build_request, url_limit)

        if self.canonical_url is None:
            # Original URL was un-constructable (httpx.InvalidURL); fall
            # back to the worst-case sub-request URL so
            # ``BaseMetadata.url`` still surfaces something
            # informative. If even that overflows, leave canonical_url
            # as None (set above) and let the response's own URL stand.
            with suppress(httpx.InvalidURL):
                self.canonical_url = str(build_request(**self._worst_case_args()).url)

    def _plan(
        self,
        build_request: Callable[..., httpx.Request],
        url_limit: int,
    ) -> None:
        """
        Greedy-halve the biggest chunk across all axes until the
        worst-case sub-request URL fits ``url_limit``. Mutates
        ``self.chunks`` in place; treats list axes and the filter axis
        uniformly — each is just a list of atoms joined by its axis's
        separator.

        Raises
        ------
        Unchunkable
            If even the singleton plan (every axis at one atom per
            chunk) still exceeds ``url_limit``.
        """
        while True:
            worst = self._worst_case_args()
            if _safe_request_bytes(build_request, worst, url_limit) <= url_limit:
                return

            biggest_axis: _Axis | None = None
            biggest_idx = -1
            biggest_size = -1
            for axis in self.axes:
                for idx, chunk in enumerate(self.chunks[axis.arg_key]):
                    if len(chunk) <= 1:
                        continue
                    size = axis.chunk_bytes(chunk)
                    if size > biggest_size:
                        biggest_axis, biggest_idx, biggest_size = axis, idx, size

            if biggest_axis is None:
                raise Unchunkable(
                    f"Request exceeds {url_limit} bytes (URL + body) at the "
                    f"smallest reducible plan (every axis at one atom per "
                    f"sub-request). Reduce input sizes, shorten or simplify "
                    f"the filter, or split the call manually."
                )
            axis_chunks = self.chunks[biggest_axis.arg_key]
            chunk = axis_chunks[biggest_idx]
            mid = len(chunk) // 2
            axis_chunks[biggest_idx : biggest_idx + 1] = [chunk[:mid], chunk[mid:]]

    def _worst_case_args(self) -> dict[str, Any]:
        """
        Args dict representing the largest sub-request the current
        ``self.chunks`` partition will issue — each axis's longest
        (by URL-encoded bytes) chunk rendered back in.
        """
        out = dict(self.args)
        for axis in self.axes:
            worst = max(self.chunks[axis.arg_key], key=axis.chunk_bytes)
            out[axis.arg_key] = axis.render(worst)
        return out

    @property
    def total(self) -> int:
        """
        Total sub-request count: product of per-axis chunk counts.

        Returns
        -------
        int
            ``1`` for the passthrough plan, otherwise the cartesian
            product of ``len(chunks[ax.arg_key])`` across all axes.
        """
        return math.prod((len(self.chunks[ax.arg_key]) for ax in self.axes), start=1)

    def iter_sub_args(self) -> Iterator[dict[str, Any]]:
        """
        Yield substituted args for each sub-request, in deterministic
        order — cartesian product over axes in extraction order.

        The same plan yields the same sub-args sequence on every
        invocation, so resume is well-defined.

        Yields
        ------
        dict[str, Any]
            A copy of ``self.args`` with each axis's current chunk
            substituted under its ``arg_key``.
        """
        if not self.axes:
            yield dict(self.args)
            return
        chunk_lists = [self.chunks[ax.arg_key] for ax in self.axes]
        for combo in itertools.product(*chunk_lists):
            sub_args = dict(self.args)
            for axis, chunk in zip(self.axes, combo):
                sub_args[axis.arg_key] = axis.render(chunk)
            yield sub_args


def _combine_chunk_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Concatenate per-chunk frames, dropping empties and deduping by ``id``.

    Parameters
    ----------
    frames : list[pandas.DataFrame]
        One frame per completed sub-request.

    Returns
    -------
    pandas.DataFrame
        The concatenated, deduplicated result. Empty when every input
        frame is empty.

    Notes
    -----
    An empty chunk can be a plain ``pd.DataFrame()`` (no geopandas);
    concatenating it with real ``GeoDataFrame``s downgrades the result
    to plain ``DataFrame`` and strips geometry/CRS, so empties are
    dropped first. Dedup on the pre-rename feature ``id`` keeps
    overlapping user OR-clauses from producing duplicate rows across
    chunks.

    Dedup is restricted to rows whose ``id`` is non-null. ``pandas``
    treats NaN==NaN as a duplicate for ``drop_duplicates``, so a
    blanket call would collapse every id-less row into a single one —
    silent data loss if any chunk emits features without an
    ``id`` field.
    """
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        # Preserve the frame type (GeoDataFrame vs DataFrame) of the
        # input even when every chunk is empty — ``_get_resp_data``
        # returns ``gpd.GeoDataFrame()`` on empty geopd responses, and
        # returning a plain ``pd.DataFrame()`` here would downgrade
        # the type in a downstream ``pd.concat([result, geo_page])`` to
        # a plain DataFrame and strip geometry/CRS.
        return frames[0] if frames else pd.DataFrame()
    if len(non_empty) == 1:
        # Single-completed-chunk fast path. Return a copy so callers
        # who treat ``ChunkedCall.partial_frame`` as a fresh result
        # (the property docstring says "live; recomputed per access")
        # don't accidentally mutate ``_chunks[0][0]`` in place.
        return non_empty[0].copy()
    combined = pd.concat(non_empty, ignore_index=True)
    if "id" in combined.columns:
        has_id = combined["id"].notna()
        if has_id.all():
            combined = combined.drop_duplicates(subset="id", ignore_index=True)
        elif has_id.any():
            # Mixed: dedupe only the id-bearing rows; preserve id-less
            # rows verbatim (their order relative to id-bearing rows
            # may shift, which is acceptable — dedup can't be id-keyed
            # for rows without an id).
            id_rows = combined[has_id].drop_duplicates(subset="id")
            no_id_rows = combined[~has_id]
            combined = pd.concat([id_rows, no_id_rows], ignore_index=True)
    return combined


def _combine_chunk_responses(
    responses: list[httpx.Response], canonical_url: str | None
) -> httpx.Response:
    """
    Fold per-sub-request responses into a single aggregated response.

    For a multi-response input, returns a shallow copy of
    ``responses[0]`` with ``.headers`` set to the last response's (so
    ``x-ratelimit-remaining`` reflects current state), ``.elapsed`` set
    to total wall-clock across every response, and ``.url`` set to the
    canonical original-query URL (when supplied) so ``BaseMetadata``
    reflects the user's full request rather than the first chunk.

    For a single-response input with no canonical-URL override,
    ``responses[0]`` is returned unchanged to skip the copy on the
    passthrough hot path.

    Parameters
    ----------
    responses : list[httpx.Response]
        One response per completed sub-request, in execution order.
    canonical_url : str or None
        URL of the unchunked original request. ``None`` skips the URL
        override — used by the passthrough path (the fetcher's
        response already carries the original-query URL) and by the
        worst-case overflow path (no buildable canonical URL exists).

    Returns
    -------
    httpx.Response
        A shallow copy of the first response with aggregated
        ``headers``, ``elapsed``, and ``url``. The function is
        idempotent (the input responses' ``headers`` / ``elapsed`` /
        ``url`` are never mutated), so it's safe to call repeatedly
        via :attr:`ChunkedCall.partial_response` during error
        inspection or resume retries. ``headers`` on the returned
        object is a fresh ``httpx.Headers``, so mutations there don't
        back-propagate into any chunk's underlying response.
    """
    if len(responses) == 1 and canonical_url is None:
        return responses[0]

    # ``copy.copy`` lets repeated calls re-sum elapsed from scratch
    # rather than re-mutating ``responses[0]`` in place. The headers
    # dict is then rewrapped in a fresh ``httpx.Headers`` so the
    # aggregate's headers don't share identity with — or leak mutations
    # back into — any underlying response on ``ChunkedCall._chunks``.
    head = copy.copy(responses[0])
    if len(responses) > 1:
        head.headers = httpx.Headers(responses[-1].headers)
        head.elapsed = sum(
            (_safe_elapsed(r) for r in responses[1:]),
            start=_safe_elapsed(responses[0]),
        )
    else:
        head.headers = httpx.Headers(responses[0].headers)
    if canonical_url is not None:
        _set_response_url(head, canonical_url)
    return head
