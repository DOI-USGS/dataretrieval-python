"""Pure URL-byte chunk planning (no I/O).

This module holds the side-effect-free planning half of the chunker:
deciding how to split one over-budget OGC request into URL-fitting
sub-requests (:class:`ChunkPlan` and the axis/byte-accounting helpers).
It has no event loop, retry policy, or network state — those live in
:mod:`dataretrieval.ogc.chunking` (resumable execution) and
:mod:`dataretrieval.transport.retry` (retry policy), which import the plan and
drive it.

Result recombination — reassembling the per-chunk frames and responses
back into one result
(:func:`~dataretrieval.transport.combining._combine_chunk_frames`,
:func:`~dataretrieval.transport.combining._combine_chunk_responses`, etc.) —
lives in the API-neutral :mod:`dataretrieval.transport.combining` module.
"""

from __future__ import annotations

import itertools
import math
from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus

import httpx

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


def _split_at(chunks: list[list[str]], idx: int) -> None:
    """Replace ``chunks[idx]`` in place with its two contiguous halves.

    The single primitive both planning passes use to fan an axis out. It
    preserves the partition invariants every consumer relies on: *coverage*
    (each atom survives, exactly once) and *contiguous, deterministic order*
    (resume and :meth:`ChunkPlan.iter_sub_args` depend on it). Kept in one
    place so those invariants can't drift between :meth:`ChunkPlan._plan`
    (byte-driven) and :meth:`ChunkPlan._refine` (fan-out-driven).
    """
    chunk = chunks[idx]
    mid = len(chunk) // 2
    chunks[idx : idx + 1] = [chunk[:mid], chunk[mid:]]


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
        Byte budget for the request (URL + body) — a hard ceiling every
        sub-request must fit.
    max_chunks : int, optional
        Hard cap on the plan's total sub-request count (default ``1`` = off).
        ``1`` chunks only as much as ``url_limit`` requires — the most
        conservative plan, fewest sub-requests — so a fitting request is a
        passthrough. A cap of ``2`` or more fans the plan out to up to
        ``max_chunks`` sub-requests overall (the cartesian product across axes,
        never fewer than the byte budget already forces) — capped as a whole,
        not per axis, so several multi-value axes can't multiply past the cap.
        The plan never exceeds the cap and may land below it when no whole
        split lands on it exactly. ``max_chunks`` is a sub-request count, so a
        value below ``1`` (``0`` or negative) is a caller error and raises
        ``ValueError``. Set from the
        :func:`~dataretrieval.ogc.chunking.parallel_chunks` ``n``; see
        :meth:`_refine`.

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
    ValueError
        If ``max_chunks`` is less than 1 (0 or negative).
    """

    def __init__(
        self,
        args: dict[str, Any],
        build_request: Callable[..., httpx.Request],
        url_limit: int,
        max_chunks: int = 1,
    ) -> None:
        if max_chunks < 1:
            # ``max_chunks`` is a sub-request *count*: the minimum is ``1``
            # (the ambient default outside any ``parallel_chunks`` block),
            # which means "off — no extra fan-out". ``0`` or negative is a
            # meaningless count and can only be a caller bug, so fail loudly
            # rather than silently no-op. The public ``parallel_chunks(n)``
            # already rejects ``n < 1``; this guards direct construction.
            raise ValueError(
                f"max_chunks must be >= 1 (1 disables fan-out); got {max_chunks!r}."
            )

        self.args = args
        self.axes: list[_Axis] = []
        self.chunks: dict[str, list[list[str]]] = {}
        self.canonical_url: str | None = None

        axes = _extract_axes(args)
        if not axes:
            # No chunkable axis: nothing to split, and ``parallel_chunks`` has
            # nothing to act on either. If the single request fits, run it
            # verbatim (the common passthrough). ``_safe_request_bytes`` treats
            # an un-constructable URL (httpx.InvalidURL, > 64 KB) as over budget.
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

        fits = False
        if initial_request is not None:
            self.canonical_url = str(initial_request.url)
            fits = _request_bytes(initial_request) <= url_limit

        # A request that already fits and hasn't opted into finer chunking is
        # the common passthrough: leave ``axes``/``chunks`` empty so
        # ``total == 1`` and ``iter_sub_args`` yields the original args
        # verbatim. ``max_chunks == 1`` (off / no extra fan-out) means
        # "don't split", so it takes this path; only ``max_chunks >= 2`` asks
        # for extra fan-out and sets the axes up to be refined below.
        if fits and max_chunks <= 1:
            return

        self.axes = axes
        self.chunks = {axis.arg_key: [list(axis.atoms)] for axis in axes}
        if not fits:
            # Hard pass: greedy-halve until every worst-case sub-request fits
            # the byte budget (may raise ``Unchunkable``).
            self._plan(build_request, url_limit)
        # Soft pass: optionally split further than the byte budget requires.
        # Purely additive — never re-raises, and the byte budget stays
        # satisfied; a no-op at ``max_chunks == 1``.
        self._refine(max_chunks)

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
            _split_at(self.chunks[biggest_axis.arg_key], biggest_idx)

    def _refine(self, max_chunks: int) -> None:
        """
        Fan the plan out more finely than the byte budget alone requires —
        the ``parallel_chunks`` dial (see
        :func:`~dataretrieval.ogc.chunking.parallel_chunks` for why a caller
        would want this, and :class:`ChunkPlan`'s ``max_chunks`` parameter for
        the cap's contract: total-not-per-axis, a hard ceiling that may land
        below the cap).

        Implementation. Each split multiplies the plan by ``(k+1)/k`` for the
        chosen axis (adding ``total // k`` sub-requests, not one), so a split
        is taken only when it keeps :attr:`total` within the cap; when no
        in-budget split remains the plan stops *below* the cap rather than
        overshooting (two even axes can reach 4 but not 5, so a cap of 5 yields
        4). Each split picks the single largest splittable chunk among the
        in-budget axes (ties broken by axis-extraction order, then lowest
        index), so growth is distributed round-robin rather than one axis
        saturating before another is touched. Purely additive — only ever
        *splits* existing chunks, so the byte pass's work and the ``url_limit``
        invariant are both preserved, and it never raises. A no-op at
        ``max_chunks == 1``.

        Parameters
        ----------
        max_chunks : int
            The ``parallel_chunks(n)`` value; see :class:`ChunkPlan`'s
            ``max_chunks`` parameter for the full contract.
        """
        if max_chunks <= 1:
            return
        while True:
            total = self.total
            if total >= max_chunks:
                return
            # Largest splittable chunk among the axes whose split still fits the
            # cap. Splitting any chunk of an axis with ``k`` chunks turns that
            # ``k`` into ``k+1``, so it adds ``total // k`` sub-requests (the
            # product of the other axes) regardless of which chunk — hence the
            # budget test is per axis, not per chunk. Skipping an over-budget
            # axis makes ``max_chunks`` a true ceiling. The ranking key is atom
            # count (``len``), not URL bytes like ``_plan`` — this pass balances
            # work across sub-requests rather than fitting a byte budget. A
            # chunk of size 1 can't be split further. Stable input order breaks
            # ties by axis order, then lowest index within an axis.
            candidate: tuple[_Axis, int] | None = None
            candidate_size = -1
            for axis in self.axes:
                axis_chunks = self.chunks[axis.arg_key]
                if total + total // len(axis_chunks) > max_chunks:
                    continue  # any split of this axis would overshoot the cap
                for idx, chunk in enumerate(axis_chunks):
                    if len(chunk) <= 1:
                        continue
                    if len(chunk) > candidate_size:
                        candidate, candidate_size = (axis, idx), len(chunk)
            if candidate is None:
                # Every axis is saturated at one atom per chunk or would
                # overshoot the cap; stop below it rather than exceed it.
                return
            axis, idx = candidate
            _split_at(self.chunks[axis.arg_key], idx)

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
            for axis, chunk in zip(self.axes, combo, strict=False):
                sub_args[axis.arg_key] = axis.render(chunk)
            yield sub_args
