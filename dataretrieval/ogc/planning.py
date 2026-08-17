"""Pure URL-byte chunk planning (no I/O).

This module holds the side-effect-free planning half of the chunker:
deciding how to split one over-budget OGC request into URL-fitting
chunks (:class:`ChunkPlan` and the axis/byte-accounting helpers).
It has no event loop, retry policy, or network state — those live in
:mod:`dataretrieval.ogc.chunking` (resumable execution) and
:mod:`dataretrieval.transport.retry` (retry policy), which import the plan and
drive it.

Result recombination — reassembling the per-chunk frames and responses
back into one result
(:func:`~dataretrieval.combining._combine_chunk_frames`,
:func:`~dataretrieval.combining._combine_chunk_responses`, etc.) —
lives in the top-level :mod:`dataretrieval.combining` module.
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


def _try_build(
    build_request: Callable[..., httpx.Request],
    args: dict[str, Any],
) -> httpx.Request | None:
    """Attempt to construct a request, returning ``None`` on overflow.

    ``httpx.URL`` enforces a hard 64 KB cap per URL component and raises
    ``httpx.InvalidURL`` for anything bigger.  Both :func:`_safe_request_bytes`
    and :meth:`ChunkPlan._probe_initial_request` need exactly this
    "build-or-None" step, so it lives here once.

    Parameters
    ----------
    build_request : Callable[..., httpx.Request]
        Factory that turns a kwargs dict into a sized request.
    args : dict[str, Any]
        Per-chunk kwargs to pass through to ``build_request``.

    Returns
    -------
    httpx.Request or None
        The built request, or ``None`` when construction raised
        ``httpx.InvalidURL``.
    """
    try:
        return build_request(**args)
    except httpx.InvalidURL:
        return None


def _safe_request_bytes(
    build_request: Callable[..., httpx.Request],
    args: dict[str, Any],
    url_limit: int,
) -> int:
    """
    Size a candidate chunk, treating ``httpx.InvalidURL`` as "too large".

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
        Per-chunk kwargs to pass through to ``build_request``.
    url_limit : int
        The chunker's byte budget; returned + 1 on overflow.

    Returns
    -------
    int
        Real byte count when the request builds, otherwise
        ``url_limit + 1`` so the planner's "too large" branch keeps
        halving.
    """
    req = _try_build(build_request, args)
    return _request_bytes(req) if req is not None else url_limit + 1


@dataclass(frozen=True)
class _Axis:
    """
    A single chunkable axis of one user-level request.

    An axis is a list of atomic units plus the separator that joins them in
    the URL. Both multi-value list parameters (``sites=[...]``, joiner ``","``)
    and the cql-text ``filter`` (split on top-level ``OR``, joiner
    ``" OR "``) fit this shape, so a single greedy halving loop in
    ``ChunkPlan._plan`` handles both — no need for two separate
    algorithms.

    Attributes
    ----------
    arg_key : str
        The args-dict key this axis substitutes back into when a
        chunk is rendered.
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
        Return the URL-encoded byte count this chunk contributes to the request.

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


def _filter_axis(args: dict[str, Any]) -> _Axis | None:
    """Build the filter axis from CQL-text ``filter``, if chunkable.

    Returns an :class:`_Axis` whose atoms are top-level OR-clauses when the
    filter has two or more splittable clauses; ``None`` otherwise.
    """
    filter_expr = args.get("filter")
    if filter_expr is None or not _is_chunkable(filter_expr, args.get("filter_lang")):
        return None
    _check_numeric_filter_pitfall(filter_expr)
    clauses = _split_top_level_or(filter_expr)
    if len(clauses) < 2:
        return None
    return _Axis(arg_key="filter", atoms=tuple(clauses), joiner=_OR_SEP)


def _extract_axes(args: dict[str, Any]) -> list[_Axis]:
    """
    Build the chunkable-axis set from a request's args.

    Multi-value list params with more than one element each become an
    axis. The cql-text filter (when chunkable and split into more than
    one top-level OR-clause) becomes one too. Anything in
    ``_NEVER_CHUNK`` is excluded except ``filter`` itself, which is
    handled separately so its atoms are clauses, not characters.

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
    axes: list[_Axis] = [
        _Axis(arg_key=key, atoms=tuple(value), joiner=_LIST_SEP)
        for key, value in args.items()
        if key not in _NEVER_CHUNK
        and isinstance(value, (list, tuple))
        and len(value) > 1
    ]
    fax = _filter_axis(args)
    if fax is not None:
        axes.append(fax)
    return axes


def _split_at(chunks: list[list[str]], idx: int) -> None:
    """Replace ``chunks[idx]`` in place with its two contiguous halves.

    The single primitive both planning passes use to fan an axis out. It
    preserves the partition invariants every consumer relies on: *coverage*
    (each atom survives, exactly once) and *contiguous, deterministic order*
    (resume and :meth:`ChunkPlan.iter_chunk_args` depend on it). Kept in one
    place so those invariants can't drift between :meth:`ChunkPlan._plan`
    (byte-driven) and :meth:`ChunkPlan._refine` (fan-out-driven).
    """
    chunk = chunks[idx]
    mid = len(chunk) // 2
    chunks[idx : idx + 1] = [chunk[:mid], chunk[mid:]]


class ChunkPlan:
    """
    Strategy for issuing one user-level request as URL-fitting chunks.

    Every chunk URL fits ``url_limit``. Constructing a plan *is* planning:
    ``ChunkPlan(args, build_request, url_limit)`` extracts the
    chunkable axes, runs greedy halving on the biggest chunk across
    all axes, and stores the result.

    Passthrough requests (no chunkable axes, or already fitting) are
    represented as a trivial plan with empty ``axes`` / ``chunks`` and
    ``total == 1``; :meth:`iter_chunk_args` yields the original args
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
        chunk must fit.
    max_chunks : int, optional
        Hard cap on the plan's total chunk count (default ``1`` = off).
        ``1`` chunks only as much as ``url_limit`` requires — the most
        conservative plan, fewest chunks — so a fitting request is a
        passthrough. A cap of ``2`` or more fans the plan out to up to
        ``max_chunks`` chunks overall (the cartesian product across axes,
        never fewer than the byte budget already forces). The cap applies to
        the plan as a whole, not per axis, so several multi-value axes can't
        multiply past it. The plan never exceeds the cap and may land below it
        when no whole split lands on it exactly. ``max_chunks`` is a
        chunk count, so a value below ``1`` (``0`` or negative) is a
        caller error and raises ``ValueError``. Set from the
        :func:`~dataretrieval.ogc.chunking.parallel_chunks` ``n``; see
        :meth:`_refine`.

    Attributes
    ----------
    args : dict
        The original user-level args this plan was built for. Bound to
        the plan so :meth:`iter_chunk_args` is self-contained.
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
            raise ValueError(
                f"max_chunks must be >= 1 (1 disables fan-out); got {max_chunks!r}."
            )

        self.args = args
        self.axes: list[_Axis] = []
        self.chunks: dict[str, list[list[str]]] = {}
        self.canonical_url: str | None = None

        axes = _extract_axes(args)
        if not axes:
            self._handle_no_axes(args, build_request, url_limit)
            return

        initial_request, fits = self._probe_initial_request(
            args, build_request, url_limit
        )
        if initial_request is not None:
            self.canonical_url = str(initial_request.url)

        if fits and max_chunks <= 1:
            return

        self.axes = axes
        self.chunks = {axis.arg_key: [list(axis.atoms)] for axis in axes}
        if not fits:
            self._plan(build_request, url_limit)
        self._refine(max_chunks)

        if self.canonical_url is None:
            with suppress(httpx.InvalidURL):
                self.canonical_url = str(build_request(**self._worst_case_args()).url)

    def _handle_no_axes(
        self,
        args: dict[str, Any],
        build_request: Callable[..., httpx.Request],
        url_limit: int,
    ) -> None:
        """Handle the case where no chunkable axes exist.

        Passthrough when the single request fits or when the filter is in a
        language the chunker doesn't manage (cql-json). Raises
        :class:`~dataretrieval.exceptions.Unchunkable` when the request is
        over budget and has nothing to split.
        """
        if _safe_request_bytes(build_request, args, url_limit) <= url_limit:
            return
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

    @staticmethod
    def _probe_initial_request(
        args: dict[str, Any],
        build_request: Callable[..., httpx.Request],
        url_limit: int,
    ) -> tuple[httpx.Request | None, bool]:
        """Try to construct the un-chunked request and measure it.

        Returns ``(request, fits)`` where ``request`` is ``None`` when
        construction raised ``httpx.InvalidURL`` (URL > 64 KB).
        """
        initial_request = _try_build(build_request, args)
        if initial_request is None:
            return None, False
        return initial_request, _request_bytes(initial_request) <= url_limit

    def _plan(
        self,
        build_request: Callable[..., httpx.Request],
        url_limit: int,
    ) -> None:
        """
        Greedy-halve the biggest chunk across axes until every URL fits.

        Halving continues until the worst-case chunk URL fits
        ``url_limit``, mutating ``self.chunks`` in place. List axes and the
        filter axis are treated uniformly — each is just a list of atoms
        joined by its axis's separator.

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

            biggest_axis, biggest_idx = self._largest_splittable_chunk_by_bytes()
            if biggest_axis is None:
                raise Unchunkable(
                    f"Request exceeds {url_limit} bytes (URL + body) at the "
                    f"smallest reducible plan (every axis at one atom per "
                    f"chunk). Reduce input sizes, shorten or simplify "
                    f"the filter, or split the call manually."
                )
            _split_at(self.chunks[biggest_axis.arg_key], biggest_idx)

    def _largest_splittable_chunk_by_bytes(self) -> tuple[_Axis | None, int]:
        """Find the largest splittable chunk ranked by URL-encoded byte size.

        Returns ``(axis, index)`` of the biggest chunk with more than one atom,
        or ``(None, -1)`` when every axis is at one atom per chunk (saturated).
        """
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
        return biggest_axis, biggest_idx

    def _refine(self, max_chunks: int) -> None:
        """
        Fan the plan out more finely than the byte budget alone requires.

        This is the ``parallel_chunks`` dial: see
        :func:`~dataretrieval.ogc.chunking.parallel_chunks` for why a caller
        would want this, and :class:`ChunkPlan`'s ``max_chunks`` parameter for
        the cap's contract (total-not-per-axis, a hard ceiling that may land
        below the cap).

        Implementation. Each split multiplies the plan by ``(k+1)/k`` for the
        chosen axis (adding ``total // k`` chunks, not one), so a split
        is taken only when it keeps :attr:`total` within the cap. When no
        in-budget split remains, the plan stops *below* the cap rather than
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
            candidate = self._best_refine_candidate(total, max_chunks)
            if candidate is None:
                return
            axis, idx = candidate
            _split_at(self.chunks[axis.arg_key], idx)

    @staticmethod
    def _largest_chunk_in(axis_chunks: list[list[str]]) -> tuple[int, int]:
        """Return ``(index, atom_count)`` of the largest splittable chunk.

        A chunk is splittable when it has more than one atom.  Returns
        ``(-1, -1)`` when no chunk qualifies.
        """
        best_idx = -1
        best_size = -1
        for idx, chunk in enumerate(axis_chunks):
            if len(chunk) > 1 and len(chunk) > best_size:
                best_idx, best_size = idx, len(chunk)
        return best_idx, best_size

    def _best_refine_candidate(
        self, total: int, max_chunks: int
    ) -> tuple[_Axis, int] | None:
        """Find the best chunk to split during the refine pass.

        Returns the largest splittable chunk (by atom count) among axes whose
        split stays within the ``max_chunks`` cap, or ``None`` when no
        in-budget split remains. Splitting any chunk of an axis with ``k``
        chunks adds ``total // k`` chunks (the product of the other axes),
        so the budget test is per axis rather than per chunk. The ranking key
        is atom count (not URL bytes like ``_plan``) because this pass
        balances work across chunks rather than fitting a byte budget.
        Stable input order breaks ties by axis order, then lowest index.
        """
        candidate: tuple[_Axis, int] | None = None
        candidate_size = -1
        for axis in self.axes:
            axis_chunks = self.chunks[axis.arg_key]
            if total + total // len(axis_chunks) > max_chunks:
                continue  # any split of this axis would overshoot the cap
            axis_best, axis_best_size = self._largest_chunk_in(axis_chunks)
            if axis_best_size > candidate_size:
                candidate, candidate_size = (axis, axis_best), axis_best_size
        return candidate

    def _worst_case_args(self) -> dict[str, Any]:
        """
        Args for the largest chunk the current partition will issue.

        Each axis contributes its longest chunk (by URL-encoded bytes),
        rendered back into the args dict.
        """
        out = dict(self.args)
        for axis in self.axes:
            worst = max(self.chunks[axis.arg_key], key=axis.chunk_bytes)
            out[axis.arg_key] = axis.render(worst)
        return out

    @property
    def total(self) -> int:
        """
        Total chunk count: product of per-axis chunk counts.

        Returns
        -------
        int
            ``1`` for the passthrough plan, otherwise the cartesian
            product of ``len(chunks[ax.arg_key])`` across all axes.
        """
        return math.prod((len(self.chunks[ax.arg_key]) for ax in self.axes), start=1)

    def iter_chunk_args(self) -> Iterator[dict[str, Any]]:
        """
        Yield substituted args for each chunk, in deterministic order.

        The order is the cartesian product over axes in extraction order. The
        same plan yields the same sub-args sequence on every invocation, so
        resume is well-defined.

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
            chunk_args = dict(self.args)
            for axis, chunk in zip(self.axes, combo, strict=False):
                chunk_args[axis.arg_key] = axis.render(chunk)
            yield chunk_args

    # ``total`` and ``iter_chunk_args`` are this class's domain vocabulary and
    # stay as they are. The dunders are how a plan satisfies
    # :class:`~dataretrieval.transport.fanout.FanOutPlan`, which asks for a
    # sized iterable and nothing chunking-specific. They delegate rather than
    # duplicate, so ``len(plan)`` cannot disagree with what iterating yields.
    def __len__(self) -> int:
        return self.total

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self.iter_chunk_args()
