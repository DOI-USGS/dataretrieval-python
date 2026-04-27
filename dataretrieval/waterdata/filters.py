"""CQL ``filter`` support for the Water Data OGC getters.

Everything related to the ``filter`` / ``filter_lang`` kwargs lives in
this module: the ``FILTER_LANG`` type alias, the top-level ``OR``
splitter / chunker, the per-request URL-budget probe, the
lexicographic-pitfall guard (see the module comment on
``_NUMERIC_COMPARE_RE`` for why), and the ``chunked`` decorator that
``utils.py`` applies to its single-request fetch function.

Scope — what this module parses vs. what passes through
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The CQL-text filter the caller supplies is **forwarded verbatim** to
the server as a ``filter=`` query parameter. This module doesn't
parse CQL semantics; it inspects the string for exactly two
purposes:

1. **Chunking** (``_chunk_cql_or``) splits the filter on top-level
   ``OR`` boundaries so the full URL can stay under the server's
   ~8 KB limit. Only ``OR`` splits cleanly into independent
   sub-requests whose result sets can be union'd back together by
   ``pd.concat`` + dedup. Everything else — ``AND`` chains,
   ``NOT``, ``LIKE``, ``IS NULL``, spatial / temporal predicates,
   function calls — is sent as a single request. If such a filter
   is long enough to trip the URL limit the caller gets the
   server's ``414``; we don't try to rewrite it, because no rewrite
   preserves set semantics for those shapes.

2. **Pitfall detection** (``_check_numeric_filter_pitfall``) scans
   for three patterns where the user almost certainly means a
   numeric comparison but the server would do a lexicographic one
   (every queryable is string-typed):

   - ``<field> <op> <unquoted_num>`` (and the reverse)
   - ``<field> [NOT] IN (<unquoted_num>, ...)``
   - ``<field> [NOT] BETWEEN <unquoted_num> AND <unquoted_num>``

   ``LIKE``, ``IS NULL``, function-call RHS (``COUNT(x) > 5``),
   ``CAST`` expressions, and arithmetic (``value > 1 + 1`` —
   partially caught on ``value > 1``) are not flagged. The first
   three the server doesn't support anyway; the last is a minor
   offense-text imprecision rather than a correctness issue.

Isolation contract (rolling the feature back)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The feature's footprint outside this module is deliberately small
and mechanical:

- Files to delete: ``filters.py``, ``nearest.py`` (which depends on
  filters), ``tests/waterdata_filters_test.py``,
  ``tests/waterdata_nearest_test.py``.
- ``__init__.py``: drop two imports (``FILTER_LANG``,
  ``get_nearest_continuous``) and two ``__all__`` entries.
- ``utils.py``: drop the ``from . import filters`` import and the
  ``@filters.chunked(...)`` decorator on ``_fetch_once``. The two
  function bodies (``_fetch_once``, ``get_ogc_data``) are
  filter-unaware and need no changes.
- ``api.py``: drop the ``from .filters import FILTER_LANG`` import,
  the eight ``filter, filter_lang`` kwarg pairs on the OGC getters,
  and their one-line docstring pointers (now
  ``filter, filter_lang : optional — see dataretrieval.waterdata.filters``).
  Also the one compact filter example inside ``get_continuous``'s
  docstring.

The two-line ``filter_lang`` → ``filter-lang`` URL-key translation
inside ``_construct_api_requests`` becomes unreachable dead code (no
caller sets it); removing it is optional.

Only two names are imported by other modules — ``FILTER_LANG`` and
``chunked``. Everything else is package-private.
"""

from __future__ import annotations

import functools
import re
from collections.abc import Callable, Iterator
from typing import Any, Literal, TypeVar
from urllib.parse import quote_plus

import pandas as pd
import requests

FILTER_LANG = Literal["cql-text", "cql-json"]


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

# Conservative fallback budget (characters) for a single CQL ``filter``
# query parameter, used when ``_chunk_cql_or`` is called without a
# ``max_len``. The ``chunked`` decorator computes a tighter
# per-request budget from ``_WATERDATA_URL_BYTE_LIMIT`` below.
_CQL_FILTER_CHUNK_LEN = 5000

# Total URL byte limit the Water Data API will accept before replying
# HTTP 414 (Request-URI Too Large). Empirically the cliff sits at
# ~8,200 bytes of full URL, which lines up with nginx's default
# ``large_client_header_buffers`` of 8 KB (8192). 8000 leaves ~200 bytes
# of headroom for request-line framing ("GET ... HTTP/1.1\r\n") and any
# intermediate proxy variance.
_WATERDATA_URL_BYTE_LIMIT = 8000

# Conservative over-estimate of the URL bytes consumed by everything
# *except* the filter value — the base URL, other query params, and the
# ``&filter=`` / ``&filter-lang=...`` keys. Used only to decide whether a
# filter is small enough that the expensive budget probe can be skipped.
_NON_FILTER_URL_HEADROOM = 1000


# ---------------------------------------------------------------------
# Pitfall regexes
# ---------------------------------------------------------------------

# Every queryable property on every OGC collection for the Water Data
# API is ``type: string`` (confirmed across ``continuous``, ``daily``,
# ``field-measurements``, ``monitoring-locations``,
# ``time-series-metadata``, ``latest-continuous``, ``latest-daily``,
# ``channel-measurements`` — see ``/collections/<svc>/queryables``).
# That includes fields whose *values* look numeric — ``value``,
# ``parameter_code`` (``'00060'``), ``statistic_id`` (``'00011'``),
# ``district_code`` (``'01'``), ``hydrologic_unit_code``,
# ``channel_flow``, and more. Comparing any of them to an *unquoted*
# numeric literal (``value >= 1000``) triggers a lexicographic sort on
# the server and silently produces wrong results — zero-padded codes
# are especially nasty (``parameter_code = 60`` matches nothing because
# the real values are all ``'00060'``-shaped). So the rule we enforce
# client-side is the general one: any ``<identifier> <op> <unquoted
# numeric>`` is a bug — quote the literal or drop the comparison and
# filter in pandas.

# Unquoted numeric literal: integer, decimal (with or without leading
# zero), or scientific notation. ``\d+(?:\.\d+)?`` covers ``1``,
# ``1.5``; ``\.\d+`` covers the leading-dot form ``.5`` that users
# sometimes write as a fraction. Trailing-dot ``5.`` is deliberately
# not accepted (not a common numeric spelling and not required CQL).
_NUM = r"-?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?"
_IDENT = r"[A-Za-z_]\w*"
_OP = r">=|<=|<>|!=|==|=|>|<"

# ``<field>`` in ``IN`` / ``BETWEEN`` contexts, with optional ``NOT``
# keyword after the field. ``(?!NOT\b)`` keeps the bare keyword
# ``NOT`` from being captured as the field when the caller writes
# ``value NOT IN (…)``; the ``(?P<negated>NOT\s+)?`` after the field
# captures the negation so the error message can report the offending
# form accurately.
_FIELD_NEGATED = rf"\b(?!NOT\b)(?P<field>{_IDENT})\s+(?P<negated>NOT\s+)?"

_NUMERIC_COMPARE_RE = re.compile(
    rf"""
    (?:
        \b(?P<field1>{_IDENT})\s*
        (?P<op1>{_OP})\s*
        (?P<num1>{_NUM})\b
    |
        \b(?P<num2>{_NUM})\s*
        (?P<op2>{_OP})\s*
        (?P<field2>{_IDENT})\b
    )
    """,
    re.VERBOSE,
)

# ``<field> [NOT] IN (<numeric>, ...)`` — same footgun as a simple
# comparison but using the list form. Caught separately because
# ``IN`` isn't one of the comparison operators in ``_OP``.
_IN_NUMERIC_RE = re.compile(
    rf"{_FIELD_NEGATED}IN\s*\(\s*{_NUM}",
    re.IGNORECASE,
)

# ``<field> [NOT] BETWEEN <numeric> AND <numeric>`` — range form.
_BETWEEN_NUMERIC_RE = re.compile(
    rf"{_FIELD_NEGATED}BETWEEN\s+{_NUM}\s+AND\s+{_NUM}\b",
    re.IGNORECASE,
)

# Single-quoted string literal — used to mask out user-supplied strings
# before scanning for the pitfall patterns above (so ``name = 'x > 5'``
# doesn't false-positive as a numeric comparison).
_QUOTED_STR_RE = re.compile(r"'[^']*'")


# ---------------------------------------------------------------------
# Top-level OR splitter / chunker
# ---------------------------------------------------------------------


def _iter_or_boundaries(expr: str) -> Iterator[tuple[int, int]]:
    """Yield ``(start, end)`` spans of each top-level ``OR`` separator.

    Tracks single/double-quoted string literals and parenthesized
    sub-expressions so that ``OR`` tokens inside them are skipped.
    Matching is case-insensitive and the yielded span covers the
    surrounding whitespace on both sides.
    """
    depth = 0
    in_quote = None
    i = 0
    n = len(expr)
    while i < n:
        ch = expr[i]
        if in_quote is not None:
            if ch == in_quote:
                in_quote = None
            i += 1
            continue
        if ch in ("'", '"'):
            in_quote = ch
            i += 1
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth -= 1
            i += 1
            continue
        if depth == 0 and ch.isspace():
            j = i + 1
            while j < n and expr[j].isspace():
                j += 1
            if j + 2 <= n and expr[j : j + 2].lower() == "or":
                k = j + 2
                if k < n and expr[k].isspace():
                    m = k + 1
                    while m < n and expr[m].isspace():
                        m += 1
                    yield i, m
                    i = m
                    continue
        i += 1


def _split_top_level_or(expr: str) -> list[str]:
    """Split a CQL expression at each top-level ``OR`` separator.

    Respects parentheses and single/double-quoted string literals so that
    ``OR`` tokens inside ``(A OR B)`` or ``'word OR word'`` are left alone.
    Matching is case-insensitive. Whitespace around each emitted part is
    stripped; empty parts are dropped.
    """
    parts = []
    last = 0
    for start, end in _iter_or_boundaries(expr):
        parts.append(expr[last:start].strip())
        last = end
    parts.append(expr[last:].strip())
    return [p for p in parts if p]


def _chunk_cql_or(expr: str, max_len: int = _CQL_FILTER_CHUNK_LEN) -> list[str]:
    """Split a CQL expression into OR-chunks that each fit under ``max_len``.

    The splitter only understands top-level ``OR`` chains, since that is
    the only shape that can be recombined losslessly as a disjunction of
    independent sub-queries. Returns ``[expr]`` unchanged when the whole
    expression already fits, when it contains no top-level ``OR``, or when
    any single clause is larger than ``max_len`` on its own (we would
    rather send a too-long request and surface the server's 414 than
    silently drop data).
    """
    if len(expr) <= max_len:
        return [expr]
    parts = _split_top_level_or(expr)
    if len(parts) < 2 or any(len(p) > max_len for p in parts):
        return [expr]

    chunks = []
    current = []
    current_len = 0
    for part in parts:
        join_cost = len(" OR ") if current else 0
        if current and current_len + join_cost + len(part) > max_len:
            chunks.append(" OR ".join(current))
            current = [part]
            current_len = len(part)
        else:
            current.append(part)
            current_len += join_cost + len(part)
    if current:
        chunks.append(" OR ".join(current))
    return chunks


# ---------------------------------------------------------------------
# Per-request URL-byte budget
# ---------------------------------------------------------------------


def _effective_filter_budget(
    args: dict[str, Any],
    filter_expr: str,
    build_request: Callable[..., Any],
) -> int:
    """Compute the raw CQL byte budget for ``filter_expr`` in this request.

    The server limits total URL length (see ``_WATERDATA_URL_BYTE_LIMIT``),
    not raw CQL length. To derive a raw-byte budget we can hand to
    ``_chunk_cql_or``:

    1. Probe the URL space consumed by the other query params by building
       the request with a 1-byte placeholder filter.
    2. Subtract from the URL limit to get the bytes available for the
       encoded filter value.
    3. Convert back to raw CQL bytes using the *maximum* per-clause
       encoding ratio, not the whole-filter average. A chunk can end up
       containing only the heavier-encoding clauses (e.g. heavy ones
       clustered at one end of the filter), so budgeting against the
       average lets such a chunk overflow the URL limit by a few bytes.
    """
    # Fast path: if the whole encoded filter already fits with room for
    # any plausible non-filter URL overhead, skip the probe and the
    # splitter entirely. Signals pass-through via a budget larger than
    # the filter. Saves a PreparedRequest build + a full splitter scan
    # on every short-filter call.
    encoded_len = len(quote_plus(filter_expr))
    if encoded_len + _NON_FILTER_URL_HEADROOM <= _WATERDATA_URL_BYTE_LIMIT:
        return len(filter_expr) + 1

    probe = build_request(**{**args, "filter": "x"})
    non_filter_url_bytes = len(probe.url) - 1
    available_url_bytes = _WATERDATA_URL_BYTE_LIMIT - non_filter_url_bytes
    if available_url_bytes <= 0:
        # The non-filter URL already exceeds the byte limit, so no chunk
        # we could produce would fit. Return a budget larger than the
        # filter so _chunk_cql_or passes it through unchanged — one 414
        # from the server is clearer than a burst of N failing sub-requests.
        return len(filter_expr) + 1
    parts = _split_top_level_or(filter_expr) or [filter_expr]
    encoding_ratio = max(len(quote_plus(p)) / len(p) for p in parts)
    return max(100, int(available_url_bytes / encoding_ratio))


# ---------------------------------------------------------------------
# Lexicographic-pitfall guard
# ---------------------------------------------------------------------


def _check_numeric_filter_pitfall(filter_expr: str) -> None:
    """Raise if the filter pairs any field with an unquoted numeric literal.

    Every queryable property on this API is typed as a string on the
    server, so any numeric-looking comparison — ``value >= 1000``,
    ``parameter_code = 60``, ``parameter_code IN (60, 61)``,
    ``value BETWEEN 5 AND 10`` — either gets rejected with HTTP 500
    or silently produces lexicographic results. Zero-padded codes are
    especially nasty (``parameter_code = '60'`` matches nothing because
    the real codes are ``'00060'``-shaped).

    Explicit string comparisons with quoted literals
    (``value >= '1000'``) are not flagged — the caller has signalled
    they know the column is textual.
    """
    # Mask quoted string literals so ``name = 'value > 5'`` doesn't
    # false-positive; the ``"'" in`` pre-check skips the substitution
    # on quote-free filters.
    masked = (
        _QUOTED_STR_RE.sub("''", filter_expr) if "'" in filter_expr else filter_expr
    )

    compare = _NUMERIC_COMPARE_RE.search(masked)
    if compare:
        field = compare.group("field1") or compare.group("field2")
        op = compare.group("op1") or compare.group("op2")
        num = compare.group("num1") or compare.group("num2")
        _raise_pitfall(field, f"{field} {op} {num}")

    membership = _IN_NUMERIC_RE.search(masked)
    if membership:
        field = membership.group("field")
        op = "NOT IN" if membership.group("negated") else "IN"
        _raise_pitfall(field, f"{field} {op} (…)")

    between = _BETWEEN_NUMERIC_RE.search(masked)
    if between:
        field = between.group("field")
        op = "NOT BETWEEN" if between.group("negated") else "BETWEEN"
        _raise_pitfall(field, f"{field} {op} …")


def _raise_pitfall(field: str, offense: str) -> None:
    raise ValueError(
        f"Filter uses an unquoted numeric comparison against {field!r} "
        f"(``{offense}``). Every queryable on the Water Data API is "
        f"typed as a string, so the server rejects unquoted numeric "
        f"literals with HTTP 500; even quoting the literal gives a "
        f"lexicographic comparison (``value > '10'`` matches "
        f"``value='34.52'``, ``parameter_code = '60'`` matches nothing "
        f"because the real codes are ``'00060'``-shaped). For a true "
        f"numeric filter, fetch a wider result and reduce in pandas."
    )


# ---------------------------------------------------------------------
# Chunked fan-out (decorator)
# ---------------------------------------------------------------------


def _is_chunkable(filter_expr: Any, filter_lang: Any) -> bool:
    """Only non-empty cql-text filters can be safely split at top-level OR."""
    return (
        isinstance(filter_expr, str)
        and bool(filter_expr)
        and filter_lang in {None, "cql-text"}
    )


def _combine_chunk_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate per-chunk frames, handling the edge cases.

    Drops empty frames before concat — ``_get_resp_data`` returns a
    plain ``pd.DataFrame()`` on empty responses, which would downgrade
    a concat of real GeoDataFrames back to a plain DataFrame and strip
    geometry/CRS. Also dedups on the pre-rename feature ``id`` so
    overlapping user-supplied OR-clauses don't produce duplicate rows
    across chunks.
    """
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        return pd.DataFrame()
    if len(non_empty) == 1:
        return non_empty[0]
    combined = pd.concat(non_empty, ignore_index=True)
    if "id" in combined.columns:
        combined = combined.drop_duplicates(subset="id", ignore_index=True)
    return combined


def _aggregate_chunk_responses(
    responses: list[requests.Response],
) -> requests.Response:
    """Return a response whose URL/headers come from the first chunk and
    whose ``elapsed`` is the sum across all chunks.

    Mutates the first response in place (adjusting only ``elapsed``) so
    the caller can still wrap it in ``BaseMetadata`` as it would any
    single-request response — the decorator's output shape matches the
    undecorated function's output shape.
    """
    metadata_response = responses[0]
    if len(responses) > 1:
        metadata_response.elapsed = sum(
            (r.elapsed for r in responses[1:]),
            start=metadata_response.elapsed,
        )
    return metadata_response


_FetchOnce = TypeVar(
    "_FetchOnce",
    bound=Callable[[dict[str, Any]], tuple[pd.DataFrame, requests.Response]],
)


def chunked(*, build_request: Callable[..., Any]) -> Callable[[_FetchOnce], _FetchOnce]:
    """Decorator that adds CQL-filter chunking to a single-request fetch.

    The wrapped function must have signature
    ``(args: dict) -> (pd.DataFrame, requests.Response)`` and represent
    one HTTP round-trip (build a request, walk its pages). The
    decorator inspects ``args``:

    - If no chunkable filter is present, it calls the wrapped function
      once and returns the result unchanged.
    - If a chunkable cql-text filter is present, it validates the
      filter against the lexicographic-comparison pitfall, splits it
      into URL-length-safe sub-expressions, calls the wrapped function
      once per chunk with ``{**args, "filter": chunk}``, concatenates
      the resulting frames (dropping empties, dedup'ing by feature
      ``id``), and returns an aggregated response (first chunk's
      URL/headers + summed ``elapsed``).

    Either way the return type matches the wrapped function's — the
    caller wraps the response in ``BaseMetadata`` the same way in
    both paths. That's what lets the feature be removed by dropping
    just the decorator line.

    ``build_request`` is injected so the decorator can probe URL
    length for budget computation without importing any specific HTTP
    builder. It receives the same kwargs the wrapped function's
    ``args`` would, and returns a prepared-request-like object with a
    ``.url`` attribute.
    """

    def decorator(fetch_once: _FetchOnce) -> _FetchOnce:
        @functools.wraps(fetch_once)
        def wrapper(
            args: dict[str, Any],
        ) -> tuple[pd.DataFrame, requests.Response]:
            filter_expr = args.get("filter")
            if not _is_chunkable(filter_expr, args.get("filter_lang")):
                return fetch_once(args)

            _check_numeric_filter_pitfall(filter_expr)
            budget = _effective_filter_budget(args, filter_expr, build_request)
            chunks = _chunk_cql_or(filter_expr, max_len=budget)

            frames: list[pd.DataFrame] = []
            responses: list[requests.Response] = []
            for chunk in chunks:
                frame, response = fetch_once({**args, "filter": chunk})
                frames.append(frame)
                responses.append(response)

            return _combine_chunk_frames(frames), _aggregate_chunk_responses(responses)

        return wrapper  # type: ignore[return-value]

    return decorator
