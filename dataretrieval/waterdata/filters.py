"""CQL ``filter`` support for the Water Data OGC getters.

Two names are public to the rest of the package:

- ``FILTER_LANG``: the type alias used for the ``filter_lang`` kwarg.
- ``chunked``: the decorator ``utils.py`` applies to its single-request
  fetch function. It runs the lexicographic-comparison pitfall guard,
  splits long cql-text filters at top-level ``OR`` so each sub-request
  fits under the server's URL byte limit, and concatenates the results.

Other CQL shapes (``AND``, ``NOT``, ``LIKE``, spatial/temporal predicates,
function calls) are forwarded verbatim — only top-level ``OR`` chunks
losslessly into independent sub-queries whose result sets can be union'd.
"""

from __future__ import annotations

import functools
import re
from collections.abc import Callable
from typing import Any, Literal, TypeVar
from urllib.parse import quote_plus

import pandas as pd
import requests

FILTER_LANG = Literal["cql-text", "cql-json"]

# Conservative fallback budget when ``_chunk_cql_or`` is called without
# an explicit ``max_len``. The ``chunked`` decorator computes a tighter
# per-request budget from ``_WATERDATA_URL_BYTE_LIMIT``.
_CQL_FILTER_CHUNK_LEN = 5000

# Empirically the API replies HTTP 414 above ~8200 bytes of full URL —
# matches nginx's default ``large_client_header_buffers`` of 8 KB. 8000
# leaves ~200 bytes for request-line framing and proxy variance.
_WATERDATA_URL_BYTE_LIMIT = 8000

# Conservative over-estimate of URL bytes used by everything *except*
# the filter value. Used only by the fast path in
# ``_effective_filter_budget`` to skip the probe when the encoded filter
# clearly already fits.
_NON_FILTER_URL_HEADROOM = 1000


_NUM = r"-?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?"
_IDENT = r"[A-Za-z_]\w*"
_OP = r">=|<=|<>|!=|==|=|>|<"
_FIELD_NEGATED = rf"\b(?!NOT\b)(?P<field>{_IDENT})\s+(?P<negated>NOT\s+)?"

_NUMERIC_COMPARE_RE = re.compile(
    rf"""
    (?:
        \b(?P<field1>{_IDENT})\s*(?P<op1>{_OP})\s*(?P<num1>{_NUM})\b
    |
        \b(?P<num2>{_NUM})\s*(?P<op2>{_OP})\s*(?P<field2>{_IDENT})\b
    )
    """,
    re.VERBOSE,
)
_IN_NUMERIC_RE = re.compile(
    rf"{_FIELD_NEGATED}IN\s*\([^)]*\b{_NUM}\b[^)]*\)",
    re.IGNORECASE,
)
_BETWEEN_NUMERIC_RE = re.compile(
    rf"{_FIELD_NEGATED}BETWEEN\s+(?:{_NUM}\b[^)]*?\bAND\b|[^)]*?\bAND\s+{_NUM}\b)",
    re.IGNORECASE,
)
_QUOTED_STR_RE = re.compile(r"'[^']*'")


def _split_top_level_or(expr: str) -> list[str]:
    """Split ``expr`` at each top-level ``OR``, respecting quotes and parens.

    ``OR`` tokens inside ``(A OR B)`` or ``'word OR word'`` are left alone.
    Matching is case-insensitive; whitespace around each part is stripped;
    empty parts are dropped.
    """
    parts: list[str] = []
    last = 0
    depth = 0
    in_quote: str | None = None
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
                    parts.append(expr[last:i].strip())
                    last = m
                    i = m
                    continue
        i += 1
    parts.append(expr[last:].strip())
    return [p for p in parts if p]


def _chunk_cql_or(expr: str, max_len: int = _CQL_FILTER_CHUNK_LEN) -> list[str]:
    """Split ``expr`` into OR-chunks each under ``max_len`` characters.

    Only top-level ``OR`` chains can be recombined losslessly as a disjunction
    of independent sub-queries. Returns ``[expr]`` unchanged when the whole
    expression already fits, when there is no top-level ``OR``, or when any
    single clause exceeds ``max_len`` (sending it as-is and surfacing the
    server's 414 is clearer than silently dropping data).
    """
    if len(expr) <= max_len:
        return [expr]
    parts = _split_top_level_or(expr)
    if len(parts) < 2 or any(len(p) > max_len for p in parts):
        return [expr]

    chunks = []
    current: list[str] = []
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


def _effective_filter_budget(
    args: dict[str, Any],
    filter_expr: str,
    build_request: Callable[..., Any],
) -> int:
    """Raw-CQL byte budget that, after URL-encoding, fits the URL byte limit.

    The server caps total URL length, not raw CQL length. We probe the
    non-filter URL bytes by building the request with a 1-byte placeholder
    filter, subtract from the URL limit to get the bytes available for the
    encoded filter, then convert back to raw CQL bytes via the *maximum*
    per-clause encoding ratio (a chunk could contain only the heavier-encoding
    clauses, so budgeting by the average ratio could overflow).
    """
    # Fast path: encoded filter clearly fits with room for any plausible
    # non-filter URL. Skips the PreparedRequest build and splitter scan.
    encoded_len = len(quote_plus(filter_expr))
    if encoded_len + _NON_FILTER_URL_HEADROOM <= _WATERDATA_URL_BYTE_LIMIT:
        return len(filter_expr) + 1

    probe = build_request(**{**args, "filter": "x"})
    available_url_bytes = _WATERDATA_URL_BYTE_LIMIT - (len(probe.url) - 1)
    if available_url_bytes <= 0:
        # Non-filter URL already over the limit. Pass through unchanged so
        # the caller sees one 414 instead of N parallel sub-request failures.
        return len(filter_expr) + 1
    parts = _split_top_level_or(filter_expr) or [filter_expr]
    encoding_ratio = max(len(quote_plus(p)) / len(p) for p in parts)
    return max(100, int(available_url_bytes / encoding_ratio))


def _check_numeric_filter_pitfall(filter_expr: str) -> None:
    """Raise if the filter pairs a field with an unquoted numeric literal.

    Every queryable on the Water Data OGC API is typed as a string, including
    fields whose *values* look numeric (``value``, ``parameter_code`` like
    ``'00060'``, ``statistic_id`` like ``'00011'``, ``district_code``,
    ``hydrologic_unit_code``, ``channel_flow``). Any unquoted numeric
    comparison — ``value >= 1000``, ``parameter_code = 60``,
    ``parameter_code IN (60, 61)``, ``value BETWEEN 5 AND 10`` — either gets
    rejected with HTTP 500 or silently produces lexicographic results;
    zero-padded codes are the worst case (``parameter_code = '60'`` matches
    nothing because the real codes are ``'00060'``-shaped).

    Quoted literals (``value >= '1000'``) are not flagged — the caller has
    signalled they know the column is textual.
    """
    # Mask quoted strings so ``name = 'value > 5'`` doesn't false-positive.
    masked = (
        _QUOTED_STR_RE.sub("''", filter_expr) if "'" in filter_expr else filter_expr
    )

    def fail(field: str, offense: str) -> None:
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

    compare = _NUMERIC_COMPARE_RE.search(masked)
    if compare:
        field = compare.group("field1") or compare.group("field2")
        op = compare.group("op1") or compare.group("op2")
        num = compare.group("num1") or compare.group("num2")
        fail(field, f"{field} {op} {num}")

    membership = _IN_NUMERIC_RE.search(masked)
    if membership:
        field = membership.group("field")
        op = "NOT IN" if membership.group("negated") else "IN"
        fail(field, f"{field} {op} (…)")

    between = _BETWEEN_NUMERIC_RE.search(masked)
    if between:
        field = between.group("field")
        op = "NOT BETWEEN" if between.group("negated") else "BETWEEN"
        fail(field, f"{field} {op} …")


def _is_chunkable(filter_expr: Any, filter_lang: Any) -> bool:
    """Only non-empty cql-text filters can be safely split at top-level OR."""
    return (
        isinstance(filter_expr, str)
        and bool(filter_expr)
        and filter_lang in {None, "cql-text"}
    )


def _combine_chunk_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate per-chunk frames, dropping empties and deduping by ``id``.

    ``_get_resp_data`` returns a plain ``pd.DataFrame()`` on empty responses;
    concat'ing it with real GeoDataFrames downgrades the result to plain
    DataFrame and strips geometry/CRS, so empties are dropped first. Dedup
    on the pre-rename feature ``id`` keeps overlapping user OR-clauses from
    producing duplicate rows across chunks.
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


def _combine_chunk_responses(
    responses: list[requests.Response],
) -> requests.Response:
    """Return one response: first chunk's URL (for query identity) +
    last chunk's headers (for current rate-limit state) + summed
    ``elapsed`` (for total wall-clock).

    Splitting fields this way keeps ``BaseMetadata.url`` reflecting the
    user's original query (useful for reproduction and debugging) while
    still surfacing current ``x-ratelimit-remaining`` to the outer
    ``multi_value_chunked`` decorator's ``QuotaExhausted`` guard.

    Mutates the first response in place: ``.headers`` is replaced with
    the last response's headers and ``.elapsed`` is accumulated across
    all chunks. Downstream reads ``.url``, ``.headers``, and
    ``.elapsed`` (via ``BaseMetadata``).
    """
    head = responses[0]
    if len(responses) > 1:
        head.headers = responses[-1].headers
        head.elapsed = sum((r.elapsed for r in responses[1:]), start=head.elapsed)
    return head


_FetchOnce = TypeVar(
    "_FetchOnce",
    bound=Callable[[dict[str, Any]], tuple[pd.DataFrame, requests.Response]],
)


def chunked(*, build_request: Callable[..., Any]) -> Callable[[_FetchOnce], _FetchOnce]:
    """Decorator that adds CQL-filter chunking to a single-request fetch.

    The wrapped function has signature ``(args: dict) -> (frame, response)``
    and represents one HTTP round-trip. The decorator inspects ``args``:

    - No chunkable filter: pass through unchanged.
    - Chunkable cql-text filter: run the lexicographic-pitfall guard, split
      into URL-length-safe sub-expressions, call the wrapped function once
      per chunk, concatenate frames (drop empties, dedup by feature ``id``),
      and return an aggregated response — first chunk's URL (so
      ``BaseMetadata.url`` still reflects the user's original query), last
      chunk's headers (so callers see current ``x-ratelimit-remaining``,
      which the outer ``multi_value_chunked`` decorator's ``QuotaExhausted``
      guard depends on), and summed ``elapsed``.

    Either way the return shape matches the undecorated function's, so the
    caller wraps the response in ``BaseMetadata`` the same way in both paths.

    ``build_request`` is injected so the decorator can probe URL length
    without importing any specific HTTP builder; it receives the same kwargs
    the wrapped function's ``args`` would and returns a prepared-request-like
    object with a ``.url`` attribute.
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

            return _combine_chunk_frames(frames), _combine_chunk_responses(responses)

        return wrapper  # type: ignore[return-value]

    return decorator
