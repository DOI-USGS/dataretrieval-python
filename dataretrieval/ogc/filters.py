"""CQL ``filter`` support for the OGC getters.

Public:

- ``FILTER_LANG``: the type alias used for the ``filter_lang`` kwarg.

Internal helpers used by ``chunking.multi_value_chunked``'s joint
planner: ``_split_top_level_or`` (clause partitioning),
``_is_chunkable`` (filter-language gate), and
``_check_numeric_filter_pitfall`` (the lexicographic-comparison guard).
``_quote_cql_str`` escapes a single CQL-text string literal, shared by any
getter that *builds* a CQL filter (e.g. ``waterdata.ratings``).

Other CQL shapes (``AND``, ``NOT``, ``LIKE``, spatial/temporal
predicates, function calls) are forwarded verbatim — only top-level
``OR`` chunks losslessly into independent sub-queries whose result sets
can be union'd.
"""

from __future__ import annotations

import re
from typing import Any, Literal

FILTER_LANG = Literal["cql-text", "cql-json"]


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


def _quote_cql_str(value: str) -> str:
    """Escape a single-quoted CQL2-text literal by doubling embedded quotes.

    CQL2 text escapes a ``'`` inside a string literal by doubling it, so
    ``O'Brien`` becomes ``O''Brien`` (wrap the result in ``'…'`` at the call
    site). Defends against malformed filters / injection on arbitrary user
    input. Shared by every getter that builds a CQL-text literal (e.g. the
    STAC ``/search`` filter in ``waterdata.ratings``).
    """
    return value.replace("'", "''")


def _skip_space(expr: str, i: int) -> int:
    """Index of the first non-space character at or after ``i``."""
    while i < len(expr) and expr[i].isspace():
        i += 1
    return i


def _resume_after_or(expr: str, i: int) -> int | None:
    """Where the clause after a top-level ``OR`` begins, if one starts at ``i``.

    ``i`` is the index of a space that may open a ``<space>OR<space>``
    separator. Returns the index of the next clause's first character, or
    ``None`` when this space does not begin one -- so the caller's test is
    "is this a separator?" rather than four nested boundary checks.

    The trailing space is required: without it ``A ORDER BY b`` would split on
    the ``OR`` inside ``ORDER``.
    """
    word_start = _skip_space(expr, i)
    if expr[word_start : word_start + 2].lower() != "or":
        return None
    after_word = word_start + 2
    if after_word >= len(expr) or not expr[after_word].isspace():
        return None
    return _skip_space(expr, after_word)


def _advance_char(ch: str, depth: int, in_quote: str | None) -> tuple[int, str | None]:
    """Update parser state for a single character (depth and quote tracking).

    Returns the updated ``(depth, in_quote)`` pair without the OR-split logic,
    keeping the state machine's character handling flat.
    """
    if in_quote is not None:
        if ch == in_quote:
            return depth, None
        return depth, in_quote
    if ch in ("'", '"'):
        return depth, ch
    if ch == "(":
        return depth + 1, None
    if ch == ")":
        return depth - 1, None
    return depth, None


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
        depth, in_quote = _advance_char(ch, depth, in_quote)
        if in_quote is None and depth == 0 and ch.isspace():
            resume = _resume_after_or(expr, i + 1)
            if resume is not None:
                parts.append(expr[last:i].strip())
                last = i = resume
                continue
        i += 1
    parts.append(expr[last:].strip())
    return [p for p in parts if p]


def _numeric_pitfall_error(field: str, offense: str) -> ValueError:
    """Build the error for an unquoted numeric comparison."""
    return ValueError(
        f"Filter uses an unquoted numeric comparison against {field!r} "
        f"(``{offense}``). Every queryable on the Water Data API is "
        f"typed as a string, so the server rejects unquoted numeric "
        f"literals with HTTP 500; even quoting the literal gives a "
        f"lexicographic comparison (``value > '10'`` matches "
        f"``value='34.52'``, ``parameter_code = '60'`` matches nothing "
        f"because the real codes are ``'00060'``-shaped). For a true "
        f"numeric filter, fetch a wider result and reduce in pandas."
    )


def _check_compare(masked: str) -> None:
    """Raise on a bare ``field op number`` or ``number op field`` pattern."""
    compare = _NUMERIC_COMPARE_RE.search(masked)
    if not compare:
        return
    field = compare.group("field1") or compare.group("field2")
    op = compare.group("op1") or compare.group("op2")
    num = compare.group("num1") or compare.group("num2")
    raise _numeric_pitfall_error(field, f"{field} {op} {num}")


def _check_in_membership(masked: str) -> None:
    """Raise on a ``field [NOT] IN (…numeric…)`` pattern."""
    membership = _IN_NUMERIC_RE.search(masked)
    if not membership:
        return
    field = membership.group("field")
    op = "NOT IN" if membership.group("negated") else "IN"
    raise _numeric_pitfall_error(field, f"{field} {op} (…)")


def _check_between(masked: str) -> None:
    """Raise on a ``field [NOT] BETWEEN … AND …`` pattern with numerics."""
    between = _BETWEEN_NUMERIC_RE.search(masked)
    if not between:
        return
    field = between.group("field")
    op = "NOT BETWEEN" if between.group("negated") else "BETWEEN"
    raise _numeric_pitfall_error(field, f"{field} {op} …")


def _check_numeric_filter_pitfall(filter_expr: str) -> None:
    """Raise if the filter pairs a field with an unquoted numeric literal.

    Every queryable on the Water Data OGC API is typed as a string, including
    fields whose *values* look numeric (``value``, ``parameter_code`` like
    ``'00060'``, ``statistic_id`` like ``'00011'``, ``district_code``,
    ``hydrologic_unit_code``, ``channel_flow``). Any unquoted numeric
    comparison — ``value >= 1000``, ``parameter_code = 60``,
    ``parameter_code IN (60, 61)``, ``value BETWEEN 5 AND 10`` — either gets
    rejected with HTTP 500 or silently produces lexicographic results.
    Zero-padded codes are the worst case (``parameter_code = '60'`` matches
    nothing because the real codes are ``'00060'``-shaped).

    Quoted literals (``value >= '1000'``) are not flagged — the caller has
    signalled they know the column is textual.
    """
    # Mask quoted strings so ``name = 'value > 5'`` doesn't false-positive.
    masked = (
        _QUOTED_STR_RE.sub("''", filter_expr) if "'" in filter_expr else filter_expr
    )
    _check_compare(masked)
    _check_in_membership(masked)
    _check_between(masked)


def _is_chunkable(filter_expr: Any, filter_lang: Any) -> bool:
    """Only non-empty cql-text filters can be safely split at top-level OR."""
    return (
        isinstance(filter_expr, str)
        and bool(filter_expr)
        and filter_lang in {None, "cql-text"}
    )
