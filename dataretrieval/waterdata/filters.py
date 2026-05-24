"""CQL ``filter`` support for the Water Data OGC getters.

Public:

- ``FILTER_LANG``: the type alias used for the ``filter_lang`` kwarg.

Internal helpers used by ``chunking.multi_value_chunked``'s joint
planner: ``_split_top_level_or`` (clause partitioning),
``_is_chunkable`` (filter-language gate), and
``_check_numeric_filter_pitfall`` (the lexicographic-comparison guard).

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
