"""Low-level OGC policy: the dialect type and control validation.

This module is the one definition of the :class:`OgcDialect` type
(per-API quirks the generic request builder needs) and OGC control validation.
It depends only on the stdlib and the interval grammar's dialect type, so any
OGC submodule can import it without creating cycles.

It names no endpoint: which API an OGC call targets is the *adapter's*
policy, supplied per call as ``base_url``. A default here would
direct every generic OGC caller to one API.

It must not import engine, shaping, or any collection adapter.
"""

from __future__ import annotations

import numbers
from dataclasses import dataclass, field

from dataretrieval._intervals import DateDialect


def _require_positive_int(
    value: int, name: str, *, examples: str | None = None
) -> None:
    """Validate a positive-integer OGC count control.

    Any :class:`numbers.Integral` is accepted, including NumPy and pandas
    integers, but ``bool`` is rejected despite being an ``Integral`` subtype.
    """
    if not isinstance(value, numbers.Integral) or isinstance(value, bool) or value < 1:
        eg = f", e.g. {examples}" if examples else ""
        raise ValueError(f"{name} must be a positive integer{eg} (got {value!r}).")


# ---------------------------------------------------------------------------
# Dialect type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OgcDialect:
    """Per-API differences the generic request builder must handle.

    Attributes
    ----------
    lists_as_cql2 : bool
        Whether a filter with more than one value is sent as a CQL2 ``in``
        predicate, rather than as a comma-joined GET value (``site=A,B``, the
        default) that the API would read as one value.
    date_params : frozenset[str]
        The parameters that carry a date range. The standard has one,
        ``datetime``; an API that filters other fields by date lists them all.
    date_only_collections : frozenset[str]
        Collections whose dates are written as ``YYYY-MM-DD`` rather than as a
        UTC datetime, because their values are calendar days.
    timed_params : frozenset[str]
        Date parameters written as a UTC datetime even in a date-only
        collection, because they filter a timestamp.
    time_cols : frozenset[str]
        Result columns to coerce to datetime when ``convert_type`` is set. Empty by
        default, so the generic engine holds no API-specific column list; each API
        supplies its own.
    numerical_cols : frozenset[str]
        Result columns to coerce to numeric when ``convert_type`` is set.
    sort_cols : tuple[str, ...]
        Columns to sort the combined result by, in priority order. Sorting
        is applied only when the first (primary) column is present; any
        later columns also present are added as secondary keys.
    dates : DateDialect
        How the API writes date ranges: the interval grammar it shares with
        STAC. The default follows the standard.
    """

    lists_as_cql2: bool = False
    date_params: frozenset[str] = frozenset({"datetime"})
    date_only_collections: frozenset[str] = field(default_factory=frozenset)
    timed_params: frozenset[str] = field(default_factory=frozenset)
    time_cols: frozenset[str] = field(default_factory=frozenset)
    numerical_cols: frozenset[str] = field(default_factory=frozenset)
    sort_cols: tuple[str, ...] = field(default_factory=tuple)
    dates: DateDialect = field(default_factory=DateDialect)


# Default dialect: a plain OGC API, with lists comma-joined in the URL and every
# time argument rendered as a full UTC datetime.
DEFAULT_DIALECT = OgcDialect()
