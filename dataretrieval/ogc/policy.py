"""Low-level OGC policy: the dialect types and control validation.

This module is the one definition of :class:`OgcDialect` (how an API departs
from OGC API Features, read by the request builder and the result finalizer),
its :class:`ShapingDialect` group, and OGC control validation.
It depends only on the stdlib and the interval grammar's dialect type, so any
OGC submodule can import it without creating cycles.

It names no API: which one an OGC call targets is the *adapter's* policy,
supplied per call as an endpoint (ADR 0014). A default here would direct every
generic OGC caller to one API.

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
class ShapingDialect:
    """How an API's results are shaped into a frame: the fields the result
    finalizer reads together. Each default leaves the result as sent.

    Attributes
    ----------
    time_cols : frozenset[str]
        Result columns to coerce to datetime when ``convert_type`` is set.
    numerical_cols : frozenset[str]
        Result columns to coerce to numeric when ``convert_type`` is set.
    sort_cols : tuple[str, ...]
        Columns to sort the combined result by, in priority order. Sorting
        is applied only when the first (primary) column is present; any
        later columns also present are added as secondary keys.
    extra_id_cols : frozenset[str]
        Synthetic per-record id columns, with no meaning to a user, moved to
        the end of a result frame when ``properties`` was not given.
    """

    time_cols: frozenset[str] = frozenset()
    numerical_cols: frozenset[str] = frozenset()
    sort_cols: tuple[str, ...] = ()
    extra_id_cols: frozenset[str] = frozenset()


@dataclass(frozen=True)
class OgcDialect:
    """How one OGC API departs from OGC API Features (ADR 0014).

    Each default is the standard, so ``OgcDialect()`` describes an API that
    follows it.

    Attributes
    ----------
    lists_as_cql2 : bool
        Whether a filter with more than one value is sent as a CQL2 ``in``
        predicate, rather than as a comma-joined GET value (``site=A,B``, the
        default) that the API would read as one value.
    dates : DateDialect
        How the API's date parameters are read and written.
    shaping : ShapingDialect
        How the API's results are shaped into a frame.
    """

    lists_as_cql2: bool = False
    dates: DateDialect = field(default_factory=DateDialect)
    shaping: ShapingDialect = field(default_factory=ShapingDialect)
