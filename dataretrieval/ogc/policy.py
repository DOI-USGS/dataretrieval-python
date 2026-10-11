"""Low-level OGC policy: the dialect type and control validation.

This module is the one definition of the :class:`OgcDialect` type
(per-API quirks the generic request builder needs) and OGC control validation.
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
class OgcDialect:
    """Per-API differences the generic request builder must handle.

    Attributes
    ----------
    cql2_services : frozenset[str]
        Collections that don't accept comma-separated multi-value GET
        parameters and so must be queried via POST with a CQL2 JSON body.
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
        How the API's date parameters are read and written (ADR 0014).
    """

    cql2_services: frozenset[str] = field(default_factory=frozenset)
    time_cols: frozenset[str] = field(default_factory=frozenset)
    numerical_cols: frozenset[str] = field(default_factory=frozenset)
    sort_cols: tuple[str, ...] = field(default_factory=tuple)
    dates: DateDialect = field(default_factory=DateDialect)
