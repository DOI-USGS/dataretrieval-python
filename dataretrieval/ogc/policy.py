"""Low-level OGC policy: the dialect type and control validation.

This module is the one definition of the :class:`OgcDialect` type
(per-API quirks the generic request builder needs) and OGC control validation.
It depends only on the stdlib, so any OGC submodule can import it without
creating cycles.

It names no endpoint: which API an OGC call targets is the *adapter's*
policy, supplied per call as ``base_url``. A default here would
direct every generic OGC caller to one API.

It must not import engine, shaping, or any collection adapter.
"""

from __future__ import annotations

import numbers
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from typing import Any


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
# Shaping options
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ShapingOptions:
    """How a result frame is shaped, carried apart from the query.

    A *shaping option* controls the DataFrame the getter returns, not the
    records the service is asked for. The query keys travel as a dict the
    engine chunks, paginates, and sends; these travel beside it as one typed
    value the engine hands to :func:`~dataretrieval.ogc.shaping._finalize_ogc`.
    This class is the one definition of the set: adding an option is a field
    here plus the matching keyword on the getters, and every hop between reads
    the field rather than popping a key or threading a parameter of its own.

    The split matters because the two are consumed in different places and at
    different times. A query key is normalized by
    :func:`~dataretrieval.ogc.requests.prepare_request_args`, sized by the
    planner, and sent; a shaping option is never sent and is applied once to
    the combined frame, so a chunked or resumed call shapes its result the
    same way an un-chunked one does (ADR 0008's finalize-hook contract).

    Attributes
    ----------
    convert_type : bool
        Coerce the dialect's ``time_cols`` / ``numerical_cols`` to datetime /
        numeric. Default ``False`` — the typed getters pass ``True``.
    max_rows : int, optional
        Stop paginating once this many rows have accumulated and truncate the
        combined frame to exactly this many. ``None`` (default) fetches the
        full result. Validated here, before any request, so an invalid value
        raises without spending quota.
    """

    convert_type: bool = False
    max_rows: int | None = None

    def __post_init__(self) -> None:
        # Validate before any request: a float (even ``10.0``) or ``bool``
        # passes a bare ``< 1`` check and then raises an unclear ``TypeError``
        # from ``pd.DataFrame.head`` after the HTTP requests have been sent.
        if self.max_rows is not None:
            _require_positive_int(self.max_rows, "max_rows")

    @classmethod
    def field_names(cls) -> frozenset[str]:
        """The option names, so request building excludes exactly these keys.

        One list, derived from the fields, so the keys the query must not carry
        and the options this class defines cannot drift apart.
        """
        return frozenset(f.name for f in fields(cls))

    @classmethod
    def take(cls, local_vars: Mapping[str, Any]) -> ShapingOptions:
        """Build options from a getter's ``locals()``, ignoring absent keys.

        A getter names each option as a keyword and passes ``locals()``; a
        getter without one (a reference-table fetch has no ``convert_type``)
        simply leaves that field at its default. The companion to
        :func:`~dataretrieval.ogc.requests.prepare_request_args`, which drops
        these same keys from the query.
        """
        present = {
            name: local_vars[name] for name in cls.field_names() if name in local_vars
        }
        return cls(**present)


# A plain, shared default: the dataclass is frozen, so one instance is safe to
# reuse as every caller's "no shaping" default rather than building one per call.
DEFAULT_SHAPING = ShapingOptions()


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
    date_only_services : frozenset[str]
        Collections whose time arguments are rendered date-only
        (``YYYY-MM-DD``) rather than as a full UTC datetime. The
        ``last_modified`` parameter is always rendered as a full datetime
        regardless of this set.
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
    """

    cql2_services: frozenset[str] = field(default_factory=frozenset)
    date_only_services: frozenset[str] = field(default_factory=frozenset)
    time_cols: frozenset[str] = field(default_factory=frozenset)
    numerical_cols: frozenset[str] = field(default_factory=frozenset)
    sort_cols: tuple[str, ...] = field(default_factory=tuple)


# Default dialect: a plain OGC API with no CQL2-only collections and no
# date-only collections (every time argument rendered as a full UTC datetime).
DEFAULT_DIALECT = OgcDialect()
