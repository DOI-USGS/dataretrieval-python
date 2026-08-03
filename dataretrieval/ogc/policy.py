"""Low-level OGC policy: dialect types and default endpoint constants.

This module is the single source of truth for the :class:`OgcDialect` type
(per-API quirks the generic request builder needs) and the default endpoint
constants used by the Water Data OGC API. It depends only on the stdlib so it
can be imported safely by any OGC submodule without creating cycles.

It must NOT import engine, shaping, or any service adapter.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Endpoint constants
# ---------------------------------------------------------------------------

BASE_URL = "https://api.waterdata.usgs.gov"
OGC_API_VERSION = "v0"
OGC_API_URL = f"{BASE_URL}/ogcapi/{OGC_API_VERSION}"

# ---------------------------------------------------------------------------
# Dialect type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OgcDialect:
    """Per-API quirks the generic request builder needs to know about.

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
        Result columns to coerce to datetime when ``convert_type`` is set.
        Empty by default, so the generic engine carries no API-specific
        column knowledge; each API supplies its own.
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
