"""Shared module-level state for the OGC API engine.

This is the dependency-graph leaf for the ``dataretrieval.ogc`` engine: it
holds every module-level constant, regex, param-set, the package logger, and the
optional geopandas import probe. The per-call ambient context variables live one
layer up in :mod:`dataretrieval.ogc._context`. Every other engine module — and
the :mod:`dataretrieval.ogc.engine` facade itself — imports its shared state
from here (or from ``_context``), so there are no import cycles.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import TypeVar

try:
    import geopandas as gpd

    GEOPANDAS = True
except ImportError:
    # Bind ``gpd`` to ``None`` (not unbound) so sibling modules can do
    # ``from dataretrieval.ogc._constants import gpd`` unconditionally; the
    # geopandas code paths are all gated on ``GEOPANDAS`` / a ``geopd`` flag,
    # so ``gpd`` is never dereferenced when it is ``None``. geopandas is an
    # optional dependency, so this branch is a supported install.
    gpd = None
    GEOPANDAS = False

# ``gpd`` is the single home of the (optional) geopandas handle, re-exported by
# the engine façade and resolved as ``_responses.gpd`` (the geopandas parse
# functions live there, and tests patch it there). Declare it in ``__all__`` so
# ``mypy --strict`` (which forbids implicit re-export) treats it as an explicit
# export.
__all__ = ["gpd"]

# Set up logger for this module
logger = logging.getLogger(__name__)

# Whether geopandas is present is a static, environment-level fact, so warn once
# here at import time rather than per query/chunk. That avoids the warning
# repeating on every call and avoids it interleaving with the progress line's
# carriage-return rewrites.
if not GEOPANDAS:
    logger.warning(
        "Geopandas not installed. Geometries will be flattened into pandas DataFrames."
    )

BASE_URL = "https://api.waterdata.usgs.gov"
OGC_API_VERSION = "v0"
OGC_API_URL = f"{BASE_URL}/ogcapi/{OGC_API_VERSION}"


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
_DEFAULT_DIALECT = OgcDialect()

_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)

# Anchored to ``[Pp]\d`` so a normal word containing ``p`` (e.g. ``"Apr"``)
# doesn't get mis-classified as an ISO 8601 duration; the optional ``T``
# admits time-only forms like ``PT36H``.
_DURATION_RE = re.compile(r"^[Pp]T?\d")

# OGC API parameters that carry a date/datetime value (single string,
# two-element range, or interval/duration string) rather than a multi-value
# string list. Used by ``_construct_api_requests`` to keep them out of the
# POST/CQL2 multi-value path and to route them through ``_format_api_dates``,
# and by the default ``_get_args`` no-normalize set to bypass string-iterable
# normalization.
_DATE_RANGE_PARAMS = frozenset(
    {"datetime", "last_modified", "begin", "begin_utc", "end", "end_utc", "time"}
)

_Cursor = TypeVar("_Cursor")

# Matches a lowercase letter or digit immediately followed by an uppercase
# letter — the camelCase/PascalCase word boundary where a ``_`` is inserted.
# A letter/digit boundary is intentionally NOT split (so ``navd88`` stays put).
_CAMEL_BOUNDARY_RE = re.compile(r"([a-z0-9])([A-Z])")

# ``AGENCY-ID``: a hyphen-separated agency prefix and local id. The local id
# may itself contain hyphens (``\S+`` after the first separator) — NGWMN
# aggregates many non-USGS agencies whose local ids aren't bare digits, so
# only the agency prefix is constrained to be hyphen/space-free.
_MONITORING_LOCATION_ID_RE = re.compile(r"[^-\s]+-\S+")

# Default set of iterable-shaped params that ``_get_args`` must NOT push
# through ``_normalize_str_iterable`` (date-range params may carry
# ``pd.NaT``/None or interval strings; ``bbox`` is ``list[float]``). Callers
# with extra numeric params (e.g. the Water Data API's ``water_year``,
# ``thresholds``) pass their own superset.
_NO_NORMALIZE_PARAMS = _DATE_RANGE_PARAMS | {"bbox"}
