"""Dependency-free constants for the Water Data internals.

URLs, the service ``id``-column map, datetime/duration regexes, and the
parameter-classification frozensets shared across the request-building,
response-parsing, validation, and pagination layers. This module imports
nothing from the rest of the package so every other ``waterdata`` internal
module can depend on it without risking an import cycle.
"""

from __future__ import annotations

import re

BASE_URL = "https://api.waterdata.usgs.gov"
OGC_API_VERSION = "v0"
OGC_API_URL = f"{BASE_URL}/ogcapi/{OGC_API_VERSION}"
SAMPLES_URL = f"{BASE_URL}/samples-data"
STATISTICS_API_VERSION = "v0"
STATISTICS_API_URL = f"{BASE_URL}/statistics/{STATISTICS_API_VERSION}"

# Maps each OGC waterdata service to its user-facing ``id`` column (the name the
# typed getters rename the wire ``id`` to, e.g. ``daily`` -> ``daily_id``).
# ``get_cql`` validates its ``service`` argument against these keys and
# uses the value as the ``output_id`` for result shaping. Keep in sync with the
# ``types.WATERDATA_SERVICES`` Literal (same keys).
_OUTPUT_ID_BY_SERVICE: dict[str, str] = {
    "channel-measurements": "channel_measurements_id",
    "combined-metadata": "combined_meta_id",
    "continuous": "continuous_id",
    "daily": "daily_id",
    "field-measurements": "field_measurement_id",
    "field-measurements-metadata": "field_series_id",
    "latest-continuous": "latest_continuous_id",
    "latest-daily": "latest_daily_id",
    "monitoring-locations": "monitoring_location_id",
    "peaks": "peak_id",
    "time-series-metadata": "time_series_id",
}

# Every service's output id EXCEPT the two that are genuinely user-facing
# (``monitoring_location_id`` and ``time_series_id``). The rest are synthetic
# per-record ids that ``_arrange_cols`` moves to the end of a result frame.
# Derived from ``_OUTPUT_ID_BY_SERVICE`` so adding a service can't silently
# leave a stray id column at the front again.
_EXTRA_ID_COLS = set(_OUTPUT_ID_BY_SERVICE.values()) - {
    "monitoring_location_id",
    "time_series_id",
}

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
# and by ``_NO_NORMALIZE_PARAMS`` to bypass string-iterable normalization.
_DATE_RANGE_PARAMS = frozenset(
    {"datetime", "last_modified", "begin", "begin_utc", "end", "end_utc", "time"}
)

# Services that don't support comma-separated values for multi-value GET
# parameters and require POST with CQL2 JSON instead.
_CQL2_REQUIRED_SERVICES = frozenset({"monitoring-locations"})

_MONITORING_LOCATION_ID_RE = re.compile(r"[^-\s]+-[^-\s]+")


# Iterable-shaped params that ``_get_args`` must NOT push through
# ``_normalize_str_iterable`` (scalar non-string knobs are caught by runtime
# type, so only iterables with special handling need to be named here):
#   - date-range params may contain ``pd.NaT``/None or interval strings
#   - ``bbox``/``boundingBox`` are ``list[float]``, sometimes ``numpy.ndarray``
#   - ``get_peaks``'s int-valued filters (``water_year`` etc.) are ``list[int]``
#   - ``get_combined_metadata``'s ``thresholds`` is ``list[float]``
_NO_NORMALIZE_PARAMS = _DATE_RANGE_PARAMS | {
    "bbox",
    "boundingBox",
    "water_year",
    "year",
    "month",
    "day",
    "peak_since",
    "thresholds",
}
