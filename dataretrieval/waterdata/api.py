"""Backward-compatible facade for Water Data collection-family adapters."""

from __future__ import annotations

from dataretrieval.waterdata import samples as _samples
from dataretrieval.waterdata.cql import get_cql
from dataretrieval.waterdata.measurements import (
    get_channel,
    get_field_measurements,
    get_peaks,
)
from dataretrieval.waterdata.metadata import (
    get_combined_metadata,
    get_field_measurements_metadata,
    get_monitoring_locations,
    get_time_series_metadata,
)
from dataretrieval.waterdata.reference import get_queryables, get_reference_table
from dataretrieval.waterdata.samples import (
    get_codes,
    get_samples,
    get_samples_summary,
)
from dataretrieval.waterdata.time_series import (
    get_continuous,
    get_daily,
    get_latest_continuous,
    get_latest_daily,
    get_stats_date_range,
    get_stats_por,
)
from dataretrieval.waterdata.utils import get_ogc_data as _get_ogc_data

__all__ = [
    "get_channel",
    "get_codes",
    "get_combined_metadata",
    "get_continuous",
    "get_cql",
    "get_daily",
    "get_field_measurements",
    "get_field_measurements_metadata",
    "get_latest_continuous",
    "get_latest_daily",
    "get_monitoring_locations",
    "get_peaks",
    "get_queryables",
    "get_reference_table",
    "get_samples",
    "get_samples_summary",
    "get_stats_date_range",
    "get_stats_por",
    "get_time_series_metadata",
]

# Preserve the documented legacy implementation path for introspection and
# Sphinx while the function objects live in cohesive family modules.
for _name in __all__:
    globals()[_name].__module__ = __name__
del _name

# Private compatibility names used by existing callers and patch targets.
_SAMPLES_PARAM_TO_API = _samples._SAMPLES_PARAM_TO_API
_SAMPLES_LEGACY_KWARGS = _samples._SAMPLES_LEGACY_KWARGS
get_ogc_data = _get_ogc_data
