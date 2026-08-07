"""Compatibility facade: the import path the collection getters used to have.

Every getter here is defined in a collection-family module and re-exported
unchanged. The path is kept because it is published, and this file exists only
to preserve it -- it holds no logic, and a test enforces that.

Import from :mod:`dataretrieval.waterdata` instead.
"""

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

# Private compatibility names used by existing callers and patch targets.
_SAMPLES_PARAM_TO_API = _samples._SAMPLES_PARAM_TO_API
_SAMPLES_LEGACY_KWARGS = _samples._SAMPLES_LEGACY_KWARGS
