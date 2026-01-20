"""
Water Data API module for accessing USGS water data services.

This module provides functions for downloading data from the Water Data APIs,
including the USGS Aquarius Samples database.

See https://api.waterdata.usgs.gov/ for API reference.
"""

from __future__ import annotations

# Public API exports
from .api import (
    get_codes,
    get_continuous,
    get_daily,
    get_field_measurements,
    get_latest_continuous,
    get_latest_daily,
    get_monitoring_locations,
    get_reference_table,
    get_samples,
    get_time_series_metadata,
)
from .types import (
    CODE_SERVICES,
    PROFILE_LOOKUP,
    PROFILES,
    SERVICES,
)

__all__ = [
    "get_codes",
    "get_continuous",
    "get_daily",
    "get_field_measurements",
    "get_latest_continuous",
    "get_latest_daily",
    "get_monitoring_locations",
    "get_reference_table",
    "get_samples",
    "get_time_series_metadata",
    "CODE_SERVICES",
    "SERVICES",
    "PROFILES",
    "PROFILE_LOOKUP",
]
