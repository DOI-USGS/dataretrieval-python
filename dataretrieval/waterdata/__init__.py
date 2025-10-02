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
    get_daily,
    get_field_measurements,
    get_latest_continuous,
    get_monitoring_locations,
    get_samples,
    get_time_series_metadata,
    _check_profiles,
)
from .types import (
    CODE_SERVICES,
    SERVICES,
    PROFILES,
    PROFILE_LOOKUP,
)

__all__ = [
    "get_codes",
    "get_daily",
    "get_field_measurements",
    "get_latest_continuous",
    "get_monitoring_locations",
    "get_samples",
    "get_time_series_metadata",
    "_check_profiles",
    "CODE_SERVICES",
    "SERVICES",
    "PROFILES",
    "PROFILE_LOOKUP",
]
