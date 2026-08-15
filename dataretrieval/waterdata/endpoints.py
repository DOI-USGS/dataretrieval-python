"""Every Water Data endpoint this package talks to, in one place.

"Which services does Water Data reach, and at what URL" is a single question
with a single answer, so the answer lives in one file rather than being spelled
out again in each family module. The host is the authority of the credentials
leaf -- the host that serves these endpoints is the host that honors the API
key -- while the paths below stay here rather than importing OGC policy
internals.

This module imports only leaves -- the credentials host and the configuration
chain -- so a family module can name its endpoint, and honor a caller's
redirect, without also taking on an OGC or transport edge.
"""

from __future__ import annotations

from dataretrieval import configuration as _configuration
from dataretrieval.credentials import WATERDATA_BASE_URL

#: Canonical paths below the Water Data root. They are not endpoints on their
#: own: callers obtain complete destinations through the functions below, which
#: makes scoped redirection part of endpoint acquisition rather than a wrapper
#: every use site must remember.
_OGC_API_PATH = "/ogcapi/v0"
_SAMPLES_PATH = "/samples-data"
_STATISTICS_API_PATH = "/statistics/v0"
_RATINGS_CATALOG_PATH = "/stac/v0"

# Default-value compatibility for the documented ``waterdata.utils`` constants.
# Production collection-family modules do not import these raw values.
_DEFAULT_BASE_URL = WATERDATA_BASE_URL
_DEFAULT_OGC_API_URL = f"{_DEFAULT_BASE_URL}{_OGC_API_PATH}"
_DEFAULT_SAMPLES_URL = f"{_DEFAULT_BASE_URL}{_SAMPLES_PATH}"


def _endpoint(path: str) -> str:
    """Return *path* beneath the effective Water Data root for this call."""
    root = _configuration.base_url(adapter="waterdata", default=WATERDATA_BASE_URL)
    return f"{root}{path}"


def ogc_api_url() -> str:
    """Return the OGC collections endpoint for the effective configuration."""
    return _endpoint(_OGC_API_PATH)


def samples_url() -> str:
    """Return the Samples endpoint for the effective configuration."""
    return _endpoint(_SAMPLES_PATH)


def statistics_api_url() -> str:
    """Return the Statistics endpoint for the effective configuration."""
    return _endpoint(_STATISTICS_API_PATH)


def ratings_catalog_url() -> str:
    """Return the Ratings catalog endpoint for the effective configuration."""
    return _endpoint(_RATINGS_CATALOG_PATH)


__all__ = [
    "ogc_api_url",
    "ratings_catalog_url",
    "samples_url",
    "statistics_api_url",
]
