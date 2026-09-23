"""Every Water Data endpoint this package requests, in one place.

The credentials leaf defines the host -- the host that serves these
endpoints is the host that accepts the API key -- and the paths below are
defined here rather than importing OGC policy internals. This module imports only
leaves: the credentials host and the configuration chain (ADR 0003).
"""

from __future__ import annotations

from dataretrieval import configuration as _configuration
from dataretrieval.credentials import WATERDATA_BASE_URL

#: The version of the Water Data OGC API this release requests by default and
#: shapes responses for.
OGC_API_VERSION = "v1"

#: Canonical paths below the Water Data root. They are not endpoints on their
#: own: callers obtain complete destinations through the request-time functions
#: below (ADR 0011). Only the OGC family takes its version at request time; the
#: Statistics service and the STAC catalog publish their own versions and have
#: no v1 (checked 2026-09-22).
_OGC_API_PATH = "/ogcapi"
_SAMPLES_PATH = "/samples-data"
_STATISTICS_API_PATH = "/statistics/v0"
_RATINGS_CATALOG_PATH = "/stac/v0"

# Default-value compatibility for the documented ``waterdata.utils`` constants.
# Production collection-family modules do not import these raw values.
_DEFAULT_BASE_URL = WATERDATA_BASE_URL
_DEFAULT_OGC_API_URL = f"{_DEFAULT_BASE_URL}{_OGC_API_PATH}/{OGC_API_VERSION}"
_DEFAULT_SAMPLES_URL = f"{_DEFAULT_BASE_URL}{_SAMPLES_PATH}"


def _endpoint(path: str) -> str:
    """Return *path* beneath the effective Water Data root for this call."""
    root = _configuration.base_url(adapter="waterdata", default=WATERDATA_BASE_URL)
    return f"{root}{path}"


def ogc_api_url(api_version: str | None = None) -> str:
    """Return the OGC collections endpoint for the effective configuration.

    Parameters
    ----------
    api_version : str, optional
        Version for this one request, used by a getter that must reach a
        version other than the configured one. ``None`` (the default) resolves
        the version through the configuration chain. A getter passes this
        instead of entering a ``configure`` block, because a block set by the
        library would override the caller's own setting (ADR 0011).
    """
    if api_version is None:
        api_version = _configuration.api_version(
            adapter="waterdata", default=OGC_API_VERSION
        )
    return _endpoint(f"{_OGC_API_PATH}/{api_version}")


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
    "OGC_API_VERSION",
    "ogc_api_url",
    "ratings_catalog_url",
    "samples_url",
    "statistics_api_url",
]
