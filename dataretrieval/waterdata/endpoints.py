"""Every Water Data endpoint this package talks to, in one place.

"Which services does Water Data reach, and at what URL" is a single question
with a single answer, so the answer lives in one file rather than being spelled
out again in each family module. The host is the authority of the credentials
leaf -- the host that serves these endpoints is the host that honors the API
key -- while the paths below stay here rather than importing OGC policy
internals.

This module imports nothing but that leaf, so a family module can name its
endpoint without also taking on an OGC or transport edge.
"""

from __future__ import annotations

from dataretrieval.credentials import WATERDATA_BASE_URL

#: Root of the modernized Water Data APIs.
BASE_URL = WATERDATA_BASE_URL

#: OGC API - Features service backing the typed collection getters.
OGC_API_URL = f"{BASE_URL}/ogcapi/v0"

#: Samples database (discrete water-quality results, WQX3 CSV).
SAMPLES_URL = f"{BASE_URL}/samples-data"

#: Daily-statistics service (period-of-record and date-range normals).
STATISTICS_API_VERSION = "v0"
STATISTICS_API_URL = f"{BASE_URL}/statistics/{STATISTICS_API_VERSION}"

#: STAC catalog serving NWIS rating-curve assets.
STAC_URL = f"{BASE_URL}/stac/v0"

__all__ = [
    "BASE_URL",
    "OGC_API_URL",
    "SAMPLES_URL",
    "STAC_URL",
    "STATISTICS_API_URL",
    "STATISTICS_API_VERSION",
]
