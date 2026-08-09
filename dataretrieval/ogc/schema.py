"""Asking an OGC service to describe itself.

Queryables and collection schemas: which properties a collection accepts, and
what columns it returns. Separate from request construction because answering
these questions means *issuing* a request, and building one must not.
"""

from __future__ import annotations

from typing import Any, cast

import httpx

from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.ogc.policy import OGC_API_URL
from dataretrieval.transport.http import HTTPX_DEFAULTS
from dataretrieval.transport.http import default_headers as _default_headers
from dataretrieval.transport.http import get as _get


def _check_ogc_requests(
    endpoint: str, req_type: str = "queryables", *, base_url: str = OGC_API_URL
) -> tuple[dict[str, Any], httpx.Response]:
    """Retrieve one collection's queryables or response schema."""
    if req_type not in ("queryables", "schema"):
        raise ValueError(f"req_type must be 'queryables' or 'schema', got {req_type!r}")
    url = f"{base_url}/collections/{endpoint}/{req_type}"
    response = _get(url, headers=_default_headers(url), **HTTPX_DEFAULTS)
    _raise_for_non_200(response)
    return cast("dict[str, Any]", response.json()), response
