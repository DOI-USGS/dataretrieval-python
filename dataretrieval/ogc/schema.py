"""Asking an OGC collection to describe itself.

Queryables and collection schemas: which properties a collection accepts, and
what columns it returns. Separate from request construction because answering
these questions means *issuing* a request, and building one must not.
"""

from __future__ import annotations

from typing import Any, cast

import httpx
import pandas as pd

from dataretrieval._response_metadata import BaseMetadata
from dataretrieval._validation import require_one_of
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.transport.http import HTTPX_DEFAULTS
from dataretrieval.transport.http import default_headers as _default_headers
from dataretrieval.transport.http import get as _get


def _check_ogc_requests(
    endpoint: str, req_type: str = "queryables", *, base_url: str
) -> tuple[dict[str, Any], httpx.Response]:
    """Retrieve one collection's queryables or response schema.

    ``base_url`` names the API to ask; it defaults to the one in scope for the
    current call rather than to any particular collection.
    """
    require_one_of(req_type, ("queryables", "schema"), name="req_type")
    url = f"{base_url}/collections/{endpoint}/{req_type}"
    response = _get(url, headers=_default_headers(url), **HTTPX_DEFAULTS)
    _raise_for_non_200(response)
    return cast("dict[str, Any]", response.json()), response


def queryables_frame(
    collection: str, *, base_url: str
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Tabulate one collection's queryable properties.

    Reading an OGC queryables document is protocol knowledge, not collection
    knowledge, so it lives here rather than in any one API's getters -- every
    OGC adapter in the package can offer the same table. ``base_url`` names
    the API to ask, defaulting to the one in scope for the current call.

    Returns
    -------
    pd.DataFrame
        One row per queryable, sorted by name, with columns ``queryable``,
        ``type``, ``title``, and ``description``.
    BaseMetadata
        Metadata describing the request (URL, query time, response headers).
    """
    # The OGC queryables document is a JSON Schema whose ``properties`` map each
    # filterable property name to a ``{title, type, description}`` definition.
    body, response = _check_ogc_requests(
        endpoint=collection, req_type="queryables", base_url=base_url
    )
    properties: dict[str, Any] = body.get("properties", {})
    df = pd.DataFrame(
        [
            {
                "queryable": name,
                "type": prop.get("type"),
                "title": prop.get("title"),
                "description": (prop.get("description") or "").strip(),
            }
            for name, prop in sorted(properties.items())
        ],
        columns=["queryable", "type", "title", "description"],
    )
    return df, BaseMetadata(response)
