"""OGC request preparation, construction, and schema/queryables lookup.

This module owns the machinery for building OGC API requests (both GET and
POST/CQL2 paths), the ambient base-URL and dialect state that request builders
read, and the queryables/schema request helper used by empty-result shaping.

It depends on :mod:`~dataretrieval.ogc.policy` (the dialect type and endpoint
constants), :mod:`~dataretrieval.ogc.dates`, :mod:`~dataretrieval.ogc.errors`,
and :mod:`~dataretrieval.utils` (shared HTTP primitives). It must NOT import
engine or shaping.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable, Mapping
from typing import Any, cast

import httpx

from dataretrieval.ogc.dates import _DATE_RANGE_PARAMS, _format_api_dates
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.ogc.policy import DEFAULT_DIALECT, OGC_API_URL, OgcDialect
from dataretrieval.transport.http import (
    HTTPX_DEFAULTS,
)
from dataretrieval.transport.http import (
    default_headers as _default_headers,
)
from dataretrieval.transport.http import (
    get as _get,
)
from dataretrieval.utils import Ambient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ambient per-call state
# ---------------------------------------------------------------------------

# Optional cap on the rows one paginated call accumulates before it stops
# following ``next`` links (``None`` = uncapped). Set by :func:`get_reference_table`
# to preview large tables without downloading every page.
_row_cap: Ambient[int | None] = Ambient("ogc_row_cap", None)

# OGC base URL the shared request builder (:func:`_construct_api_requests`)
# targets — the main Water Data API or, for NGWMN collections, their own base.
_ogc_base_url: Ambient[str] = Ambient("ogc_base_url", OGC_API_URL)

# Per-call OGC dialect the request builder reads for CQL2-vs-GET routing and
# date-only formatting (default: a plain OGC API).
_dialect: Ambient[OgcDialect] = Ambient("ogc_dialect", DEFAULT_DIALECT)


# ---------------------------------------------------------------------------
# Monitoring location ID validation
# ---------------------------------------------------------------------------

# ``AGENCY-ID``: a hyphen-separated agency prefix and local id. The local id
# may itself contain hyphens (``\S+`` after the first separator) — NGWMN
# aggregates many non-USGS agencies whose local ids aren't bare digits, so
# only the agency prefix is constrained to be hyphen/space-free.
_MONITORING_LOCATION_ID_RE = re.compile(r"[^-\s]+-\S+")


# ---------------------------------------------------------------------------
# Request building helpers
# ---------------------------------------------------------------------------


def _switch_arg_id(ls: dict[str, Any], id_name: str, service: str) -> dict[str, Any]:
    """Switch argument id from its package-specific identifier to the
    standardized "id" key that the API recognizes."""
    service_id = service.replace("-", "_") + "_id"
    if "id" not in ls:
        if service_id in ls:
            ls["id"] = ls[service_id]
        elif id_name in ls:
            ls["id"] = ls[id_name]
    ls.pop(service_id, None)
    ls.pop(id_name, None)
    return ls


def _switch_properties_id(
    properties: list[str] | None, id_name: str, service: str
) -> list[str]:
    """Build the wire ``properties`` list, dropping every id alias and
    ``geometry``."""
    if not properties:
        return []
    service_id = service.replace("-", "_") + "_id"
    drop = {"id", "geometry", id_name, service_id}
    normalized = (p.replace("-", "_") for p in properties)
    return [p for p in normalized if p not in drop]


def _cql2_param(args: dict[str, Any]) -> str:
    """Convert query parameters to CQL2 JSON format for POST requests."""
    query = {
        "op": "and",
        "args": [
            {"op": "in", "args": [{"property": key}, values]}
            for key, values in args.items()
        ],
    }
    return json.dumps(query, separators=(",", ":"))


def _ogc_query_params(
    params: dict[str, Any],
    *,
    properties: list[str] | None,
    bbox: list[float] | None,
    limit: int | None,
    skip_geometry: bool | None,
) -> dict[str, Any]:
    """Add the shared OGC query knobs to ``params`` (mutated in place)."""
    if skip_geometry is not None:
        params["skipGeometry"] = skip_geometry
    params["limit"] = 50000 if limit is None or limit > 50000 else limit
    if bbox is not None and len(bbox) > 0:
        params["bbox"] = ",".join(map(str, bbox))
    if properties:
        params["properties"] = ",".join(properties)
    return params


def _construct_api_requests(
    service: str,
    properties: list[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    skip_geometry: bool | None = None,
    **kwargs: Any,
) -> httpx.Request:
    """Construct an HTTP request object for the specified OGC API service."""
    service_url = f"{_ogc_base_url.get()}/collections/{service}/items"
    dialect = _dialect.get()

    for key in _DATE_RANGE_PARAMS:
        if key in kwargs:
            kwargs[key] = _format_api_dates(
                kwargs[key],
                date=(service in dialect.date_only_services and key != "last_modified"),
            )

    if service in dialect.cql2_services:
        post_params = {
            k: v
            for k, v in kwargs.items()
            if isinstance(v, (list, tuple)) and len(v) > 1
        }
        params = {k: v for k, v in kwargs.items() if k not in post_params}
    else:
        post_params = {}
        params = {
            k: ",".join(str(x) for x in v) if isinstance(v, (list, tuple)) else v
            for k, v in kwargs.items()
            if not (isinstance(v, (list, tuple)) and len(v) == 0)
        }

    _ogc_query_params(
        params,
        properties=properties,
        bbox=bbox,
        limit=limit,
        skip_geometry=skip_geometry,
    )

    if "filter_lang" in params:
        params["filter-lang"] = params.pop("filter_lang")

    headers = _default_headers(service_url)

    if post_params:
        headers["Content-Type"] = "application/query-cql-json"
        return httpx.Request(
            method="POST",
            url=service_url,
            headers=headers,
            content=_cql2_param(post_params),
            params=params,
        )
    return httpx.Request(
        method="GET",
        url=service_url,
        headers=headers,
        params=params,
    )


def _construct_cql_request(
    service: str,
    cql_body: str,
    *,
    properties: list[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    skip_geometry: bool | None = None,
) -> httpx.Request:
    """Build a POST/CQL2 request from a verbatim CQL2 body."""
    service_url = f"{_ogc_base_url.get()}/collections/{service}/items"
    params = _ogc_query_params(
        {},
        properties=properties,
        bbox=bbox,
        limit=limit,
        skip_geometry=skip_geometry,
    )
    headers = _default_headers(service_url)
    headers["Content-Type"] = "application/query-cql-json"
    return httpx.Request(
        method="POST",
        url=service_url,
        headers=headers,
        content=cql_body,
        params=params,
    )


def _check_ogc_requests(
    endpoint: str, req_type: str = "queryables"
) -> tuple[dict[str, Any], httpx.Response]:
    """Send an HTTP GET request to the OGC endpoint for queryables/schema."""
    if req_type not in ("queryables", "schema"):
        raise ValueError(f"req_type must be 'queryables' or 'schema', got {req_type!r}")
    url = f"{_ogc_base_url.get()}/collections/{endpoint}/{req_type}"
    resp = _get(url, headers=_default_headers(url), **HTTPX_DEFAULTS)
    _raise_for_non_200(resp)
    return cast("dict[str, Any]", resp.json()), resp


# ---------------------------------------------------------------------------
# Argument normalization helpers
# ---------------------------------------------------------------------------

# Default set of iterable-shaped params that ``_get_args`` must NOT push
# through ``_normalize_str_iterable`` (date-range params may carry
# ``pd.NaT``/None or interval strings; ``bbox`` is ``list[float]``). Callers
# with extra numeric params pass their own superset.
_NO_NORMALIZE_PARAMS = _DATE_RANGE_PARAMS | {"bbox"}


def _normalize_str_iterable(
    value: str | Iterable[str] | None,
    param_name: str = "value",
) -> str | list[str] | None:
    """Validate that ``value`` is None, a string, or an iterable of strings."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping) or not isinstance(value, Iterable):
        raise TypeError(
            f"{param_name} must be a string or iterable of strings, "
            f"not {type(value).__name__} (got {value!r})."
        )
    values: list[str] = []
    for v in value:
        if not isinstance(v, str):
            raise TypeError(
                f"{param_name} elements must be strings, "
                f"not {type(v).__name__} (got {v!r})."
            )
        values.append(v)
    return values


def _as_str_list(
    value: str | Iterable[str] | None,
    param_name: str = "value",
) -> list[str] | None:
    """Normalize ``value`` to ``list[str]`` (``None`` passes through)."""
    normalized = _normalize_str_iterable(value, param_name)
    if isinstance(normalized, str):
        return [normalized]
    return normalized


def _check_monitoring_location_id(
    monitoring_location_id: str | Iterable[str] | None,
) -> str | list[str] | None:
    """Validate and normalize a ``monitoring_location_id`` value."""
    try:
        value = _normalize_str_iterable(
            monitoring_location_id, "monitoring_location_id"
        )
    except TypeError as exc:
        raise TypeError(
            f"{exc} Expected 'AGENCY-ID' format, e.g., 'USGS-01646500'."
        ) from None
    if value is None:
        return None
    for item in (value,) if isinstance(value, str) else value:
        if not _MONITORING_LOCATION_ID_RE.fullmatch(item):
            raise ValueError(
                f"Invalid monitoring_location_id: {item!r}. "
                f"Expected 'AGENCY-ID' format, e.g., 'USGS-01646500'."
            )
    return value


def prepare_request_args(
    local_vars: dict[str, Any],
    exclude: set[str] | None = None,
    *,
    no_normalize: frozenset[str] | set[str] = _NO_NORMALIZE_PARAMS,
) -> dict[str, Any]:
    """Build OGC request kwargs from a getter's ``locals()``.

    Internal bookkeeping keys, caller-supplied exclusions, and ``None`` values
    are omitted. Identifiers and properties are validated; other iterables are
    normalized unless listed in ``no_normalize``.
    """
    to_exclude = {"service", "output_id"}
    if exclude:
        to_exclude.update(exclude)

    args: dict[str, Any] = {}
    for k, v in local_vars.items():
        if k in to_exclude or v is None:
            continue
        if k == "monitoring_location_id":
            args[k] = _check_monitoring_location_id(v)
        elif k == "properties":
            args[k] = _as_str_list(v, k)
        elif k in no_normalize and isinstance(v, Iterable) and not isinstance(v, str):
            args[k] = v.tolist() if hasattr(v, "tolist") else list(v)
        elif isinstance(v, str) or not isinstance(v, Iterable):
            args[k] = v
        else:
            args[k] = _normalize_str_iterable(v, k)
    return args


# Compatibility alias for existing private imports from ``ogc.engine``.
_get_args = prepare_request_args
