"""OGC argument normalization and HTTP request construction.

Ambient request state lives in :mod:`dataretrieval.ogc.context`; queryables and
schema execution live in :mod:`dataretrieval.ogc.schema`. Neither is re-exported
from here -- importing the schema helper only to forward it would give this
module an edge to the one part of OGC that executes HTTP, which is exactly what
request *construction* is supposed to be free of.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

import httpx

from dataretrieval.ogc.context import _dialect as _dialect
from dataretrieval.ogc.context import _ogc_base_url as _ogc_base_url
from dataretrieval.ogc.dates import _DATE_RANGE_PARAMS, _format_api_dates
from dataretrieval.transport.http import default_headers as _default_headers

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


def _partition_request_params(
    params: dict[str, Any], *, use_cql2: bool
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split URL parameters from multi-value CQL2 POST predicates."""
    if use_cql2:
        post_params = {
            key: value
            for key, value in params.items()
            if isinstance(value, (list, tuple)) and len(value) > 1
        }
        return (
            {key: value for key, value in params.items() if key not in post_params},
            post_params,
        )

    get_params = {
        key: ",".join(str(item) for item in value)
        if isinstance(value, (list, tuple))
        else value
        for key, value in params.items()
        if not (isinstance(value, (list, tuple)) and len(value) == 0)
    }
    return get_params, {}


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
                date=service in dialect.date_only_services and key != "last_modified",
            )
    params, post_params = _partition_request_params(
        kwargs, use_cql2=service in dialect.cql2_services
    )

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
