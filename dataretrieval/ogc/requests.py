"""OGC argument normalization and HTTP request construction.

The API to target and its quirks are explicit parameters (``base_url``,
``dialect``) -- construction states everything it needs. Queryables and schema
execution live in :mod:`dataretrieval.ogc.schema`, not re-exported from here --
importing the schema helper only to forward it would give this module an edge
to the one part of OGC that executes HTTP, which is exactly what request
*construction* is supposed to be free of.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

import httpx

from dataretrieval.ogc.dates import _DATE_RANGE_PARAMS, _format_api_dates
from dataretrieval.ogc.policy import DEFAULT_DIALECT, OgcDialect
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


def _switch_arg_id(ls: dict[str, Any], id_name: str, collection: str) -> dict[str, Any]:
    """Switch argument id from its package-specific identifier to the
    standardized "id" key that the API recognizes."""
    collection_id = collection.replace("-", "_") + "_id"
    if "id" not in ls:
        if collection_id in ls:
            ls["id"] = ls[collection_id]
        elif id_name in ls:
            ls["id"] = ls[id_name]
    ls.pop(collection_id, None)
    ls.pop(id_name, None)
    return ls


def _switch_properties_id(
    properties: list[str] | None, id_name: str, collection: str
) -> list[str]:
    """Build the wire ``properties`` list, dropping every id alias and
    ``geometry``."""
    if not properties:
        return []
    collection_id = collection.replace("-", "_") + "_id"
    drop = {"id", "geometry", id_name, collection_id}
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


def _items_url(collection: str, base_url: str) -> str:
    """The OGC items endpoint for ``collection`` under ``base_url``."""
    return f"{base_url}/collections/{collection}/items"


def _cql2_post_request(
    service_url: str, *, content: str, params: dict[str, Any]
) -> httpx.Request:
    """A POST/CQL2 request: the media type the API requires, in one place."""
    headers = _default_headers(service_url)
    headers["Content-Type"] = "application/query-cql-json"
    return httpx.Request(
        method="POST",
        url=service_url,
        headers=headers,
        content=content,
        params=params,
    )


def _construct_api_requests(
    collection: str,
    properties: list[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    skip_geometry: bool | None = None,
    *,
    base_url: str,
    dialect: OgcDialect | None = None,
    **kwargs: Any,
) -> httpx.Request:
    """Construct an HTTP request object for the specified OGC API collection.

    ``base_url`` is required: this package is API-neutral and names no API of
    its own, so the adapter naming the collection states the API it targets.
    ``dialect`` defaults to a plain OGC API with no per-collection quirks.
    """
    service_url = _items_url(collection, base_url)
    if dialect is None:
        dialect = DEFAULT_DIALECT
    for key in _DATE_RANGE_PARAMS:
        if key in kwargs:
            kwargs[key] = _format_api_dates(
                kwargs[key],
                date=(
                    collection in dialect.date_only_services and key != "last_modified"
                ),
            )
    params, post_params = _partition_request_params(
        kwargs, use_cql2=collection in dialect.cql2_services
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

    if post_params:
        return _cql2_post_request(
            service_url, content=_cql2_param(post_params), params=params
        )
    return httpx.Request(
        method="GET",
        url=service_url,
        headers=_default_headers(service_url),
        params=params,
    )


def _construct_cql_request(
    collection: str,
    cql_body: str,
    *,
    base_url: str,
    properties: list[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    skip_geometry: bool | None = None,
) -> httpx.Request:
    """Build a POST/CQL2 request from a verbatim CQL2 body.

    ``base_url`` is required for the same reason as in
    :func:`_construct_api_requests`.
    """
    service_url = _items_url(collection, base_url)
    params = _ogc_query_params(
        {},
        properties=properties,
        bbox=bbox,
        limit=limit,
        skip_geometry=skip_geometry,
    )
    return _cql2_post_request(service_url, content=cql_body, params=params)


# ---------------------------------------------------------------------------
# Argument normalization helpers
# ---------------------------------------------------------------------------

# Iterable-shaped params that ``_get_args`` must NOT push through
# ``_normalize_str_iterable`` (date-range params may carry ``pd.NaT``/None or
# interval strings; ``bbox`` is ``list[float]``). Every OGC caller gets these;
# an adapter with extra numeric params names only its extras via
# ``prepare_request_args(..., extra_no_normalize=...)``.
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
    extra_no_normalize: frozenset[str] | set[str] = frozenset(),
) -> dict[str, Any]:
    """Build OGC request kwargs from a getter's ``locals()``.

    Internal bookkeeping keys, caller-supplied exclusions, and ``None`` values
    are omitted. Identifiers and properties are validated; other iterables are
    normalized unless exempted.

    ``extra_no_normalize`` *adds* to the engine's own
    :data:`_NO_NORMALIZE_PARAMS` rather than replacing it, so an adapter names
    only the params it owns and cannot silently drop the date-range exemptions
    by forgetting to union them back in.
    """
    no_normalize = _NO_NORMALIZE_PARAMS | frozenset(extra_no_normalize)
    # Both spellings: this drops the caller's collection selector out of the
    # query string, and public getters still name that local ``service``
    # (waterdata.get_samples) or ``service=`` during the get_cql deprecation.
    to_exclude = {"collection", "service", "output_id"}
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
