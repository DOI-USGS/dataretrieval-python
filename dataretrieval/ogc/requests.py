"""OGC argument normalization and HTTP request construction.

The API to target and its quirks are one explicit parameter, the endpoint
(ADR 0014) -- construction takes everything it needs as parameters.
Queryables and schema execution are in :mod:`dataretrieval.ogc.schema` and are
not re-exported here,
so this module does not depend on the part of OGC that executes HTTP (ADR 0007).
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

import httpx

from dataretrieval._dates import read_date_range
from dataretrieval._intervals import DateDialect, write_interval
from dataretrieval.ogc.filters import _cql_in_clause
from dataretrieval.transport.http import default_headers as _default_headers

if TYPE_CHECKING:
    from dataretrieval._service import Endpoint
    from dataretrieval.ogc.policy import OgcDialect

# ---------------------------------------------------------------------------
# Monitoring location ID validation
# ---------------------------------------------------------------------------

# ``AGENCY-ID``: a hyphen-separated agency prefix and local id. The local id
# may itself contain hyphens (``\S+`` after the first separator) — NGWMN
# aggregates many non-USGS agencies whose local ids are not only digits, so
# only the agency prefix is constrained to be hyphen/space-free.
_MONITORING_LOCATION_ID_RE = re.compile(r"[^-\s]+-\S+")


# ---------------------------------------------------------------------------
# Request building helpers
# ---------------------------------------------------------------------------


def _switch_arg_id(ls: dict[str, Any], id_name: str, collection: str) -> dict[str, Any]:
    """Switch argument id from its package-specific identifier to the
    standardized "id" key that the API accepts."""
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


def _cql2_param(args: dict[str, Any], json_filter: dict[str, Any] | None = None) -> str:
    """Convert query parameters, and a caller's CQL2 JSON filter, to the CQL2
    JSON body of a POST request."""
    if not args and json_filter is not None:
        return json.dumps(json_filter, separators=(",", ":"))
    predicates: list[dict[str, Any]] = [
        {"op": "in", "args": [{"property": key}, values]}
        for key, values in args.items()
    ]
    if json_filter is not None:
        predicates.append(json_filter)
    return json.dumps({"op": "and", "args": predicates}, separators=(",", ":"))


def _pop_json_filter(params: dict[str, Any]) -> dict[str, Any] | None:
    """Remove a ``filter_lang="cql-json"`` filter from ``params`` and return it
    parsed, for the POST body: the APIs answer one in the URL with HTTP 400."""
    if params.get("filter_lang") != "cql-json" or "filter" not in params:
        return None
    del params["filter_lang"]
    raw = params.pop("filter")
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        parsed = None
    if not isinstance(parsed, dict):
        raise ValueError(
            f"filter is not a CQL2 JSON object: {raw!r}. Pass one as a string, "
            'such as \'{"op": "=", "args": [{"property": "approval_status"}, '
            '"Approved"]}\', or pass filter_lang="cql-text" with a text filter.'
        )
    return parsed


def _ogc_query_params(
    params: dict[str, Any],
    *,
    properties: list[str] | None,
    bbox: list[float] | None,
    limit: int | None,
    skip_geometry: bool | None,
) -> dict[str, Any]:
    """Add the shared OGC query parameters to ``params`` (mutated in place)."""
    if skip_geometry is not None:
        params["skipGeometry"] = skip_geometry
    params["limit"] = 50000 if limit is None or limit > 50000 else limit
    if bbox is not None and len(bbox) > 0:
        params["bbox"] = ",".join(map(str, bbox))
    if properties:
        params["properties"] = ",".join(properties)
    return params


def _is_list(value: Any) -> bool:
    return isinstance(value, (list, tuple))


def _join_get_value(value: Any) -> Any:
    """Comma-join list/tuple values for GET params; pass scalars through."""
    if _is_list(value):
        return ",".join(str(item) for item in value)
    return value


def _partition_request_params(
    params: dict[str, Any], *, use_cql2: bool
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split URL parameters from multi-value CQL2 predicates.

    Empty lists are dropped. With ``use_cql2``, a list of more than one value is
    a predicate; otherwise every list is comma-joined into the URL.
    """
    params = {k: v for k, v in params.items() if not (_is_list(v) and len(v) == 0)}
    predicates = (
        {k: v for k, v in params.items() if _is_list(v) and len(v) > 1}
        if use_cql2
        else {}
    )
    url_params = {
        k: _join_get_value(v) for k, v in params.items() if k not in predicates
    }
    return url_params, predicates


_FOLD_REMEDY = (
    "Remove filter= so the list is sent in the request body, or write the "
    'filter as CQL2 JSON with filter_lang="cql-json".'
)


def _and_in_clauses(predicates: dict[str, Any], filter_text: str) -> str:
    """``prop IN ('a', 'b') AND ... AND (filter_text)``."""
    clauses = [
        _cql_in_clause(key, values, remedy=_FOLD_REMEDY)
        for key, values in predicates.items()
    ]
    return " AND ".join([*clauses, f"({filter_text})"])


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
    endpoint: Endpoint[OgcDialect],
    **kwargs: Any,
) -> httpx.Request:
    """Construct an HTTP request object for the specified OGC API collection.

    ``endpoint`` is required: this package names no API of its own, so the
    adapter naming the collection also names the API it targets.
    """
    service_url = _items_url(collection, endpoint.url)
    dialect = endpoint.dialect
    dates = dialect.dates
    for key in dates.params & kwargs.keys():
        date_only = (
            collection in dates.date_only_collections and key not in dates.timed_params
        )
        kwargs[key] = write_interval(kwargs[key], dates, date_only=date_only)
    json_filter = _pop_json_filter(kwargs)
    params, post_params = _partition_request_params(
        kwargs, use_cql2=dialect.lists_as_cql2
    )
    if post_params and params.get("filter"):
        # The server ignores a POST body when the URL carries a text filter.
        params["filter"] = _and_in_clauses(post_params, params["filter"])
        post_params = {}

    _ogc_query_params(
        params,
        properties=properties,
        bbox=bbox,
        limit=limit,
        skip_geometry=skip_geometry,
    )

    if "filter_lang" in params:
        params["filter-lang"] = params.pop("filter_lang")

    if post_params or json_filter is not None:
        return _cql2_post_request(
            service_url, content=_cql2_param(post_params, json_filter), params=params
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

    ``base_url`` is required: this package names no API of its own.
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

# Iterable params that ``_get_args`` must not pass through
# ``_normalize_str_iterable`` (``bbox`` is ``list[float]``). These apply to every
# OGC caller; an adapter with extra numeric params names only its extras via
# ``prepare_request_args(..., extra_no_normalize=...)``.
_NO_NORMALIZE_PARAMS = frozenset({"bbox"})


def _normalize_str_iterable(
    value: str | Iterable[str] | None,
    param_name: str,
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
    param_name: str,
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


def _normalize_arg(
    key: str,
    value: Any,
    no_normalize: frozenset[str],
    dates: DateDialect,
) -> Any:
    """Normalize a single request argument value based on its key."""
    if key == "monitoring_location_id":
        return _check_monitoring_location_id(value)
    if key == "properties":
        return _as_str_list(value, key)
    if key in dates.params:
        return read_date_range(value, name=key, durations=dates.durations)
    if (
        key in no_normalize
        and isinstance(value, Iterable)
        and not isinstance(value, str)
    ):
        return value.tolist() if hasattr(value, "tolist") else list(value)
    if isinstance(value, str) or not isinstance(value, Iterable):
        return value
    return _normalize_str_iterable(value, key)


def prepare_request_args(
    local_vars: dict[str, Any],
    exclude: set[str] | None = None,
    *,
    dates: DateDialect,
    extra_no_normalize: frozenset[str] | set[str] = frozenset(),
) -> dict[str, Any]:
    """Build OGC request kwargs from a getter's ``locals()``.

    Internal control keys, caller-supplied exclusions, and ``None`` values
    are omitted. Identifiers and properties are validated, the parameters
    ``dates`` lists are read into a ``DateRange`` (and omitted when every bound
    is open), and other iterables are normalized unless exempted.

    ``extra_no_normalize`` *adds* to the engine's own
    :data:`_NO_NORMALIZE_PARAMS` rather than replacing it, so an adapter names
    only the params it owns and cannot drop the ``bbox`` exemption by
    omitting it.
    """
    no_normalize = _NO_NORMALIZE_PARAMS | frozenset(extra_no_normalize)
    to_exclude = {"collection", "service", "output_id"}
    if exclude:
        to_exclude.update(exclude)

    args: dict[str, Any] = {}
    for k, v in local_vars.items():
        if k in to_exclude:
            continue
        normalized = _normalize_arg(k, v, no_normalize, dates)
        if normalized is not None:
            args[k] = normalized
    return args
