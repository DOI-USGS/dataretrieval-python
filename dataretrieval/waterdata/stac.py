"""Raw access to the public USGS Water Data STAC API.

STAC catalogs, JSON Schemas, Collections, and GeoJSON ItemCollections have
meaningful document-level fields and links that do not share one tabular shape.
These helpers therefore return each response document unchanged, alongside the
same response metadata object used by the package's DataFrame getters.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from typing import Any, Literal
from urllib.parse import quote

import httpx

from dataretrieval._response_metadata import BaseMetadata
from dataretrieval.exceptions import DataRetrievalError
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.transport.http import (
    HTTPX_DEFAULTS,
    default_headers,
    request,
)
from dataretrieval.transport.retry import RetryPolicy, retry_sync
from dataretrieval.waterdata.endpoints import ratings_catalog_url

STAC_SEARCH_METHOD = Literal["GET", "POST"]
STAC_FILTER_LANG = Literal["cql2-json", "cql2-text"]
STACDocument = dict[str, Any]

__all__ = [
    "get_catalog",
    "get_collection",
    "get_collections",
    "get_conformance",
    "get_item",
    "get_items",
    "get_queryables",
    "search",
]


def get_catalog(*, ssl_check: bool = True) -> tuple[STACDocument, BaseMetadata]:
    """Return the STAC landing-page Catalog document.

    Parameters
    ----------
    ssl_check : bool, default True
        Verify the server's SSL certificate.

    Returns
    -------
    dict, BaseMetadata
        The unchanged STAC Catalog and response metadata.
    """
    return _get("/", ssl_check=ssl_check)


def get_conformance(*, ssl_check: bool = True) -> tuple[STACDocument, BaseMetadata]:
    """Return the STAC and OGC conformance declarations.

    Parameters
    ----------
    ssl_check : bool, default True
        Verify the server's SSL certificate.

    Returns
    -------
    dict, BaseMetadata
        The unchanged conformance document and response metadata.
    """
    return _get("/conformance", ssl_check=ssl_check)


def get_collections(
    *,
    bbox: Sequence[int | float] | None = None,
    datetime: str | None = None,
    limit: int | None = None,
    query: Any | None = None,
    sortby: Any | None = None,
    fields: Any | None = None,
    filter: Any | None = None,
    filter_crs: str | None = None,
    filter_lang: STAC_FILTER_LANG | None = None,
    q: str | None = None,
    offset: int | None = None,
    ssl_check: bool = True,
) -> tuple[STACDocument, BaseMetadata]:
    """Return one page of STAC Collections matching collection-search filters.

    The returned document retains its standard ``links`` and pagination counts.
    Structured ``query``, ``sortby``, ``fields``, and ``filter`` values are JSON
    encoded for the GET endpoint; encoded strings pass through unchanged.

    Parameters
    ----------
    bbox : sequence of numbers, optional
        Four- or six-coordinate bounding box intersecting each collection.
    datetime : str, optional
        RFC 3339 instant or interval intersecting each collection's extent.
    limit : int, optional
        Maximum collections returned in this page.
    query : object or str, optional
        STAC Query extension expression or its encoded JSON form.
    sortby : object or str, optional
        STAC sort expression or its encoded wire form.
    fields : object or str, optional
        STAC Fields extension include/exclude expression.
    filter : object or str, optional
        CQL2 JSON object or CQL2 text expression.
    filter_crs : str, optional
        CRS URI used by spatial literals in ``filter``.
    filter_lang : {"cql2-json", "cql2-text"}, optional
        Encoding used by ``filter``.
    q : str, optional
        Free-text collection search.
    offset : int, optional
        Collection offset for the requested page.
    ssl_check : bool, default True
        Verify the server's SSL certificate.

    Returns
    -------
    dict, BaseMetadata
        The unchanged STAC Collections document and response metadata.
    """
    params = _get_params(
        bbox=_csv(bbox),
        datetime=datetime,
        limit=limit,
        query=_encode_get_value(query),
        sortby=_encode_get_value(sortby),
        fields=_encode_get_value(fields),
        filter=_encode_get_value(filter),
        filter_crs=filter_crs,
        filter_lang=filter_lang,
        q=q,
        offset=offset,
    )
    return _get("/collections", params=params, ssl_check=ssl_check)


def get_collection(
    collection_id: str, *, ssl_check: bool = True
) -> tuple[STACDocument, BaseMetadata]:
    """Return one STAC Collection by identifier.

    Parameters
    ----------
    collection_id : str
        Collection identifier from :func:`get_collections`.
    ssl_check : bool, default True
        Verify the server's SSL certificate.

    Returns
    -------
    dict, BaseMetadata
        The unchanged STAC Collection and response metadata.
    """
    return _get(f"/collections/{_path_part(collection_id)}", ssl_check=ssl_check)


def get_items(
    collection_id: str,
    *,
    limit: int | None = None,
    bbox: Sequence[int | float] | None = None,
    datetime: str | None = None,
    query: Any | None = None,
    sortby: Any | None = None,
    fields: Any | None = None,
    filter: Any | None = None,
    filter_crs: str | None = None,
    filter_lang: STAC_FILTER_LANG | None = None,
    page_token: str | None = None,
    ssl_check: bool = True,
) -> tuple[STACDocument, BaseMetadata]:
    """Return one GeoJSON ItemCollection page from a STAC Collection.

    The response's standard ``next`` link is preserved. Supply its continuation
    value as ``page_token`` to retrieve the next page.

    Parameters
    ----------
    collection_id : str
        Collection whose Items should be listed.
    limit : int, optional
        Maximum Items returned in this page.
    bbox : sequence of numbers, optional
        Four- or six-coordinate bounding box intersecting returned Items.
    datetime : str, optional
        RFC 3339 instant or interval intersecting returned Items.
    query : object or str, optional
        STAC Query extension expression or its encoded JSON form.
    sortby : object or str, optional
        STAC sort expression or its encoded wire form.
    fields : object or str, optional
        STAC Fields extension include/exclude expression.
    filter : object or str, optional
        CQL2 JSON object or CQL2 text expression.
    filter_crs : str, optional
        CRS URI used by spatial literals in ``filter``.
    filter_lang : {"cql2-json", "cql2-text"}, optional
        Encoding used by ``filter``.
    page_token : str, optional
        Opaque continuation value sent as the STAC ``token`` query parameter.
    ssl_check : bool, default True
        Verify the server's SSL certificate.

    Returns
    -------
    dict, BaseMetadata
        The unchanged GeoJSON ItemCollection and response metadata.
    """
    params = _get_params(
        limit=limit,
        bbox=_csv(bbox),
        datetime=datetime,
        query=_encode_get_value(query),
        sortby=_encode_get_value(sortby),
        fields=_encode_get_value(fields),
        filter=_encode_get_value(filter),
        filter_crs=filter_crs,
        filter_lang=filter_lang,
        token=page_token,
    )
    path = f"/collections/{_path_part(collection_id)}/items"
    return _get(path, params=params, ssl_check=ssl_check)


def get_item(
    collection_id: str,
    item_id: str,
    *,
    ssl_check: bool = True,
) -> tuple[STACDocument, BaseMetadata]:
    """Return one GeoJSON STAC Item by collection and item identifier.

    Parameters
    ----------
    collection_id : str
        Collection containing the Item.
    item_id : str
        Item identifier.
    ssl_check : bool, default True
        Verify the server's SSL certificate.

    Returns
    -------
    dict, BaseMetadata
        The unchanged GeoJSON STAC Item and response metadata.
    """
    path = f"/collections/{_path_part(collection_id)}/items/{_path_part(item_id)}"
    return _get(path, ssl_check=ssl_check)


def get_queryables(
    collection_id: str | None = None,
    *,
    ssl_check: bool = True,
) -> tuple[STACDocument, BaseMetadata]:
    """Return catalog-wide or collection-specific STAC queryables.

    This is distinct from the tabular
    :func:`dataretrieval.waterdata.get_queryables` helper, which describes a Water
    Data OGC API collection under ``/ogcapi/v0`` rather than this STAC catalog.

    Parameters
    ----------
    collection_id : str, optional
        Collection whose queryables should be returned. Omit it for the
        catalog-wide queryables document.
    ssl_check : bool, default True
        Verify the server's SSL certificate.

    Returns
    -------
    dict, BaseMetadata
        The unchanged JSON Schema queryables document and response metadata.
    """
    path = (
        "/queryables"
        if collection_id is None
        else f"/collections/{_path_part(collection_id)}/queryables"
    )
    return _get(path, ssl_check=ssl_check)


def search(
    *,
    method: STAC_SEARCH_METHOD = "GET",
    collections: str | Iterable[str] | None = None,
    ids: str | Iterable[str] | None = None,
    bbox: Sequence[int | float] | None = None,
    intersects: STACDocument | str | None = None,
    datetime: str | None = None,
    limit: int | None = None,
    conf: STACDocument | None = None,
    query: Any | None = None,
    sortby: Any | None = None,
    fields: Any | None = None,
    filter: Any | None = None,
    filter_crs: str | None = None,
    filter_lang: STAC_FILTER_LANG | None = None,
    page_token: str | None = None,
    ssl_check: bool = True,
) -> tuple[STACDocument, BaseMetadata]:
    """Search STAC Items using the advertised GET or POST representation.

    GET accepts wire-ready strings or Python structures, which are comma-joined
    or JSON encoded as required. POST sends native arrays and objects. The
    returned GeoJSON ItemCollection remains unchanged, including its links.

    Parameters
    ----------
    method : {"GET", "POST"}, default "GET"
        STAC search representation to use.
    collections : str or iterable of str, optional
        Collection identifiers to search.
    ids : str or iterable of str, optional
        Item identifiers to return.
    bbox : sequence of numbers, optional
        Four- or six-coordinate bounding box intersecting returned Items.
    intersects : dict or str, optional
        GeoJSON geometry intersecting returned Items. Mutually exclusive with
        ``bbox`` according to the service contract.
    datetime : str, optional
        RFC 3339 instant or interval intersecting returned Items.
    limit : int, optional
        Maximum Items returned in this page, capped upstream at 10,000.
    conf : dict, optional
        POST-only server configuration object.
    query : object or str, optional
        STAC Query extension expression. Structured values are native JSON for
        POST and encoded for GET.
    sortby : object or str, optional
        STAC sort expression.
    fields : object or str, optional
        STAC Fields extension include/exclude expression.
    filter : object or str, optional
        CQL2 JSON object or CQL2 text expression.
    filter_crs : str, optional
        CRS URI used by spatial literals in ``filter``.
    filter_lang : {"cql2-json", "cql2-text"}, optional
        Encoding used by ``filter``.
    page_token : str, optional
        Opaque continuation value sent as the STAC ``token`` field.
    ssl_check : bool, default True
        Verify the server's SSL certificate.

    Returns
    -------
    dict, BaseMetadata
        The unchanged GeoJSON ItemCollection and response metadata.

    Raises
    ------
    ValueError
        If ``method`` is not GET or POST, or ``conf`` is supplied for GET.
    """
    normalized_method = method.upper()
    if normalized_method not in ("GET", "POST"):
        raise ValueError(f"method must be GET or POST (got {method!r}).")
    if normalized_method == "GET":
        if conf is not None:
            raise ValueError("conf is supported only by the POST STAC search.")
        params = _get_params(
            collections=_csv(_strings(collections)),
            ids=_csv(_strings(ids)),
            bbox=_csv(bbox),
            intersects=_encode_get_value(intersects),
            datetime=datetime,
            limit=limit,
            query=_encode_get_value(query),
            sortby=_encode_get_value(sortby),
            fields=_encode_get_value(fields),
            filter=_encode_get_value(filter),
            filter_crs=filter_crs,
            filter_lang=filter_lang,
            token=page_token,
        )
        return _get("/search", params=params, ssl_check=ssl_check)

    body = _wire_names(
        _without_none(
            collections=_strings(collections),
            ids=_strings(ids),
            bbox=None if bbox is None else list(bbox),
            intersects=intersects,
            datetime=datetime,
            limit=limit,
            conf=conf,
            query=query,
            sortby=sortby,
            fields=fields,
            filter=filter,
            filter_crs=filter_crs,
            filter_lang=filter_lang,
            token=page_token,
        )
    )
    return _request_document("POST", "/search", json_body=body, ssl_check=ssl_check)


def _get(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    ssl_check: bool,
) -> tuple[STACDocument, BaseMetadata]:
    return _request_document("GET", path, params=params, ssl_check=ssl_check)


def _request_document(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: STACDocument | None = None,
    ssl_check: bool,
) -> tuple[STACDocument, BaseMetadata]:
    url = f"{ratings_catalog_url()}{path}"

    def attempt() -> httpx.Response:
        response = request(
            method,
            url,
            params=params,
            json=json_body,
            headers=default_headers(url),
            verify=ssl_check,
            **HTTPX_DEFAULTS,
        )
        _raise_for_non_200(response)
        return response

    response = retry_sync(attempt, RetryPolicy.from_configuration(adapter="waterdata"))
    try:
        document = response.json()
    except ValueError as exc:
        raise DataRetrievalError(
            f"The STAC service returned invalid JSON (URL: {response.url})."
        ) from exc
    if not isinstance(document, dict):
        raise DataRetrievalError(
            "The STAC service returned a JSON value instead of a document "
            f"(URL: {response.url})."
        )
    return document, BaseMetadata(response)


def _strings(value: str | Iterable[str] | None) -> list[str] | None:
    if value is None:
        return None
    return [value] if isinstance(value, str) else list(value)


def _csv(value: Sequence[Any] | None) -> str | None:
    return None if value is None else ",".join(map(str, value))


def _encode_get_value(value: Any | None) -> Any | None:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Sequence) and all(isinstance(part, str) for part in value):
        return ",".join(value)
    return json.dumps(value, separators=(",", ":"))


def _without_none(**values: Any) -> dict[str, Any]:
    return {name: value for name, value in values.items() if value is not None}


def _wire_names(values: dict[str, Any]) -> dict[str, Any]:
    return {name.replace("_", "-"): value for name, value in values.items()}


def _get_params(**values: Any) -> dict[str, Any]:
    return _wire_names(_without_none(**values))


def _path_part(value: str) -> str:
    return quote(value, safe="")
