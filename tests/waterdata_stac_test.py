"""Offline coverage for the public USGS Water Data STAC API client."""

from __future__ import annotations

import json
import re

import pytest

import dataretrieval
from dataretrieval import Configuration, HTTPError, ServiceUnavailable, waterdata
from dataretrieval.waterdata import WaterdataConfiguration

_STAC = "https://api.waterdata.usgs.gov/stac/v0"
_STAC_EXPORTS = [
    "get_catalog",
    "get_collection",
    "get_collections",
    "get_conformance",
    "get_item",
    "get_items",
    "get_queryables",
    "search",
]
_OLD_FLAT_EXPORTS = [
    "get_stac_catalog",
    "get_stac_collection",
    "get_stac_collections",
    "get_stac_conformance",
    "get_stac_item",
    "get_stac_items",
    "get_stac_queryables",
    "search_stac",
]


def test_stac_api_is_nested_without_flat_aliases():
    assert waterdata.stac.__all__ == _STAC_EXPORTS
    assert all(hasattr(waterdata.stac, name) for name in _STAC_EXPORTS)
    assert all(not hasattr(waterdata, name) for name in _OLD_FLAT_EXPORTS)


def test_catalog_conformance_and_queryables_resources(httpx_mock):
    documents = [
        {"type": "Catalog", "id": "usgs-water-data-stac"},
        {"conformsTo": ["https://api.stacspec.org/v1.0.0/core"]},
        {"$schema": "https://json-schema.org/draft/2019-09/schema"},
        {"properties": {"file_type": {"type": "string"}}},
    ]
    urls = [
        f"{_STAC}/",
        f"{_STAC}/conformance",
        f"{_STAC}/queryables",
        f"{_STAC}/collections/ratings/queryables",
    ]
    for url, document in zip(urls, documents, strict=True):
        httpx_mock.add_response(method="GET", url=url, json=document)

    calls = [
        waterdata.stac.get_catalog(),
        waterdata.stac.get_conformance(),
        waterdata.stac.get_queryables(),
        waterdata.stac.get_queryables("ratings"),
    ]

    for (document, metadata), expected, url in zip(calls, documents, urls, strict=True):
        assert document == expected
        assert metadata.url == url


def test_collection_and_item_resource_paths_escape_identifiers(httpx_mock):
    collection = {"type": "Collection", "id": "rating curves"}
    item = {"type": "Feature", "id": "USGS/01234567.exsa.rdb"}
    httpx_mock.add_response(
        method="GET",
        url=f"{_STAC}/collections/rating%20curves",
        json=collection,
    )
    httpx_mock.add_response(
        method="GET",
        url=(f"{_STAC}/collections/rating%20curves/items/USGS%2F01234567.exsa.rdb"),
        json=item,
    )

    assert waterdata.stac.get_collection("rating curves")[0] == collection
    assert waterdata.stac.get_item("rating curves", "USGS/01234567.exsa.rdb")[0] == item


def test_get_collections_forwards_every_advertised_parameter(httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url=re.compile(rf"^{re.escape(_STAC)}/collections(?:\?.*)?$"),
        json={},
    )

    waterdata.stac.get_collections(
        bbox=[-95, 40, -92, 42],
        datetime="2026-01-01/..",
        limit=25,
        query={"title": {"eq": "ratings"}},
        sortby=[{"field": "id", "direction": "asc"}],
        fields={"include": ["id"], "exclude": ["links"]},
        filter={"op": "=", "args": [{"property": "id"}, "ratings"]},
        filter_crs="http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        filter_lang="cql2-json",
        q="rating curves",
        offset=10,
    )

    params = httpx_mock.get_requests()[0].url.params
    assert params["bbox"] == "-95,40,-92,42"
    assert params["datetime"] == "2026-01-01/.."
    assert params["limit"] == "25"
    assert json.loads(params["query"]) == {"title": {"eq": "ratings"}}
    assert json.loads(params["sortby"]) == [{"field": "id", "direction": "asc"}]
    assert json.loads(params["fields"]) == {
        "include": ["id"],
        "exclude": ["links"],
    }
    assert json.loads(params["filter"]) == {
        "op": "=",
        "args": [{"property": "id"}, "ratings"],
    }
    assert params["filter-crs"].endswith("CRS84")
    assert params["filter-lang"] == "cql2-json"
    assert params["q"] == "rating curves"
    assert params["offset"] == "10"


def test_get_collection_items_forwards_every_advertised_parameter(httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url=re.compile(rf"^{re.escape(_STAC)}/collections/ratings/items(?:\?.*)?$"),
        json={"features": []},
    )

    waterdata.stac.get_items(
        "ratings",
        limit=50,
        bbox=[-95, 40, -92, 42],
        datetime="2026-01-01/..",
        query='{"file_type":{"eq":"exsa"}}',
        sortby="-properties.updated",
        fields="+id,-geometry",
        filter="file_type = 'exsa'",
        filter_crs="http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        filter_lang="cql2-text",
        page_token="next-page",
    )

    params = httpx_mock.get_requests()[0].url.params
    assert dict(params) == {
        "limit": "50",
        "bbox": "-95,40,-92,42",
        "datetime": "2026-01-01/..",
        "query": '{"file_type":{"eq":"exsa"}}',
        "sortby": "-properties.updated",
        "fields": "+id,-geometry",
        "filter": "file_type = 'exsa'",
        "filter-crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        "filter-lang": "cql2-text",
        "token": "next-page",
    }


def test_get_search_serializes_get_parameters(httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url=re.compile(rf"^{re.escape(_STAC)}/search(?:\?.*)?$"),
        json={"features": []},
    )

    document, _ = waterdata.stac.search(
        method="GET",
        collections=["ratings", "other"],
        ids=["one", "two"],
        bbox=[-95, 40, -92, 42],
        intersects={"type": "Point", "coordinates": [-93, 41]},
        datetime="2026-01-01/..",
        limit=100,
        query={"file_type": {"eq": "exsa"}},
        sortby=["-properties.updated"],
        fields=["+id", "-geometry"],
        filter="file_type = 'exsa'",
        filter_crs="http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        filter_lang="cql2-text",
        page_token="next-page",
    )

    assert document == {"features": []}
    params = httpx_mock.get_requests()[0].url.params
    assert params["collections"] == "ratings,other"
    assert params["ids"] == "one,two"
    assert params["bbox"] == "-95,40,-92,42"
    assert json.loads(params["intersects"]) == {
        "type": "Point",
        "coordinates": [-93, 41],
    }
    assert params["datetime"] == "2026-01-01/.."
    assert params["limit"] == "100"
    assert json.loads(params["query"]) == {"file_type": {"eq": "exsa"}}
    assert params["sortby"] == "-properties.updated"
    assert params["fields"] == "+id,-geometry"
    assert params["filter"] == "file_type = 'exsa'"
    assert params["filter-crs"].endswith("CRS84")
    assert params["filter-lang"] == "cql2-text"
    assert params["token"] == "next-page"


def test_post_search_sends_native_json_parameters(httpx_mock):
    httpx_mock.add_response(method="POST", url=f"{_STAC}/search", json={"features": []})
    cql = {"op": "=", "args": [{"property": "file_type"}, "exsa"]}

    waterdata.stac.search(
        method="POST",
        collections="ratings",
        ids="USGS-01104475.exsa.rdb",
        bbox=[-95, 40, -92, 42],
        intersects={"type": "Point", "coordinates": [-93, 41]},
        datetime="2026-01-01/..",
        limit=100,
        conf={"invalid": "match"},
        query={"file_type": {"eq": "exsa"}},
        sortby=[{"field": "properties.updated", "direction": "desc"}],
        fields={"include": ["id"], "exclude": ["geometry"]},
        filter=cql,
        filter_crs="http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        filter_lang="cql2-json",
        page_token="next-page",
    )

    request = httpx_mock.get_requests()[0]
    assert request.headers["content-type"].startswith("application/json")
    assert json.loads(request.content) == {
        "collections": ["ratings"],
        "ids": ["USGS-01104475.exsa.rdb"],
        "bbox": [-95, 40, -92, 42],
        "intersects": {"type": "Point", "coordinates": [-93, 41]},
        "datetime": "2026-01-01/..",
        "limit": 100,
        "conf": {"invalid": "match"},
        "query": {"file_type": {"eq": "exsa"}},
        "sortby": [{"field": "properties.updated", "direction": "desc"}],
        "fields": {"include": ["id"], "exclude": ["geometry"]},
        "filter": cql,
        "filter-crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        "filter-lang": "cql2-json",
        "token": "next-page",
    }


def test_search_rejects_invalid_method_and_post_only_conf():
    with pytest.raises(ValueError, match="GET or POST"):
        waterdata.stac.search(method="PUT")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="conf.*POST"):
        waterdata.stac.search(method="GET", conf={})


def test_stac_uses_typed_errors_and_waterdata_retries(httpx_mock, monkeypatch):
    monkeypatch.setenv("API_USGS_RETRIES", "1")
    monkeypatch.setattr("dataretrieval.transport.retry._RETRY_BASE_BACKOFF", 0)
    httpx_mock.add_response(
        method="GET", url=f"{_STAC}/collections/missing", status_code=503
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{_STAC}/collections/missing",
        json={"type": "Collection", "id": "recovered"},
    )

    assert waterdata.stac.get_collection("missing")[0]["id"] == "recovered"

    httpx_mock.add_response(
        method="GET", url=f"{_STAC}/collections/absent", status_code=404
    )
    with pytest.raises(HTTPError) as excinfo:
        waterdata.stac.get_collection("absent")
    assert not isinstance(excinfo.value, ServiceUnavailable)


def test_stac_honors_redirect_and_scopes_api_key(httpx_mock, monkeypatch):
    mirror = "https://mirror.example/waterdata"
    monkeypatch.setenv("API_USGS_PAT", "not-a-secret")
    httpx_mock.add_response(method="GET", url=f"{mirror}/stac/v0/", json={})
    httpx_mock.add_response(method="GET", url=f"{_STAC}/", json={})

    with dataretrieval.configure(WaterdataConfiguration(base_url=mirror)):
        waterdata.stac.get_catalog()
    with dataretrieval.configure(Configuration(api_key="not-a-secret")):
        waterdata.stac.get_catalog()

    redirected, direct = httpx_mock.get_requests()
    assert "X-Api-Key" not in redirected.headers
    assert direct.headers["X-Api-Key"] == "not-a-secret"
