import sys
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from dataretrieval.waterdata.utils import (
    _CQL_FILTER_CHUNK_LEN,
    _chunk_cql_or,
    _construct_api_requests,
    _get_args,
    _split_top_level_or,
    _walk_pages,
)

OGC_CONTINUOUS_URL = (
    "https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items"
)


def _query_params(prepared_request):
    return parse_qs(urlsplit(prepared_request.url).query)


def test_get_args_basic():
    local_vars = {
        "monitoring_location_id": "123",
        "service": "daily",
        "output_id": "daily_id",
        "none_val": None,
        "other": "val",
    }
    result = _get_args(local_vars)
    assert result == {"monitoring_location_id": "123", "other": "val"}


def test_get_args_with_exclude():
    local_vars = {
        "monitoring_location_id": "123",
        "service": "daily",
        "output_id": "daily_id",
        "to_exclude": "secret",
        "other": "val",
    }
    result = _get_args(local_vars, exclude={"to_exclude"})
    assert result == {"monitoring_location_id": "123", "other": "val"}


def test_get_args_empty():
    assert _get_args({}) == {}


def test_walk_pages_multiple_mocked():
    # Setup mock responses
    resp1 = mock.MagicMock()
    resp1.json.return_value = {
        "numberReturned": 1,
        "features": [{"id": "1", "properties": {"val": "a"}}],
        "links": [{"rel": "next", "href": "https://example.com/page2"}],
    }
    # Mock headers and links
    resp1.headers = {}
    resp1.links = {"next": {"url": "https://example.com/page2"}}
    resp1.status_code = 200

    resp2 = mock.MagicMock()
    resp2.json.return_value = {
        "numberReturned": 1,
        "features": [{"id": "2", "properties": {"val": "b"}}],
        "links": [],
    }
    resp2.headers = {}
    resp2.links = {}
    resp2.status_code = 200

    # Mock client (Session)
    mock_client = mock.MagicMock(spec=requests.Session)
    # First call to send() returns resp1, then call to request() in loop returns resp2
    mock_client.send.return_value = resp1
    mock_client.request.return_value = resp2

    # Mock request (PreparedRequest)
    mock_req = mock.MagicMock(spec=requests.PreparedRequest)
    mock_req.method = "GET"
    mock_req.headers = {}
    mock_req.url = "https://example.com/page1"

    # Call _walk_pages
    df, final_resp = _walk_pages(geopd=False, req=mock_req, client=mock_client)

    assert len(df) == 2
    assert list(df["val"]) == ["a", "b"]
    assert list(df["id"]) == ["1", "2"]
    assert mock_client.send.called
    assert mock_client.request.called
    assert mock_client.request.call_args[0][1] == "https://example.com/page2"


def test_construct_filter_passthrough():
    """`filter` is forwarded verbatim as a query parameter."""
    expr = (
        "(time >= '2023-01-06T16:00:00Z' AND time <= '2023-01-06T18:00:00Z') "
        "OR (time >= '2023-01-10T18:00:00Z' AND time <= '2023-01-10T20:00:00Z')"
    )
    req = _construct_api_requests(
        service="continuous",
        monitoring_location_id="USGS-07374525",
        parameter_code="72255",
        filter=expr,
    )
    qs = _query_params(req)
    assert qs["filter"] == [expr]


def test_construct_filter_lang_hyphenated():
    """The Python kwarg `filter_lang` is sent as URL key `filter-lang`."""
    req = _construct_api_requests(
        service="continuous",
        monitoring_location_id="USGS-07374525",
        parameter_code="72255",
        filter="time >= '2023-01-01T00:00:00Z'",
        filter_lang="cql-text",
    )
    qs = _query_params(req)
    assert qs["filter-lang"] == ["cql-text"]
    assert "filter_lang" not in qs


def test_split_top_level_or_simple():
    parts = _split_top_level_or("A OR B OR C")
    assert parts == ["A", "B", "C"]


def test_split_top_level_or_case_insensitive():
    assert _split_top_level_or("A or B Or C") == ["A", "B", "C"]


def test_split_top_level_or_respects_parens():
    assert _split_top_level_or("(A OR B) OR (C OR D)") == ["(A OR B)", "(C OR D)"]


def test_split_top_level_or_respects_quotes():
    expr = "name = 'foo OR bar' OR id = 1"
    assert _split_top_level_or(expr) == ["name = 'foo OR bar'", "id = 1"]


def test_split_top_level_or_single_clause():
    assert _split_top_level_or("time >= '2023-01-01T00:00:00Z'") == [
        "time >= '2023-01-01T00:00:00Z'"
    ]


def test_chunk_cql_or_short_passthrough():
    expr = "time >= '2023-01-01T00:00:00Z'"
    assert _chunk_cql_or(expr, max_len=1000) == [expr]


def test_chunk_cql_or_splits_into_multiple():
    clause = "(time >= '2023-01-01T00:00:00Z' AND time <= '2023-01-01T00:30:00Z')"
    expr = " OR ".join([clause] * 200)
    chunks = _chunk_cql_or(expr, max_len=1000)
    # each chunk must be under the budget
    assert all(len(c) <= 1000 for c in chunks)
    # rejoined chunks must cover every clause
    rejoined_clauses = sum(len(c.split(" OR ")) for c in chunks)
    assert rejoined_clauses == 200
    # and must be a valid OR chain (each chunk is itself a top-level OR of clauses)
    assert len(chunks) > 1


def test_chunk_cql_or_unsplittable_returns_input():
    big = "value > 0 AND " + ("A " * 4000)
    assert _chunk_cql_or(big, max_len=1000) == [big]


def test_chunk_cql_or_single_clause_over_budget_returns_input():
    huge_clause = "(value > " + "9" * 6000 + ")"
    expr = f"{huge_clause} OR (value > 0)"
    assert _chunk_cql_or(expr, max_len=1000) == [expr]


@pytest.mark.parametrize(
    "service",
    [
        "daily",
        "continuous",
        "monitoring-locations",
        "time-series-metadata",
        "latest-continuous",
        "latest-daily",
        "field-measurements",
        "channel-measurements",
    ],
)
def test_construct_filter_on_all_ogc_services(service):
    """Filter passthrough works uniformly for every OGC collection endpoint."""
    req = _construct_api_requests(
        service=service,
        filter="value > 0",
        filter_lang="cql-text",
    )
    qs = _query_params(req)
    assert qs["filter"] == ["value > 0"]
    assert qs["filter-lang"] == ["cql-text"]


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="get_continuous requires py>=3.10 (see tests/waterdata_test.py)",
)
def test_long_filter_fans_out_into_multiple_requests(requests_mock):
    """An oversized top-level OR filter triggers multiple HTTP requests
    whose results are concatenated."""
    from dataretrieval.waterdata import get_continuous

    clause = (
        "(time >= '2023-01-{day:02d}T00:00:00Z' "
        "AND time <= '2023-01-{day:02d}T00:30:00Z')"
    )
    expr = " OR ".join(clause.format(day=(i % 28) + 1) for i in range(300))
    assert len(expr) > _CQL_FILTER_CHUNK_LEN

    call_count = {"n": 0}

    def respond(request, context):
        context.status_code = 200
        call_count["n"] += 1
        return {
            "type": "FeatureCollection",
            "numberReturned": 1,
            "features": [
                {
                    "type": "Feature",
                    "id": f"chunk-{call_count['n']}",
                    "geometry": None,
                    "properties": {
                        "continuous_id": f"chunk-{call_count['n']}",
                        "value": call_count["n"],
                    },
                }
            ],
            "links": [],
        }

    requests_mock.get(OGC_CONTINUOUS_URL, json=respond)

    df, _ = get_continuous(
        monitoring_location_id="USGS-07374525",
        parameter_code="72255",
        filter=expr,
        filter_lang="cql-text",
    )

    # Mirror the library's splitter so the test doesn't hardcode a chunk count.
    expected_chunks = _chunk_cql_or(expr)
    assert len(expected_chunks) > 1
    assert call_count["n"] == len(expected_chunks)
    assert len(df) == len(expected_chunks)
    for req in requests_mock.request_history:
        filter_qs = parse_qs(urlsplit(req.url).query).get("filter", [""])[0]
        assert len(filter_qs) <= _CQL_FILTER_CHUNK_LEN
