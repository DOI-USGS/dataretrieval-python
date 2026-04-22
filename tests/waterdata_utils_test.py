import warnings
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from dataretrieval.waterdata.utils import (
    _construct_api_requests,
    _get_args,
    _walk_pages,
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
    # The underscore form must NOT appear in the URL
    assert "filter_lang" not in qs


def test_construct_long_filter_emits_warning():
    """CQL filter strings longer than 5000 characters warn about URI limits."""
    long_clause = "(time >= '2023-01-01T00:00:00Z' AND time <= '2023-01-01T00:30:00Z')"
    # Build a filter string that comfortably exceeds the threshold
    big = " OR ".join([long_clause] * 100)
    assert len(big) > 5000
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _construct_api_requests(
            service="continuous",
            monitoring_location_id="USGS-07374525",
            filter=big,
        )
    matching = [
        w
        for w in caught
        if issubclass(w.category, UserWarning) and "414" in str(w.message)
    ]
    seen = [str(w.message) for w in caught]
    assert matching, f"expected a URI-length warning, got: {seen}"


def test_construct_short_filter_does_not_warn():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _construct_api_requests(
            service="continuous",
            monitoring_location_id="USGS-07374525",
            filter="time >= '2023-01-01T00:00:00Z'",
        )
    assert not [
        w
        for w in caught
        if issubclass(w.category, UserWarning) and "414" in str(w.message)
    ]


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
