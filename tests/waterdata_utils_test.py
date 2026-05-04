from unittest import mock

import pandas as pd
import requests

from dataretrieval.waterdata.utils import (
    _arrange_cols,
    _get_args,
    _handle_stats_nesting,
    _walk_pages,
)


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


def test_handle_stats_nesting_tolerates_missing_drop_columns():
    """If the upstream stats response shape ever changes such that one of
    the columns we try to drop ("type", "properties.data") is absent, the
    function should still return a DataFrame instead of raising KeyError.
    """
    body = {
        "next": None,
        "features": [
            {
                "properties": {
                    "monitoring_location_id": "USGS-12345",
                    "data": [
                        {
                            "parameter_code": "00060",
                            "unit_of_measure": "ft^3/s",
                            "parent_time_series_id": "ts-1",
                            "values": [{"statistic_id": "mean", "value": 10.0}],
                        }
                    ],
                },
            }
        ],
    }

    df = _handle_stats_nesting(body, geopd=False)

    assert len(df) == 1
    assert df["monitoring_location_id"].iloc[0] == "USGS-12345"


# --- _arrange_cols ----------------------------------------------------------


def test_arrange_cols_does_not_mutate_caller_properties():
    """`_arrange_cols` must not mutate the caller's `properties` list.

    Regression: previously the function did
    ``properties.append("geometry")`` and
    ``properties[properties.index("id")] = output_id`` in place, so the
    caller's list grew and was rewritten across successive calls.
    """
    df = pd.DataFrame(
        {
            "id": ["a", "b"],
            "value": [1.0, 2.0],
            "geometry": ["p1", "p2"],
        }
    )
    properties = ["id", "value"]
    snapshot = list(properties)

    _arrange_cols(df, properties, output_id="daily_id")
    _arrange_cols(df, properties, output_id="daily_id")

    assert properties == snapshot, (
        f"caller's properties list was mutated: {properties!r} != {snapshot!r}"
    )


def test_arrange_cols_swaps_id_in_returned_columns():
    """`'id'` in `properties` should still resolve to the output_id column."""
    df = pd.DataFrame({"id": ["a"], "value": [1.0]})
    result = _arrange_cols(df, ["id", "value"], output_id="daily_id")
    assert "daily_id" in result.columns
    assert "id" not in result.columns


def test_arrange_cols_keeps_geometry_when_present():
    """Geometry must come along even if the caller didn't list it."""
    df = pd.DataFrame({"id": ["a"], "value": [1.0], "geometry": ["p1"]})
    result = _arrange_cols(df, ["value"], output_id="daily_id")
    assert "geometry" in result.columns
