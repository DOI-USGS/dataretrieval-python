from unittest import mock

import pandas as pd
import pytest
import requests

from dataretrieval.waterdata.utils import (
    GEOPANDAS,
    _get_args,
    _walk_pages,
    get_stats_data,
)

if GEOPANDAS:
    import geopandas as gpd


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


def test_walk_pages_truncates_on_non_200_continuation():
    """`_walk_pages` must truncate (not silently extend) on a non-200 page.

    Regression: previously any non-200 page was appended (with whatever
    body it had) and pagination quietly stopped because `_get_resp_data`
    or `_next_req_url` raised inside the bare except. Now the explicit
    status check raises inside the loop, and the existing log-and-truncate
    handler converts that to a clean partial result. Page 1 is still
    returned; page 2 is dropped.
    """
    resp1 = mock.MagicMock()
    resp1.json.return_value = {
        "numberReturned": 1,
        "features": [{"id": "1", "properties": {"val": "a"}}],
        "links": [],
    }
    resp1.headers = {}
    resp1.links = {"next": {"url": "https://example.com/page2"}}
    resp1.status_code = 200

    resp2 = mock.MagicMock()
    resp2.status_code = 500
    resp2.text = "<html>error</html>"

    mock_client = mock.MagicMock(spec=requests.Session)
    mock_client.send.return_value = resp1
    mock_client.request.return_value = resp2

    mock_req = mock.MagicMock(spec=requests.PreparedRequest)
    mock_req.method = "GET"
    mock_req.headers = {}
    mock_req.url = "https://example.com/page1"

    df, _ = _walk_pages(geopd=False, req=mock_req, client=mock_client)

    # Page 1 still returned; page 2 logged-and-stopped after the explicit
    # status check raised. The contract here is "log + truncate", same
    # as the pre-fix bare-except behavior, but now the raise inside the
    # loop is intentional rather than incidental.
    assert len(df) == 1


# --- get_stats_data pagination ----------------------------------------------


def _stats_feature():
    """Build a single feature shaped to satisfy ``_handle_stats_nesting``."""
    return {
        "type": "Feature",
        "id": "USGS-1",
        "geometry": None,
        "properties": {
            "monitoring_location_id": "USGS-1",
            "data": [
                {
                    "parameter_code": "00060",
                    "unit_of_measure": "ft^3/s",
                    "parent_time_series_id": "abc",
                    "values": [{"value": 1.0}],
                }
            ],
        },
    }


def _stats_body(features, next_token=None):
    body = {
        "type": "FeatureCollection",
        "features": features,
        "numberReturned": len(features),
    }
    if next_token is not None:
        body["next"] = next_token
    return body


def test_get_stats_data_handles_missing_next_key():
    """A response without a ``next`` key must not raise KeyError.

    Regression: ``body["next"]`` raised when the key was absent. Now
    uses ``body.get("next")`` so a missing key means "no more pages".
    """
    resp = mock.MagicMock()
    resp.status_code = 200
    resp.json.return_value = _stats_body([_stats_feature()])
    # No "next" key at all.

    client = mock.MagicMock(spec=requests.Session)
    client.send.return_value = resp

    df, _ = get_stats_data(
        args={}, service="observationNormals", expand_percentiles=False, client=client
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 1


def test_get_stats_data_truncates_on_non_200_continuation():
    """A 4xx/5xx on a continuation page must log and stop, not crash."""
    resp1 = mock.MagicMock()
    resp1.status_code = 200
    resp1.json.return_value = _stats_body([_stats_feature()], next_token="abc")

    resp2 = mock.MagicMock()
    resp2.status_code = 503
    resp2.text = "Service Unavailable"
    resp2.url = "https://example.com/page2"

    client = mock.MagicMock(spec=requests.Session)
    client.send.return_value = resp1
    client.request.return_value = resp2

    df, _ = get_stats_data(
        args={}, service="observationNormals", expand_percentiles=False, client=client
    )
    # Page 1 still surfaces; page 2 was caught by the in-loop status check.
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 1


def _stats_feature_with_geometry(loc_id, lon, lat):
    """Stats feature with a real point geometry."""
    feat = _stats_feature()
    feat["id"] = loc_id
    feat["geometry"] = {"type": "Point", "coordinates": [lon, lat]}
    feat["properties"]["monitoring_location_id"] = loc_id
    return feat


@pytest.mark.skipif(not GEOPANDAS, reason="geopandas not installed")
def test_get_stats_data_preserves_geometry_across_pages():
    """Pages 2..N must use ``geopd=GEOPANDAS`` so a multi-page response
    stays a GeoDataFrame and doesn't lose geometry/CRS at the page-1 boundary.

    Regression: previously pages 2..N hard-coded ``geopd=False``, producing
    plain DataFrames. ``pd.concat`` then silently downgraded the result to
    a plain DataFrame.
    """
    resp1 = mock.MagicMock()
    resp1.status_code = 200
    resp1.json.return_value = _stats_body(
        [_stats_feature_with_geometry("USGS-1", -89.0, 43.0)],
        next_token="abc",
    )

    resp2 = mock.MagicMock()
    resp2.status_code = 200
    resp2.json.return_value = _stats_body(
        [_stats_feature_with_geometry("USGS-2", -90.0, 44.0)]
    )

    client = mock.MagicMock(spec=requests.Session)
    client.send.return_value = resp1
    client.request.return_value = resp2

    df, _ = get_stats_data(
        args={}, service="observationNormals", expand_percentiles=False, client=client
    )
    assert isinstance(df, gpd.GeoDataFrame)
    assert len(df) == 2
    assert df.geometry.notna().all()
