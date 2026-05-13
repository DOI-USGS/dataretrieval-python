import logging
from unittest import mock

import pandas as pd
import requests

from dataretrieval.waterdata.utils import (
    _arrange_cols,
    _format_api_dates,
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


def _resp_ok(features):
    """Build a 200-OK mock response carrying the given features list."""
    resp = mock.MagicMock()
    resp.json.return_value = {
        "numberReturned": len(features),
        "features": features,
        "links": [{"rel": "next", "href": "https://example.com/page2"}]
        if features
        else [],
    }
    resp.headers = {}
    resp.links = {"next": {"url": "https://example.com/page2"}} if features else {}
    resp.status_code = 200
    resp.url = "https://example.com/page1"
    return resp


def _walk_pages_with_failure(failure_resp_or_exc):
    """Run _walk_pages where page 1 succeeds and page 2 fails as given."""
    resp1 = _resp_ok([{"id": "1", "properties": {"val": "a"}}])

    mock_client = mock.MagicMock(spec=requests.Session)
    mock_client.send.return_value = resp1
    if isinstance(failure_resp_or_exc, BaseException):
        mock_client.request.side_effect = failure_resp_or_exc
    else:
        mock_client.request.return_value = failure_resp_or_exc

    mock_req = mock.MagicMock(spec=requests.PreparedRequest)
    mock_req.method = "GET"
    mock_req.headers = {}
    mock_req.url = "https://example.com/page1"

    return _walk_pages(geopd=False, req=mock_req, client=mock_client)


def test_walk_pages_logs_actual_exception_when_request_raises(caplog):
    """Exception from client.request() must be logged with its actual message."""
    caplog.set_level(logging.ERROR, logger="dataretrieval.waterdata.utils")

    df, _ = _walk_pages_with_failure(requests.ConnectionError("boom"))

    # First page's data is preserved (best-effort behavior).
    assert list(df["val"]) == ["a"]
    # Logged error mentions the actual ConnectionError, not a stale page body.
    error_messages = [
        r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR
    ]
    assert any("boom" in m for m in error_messages), error_messages


def test_walk_pages_surfaces_5xx_mid_pagination(caplog):
    """A non-200 mid-pagination response must be logged, not silently swallowed."""
    caplog.set_level(logging.ERROR, logger="dataretrieval.waterdata.utils")

    page2_500 = mock.MagicMock()
    page2_500.status_code = 503
    page2_500.json.return_value = {
        "code": "ServiceUnavailable",
        "description": "upstream timeout",
    }
    page2_500.url = "https://example.com/page2"

    df, _ = _walk_pages_with_failure(page2_500)

    assert list(df["val"]) == ["a"]
    error_messages = [
        r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR
    ]
    # The 5xx is now visible in the error log (it would previously have
    # been silently swallowed because _get_resp_data returned an empty
    # frame and the loop stopped quietly).
    assert any("503" in m or "ServiceUnavailable" in m for m in error_messages), (
        error_messages
    )


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


# --- _format_api_dates -------------------------------------------------------


def test_format_api_dates_iso8601_with_z():
    """ISO 8601 datetimes with a 'Z' suffix must be parsed, not dropped to None."""
    assert _format_api_dates("2018-02-12T23:20:50Z") == "2018-02-12T23:20:50Z"


def test_format_api_dates_iso8601_with_fractional_seconds():
    assert _format_api_dates("2018-02-12T23:20:50.123Z") == "2018-02-12T23:20:50Z"


def test_format_api_dates_iso8601_with_offset():
    """Numeric offsets must be converted to UTC."""
    assert _format_api_dates("2018-02-12T19:20:50-04:00") == "2018-02-12T23:20:50Z"


def test_format_api_dates_iso8601_pair():
    """A list of two ISO 8601 datetimes must be parsed into a UTC interval."""
    result = _format_api_dates(["2018-02-12T23:20:50Z", "2018-03-18T12:31:12Z"])
    assert result == "2018-02-12T23:20:50Z/2018-03-18T12:31:12Z"


def test_format_api_dates_passthrough_interval():
    assert _format_api_dates("2018-02-12T00:00:00Z/..") == "2018-02-12T00:00:00Z/.."


def test_format_api_dates_passthrough_duration():
    assert _format_api_dates("P7D") == "P7D"


def test_format_api_dates_passthrough_time_only_duration():
    """ISO 8601 time-only durations (PT...) are passed through unchanged."""
    assert _format_api_dates("PT36H") == "PT36H"


def test_format_api_dates_word_with_p_is_not_a_duration():
    """Strings containing the letter 'p' must not be misclassified as durations."""
    assert _format_api_dates("Apr") is None


def test_format_api_dates_date_only():
    assert _format_api_dates("2024-01-01", date=True) == "2024-01-01"


def test_format_api_dates_date_only_pair():
    assert (
        _format_api_dates(["2024-01-01", "2024-02-01"], date=True)
        == "2024-01-01/2024-02-01"
    )


def test_format_api_dates_space_separated_still_works():
    """The legacy space-separated format must still parse."""
    assert _format_api_dates("2024-01-01 00:00:00", date=True) == "2024-01-01"


def test_format_api_dates_open_ended_range_with_none():
    """A None / NaN endpoint becomes '..' in the output range."""
    assert _format_api_dates(["2024-01-01", None], date=True) == "2024-01-01/.."
    assert _format_api_dates([None, "2024-01-01"], date=True) == "../2024-01-01"
