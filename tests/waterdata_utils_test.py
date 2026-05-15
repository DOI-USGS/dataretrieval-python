import logging
from unittest import mock

import pandas as pd
import requests

import dataretrieval.waterdata.utils as _utils_module
from dataretrieval.waterdata.utils import (
    _arrange_cols,
    _error_body,
    _format_api_dates,
    _get_args,
    _handle_stats_nesting,
    _walk_pages,
)

_LOGGER_NAME = _utils_module.__name__


def test_get_args_basic():
    local_vars = {
        "monitoring_location_id": "USGS-123",
        "service": "daily",
        "output_id": "daily_id",
        "none_val": None,
        "other": "val",
    }
    result = _get_args(local_vars)
    assert result == {"monitoring_location_id": "USGS-123", "other": "val"}


def test_get_args_with_exclude():
    local_vars = {
        "monitoring_location_id": "USGS-123",
        "service": "daily",
        "output_id": "daily_id",
        "to_exclude": "secret",
        "other": "val",
    }
    result = _get_args(local_vars, exclude={"to_exclude"})
    assert result == {"monitoring_location_id": "USGS-123", "other": "val"}


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
    links = [{"rel": "next", "href": "https://example.com/page2"}] if features else []
    resp = mock.MagicMock()
    resp.json.return_value = {
        "numberReturned": len(features),
        "features": features,
        "links": links,
    }
    resp.headers = {}
    resp.status_code = 200
    resp.url = "https://example.com/page1"
    return resp


def _error_log_messages(caplog):
    """Pull ERROR-and-above message strings out of caplog. Shared by the
    pagination-failure tests below."""
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]


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
    caplog.set_level(logging.ERROR, logger=_LOGGER_NAME)

    df, _ = _walk_pages_with_failure(requests.ConnectionError("boom"))

    # First page's data is preserved (best-effort behavior).
    assert list(df["val"]) == ["a"]
    # Logged error mentions the actual ConnectionError, not a stale page body.
    messages = _error_log_messages(caplog)
    assert any("boom" in m for m in messages), messages


def test_walk_pages_surfaces_5xx_mid_pagination(caplog):
    """A non-200 mid-pagination response must be logged, not silently swallowed."""
    caplog.set_level(logging.ERROR, logger=_LOGGER_NAME)

    page2_503 = mock.MagicMock()
    page2_503.status_code = 503
    page2_503.json.return_value = {
        "code": "ServiceUnavailable",
        "description": "upstream timeout",
    }
    page2_503.url = "https://example.com/page2"

    df, _ = _walk_pages_with_failure(page2_503)

    assert list(df["val"]) == ["a"]
    messages = _error_log_messages(caplog)
    assert any("503" in m or "ServiceUnavailable" in m for m in messages), messages


def _stats_initial_ok():
    """A 200-OK initial stats response: empty data list, signals one more page."""
    resp = mock.MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "next": "tok2",
        "features": [],
    }
    resp.headers = {}
    resp.url = "https://example.com/stats?service=foo"
    return resp


def _run_get_stats_data_with_failure(failure_resp_or_exc, monkeypatch):
    """Exercise get_stats_data where the initial response succeeds and the
    paginated follow-up fails as given. Mirrors _walk_pages_with_failure.
    `monkeypatch` stubs ``_handle_stats_nesting`` so the synthetic minimal
    response body doesn't need to parse — these tests only assert on the
    pagination loop's error surfacing."""
    from dataretrieval.waterdata.utils import get_stats_data

    monkeypatch.setattr(
        _utils_module,
        "_handle_stats_nesting",
        mock.MagicMock(return_value=pd.DataFrame()),
    )

    mock_client = mock.MagicMock(spec=requests.Session)
    mock_client.send.return_value = _stats_initial_ok()
    if isinstance(failure_resp_or_exc, BaseException):
        mock_client.request.side_effect = failure_resp_or_exc
    else:
        mock_client.request.return_value = failure_resp_or_exc

    return get_stats_data(
        args={"monitoring_location_id": "USGS-1"},
        service="observationNormals",
        expand_percentiles=False,
        client=mock_client,
    )


def test_get_stats_data_logs_actual_exception_when_request_raises(caplog, monkeypatch):
    """get_stats_data variant of the connection-error scenario."""
    caplog.set_level(logging.ERROR, logger=_LOGGER_NAME)

    _run_get_stats_data_with_failure(
        requests.ConnectionError("stats-boom"),
        monkeypatch,
    )

    messages = _error_log_messages(caplog)
    assert any("stats-boom" in m for m in messages), messages


def test_get_stats_data_surfaces_5xx_mid_pagination(caplog, monkeypatch):
    """get_stats_data variant of the mid-pagination 5xx scenario."""
    caplog.set_level(logging.ERROR, logger=_LOGGER_NAME)

    page2_503 = mock.MagicMock()
    page2_503.status_code = 503
    page2_503.json.return_value = {
        "code": "ServiceUnavailable",
        "description": "upstream timeout",
    }
    page2_503.url = "https://example.com/stats?service=foo&next_token=tok2"

    _run_get_stats_data_with_failure(page2_503, monkeypatch)

    messages = _error_log_messages(caplog)
    assert any("503" in m or "ServiceUnavailable" in m for m in messages), messages


def test_get_stats_data_warning_includes_next_token(caplog, monkeypatch):
    """The pagination-failure warning includes the next_token so operators
    can identify which page in the sequence failed. (Addresses Copilot's
    PR #273 review note: the base URL alone drops cursor context.)"""
    caplog.set_level(logging.WARNING, logger=_LOGGER_NAME)

    page2_503 = mock.MagicMock()
    page2_503.status_code = 503
    page2_503.json.return_value = {
        "code": "ServiceUnavailable",
        "description": "upstream timeout",
    }

    _run_get_stats_data_with_failure(page2_503, monkeypatch)

    warnings_ = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    # The initial response from _stats_initial_ok carries next=tok2.
    assert any("tok2" in m for m in warnings_), warnings_


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


def test_format_api_dates_rejects_mapping():
    """`time={"2024-01-01": "x"}` would silently materialize as the keys list,
    accepting input the user clearly didn't intend.
    """
    import pytest

    with pytest.raises(TypeError, match="date input must be a string or sequence"):
        _format_api_dates({"2024-01-01": "ignored"})


def _make_response(status, body, reason=None, content_type="text/html"):
    resp = requests.Response()
    resp.status_code = status
    resp.reason = reason
    resp._content = body.encode("utf-8")
    resp.headers["Content-Type"] = content_type
    return resp


def test_error_body_handles_non_json_html_response():
    """A non-JSON 502 HTML body must be summarized, not raise JSONDecodeError."""
    html = (
        "<html>\r\n<head><title>502 Bad Gateway</title></head>"
        "<body><center><h1>502 Bad Gateway</h1></center><hr>"
        "<center>openresty</center></body></html>"
    )
    resp = _make_response(502, html, reason="Bad Gateway")
    msg = _error_body(resp)
    assert "502" in msg
    assert "Bad Gateway" in msg


def test_error_body_handles_empty_response_body():
    """An empty error body returns a status/reason message without crashing."""
    resp = _make_response(500, "", reason="Internal Server Error")
    msg = _error_body(resp)
    assert msg == "500: Internal Server Error."


def test_error_body_truncates_long_non_json_body():
    """Non-JSON bodies are truncated to 200 chars to keep the message readable."""
    body = ("x" * 200) + "Y" + ("z" * 299)
    resp = _make_response(502, body, reason="Bad Gateway")
    msg = _error_body(resp)
    assert "x" * 200 in msg
    assert (("x" * 200) + "Y") not in msg


def test_error_body_still_parses_well_formed_json():
    """JSON error bodies continue to render code/description fields."""
    resp = _make_response(
        400,
        '{"code": "BadRequest", "description": "missing parameter"}',
        reason="Bad Request",
        content_type="application/json",
    )
    msg = _error_body(resp)
    assert "400" in msg
    assert "BadRequest" in msg
    assert "missing parameter" in msg
