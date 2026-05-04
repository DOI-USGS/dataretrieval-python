from unittest import mock

import requests

from dataretrieval.waterdata.utils import (
    _format_api_dates,
    _get_args,
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
