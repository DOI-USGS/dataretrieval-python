"""Tests for the Water Data single-line progress reporter.

Covers ProgressReporter rendering / no-op behavior, TTY + environment-variable
gating, progress_context nesting, and that the pagination loop in
``_walk_pages`` reports pages and the rate-limit header through an active
reporter.
"""

import io
from unittest import mock

import requests

from dataretrieval.waterdata._progress import (
    ProgressReporter,
    current,
    progress_context,
)
from dataretrieval.waterdata.utils import _walk_pages

# -- ProgressReporter rendering ------------------------------------------------


def test_disabled_reporter_writes_nothing():
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=False)
    reporter.set_chunks(3)
    reporter.start_chunk(1)
    reporter.add_page(rows=5)
    reporter.set_rate_remaining("100")
    reporter.close()
    assert stream.getvalue() == ""


def test_renders_pages_rows_and_rate_limit():
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.set_rate_remaining("4870")
    reporter.add_page(rows=1234)
    out = stream.getvalue()
    assert "1 page" in out
    assert "1,234 rows" in out
    assert "4,870 requests left" in out


def test_page_count_is_pluralized():
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.add_page()
    assert "1 page" in stream.getvalue() and "1 pages" not in stream.getvalue()
    reporter.add_page()
    assert "2 pages" in stream.getvalue()


def test_chunk_segment_only_shown_when_multiple_chunks():
    single = io.StringIO()
    reporter = ProgressReporter(stream=single, enabled=True)
    reporter.set_chunks(1)
    reporter.add_page()
    assert "chunk" not in single.getvalue()

    many = io.StringIO()
    reporter = ProgressReporter(stream=many, enabled=True)
    reporter.set_chunks(5)
    reporter.start_chunk(2)
    assert "chunk 2/5" in many.getvalue()


def test_missing_rate_limit_does_not_blank_last_known_value():
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.set_rate_remaining("500")
    reporter.set_rate_remaining(None)
    reporter.set_rate_remaining("")
    reporter.add_page()
    assert "500 requests left" in stream.getvalue()


def test_close_terminates_active_line_with_newline():
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.add_page()
    reporter.close()
    assert stream.getvalue().endswith("\n")


def test_close_without_activity_writes_nothing():
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.close()
    assert stream.getvalue() == ""


# -- enable/disable gating -----------------------------------------------------


def test_default_disabled_for_non_tty(monkeypatch):
    monkeypatch.delenv("API_USGS_PROGRESS", raising=False)
    # io.StringIO.isatty() returns False.
    assert ProgressReporter(stream=io.StringIO()).enabled is False


def test_env_var_forces_on(monkeypatch):
    monkeypatch.setenv("API_USGS_PROGRESS", "1")
    assert ProgressReporter(stream=io.StringIO()).enabled is True


def test_env_var_forces_off_even_on_tty(monkeypatch):
    monkeypatch.setenv("API_USGS_PROGRESS", "0")
    tty = mock.MagicMock()
    tty.isatty.return_value = True
    assert ProgressReporter(stream=tty).enabled is False


# -- progress_context ----------------------------------------------------------


def test_progress_context_sets_and_clears_current(monkeypatch):
    monkeypatch.delenv("API_USGS_PROGRESS", raising=False)
    assert current() is None
    with progress_context(enabled=False) as reporter:
        assert current() is reporter
    assert current() is None


def test_nested_context_reuses_outer_reporter():
    with progress_context(enabled=False) as outer:
        with progress_context(enabled=False) as inner:
            assert inner is outer
        # Inner exit must not deactivate the outer reporter.
        assert current() is outer
    assert current() is None


# -- integration with _walk_pages ---------------------------------------------


def _resp(features, *, next_url=None, rate_remaining=None):
    resp = mock.MagicMock()
    links = [{"rel": "next", "href": next_url}] if next_url else []
    resp.json.return_value = {
        "numberReturned": len(features),
        "features": features,
        "links": links,
    }
    headers = {}
    if rate_remaining is not None:
        headers["x-ratelimit-remaining"] = rate_remaining
    resp.headers = headers
    resp.status_code = 200
    return resp


def test_walk_pages_reports_pages_and_rate_limit():
    resp1 = _resp(
        [{"id": "1", "properties": {"v": "a"}}],
        next_url="https://example.com/p2",
        rate_remaining="4999",
    )
    resp2 = _resp([{"id": "2", "properties": {"v": "b"}}], rate_remaining="4998")

    client = mock.MagicMock(spec=requests.Session)
    client.send.return_value = resp1
    client.request.return_value = resp2

    req = mock.MagicMock(spec=requests.PreparedRequest)
    req.method = "GET"
    req.headers = {}
    req.url = "https://example.com/p1"

    stream = io.StringIO()
    with progress_context(stream=stream, enabled=True):
        df, _ = _walk_pages(geopd=False, req=req, client=client)

    assert len(df) == 2
    out = stream.getvalue()
    assert "2 pages" in out
    assert "4,998 requests left" in out
    assert out.endswith("\n")


def test_walk_pages_without_context_does_not_error():
    # No active reporter: pagination must still work and stay silent.
    resp = _resp([{"id": "1", "properties": {"v": "a"}}])
    client = mock.MagicMock(spec=requests.Session)
    client.send.return_value = resp

    req = mock.MagicMock(spec=requests.PreparedRequest)
    req.method = "GET"
    req.headers = {}
    req.url = "https://example.com/p1"

    df, _ = _walk_pages(geopd=False, req=req, client=client)
    assert len(df) == 1
    assert current() is None
