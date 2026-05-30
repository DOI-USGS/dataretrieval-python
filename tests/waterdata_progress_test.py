"""Tests for the Water Data single-line progress reporter.

Covers ProgressReporter rendering / no-op behavior, TTY + environment-variable
gating, progress_context nesting, and that the pagination loop in
``_walk_pages`` reports pages and the rate-limit header through an active
reporter.
"""

import asyncio
import datetime
import io
import sys
import types
from unittest import mock

import httpx
import pandas as pd
import pytest

from dataretrieval.waterdata import _progress
from dataretrieval.waterdata._progress import (
    ProgressReporter,
    current,
    progress_context,
)
from dataretrieval.waterdata.chunking import ChunkedCall, ChunkPlan
from dataretrieval.waterdata.utils import _paginate, _walk_pages


def _run_walk_pages(*, geopd, req, client):
    """Drive the async ``_walk_pages`` to completion synchronously.

    The chunker core is async-only now, so these tests build an
    ``AsyncMock(spec=httpx.AsyncClient)`` whose ``.send``/``.request`` are
    awaitable and run the coroutine via ``asyncio.run``. The progress
    reporter is bound on a contextvar, which the coroutine inherits when
    ``asyncio.run`` copies the calling context.
    """
    return asyncio.run(_walk_pages(geopd=geopd, req=req, client=client))


@pytest.fixture(autouse=True)
def _reset_api_key_hint_latch(monkeypatch):
    """The 'no API key' pointer is latched once per process; reset it so each
    test sees a clean slate regardless of order."""
    monkeypatch.setattr(_progress, "_api_key_hint_shown", False)


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
    assert out.lstrip("\r").startswith("Progress: ")
    assert "1 page" in out
    assert "1,234 rows" in out
    assert "4,870 requests remaining" in out


def test_page_count_is_pluralized():
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.add_page()
    assert "1 page" in stream.getvalue() and "1 pages" not in stream.getvalue()
    reporter.add_page()
    assert "2 pages" in stream.getvalue()


def test_note_retry_renders_then_clears_on_next_page():
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.set_chunks(3)
    reporter.start_chunk(1)
    reporter.note_retry(attempt=2, wait=8.0)
    assert "retrying (attempt 2, waiting 8s)" in stream.getvalue()
    # The next page redraws without the note (last frame is after the
    # final carriage return).
    reporter.add_page(rows=5)
    assert "retrying" not in stream.getvalue().rsplit("\r", 1)[-1]


def test_note_retry_subsecond_wait_shows_decimal():
    # A sub-second backoff must not collapse to a misleading "0s".
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.note_retry(attempt=1, wait=0.3)
    out = stream.getvalue()
    assert "waiting 0.3s" in out and "waiting 0s" not in out


def test_note_retry_cleared_on_close():
    # An exhausted retry leaves retry_note set with no following page;
    # close() must clear it so the persisted last line isn't a stale note.
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.add_page(rows=1)
    reporter.note_retry(attempt=3, wait=5.0)
    reporter.close()
    assert "retrying" not in stream.getvalue().rsplit("\r", 1)[-1]


def test_note_retry_is_noop_when_disabled():
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=False)
    reporter.note_retry(attempt=1, wait=1.0)
    assert stream.getvalue() == ""


def test_note_retry_accepts_integer_wait():
    # An int ``wait`` (e.g. whole seconds) must render without raising:
    # ``round(int, 1)`` returns an int and ``int.is_integer()`` only exists
    # on Python 3.12+, while the package floor is 3.9. Renders like the float.
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.note_retry(attempt=1, wait=5)
    assert "retrying (attempt 1, waiting 5s)" in stream.getvalue()


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
    assert "500 requests remaining" in stream.getvalue()


def test_renders_remaining_over_limit():
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.set_rate_remaining("952", limit="1000")
    reporter.add_page(rows=1)
    assert "952/1,000 requests remaining" in stream.getvalue()


def test_no_slash_when_limit_absent():
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.set_rate_remaining("4870")  # remaining only, no limit header
    reporter.add_page()
    out = stream.getvalue()
    assert "4,870 requests remaining" in out
    assert "/" not in out


def test_service_label_leads_the_line():
    stream = io.StringIO()
    reporter = ProgressReporter(service="daily", stream=stream, enabled=True)
    reporter.add_page(rows=5)
    assert stream.getvalue().lstrip("\r").startswith("Retrieving: daily · ")


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


class _RaisingStream:
    """A stream whose writes always fail, e.g. a broken pipe (output | head)."""

    def write(self, *_):
        raise BrokenPipeError("broken pipe")

    def flush(self):
        pass


def test_reporter_swallows_stream_errors_and_disables(monkeypatch):
    monkeypatch.delenv("API_USGS_PAT", raising=False)
    reporter = ProgressReporter(stream=_RaisingStream(), enabled=True)
    reporter.add_page(rows=1)  # render write raises -> must be swallowed
    reporter.close()  # newline + hint writes raise -> must be swallowed
    assert reporter.enabled is False


# -- API-key pointer -----------------------------------------------------------


def test_hints_api_key_when_no_key_configured(monkeypatch):
    monkeypatch.delenv("API_USGS_PAT", raising=False)
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.add_page(rows=5)
    reporter.close()
    assert _progress.SIGNUP_URL in stream.getvalue()


def test_hint_fires_even_when_rate_limit_was_seen(monkeypatch):
    # Anonymous responses still carry a rate-limit header, so absence of a key
    # — not absence of the header — is what drives the pointer.
    monkeypatch.delenv("API_USGS_PAT", raising=False)
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.set_rate_remaining("891")
    reporter.add_page(rows=5)
    reporter.close()
    assert _progress.SIGNUP_URL in stream.getvalue()


def test_no_hint_when_api_key_present(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "secret")
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=True)
    reporter.add_page(rows=5)  # no rate-limit, but a key is configured
    reporter.close()
    assert _progress.SIGNUP_URL not in stream.getvalue()


def test_no_hint_when_disabled(monkeypatch):
    monkeypatch.delenv("API_USGS_PAT", raising=False)
    stream = io.StringIO()
    reporter = ProgressReporter(stream=stream, enabled=False)
    reporter.add_page(rows=5)
    reporter.close()
    assert stream.getvalue() == ""


def test_api_key_hint_shown_at_most_once(monkeypatch):
    monkeypatch.delenv("API_USGS_PAT", raising=False)

    first = io.StringIO()
    r1 = ProgressReporter(stream=first, enabled=True)
    r1.add_page(rows=5)
    r1.close()
    assert _progress.SIGNUP_URL in first.getvalue()

    second = io.StringIO()
    r2 = ProgressReporter(stream=second, enabled=True)
    r2.add_page(rows=5)
    r2.close()
    assert _progress.SIGNUP_URL not in second.getvalue()


# -- enable/disable gating -----------------------------------------------------


def test_default_disabled_for_non_tty(monkeypatch):
    monkeypatch.delenv("API_USGS_PROGRESS", raising=False)
    monkeypatch.setattr(_progress, "_in_jupyter_kernel", lambda: False)
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


def _fake_ipython(shell_class_name):
    """A stand-in IPython module whose get_ipython() returns a shell of the
    given class name (e.g. 'ZMQInteractiveShell' for a Jupyter kernel)."""
    shell = type(shell_class_name, (), {})()
    return types.SimpleNamespace(get_ipython=lambda: shell)


def test_enabled_in_jupyter_kernel(monkeypatch):
    # A Jupyter kernel's stderr isn't a TTY, but the line should still show
    # (it honors \r in the cell output, like tqdm).
    monkeypatch.delenv("API_USGS_PROGRESS", raising=False)
    monkeypatch.setitem(sys.modules, "IPython", _fake_ipython("ZMQInteractiveShell"))
    assert ProgressReporter(stream=io.StringIO()).enabled is True


def test_terminal_ipython_without_tty_stays_disabled(monkeypatch):
    # The terminal REPL is its own TTY; the kernel signal must not force the
    # line on for a non-TTY (e.g. redirected) stream.
    monkeypatch.delenv("API_USGS_PROGRESS", raising=False)
    monkeypatch.setitem(
        sys.modules, "IPython", _fake_ipython("TerminalInteractiveShell")
    )
    assert ProgressReporter(stream=io.StringIO()).enabled is False


def test_env_var_off_overrides_jupyter_kernel(monkeypatch):
    monkeypatch.setenv("API_USGS_PROGRESS", "0")
    monkeypatch.setitem(sys.modules, "IPython", _fake_ipython("ZMQInteractiveShell"))
    assert ProgressReporter(stream=io.StringIO()).enabled is False


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

    client = mock.AsyncMock(spec=httpx.AsyncClient)
    client.send.return_value = resp1
    client.request.return_value = resp2

    req = mock.MagicMock(spec=httpx.Request)
    req.method = "GET"
    req.headers = {}
    req.url = "https://example.com/p1"

    stream = io.StringIO()
    with progress_context(service="daily", stream=stream, enabled=True):
        df, _ = _run_walk_pages(geopd=False, req=req, client=client)

    assert len(df) == 2
    out = stream.getvalue()
    # The service set on the context reaches _paginate's render via the contextvar.
    assert "Retrieving: daily ·" in out
    assert "2 pages" in out
    assert "4,998 requests remaining" in out
    assert out.endswith("\n")


def test_walk_pages_without_context_does_not_error():
    # No active reporter: pagination must still work and stay silent.
    resp = _resp([{"id": "1", "properties": {"v": "a"}}])
    client = mock.AsyncMock(spec=httpx.AsyncClient)
    client.send.return_value = resp

    req = mock.MagicMock(spec=httpx.Request)
    req.method = "GET"
    req.headers = {}
    req.url = "https://example.com/p1"

    df, _ = _run_walk_pages(geopd=False, req=req, client=client)
    assert len(df) == 1
    assert current() is None


def test_broken_progress_stream_does_not_truncate_pagination():
    # A render failure (broken pipe) lands inside _walk_pages' per-page try;
    # it must NOT be mistaken for a failed request and silently drop pages.
    resp1 = _resp(
        [{"id": "1", "properties": {"v": "a"}}], next_url="https://example.com/p2"
    )
    resp2 = _resp([{"id": "2", "properties": {"v": "b"}}])
    client = mock.AsyncMock(spec=httpx.AsyncClient)
    client.send.return_value = resp1
    client.request.return_value = resp2

    req = mock.MagicMock(spec=httpx.Request)
    req.method = "GET"
    req.headers = {}
    req.url = "https://example.com/p1"

    with progress_context(stream=_RaisingStream(), enabled=True):
        df, _ = _run_walk_pages(geopd=False, req=req, client=client)

    assert len(df) == 2  # both pages returned despite the broken progress stream


# -- pagination integration ----------------------------------------------------


def test_paginate_reports_pages_through_active_reporter(monkeypatch):
    """The async paginate path must drive the same progress reporter.
    Pages and rate-limit updates from each completed page should land
    via the active ``ProgressReporter``, exactly as they would on
    ``_walk_pages``."""
    resp1 = _resp(
        [{"id": "1", "properties": {"v": "a"}}],
        next_url="https://example.com/p2",
        rate_remaining="4999",
    )
    resp2 = _resp([{"id": "2", "properties": {"v": "b"}}], rate_remaining="4998")

    async def parse_response(resp):
        body = resp.json()
        nxt = next(
            (link["href"] for link in body["links"] if link["rel"] == "next"), None
        )
        return mock.MagicMock(empty=False, __len__=lambda self: 1), nxt

    # parse_response is sync (like the page parsers).
    def parse_sync(resp):
        body = resp.json()
        nxt = next(
            (link["href"] for link in body["links"] if link["rel"] == "next"), None
        )
        return pd.DataFrame(body["features"]), nxt

    async def follow_up(cursor, sess):
        return resp2

    client = mock.AsyncMock(spec=httpx.AsyncClient)
    client.send.return_value = resp1

    req = mock.MagicMock(spec=httpx.Request)
    req.method = "GET"
    req.headers = {}
    req.url = "https://example.com/p1"

    stream = io.StringIO()

    async def run():
        with progress_context(service="continuous", stream=stream, enabled=True):
            df, _ = await _paginate(
                req,
                parse_response=parse_sync,
                follow_up=follow_up,
                client=client,
            )
        return df

    df = asyncio.run(run())
    assert len(df) == 2
    out = stream.getvalue()
    assert "Retrieving: continuous ·" in out
    assert "2 pages" in out
    assert "4,998 requests remaining" in out
    assert out.endswith("\n")


def test_fan_out_async_sets_chunks_on_active_reporter(monkeypatch):
    """The async fan-out core (``ChunkedCall._run``) records
    ``plan.total`` on the active reporter so the progress line knows how
    many sub-requests are in flight, and ticks ``current_chunk`` via
    ``start_chunk(len(completed))`` as each gathered sub-request finishes
    — reaching ``plan.total`` in the all-success case."""

    # Fake build_request whose URL length scales with the sites list,
    # mirroring the planner's _request_bytes contract. _FakeReq has the
    # same shape as httpx.Request for sizing purposes.
    class _FakeReq:
        __slots__ = ("url", "content")

        def __init__(self, url):
            self.url = url
            self.content = b""

    def build(*, sites):
        return _FakeReq("x" * (200 + len(",".join(sites))))

    sites = ["S" * 10 + str(i) for i in range(4)]
    plan = ChunkPlan({"sites": sites}, build, url_limit=240)
    assert plan.total > 1, "test setup error: plan must fan out"

    async def fetch_async(args):
        return pd.DataFrame({"id": [",".join(args["sites"])]}), mock.Mock(
            elapsed=datetime.timedelta(seconds=0.01),
            headers={"x-ratelimit-remaining": "999"},
        )

    stream = io.StringIO()

    async def run():
        # Drive the async execution core directly (the same coroutine the
        # sync ``resume()`` facade runs through the anyio portal).
        with progress_context(service="daily", stream=stream, enabled=True) as rep:
            await ChunkedCall(plan, fetch_async)._run(4)
            return rep.total_chunks, rep.current_chunk

    total_recorded, current_recorded = asyncio.run(run())
    assert total_recorded == plan.total
    # Each sub-request that completes bumps current_chunk via
    # start_chunk(len(completed)), so by the time the gather finishes
    # current_chunk reflects the total number of successful chunks —
    # plan.total in the all-success case.
    assert current_recorded == plan.total
