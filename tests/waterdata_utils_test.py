import asyncio
import datetime
import json
import logging
from unittest import mock

import httpx
import pandas as pd
import pytest

import dataretrieval.ogc.engine as _engine_module
import dataretrieval.ogc.shaping as _shaping_module
import dataretrieval.waterdata.stats as _stats_module
import dataretrieval.waterdata.utils as _utils_module
from dataretrieval.exceptions import (
    DataRetrievalError,
    HTTPError,
    RateLimited,
    ServiceUnavailable,
    TransientError,
)
from dataretrieval.waterdata import get_stats_date_range, get_stats_por
from dataretrieval.waterdata.stats import _handle_nesting, get_data
from dataretrieval.waterdata.utils import (
    OGC_API_URL,
    _arrange_cols,
    _check_ogc_requests,
    _error_body,
    _finalize_ogc,
    _format_api_dates,
    _get_args,
    _get_resp_data,
    _next_req_url,
    _parse_retry_after,
    _raise_for_non_200,
    _row_cap,
    _to_snake_case,
    _walk_pages,
)

_LOGGER_NAME = _utils_module.__name__


def _run_walk_pages(*, geopd, req, client):
    """Drive the async ``_walk_pages`` to completion synchronously.

    The chunker core is async-only now, so these tests build an
    ``AsyncMock(spec=httpx.AsyncClient)`` whose ``.send``/``.request`` are
    awaitable and run the coroutine via ``asyncio.run``. This thin shim
    keeps the historical sync-shaped call sites terse while exercising the
    real async pagination loop.
    """
    return asyncio.run(_walk_pages(geopd=geopd, req=req, client=client))


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
    mock_client = mock.AsyncMock(spec=httpx.AsyncClient)
    # First call to send() returns resp1, then call to request() in loop returns resp2
    mock_client.send.return_value = resp1
    mock_client.request.return_value = resp2

    # Mock request (PreparedRequest)
    mock_req = mock.MagicMock(spec=httpx.Request)
    mock_req.method = "GET"
    mock_req.headers = {}
    mock_req.url = "https://example.com/page1"

    # Call _walk_pages
    df, _ = _run_walk_pages(geopd=False, req=mock_req, client=mock_client)

    assert len(df) == 2
    assert list(df["val"]) == ["a", "b"]
    assert list(df["id"]) == ["1", "2"]
    assert mock_client.send.called
    assert mock_client.request.called
    assert mock_client.request.call_args[0][1] == "https://example.com/page2"


def test_row_cap_truncates_and_stops_within_first_page():
    # Regression for BUG 2: ``_row_cap`` bounds the TOTAL rows. A first page
    # already over the cap is truncated to exactly ``max_rows`` and the
    # ``next`` link is never followed.
    resp1 = mock.MagicMock()
    resp1.json.return_value = {
        "numberReturned": 3,
        "features": [{"id": str(i), "properties": {"val": i}} for i in range(3)],
        "links": [{"rel": "next", "href": "https://example.com/page2"}],
    }
    resp1.headers = {}
    resp1.status_code = 200
    resp1.url = "https://example.com/page1"

    mock_client = mock.AsyncMock(spec=httpx.AsyncClient)
    mock_client.send.return_value = resp1

    mock_req = mock.MagicMock(spec=httpx.Request)
    mock_req.method = "GET"
    mock_req.headers = {}
    mock_req.url = "https://example.com/page1"

    with _row_cap(2):
        df, _ = _run_walk_pages(geopd=False, req=mock_req, client=mock_client)

    assert len(df) == 2  # truncated to the cap, not the page's 3 rows
    assert not mock_client.request.called  # ``next`` link never followed


def test_row_cap_stops_across_pages():
    # The cap accumulates across pages: page 1 (1 row) is under the cap so
    # page 2 is fetched; once the cap (2) is met the third page is NOT.
    def _page(idx, *, has_next):
        resp = mock.MagicMock()
        nxt = f"https://example.com/page{idx + 1}"
        resp.json.return_value = {
            "numberReturned": 1,
            "features": [{"id": str(idx), "properties": {"val": idx}}],
            "links": [{"rel": "next", "href": nxt}] if has_next else [],
        }
        resp.headers = {}
        resp.status_code = 200
        resp.url = f"https://example.com/page{idx}"
        return resp

    mock_client = mock.AsyncMock(spec=httpx.AsyncClient)
    mock_client.send.return_value = _page(1, has_next=True)
    # page 2 still advertises a ``next`` (page 3) that must never be fetched.
    mock_client.request.return_value = _page(2, has_next=True)

    mock_req = mock.MagicMock(spec=httpx.Request)
    mock_req.method = "GET"
    mock_req.headers = {}
    mock_req.url = "https://example.com/page1"

    with _row_cap(2):
        df, _ = _run_walk_pages(geopd=False, req=mock_req, client=mock_client)

    assert len(df) == 2
    assert mock_client.request.call_count == 1  # fetched page 2, stopped before 3


def test_finalize_ogc_truncates_combined_to_max_rows():
    # max_rows is enforced on the *combined* frame in _finalize_ogc (after
    # dedup/sort), so it bounds the total exactly even when a chunked call's
    # per-sub-request pages overshoot the per-_paginate early-stop.
    frame = pd.DataFrame({"id": [str(i) for i in range(10)]})
    resp = mock.MagicMock()
    resp.url = "https://example.com/q"
    resp.elapsed = datetime.timedelta(seconds=0.1)
    resp.headers = {}

    df, md = _finalize_ogc(
        frame,
        resp,
        properties=None,
        output_id="thing_id",
        convert_type=False,
        service="things",
        max_rows=3,
    )
    assert len(df) == 3
    assert hasattr(md, "url")  # wrapped as BaseMetadata


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


def _walk_pages_with_failure(failure_resp_or_exc):
    """Run _walk_pages where page 1 succeeds and page 2 fails as given."""
    resp1 = _resp_ok([{"id": "1", "properties": {"val": "a"}}])

    mock_client = mock.AsyncMock(spec=httpx.AsyncClient)
    mock_client.send.return_value = resp1
    if isinstance(failure_resp_or_exc, BaseException):
        mock_client.request.side_effect = failure_resp_or_exc
    else:
        mock_client.request.return_value = failure_resp_or_exc

    mock_req = mock.MagicMock(spec=httpx.Request)
    mock_req.method = "GET"
    mock_req.headers = {}
    mock_req.url = "https://example.com/page1"

    return _run_walk_pages(geopd=False, req=mock_req, client=mock_client)


def test_walk_pages_raises_on_connection_error_mid_pagination():
    """A connection error mid-pagination must raise with the upstream cause
    chained, and the wrapper message must include recovery guidance that
    is NOT rate-limit-specific (no quota window involved)."""
    with pytest.raises(DataRetrievalError, match="Paginated request failed") as excinfo:
        _walk_pages_with_failure(httpx.ConnectError("boom"))

    msg = str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, httpx.ConnectError)
    assert "boom" in msg
    assert "retry the request" in msg
    assert "rate-limit window" not in msg


def test_walk_pages_raises_with_class_name_when_cause_stringifies_empty():
    """Some ``httpx`` exceptions (e.g. ``TimeoutException("")``)
    stringify to ``""``. The wrapper must still produce an informative
    message — fall back to the exception class name."""
    with pytest.raises(DataRetrievalError, match="Paginated request failed") as excinfo:
        _walk_pages_with_failure(httpx.TimeoutException(""))

    msg = str(excinfo.value)
    assert "Timeout" in msg, msg
    # Sanity-check the malformed-empty placeholder didn't slip through.
    assert "page(s): ." not in msg
    assert "page(s): To recover" not in msg


def test_walk_pages_raises_on_5xx_mid_pagination():
    """A 5xx mid-pagination must raise — partial data is no longer returned
    because the API has no resume cursor, so silently truncating is the
    wrong default."""
    page2_503 = mock.MagicMock()
    page2_503.status_code = 503
    page2_503.json.return_value = {
        "code": "ServiceUnavailable",
        "description": "upstream timeout",
    }
    page2_503.url = "https://example.com/page2"

    with pytest.raises(DataRetrievalError, match="Paginated request failed") as excinfo:
        _walk_pages_with_failure(page2_503)

    msg = str(excinfo.value)
    assert "503" in msg or "ServiceUnavailable" in msg
    assert "rate-limit window" not in msg  # not rate-limited


def test_walk_pages_raises_on_mid_pagination_429():
    """A 429 mid-pagination must raise. Specific status code is preserved in
    the chained cause so callers can branch on rate-limit vs other failures."""
    page2_429 = mock.MagicMock()
    page2_429.status_code = 429
    page2_429.url = "https://example.com/page2"

    with pytest.raises(DataRetrievalError, match="Paginated request failed") as excinfo:
        _walk_pages_with_failure(page2_429)

    msg = str(excinfo.value)
    assert "429" in msg
    assert "rate-limit window" in msg  # 429-specific guidance present


def test_walk_pages_wraps_initial_page_parse_error():
    """A 200 response whose body fails to parse on the FIRST page used
    to escape ``_walk_pages`` as a raw ``JSONDecodeError``, while the
    SAME failure on a subsequent page was wrapped via
    ``_paginated_failure_message``. The asymmetry meant operators got
    different exception types for the same logical bug depending on
    which page hit it. The initial-parse wrapper closes the gap."""
    resp = mock.MagicMock()
    resp.status_code = 200
    resp.url = "https://example.com/page1"
    # Body is unparseable JSON (gateway HTML page, truncated stream).
    resp.json.side_effect = json.JSONDecodeError("Expecting value", "<html>...", 0)

    mock_client = mock.AsyncMock(spec=httpx.AsyncClient)
    mock_client.send.return_value = resp

    mock_req = mock.MagicMock(spec=httpx.Request)
    mock_req.method = "GET"
    mock_req.headers = {}
    mock_req.url = "https://example.com/page1"

    with pytest.raises(DataRetrievalError, match="Paginated request failed") as excinfo:
        _run_walk_pages(geopd=False, req=mock_req, client=mock_client)

    # The JSONDecodeError causing it is on __cause__ so callers can drill in.
    assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)


def test_get_resp_data_handles_missing_features_key():
    """Regression: a 200 with ``numberReturned > 0`` but no
    ``features`` key (real schema-drift shape) used to crash
    ``_get_resp_data`` with ``KeyError`` — wrapped downstream by
    ``_paginate`` as a generic transport error. ``_handle_nesting``
    was already hardened against this; ``_get_resp_data`` now mirrors
    that defensiveness and returns an empty frame instead."""
    resp = mock.Mock()
    resp.json.return_value = {"numberReturned": 1, "links": []}
    df = _get_resp_data(resp, geopd=False)
    assert df.empty
    assert isinstance(df, pd.DataFrame)


def test_next_req_url_follows_link_without_number_returned():
    """The NGWMN OGC API omits ``numberReturned`` from its page envelope, so
    ``_next_req_url`` keys the ``next`` link off ``features`` (mirroring
    ``_get_resp_data``) rather than that count -- otherwise a page that carries
    features but no count stops pagination after page 1 and silently truncates
    every multi-page result. A page that carries features still follows its
    ``next`` link even when ``numberReturned`` is absent."""
    resp = mock.MagicMock()
    resp.url = httpx.URL("https://example.com/page1")
    body = {
        # NGWMN shape: features present, NO numberReturned key.
        "features": [{"id": "1"}],
        "links": [{"rel": "next", "href": "https://example.com/page2"}],
    }
    assert _next_req_url(resp, body=body) == "https://example.com/page2"


def test_next_req_url_stops_when_no_features():
    """A page with no features ends pagination regardless of any stray
    ``next`` link (and regardless of ``numberReturned``)."""
    resp = mock.MagicMock()
    resp.url = httpx.URL("https://example.com/page1")
    body = {"features": [], "links": [{"rel": "next", "href": "https://x/2"}]}
    assert _next_req_url(resp, body=body) is None


def test_walk_pages_does_not_mutate_initial_response():
    """The aggregated response returned from ``_walk_pages`` is built
    via ``_aggregate_paginated_response``, which returns a fresh copy.
    Any caller that inspected ``initial_response.headers`` /
    ``.elapsed`` before pagination completed (a Session response hook,
    a logging middleware) must continue to see the original first-page
    values — NOT the rewritten cumulative values."""
    page1 = mock.MagicMock()
    page1.status_code = 200
    page1.url = "https://example.com/page1"
    page1.elapsed = datetime.timedelta(seconds=1)
    page1.headers = {"x-ratelimit-remaining": "999"}
    page1.json.return_value = {
        "numberReturned": 1,
        "features": [{"id": "1", "properties": {"val": "a"}}],
        "links": [{"rel": "next", "href": "https://example.com/page2"}],
    }
    page1_initial_headers_id = id(page1.headers)
    page1_initial_elapsed = page1.elapsed

    page2 = mock.MagicMock()
    page2.status_code = 200
    page2.url = "https://example.com/page2"
    page2.elapsed = datetime.timedelta(seconds=2)
    page2.headers = {"x-ratelimit-remaining": "998"}
    page2.json.return_value = {
        "numberReturned": 1,
        "features": [{"id": "2", "properties": {"val": "b"}}],
        "links": [],
    }

    mock_client = mock.AsyncMock(spec=httpx.AsyncClient)
    mock_client.send.return_value = page1
    mock_client.request.return_value = page2

    mock_req = mock.MagicMock(spec=httpx.Request)
    mock_req.method = "GET"
    mock_req.headers = {}
    mock_req.url = "https://example.com/page1"

    df, final = _run_walk_pages(geopd=False, req=mock_req, client=mock_client)
    assert len(df) == 2

    # The original first-page response object must be unmutated:
    # both .headers (same dict object) and .elapsed unchanged.
    assert id(page1.headers) == page1_initial_headers_id
    assert page1.headers["x-ratelimit-remaining"] == "999"
    assert page1.elapsed == page1_initial_elapsed

    # The returned aggregate carries page-2 headers + cumulative elapsed.
    assert final.headers["x-ratelimit-remaining"] == "998"
    assert final.elapsed == datetime.timedelta(seconds=3)
    # And mutating the aggregate's headers doesn't leak into either page.
    final.headers["X-Trace-Id"] = "abc"
    assert "X-Trace-Id" not in page1.headers
    assert "X-Trace-Id" not in page2.headers


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


def _run_get_data_with_failure(failure_resp_or_exc, monkeypatch):
    """Exercise get_data where the initial response succeeds and the
    paginated follow-up fails as given. Mirrors _walk_pages_with_failure.
    `monkeypatch` stubs ``_handle_nesting`` so the synthetic minimal
    response body doesn't need to parse — these tests only assert on the
    pagination loop's error surfacing."""
    monkeypatch.setattr(
        _stats_module,
        "_handle_nesting",
        mock.MagicMock(return_value=pd.DataFrame()),
    )

    mock_client = mock.AsyncMock(spec=httpx.AsyncClient)
    mock_client.send.return_value = _stats_initial_ok()
    if isinstance(failure_resp_or_exc, BaseException):
        mock_client.request.side_effect = failure_resp_or_exc
    else:
        mock_client.request.return_value = failure_resp_or_exc

    return get_data(
        args={"monitoring_location_id": "USGS-1"},
        service="observationNormals",
        expand_percentiles=False,
        client=mock_client,
    )


def test_get_data_raises_on_mid_pagination_failure(monkeypatch):
    """Wiring smoke: ``get_data`` and ``_walk_pages`` share the
    same ``_paginate`` strategy helper, so error-routing behaviour is
    exercised by the ``_walk_pages`` triplet above. This single
    ``get_data`` mid-pagination case proves the stats-specific
    follow-up callback is wired into ``_paginate`` correctly."""
    with pytest.raises(DataRetrievalError, match="Paginated request failed") as excinfo:
        _run_get_data_with_failure(
            httpx.ConnectError("stats-boom"),
            monkeypatch,
        )

    assert isinstance(excinfo.value.__cause__, httpx.ConnectError)
    assert "stats-boom" in str(excinfo.value)


def test_get_data_warning_includes_next_token(caplog, monkeypatch):
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

    with pytest.raises(DataRetrievalError):
        _run_get_data_with_failure(page2_503, monkeypatch)

    warnings_ = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    # The initial response from _stats_initial_ok carries next=tok2.
    assert any("tok2" in m for m in warnings_), warnings_


def test_handle_nesting_tolerates_missing_drop_columns():
    """If the upstream stats response shape ever changes such that the nested
    ``data`` column ``_handle_nesting`` drops is absent, the function should
    still return a DataFrame instead of raising KeyError (the drop uses
    ``errors="ignore"``).
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

    df = _handle_nesting(body, geopd=False)

    assert len(df) == 1
    assert df["monitoring_location_id"].iloc[0] == "USGS-12345"


def test_handle_nesting_returns_empty_on_empty_features():
    """A mid-pagination empty page ({\"features\": [], \"next\": <tok>})
    must not crash the downstream merge with
    ``KeyError: 'monitoring_location_id'``. The function short-
    circuits to an empty DataFrame so pagination can continue."""
    df = _handle_nesting({"features": [], "next": None}, geopd=False)
    assert df.empty


def test_handle_nesting_empty_preserves_geopd_type():
    """When geopandas is available, the empty-features short-circuit
    must return a ``GeoDataFrame`` rather than a plain ``DataFrame``.
    Otherwise a subsequent ``pd.concat([empty, geo_page])`` downgrades
    the final result to a plain ``DataFrame`` and strips geometry/CRS
    — a real regression for geopd-installed users on stats queries
    that hit an empty intermediate page."""
    # Monkeypatch a stub gpd so the test runs whether or not geopandas is
    # installed. The empty-page short-circuit delegates to the shared
    # ``shaping._empty_feature_frame``, which resolves ``gpd`` from the shaping
    # namespace — so patch it there, not in the stats module.
    fake_gpd = mock.MagicMock()

    class _Sentinel:
        pass

    fake_gpd.GeoDataFrame = lambda *a, **kw: _Sentinel()
    with mock.patch.object(_shaping_module, "gpd", fake_gpd, create=True):
        result = _handle_nesting({"features": []}, geopd=True)
    assert isinstance(result, _Sentinel)


def test_get_resp_data_empty_preserves_geopd_type():
    """Same as the stats-side preservation: ``_get_resp_data``'s
    ``numberReturned == 0`` short-circuit must return a
    ``GeoDataFrame`` (not a plain ``DataFrame``) when geopd is True,
    so paginating across a sparse intermediate page doesn't downgrade
    the final concat result."""
    fake_gpd = mock.MagicMock()

    class _Sentinel:
        pass

    fake_gpd.GeoDataFrame = lambda *a, **kw: _Sentinel()

    resp = mock.MagicMock()
    resp.json.return_value = {"numberReturned": 0, "features": [], "links": []}
    # ``_get_resp_data`` resolves ``gpd`` from the shaping namespace -- patch
    # it there, not in ``utils``.
    with mock.patch.object(_shaping_module, "gpd", fake_gpd, create=True):
        result = _get_resp_data(resp, geopd=True)
    assert isinstance(result, _Sentinel)


def test_handle_nesting_tolerates_missing_features_key():
    """A 200 response with a body that doesn't carry ``features`` at
    all (rare but seen in error envelopes) must also short-circuit
    rather than KeyError before the schema-aware extraction even
    runs."""
    df = _handle_nesting({}, geopd=False)
    assert df.empty


def test_get_resp_data_always_materializes_id_column():
    """``_get_resp_data`` must always materialize the ``id`` column
    (NaN-filled when no feature carries one) so the downstream
    ``_arrange_cols`` rename to the service-specific output_id
    (``daily_id``, ``channel_measurements_id``, etc.) isn't a
    silent no-op."""
    resp = mock.MagicMock()
    resp.json.return_value = {
        "numberReturned": 2,
        "features": [
            {"properties": {"val": "a"}},  # no top-level id
            {"properties": {"val": "b"}},  # ditto
        ],
    }
    df = _get_resp_data(resp, geopd=False)
    assert "id" in df.columns
    assert df["id"].isna().all()


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
    with pytest.raises(TypeError, match="date input must be a string or sequence"):
        _format_api_dates({"2024-01-01": "ignored"})


def _make_response(status, body, reason=None, content_type="text/html"):
    headers = {"Content-Type": content_type}
    extensions = {}
    if reason is not None:
        extensions["reason_phrase"] = reason.encode("utf-8")
    return httpx.Response(
        status_code=status,
        content=body.encode("utf-8"),
        headers=headers,
        extensions=extensions,
    )


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


def test_parse_retry_after_handles_none_and_empty():
    """Absent or empty header → ``None`` (no quota signal). The chunker
    treats ``None`` as "fall back to my own retry policy," so this
    branch must not return a misleading 0."""
    assert _parse_retry_after(None) is None
    assert _parse_retry_after("") is None
    assert _parse_retry_after("   ") is None


def test_parse_retry_after_parses_delta_seconds():
    """Integer and float forms of delta-seconds (the common shape USGS
    sends) are parsed directly without touching the HTTP-date branch."""
    assert _parse_retry_after("120") == 120.0
    assert _parse_retry_after("0") == 0.0
    assert _parse_retry_after("42.5") == 42.5
    # Surrounding whitespace is stripped before parsing.
    assert _parse_retry_after("  30  ") == 30.0


def test_parse_retry_after_clamps_negative_delta_to_zero():
    """A negative delta-seconds means the server is saying "retry now."
    Returning the negative value would let callers pass it to
    ``time.sleep`` and get a ``ValueError`` — clamp at the source."""
    assert _parse_retry_after("-10") == 0.0
    assert _parse_retry_after("-0.5") == 0.0


def test_parse_retry_after_returns_none_for_unparseable():
    """Garbage values (including the RFC 1123 HTTP-date form that the
    HTTP spec allows but USGS doesn't actually send) surface as
    ``None``, letting the chunker fall back to its own retry policy
    instead of guessing a delay."""
    assert _parse_retry_after("not-a-date") is None
    assert _parse_retry_after("Wed, 21 Oct 2099 07:28:00 GMT") is None


def test_raise_for_non_200_raises_service_unavailable_for_5xx():
    """5xx must surface as the typed ``ServiceUnavailable`` so the chunker can
    wrap it as a resumable ``ServiceInterrupted`` rather than treating it as a
    fatal error."""
    resp = _make_response(503, "", reason="Service Unavailable")
    resp.headers["Retry-After"] = "120"
    with pytest.raises(ServiceUnavailable) as excinfo:
        _raise_for_non_200(resp)
    assert excinfo.value.retry_after == 120.0


def test_raise_for_non_200_attaches_retry_after_to_rate_limited():
    """``Retry-After`` on a 429 response must travel onto
    ``RateLimited.retry_after`` so the chunker can surface it on
    ``QuotaExhausted.retry_after`` for callers to honor."""
    resp = _make_response(429, "", reason="Too Many Requests")
    resp.headers["Retry-After"] = "60"
    with pytest.raises(RateLimited) as excinfo:
        _raise_for_non_200(resp)
    assert excinfo.value.retry_after == 60.0


def test_raise_for_non_200_400_raises_http_error():
    """400 raises a fatal ``HTTPError`` (status_code=400) the chunker won't
    resume. It must NOT be a ``TransientError`` so the chunker's classifier
    treats it as fatal rather than wrapping it as resumable."""
    resp = _make_response(
        400,
        '{"code": "BadRequest", "description": "missing parameter"}',
        reason="Bad Request",
        content_type="application/json",
    )
    with pytest.raises(HTTPError) as excinfo:
        _raise_for_non_200(resp)
    assert excinfo.value.status_code == 400
    # Fatal, not transient: the chunker keys off ``isinstance(_, TransientError)``
    # to decide whether to wrap a failure as a resumable ChunkInterrupted.
    assert not isinstance(excinfo.value, TransientError)


def test_next_req_url_rejects_cross_host():
    """``_next_req_url`` must refuse to follow a next-page link to a
    different host. The original request's headers (including any
    auth-like artifacts) were minted for the original host; following
    a server-supplied cross-host URL would leak them — and the URL
    itself could be sensitive."""
    resp = mock.MagicMock()
    resp.url = httpx.URL("https://api.waterdata.usgs.gov/page1")
    body = {
        "numberReturned": 1,
        "features": [{"id": "1"}],
        "links": [{"rel": "next", "href": "https://evil.example.org/secret"}],
    }
    with pytest.raises(RuntimeError, match="cross-host next-page"):
        _next_req_url(resp, body=body)


def test_check_ogc_requests_raises_typed_on_5xx(httpx_mock):
    """``_check_ogc_requests`` routes a non-200 through ``_raise_for_non_200``,
    so a 5xx surfaces as the typed ``ServiceUnavailable`` — the same typed
    contract as the main data path, not a raw ``httpx`` error."""
    httpx_mock.add_response(
        method="GET",
        url=f"{OGC_API_URL}/collections/daily/schema",
        status_code=503,
        json={"code": "ServiceUnavailable", "description": "maintenance window"},
    )
    with pytest.raises(ServiceUnavailable):
        _check_ogc_requests(endpoint="daily", req_type="schema")


@pytest.mark.parametrize(
    "name, expected",
    [
        ("waterLevelObs", "water_level_obs"),  # camelCase -> snake_case
        ("monitoring_location_id", "monitoring_location_id"),  # already snake
        ("value", "value"),  # all-lowercase unchanged
        ("navd88", "navd88"),  # letter/digit boundary NOT split
        ("someField", "some_field"),  # simple camelCase
        ("PascalCase", "pascal_case"),  # leading capital
        # Runs of capitals are best-effort: only the lower->Upper boundary
        # before the run is split, so the acronym stays glued to the next word.
        ("someXMLField", "some_xmlfield"),
    ],
)
def test_to_snake_case(name, expected):
    assert _to_snake_case(name) == expected


def test_get_stats_por_forwards_normal_type(monkeypatch):
    """``normal_type`` reaches the observationNormals request (parity with R's
    ``read_waterdata_stats_por``). Guards against the param being dropped from
    the forwarded args (e.g. accidentally added to ``_get_args``'s exclude)."""
    captured: dict = {}

    def fake_get_data(args, service, expand_percentiles, client=None):
        captured.update(args=args, service=service)
        return pd.DataFrame(), mock.Mock()

    monkeypatch.setattr(_stats_module, "get_data", fake_get_data)
    get_stats_por(monitoring_location_id="USGS-1", normal_type="MOY")
    assert captured["service"] == "observationNormals"
    assert captured["args"].get("normal_type") == "MOY"


def test_get_stats_date_range_forwards_interval_type(monkeypatch):
    """``interval_type`` (multi-value) reaches the observationIntervals request
    (parity with R's ``read_waterdata_stats_daterange``)."""
    captured: dict = {}

    def fake_get_data(args, service, expand_percentiles, client=None):
        captured.update(args=args, service=service)
        return pd.DataFrame(), mock.Mock()

    monkeypatch.setattr(_stats_module, "get_data", fake_get_data)
    get_stats_date_range(monitoring_location_id="USGS-1", interval_type=["M", "CY"])
    assert captured["service"] == "observationIntervals"
    assert captured["args"].get("interval_type") == ["M", "CY"]


def test_with_state_routes_into_native_queryable():
    """``_with_state`` resolves the canonical ``state`` argument into the
    endpoint's native queryable (any encoding -> the requested representation)
    and leaves args without ``state`` untouched."""
    assert _utils_module._with_state({"state": "WI"}, to="name", into="state_name") == {
        "state_name": "Wisconsin"
    }
    assert _utils_module._with_state(
        {"state": "Wisconsin"}, to="fips_us", into="state_code"
    ) == {"state_code": "US:55"}
    # Multi-value state fans out element-wise.
    assert _utils_module._with_state(
        {"state": ["WI", "55"]}, to="name", into="state_name"
    ) == {"state_name": ["Wisconsin", "Wisconsin"]}
    # No ``state`` -> mapping returned unchanged.
    assert _utils_module._with_state(
        {"state_name": "Ohio"}, to="name", into="state_name"
    ) == {"state_name": "Ohio"}


def test_with_state_conflict_raises():
    """Passing ``state`` together with a native ``state_code``/``state_name``
    is ambiguous and raises."""
    with pytest.raises(ValueError, match="not both"):
        _utils_module._with_state(
            {"state": "WI", "state_code": "55"}, to="name", into="state_name"
        )
    with pytest.raises(ValueError, match="not both"):
        _utils_module._with_state(
            {"state": "WI", "state_name": "Wisconsin"}, to="name", into="state_name"
        )


def test_ogc_getter_resolves_state_at_getter_layer(monkeypatch):
    """The OGC getters resolve the unified ``state`` into ``state_name``
    themselves (any encoding), so the shared ``get_ogc_data`` wrapper stays
    state-agnostic."""
    import dataretrieval.waterdata.api as _api

    captured: dict = {}

    def fake_get_ogc_data(args, service, *a, **k):
        captured.update(args=args, service=service)
        return pd.DataFrame(), mock.Mock()

    monkeypatch.setattr(_api, "get_ogc_data", fake_get_ogc_data)
    _api.get_monitoring_locations(state="55")  # FIPS in -> full name out
    assert captured["args"].get("state_name") == "Wisconsin"
    assert "state" not in captured["args"]


def test_get_ogc_data_wrapper_does_not_touch_state():
    """``get_ogc_data`` no longer rewrites a ``state`` key, so a passthrough
    query dict (e.g. from ``get_reference_table``) is forwarded untouched."""
    captured: dict = {}

    def fake_engine_get_ogc_data(args, service, output_id, **k):
        captured["args"] = dict(args)
        return pd.DataFrame(), mock.Mock()

    with mock.patch.object(_engine_module, "get_ogc_data", fake_engine_get_ogc_data):
        _utils_module.get_ogc_data({"state": "WI"}, "monitoring-locations")
    assert captured["args"] == {"state": "WI"}
