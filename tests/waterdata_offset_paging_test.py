"""End-to-end tests for offset-parallel page fetching through a real getter.

``tests/transport_test.py`` covers the service-neutral walk in isolation. What
this module pins is the *wiring*: that a Water Data getter actually dispatches
to the offset walk (rather than the cursor walk), that the offset requests carry
the parameters the API needs, and that the two documented escape hatches --
a server ignoring ``offset``, and the API's 40000 offset ceiling -- produce a
complete, correct result rather than a silently wrong one.

Every test here is fully mocked (``httpx_mock``); nothing touches the network.
The suite-wide conftest pins ``API_USGS_CONCURRENT=1``, which is the "page
sequentially" setting, so each test that wants the parallel path re-sets it.
"""

from __future__ import annotations

import dataclasses
import json

import httpx
import pytest

import dataretrieval.waterdata.utils as _wd_utils
from dataretrieval.waterdata import get_daily

_ITEMS_URL = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items"
_GEOJSON = {"Content-Type": "application/geo+json"}


def _page(rows, *, next_url: str | None = None) -> str:
    """A GeoJSON FeatureCollection page.

    ``next_url`` adds the ``next`` link the *cursor* walk follows. The offset
    walk ignores links entirely -- it computes its own offsets -- so a page can
    carry one without affecting the offset path.
    """
    rows = list(rows)
    body = {
        "type": "FeatureCollection",
        "numberReturned": len(rows),
        "features": [
            {
                "type": "Feature",
                "id": f"daily-{row}",
                "geometry": None,
                "properties": {
                    "monitoring_location_id": "USGS-01646500",
                    "value": str(row),
                },
            }
            for row in rows
        ],
        "links": [{"rel": "next", "href": next_url}] if next_url else [],
    }
    return json.dumps(body)


def _offset_of(request: httpx.Request) -> int | None:
    raw = request.url.params.get("offset")
    return None if raw is None else int(raw)


@pytest.fixture
def parallel_pages(monkeypatch):
    """Undo the conftest's sequential pin so the offset walk fans out."""
    monkeypatch.setenv("API_USGS_CONCURRENT", "4")


def _serve(httpx_mock, total_rows: int, *, limit: int) -> list[int | None]:
    """Register a callback serving ``total_rows`` rows in ``limit``-sized pages.

    Returns the list that records each request's ``offset``, so a test can
    assert on the request count -- the quota cost, which is the whole reason
    overlapping pages is preferable to splitting the query. A request with no
    ``offset`` is the cursor walk's, and is answered with a linked page 1 so a
    fallback can still complete.
    """
    seen: list[int | None] = []

    def respond(request: httpx.Request) -> httpx.Response:
        offset = _offset_of(request)
        seen.append(offset)
        if offset is None:
            return httpx.Response(
                200,
                text=_page(range(min(limit, total_rows))),
                headers=_GEOJSON,
            )
        rows = range(offset, min(offset + limit, total_rows))
        return httpx.Response(
            200,
            text=_page(rows),
            headers={**_GEOJSON, "x-ratelimit-limit": "1000"},
        )

    httpx_mock.add_callback(respond)
    return seen


def test_getter_pages_by_offset_and_returns_every_row(httpx_mock, parallel_pages):
    """The headline behavior: a multi-page result comes back complete, in
    order, fetched via computed offsets rather than followed cursors."""
    seen = _serve(httpx_mock, total_rows=25, limit=10)

    df, _ = get_daily(monitoring_location_id="USGS-01646500", limit=10)

    # ``value`` is a Water Data numerical column, so ``convert_type`` (on by
    # default) coerces it — hence ints, not the strings the mock served.
    assert df["value"].tolist() == list(range(25))
    # Every request carried an offset, so no page came from a ``next`` link.
    assert all(off is not None for off in seen)
    assert 0 in seen


def test_offset_requests_preserve_the_query(httpx_mock, parallel_pages):
    """Each page request is the planned request plus ``offset`` -- the filters
    and page size must survive, or later pages would answer a different
    question than the first."""
    _serve(httpx_mock, total_rows=25, limit=10)

    get_daily(monitoring_location_id="USGS-01646500", parameter_code="00060", limit=10)

    for request in httpx_mock.get_requests():
        params = request.url.params
        assert params["monitoring_location_id"] == "USGS-01646500"
        assert params["parameter_code"] == "00060"
        assert params["limit"] == "10"
        assert "offset" in params


def test_page_count_is_not_inflated_by_parallelism(httpx_mock, parallel_pages):
    """Offsets are speculative -- a wave may overshoot the end -- but the walk
    must not spend materially more quota than the sequential walk would. The
    ramping wave width (1, 2, 4, ...) is what holds that line: 25 rows at limit
    10 needs 3 pages, and waves of 1 then 2 cover exactly those 3, so parallel
    paging costs the *same* 3 requests the sequential walk would have spent."""
    seen = _serve(httpx_mock, total_rows=25, limit=10)

    get_daily(monitoring_location_id="USGS-01646500", limit=10)

    assert len(seen) == 3


def test_sequential_setting_uses_the_cursor_walk(httpx_mock, monkeypatch):
    """``API_USGS_CONCURRENT=1`` is the documented way back to strictly
    sequential paging, and it must use *standard* OGC paging -- following
    ``next`` links -- not offsets with a wave of one."""
    monkeypatch.setenv("API_USGS_CONCURRENT", "1")
    seen = _serve(httpx_mock, total_rows=10, limit=10)

    df, _ = get_daily(monitoring_location_id="USGS-01646500", limit=10)

    assert len(df) == 10
    assert seen == [None]  # no offset parameter was ever sent


def test_falls_back_to_cursors_when_the_server_ignores_offset(
    httpx_mock, parallel_pages, caplog
):
    """``offset`` is a Water Data extension, not part of OGC API - Features, and
    an unrecognized query parameter is conventionally *ignored*, not rejected. A
    server that ignores it answers every offset with page 1, so a naive walk
    would concatenate the same rows N times and report success. The walk must
    detect that and complete the query the standards-only way -- the result
    stays correct, only the speed changes."""
    served: list[int | None] = []

    def respond(request: httpx.Request) -> httpx.Response:
        offset = _offset_of(request)
        served.append(offset)
        if request.url.params.get("cursor") == "c1":
            return httpx.Response(200, text=_page(range(10, 15)), headers=_GEOJSON)
        # Page 1 regardless of the offset asked for; a next link so the cursor
        # fallback has somewhere to go.
        return httpx.Response(
            200,
            text=_page(range(10), next_url=f"{_ITEMS_URL}?cursor=c1"),
            headers=_GEOJSON,
        )

    httpx_mock.add_callback(respond)

    with caplog.at_level("WARNING"):
        df, _ = get_daily(monitoring_location_id="USGS-01646500", limit=10)

    # Correct, de-duplicated result via the cursor walk -- NOT page 1 repeated.
    assert df["value"].tolist() == list(range(15))
    assert "sequential pagination" in caplog.text
    # The offset attempt happened, then was abandoned in favor of a walk that
    # sends no ``offset`` at all.
    assert any(off is not None for off in served)
    assert None in served


def test_ceiling_hands_off_to_a_cursor_walk_for_the_tail(
    httpx_mock, parallel_pages, monkeypatch
):
    """The API rejects ``offset > 40000``. That is a ceiling, not an
    end-of-data signal, so the walk must continue with cursors (which have no
    ceiling) rather than truncate. This is the invariant that keeps an
    arbitrarily deep pull *complete*, not merely fast. The ceiling is lowered
    here so the test stays small; the mechanism is identical at 40000."""
    limit, ceiling, total = 10, 25, 45
    cursor_rows = {"c1": range(30, 40), "c2": range(40, total)}
    monkeypatch.setattr(
        _wd_utils,
        "WATERDATA_DIALECT",
        dataclasses.replace(_wd_utils.WATERDATA_DIALECT, max_offset=ceiling),
    )

    def respond(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        if cursor is not None:
            nxt = f"{_ITEMS_URL}?cursor=c2" if cursor == "c1" else None
            return httpx.Response(
                200, text=_page(cursor_rows[cursor], next_url=nxt), headers=_GEOJSON
            )
        offset = _offset_of(request)
        assert offset is not None and offset <= ceiling, (
            f"walk issued offset={offset}, past the ceiling of {ceiling}"
        )
        # Every offset page carries a next link. The offset walk ignores links
        # entirely, so this only matters for the tail hand-off -- a cursor walk
        # re-seeded at the last accepted offset, which follows it.
        rows = range(offset, min(offset + limit, total))
        return httpx.Response(
            200,
            text=_page(rows, next_url=f"{_ITEMS_URL}?cursor=c1"),
            headers=_GEOJSON,
        )

    httpx_mock.add_callback(respond)

    df, _ = get_daily(monitoring_location_id="USGS-01646500", limit=limit)

    # Complete and seamless: offsets covered rows 0-19, the re-seeded cursor
    # walk covered 20-44. No gap, and the one re-fetched page de-duplicates.
    assert df["value"].tolist() == list(range(total))


def test_small_result_costs_one_request_at_the_shipped_default(httpx_mock, monkeypatch):
    """A one-page result must cost exactly one request -- at the *default* width,
    which is what users actually get.

    This is the regression test for the bug that shipped: the wave width started
    at the full ``API_USGS_CONCURRENT`` (32), so a single-page query fired 21
    requests (32 offsets clipped to the 40000 ceiling) to discover it was already
    finished. Every other test in the suite pinned the width to 1 or 4 -- and the
    conftest pins 1 -- so nothing exercised the shipped value. Deleting the env
    var here, rather than setting a number, is the point of the test.
    """
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    seen = _serve(httpx_mock, total_rows=5, limit=2000)

    df, _ = get_daily(monitoring_location_id="USGS-01646500", limit=2000)

    assert len(df) == 5
    assert len(seen) == 1, f"a one-page result cost {len(seen)} requests"


def test_request_count_stays_within_twice_the_pages_needed(httpx_mock, monkeypatch):
    """The ramp's headline guarantee, at the default width: doubling means the
    sum of all prior waves is less than the current one, so total requests stay
    under 2x the pages that exist no matter how the result size falls between
    wave boundaries. A flat wave would be ``width``x on every short result."""
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    limit, total = 2000, 19_000  # 10 pages: the deep-history case
    seen = _serve(httpx_mock, total_rows=total, limit=limit)

    df, _ = get_daily(monitoring_location_id="USGS-01646500", limit=limit)

    pages_needed = -(-total // limit)
    assert len(df) == total
    assert len(seen) < 2 * pages_needed, (
        f"{len(seen)} requests for {pages_needed} pages exceeds the 2x bound"
    )


def test_offset_is_still_verified_when_the_first_wave_is_one_page(
    httpx_mock, monkeypatch, caplog
):
    """The ignore-detection guard compares two pages at different offsets. The
    ramp makes the first wave a *single* page, so the comparison has to span
    waves -- otherwise the guard silently never fires and a server that ignores
    ``offset`` would yield page 1 concatenated N times."""
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    served: list[int | None] = []

    def respond(request: httpx.Request) -> httpx.Response:
        served.append(_offset_of(request))
        if request.url.params.get("cursor") == "c1":
            return httpx.Response(200, text=_page(range(10, 15)), headers=_GEOJSON)
        # Page 1 no matter which offset was asked for.
        return httpx.Response(
            200,
            text=_page(range(10), next_url=f"{_ITEMS_URL}?cursor=c1"),
            headers=_GEOJSON,
        )

    httpx_mock.add_callback(respond)

    with caplog.at_level("WARNING"):
        df, _ = get_daily(monitoring_location_id="USGS-01646500", limit=10)

    assert df["value"].tolist() == list(range(15))
    assert "sequential pagination" in caplog.text


def test_no_data_returns_an_empty_frame_not_an_error(httpx_mock, parallel_pages):
    """A query matching nothing must return an empty DataFrame, exactly as the
    sequential walk does.

    "A no-data result is *not* an error" is a documented, load-bearing promise
    of the modern getters, and a query that matches nothing is ordinary -- a
    typo'd site id, a parameter the site doesn't measure, a gap in the record.
    The offset walk used to raise ``DataRetrievalError`` here: the single empty
    page it fetched was *discarded* as past-the-end, which left it with no
    response to report and it fell through to its "issued no requests" guard.
    """
    _serve(httpx_mock, total_rows=0, limit=10)

    df, _ = get_daily(monitoring_location_id="USGS-99999999", limit=10)

    assert len(df) == 0


def test_max_rows_is_exact_under_parallel_paging(httpx_mock, parallel_pages):
    """A wave can fetch past the requested row count, so the cap has to be
    applied to the combined frame -- otherwise ``max_rows`` would return
    whatever a wave boundary happened to land on."""
    _serve(httpx_mock, total_rows=1000, limit=10)

    df, _ = get_daily(monitoring_location_id="USGS-01646500", limit=10, max_rows=25)

    assert len(df) == 25
    assert df["value"].tolist() == list(range(25))
