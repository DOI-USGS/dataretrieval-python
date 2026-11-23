"""``get_daily`` / ``get_continuous`` under the Water Data ``bulk`` setting.

Every test runs with ``bulk="always"`` unless it configures another mode.

The STAC search and the file downloads are mocked over HTTP. The OGC query
is replaced with a recorder: its own tests live with the OGC engine, and what
matters here is which query the bulk path sends and how it merges the answer.
"""

import io
import os
import re
import urllib.parse
from datetime import date, datetime, timezone

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import dataretrieval
from dataretrieval import waterdata
from dataretrieval.exceptions import HTTPError
from dataretrieval.interruptions import FanOutInterrupted, ServiceInterrupted
from dataretrieval.waterdata import WaterdataConfiguration, _bulk
from dataretrieval.waterdata.utils import _get_args

SEARCH_RE = re.compile(r"^https://api\.waterdata\.usgs\.gov/stac/v0/search(\?.*)?$")
FILES = "https://api.waterdata.usgs.gov/stac-files"
UTC = timezone.utc
SNAPSHOT = datetime(2026, 11, 21, 4, 0, tzinfo=UTC)
ALWAYS = WaterdataConfiguration(bulk="always")
AUTO = WaterdataConfiguration(bulk="auto")
OFF = WaterdataConfiguration(bulk="off")


@pytest.fixture(autouse=True)
def _bulk_always():
    with dataretrieval.configure(ALWAYS):
        yield


def _parquet(rows, collection="daily"):
    """A file in the published schema: strings for value, list qualifier."""
    columns = {
        "time_series_id": [r["ts"] for r in rows],
        "monitoring_location_id": [r["site"] for r in rows],
        "parameter_code": [r.get("pc", "00060") for r in rows],
        "statistic_id": [r.get("stat", "00003") for r in rows],
        "time": [r["time"] for r in rows],
        "value": [r["value"] for r in rows],
        "unit_of_measure": ["ft^3/s"] * len(rows),
        "approval_status": [r.get("approval", "Approved") for r in rows],
        "qualifier": [r.get("qualifier") for r in rows],
        "last_modified": [r.get("modified", SNAPSHOT) for r in rows],
    }
    time_type = pa.date32() if collection == "daily" else pa.timestamp("us", "UTC")
    schema = pa.schema(
        [
            ("time_series_id", pa.large_string()),
            ("monitoring_location_id", pa.large_string()),
            ("parameter_code", pa.large_string()),
            ("statistic_id", pa.large_string()),
            ("time", time_type),
            ("value", pa.large_string()),
            ("unit_of_measure", pa.large_string()),
            ("approval_status", pa.large_string()),
            ("qualifier", pa.list_(pa.string())),
            ("last_modified", pa.timestamp("us", "UTC")),
        ]
    )
    buf = io.BytesIO()
    pq.write_table(pa.table(columns, schema=schema), buf)
    return buf.getvalue()


def _item(site, collection="daily", assets=("data",)):
    base = f"{FILES}/{collection}/{site}/{site}"
    hrefs = {"data": f"{base}.parquet", "current": f"{base}-hot.parquet"}
    hrefs["historical"] = f"{base}.parquet"
    return {
        "id": site,
        "geometry": {"type": "Point", "coordinates": [-74.5, 41.9]},
        "properties": {"monitoring_location_id": site},
        "assets": {a: {"href": hrefs[a], "roles": ["data", a]} for a in assets},
    }


def _api_frame(rows, collection="daily"):
    """What ``get_ogc_data`` returns, with ``skip_geometry=True``."""
    id_col = "daily_id" if collection == "daily" else "continuous_id"
    df = pd.DataFrame(
        {
            "time_series_id": [r["ts"] for r in rows],
            "monitoring_location_id": [r["site"] for r in rows],
            "parameter_code": ["00060"] * len(rows),
            "statistic_id": ["00003"] * len(rows),
            "time": pd.to_datetime([r["time"] for r in rows]).astype("datetime64[us]"),
            "value": [float(r["value"]) for r in rows],
            "unit_of_measure": ["ft^3/s"] * len(rows),
            "approval_status": [r.get("approval", "Approved") for r in rows],
            "qualifier": [None] * len(rows),
            "last_modified": pd.to_datetime([SNAPSHOT] * len(rows)),
            id_col: [f"uuid-{i}" for i in range(len(rows))],
        }
    )
    return df


@pytest.fixture
def ogc(monkeypatch):
    """Record the OGC queries and answer each from ``ogc.answers``."""

    class Recorder:
        def __init__(self):
            self.queries = []
            self.answers = []

        def __call__(self, args, collection):
            self.queries.append(dict(args))
            answer = self.answers.pop(0) if self.answers else _api_frame([])
            return answer, "md"

    recorder = Recorder()
    monkeypatch.setattr(_bulk, "get_ogc_data", recorder)
    return recorder


def test_file_rows_merge_with_api_changes_and_the_api_wins(httpx_mock, ogc):
    site = "USGS-01"
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item(site)]})
    httpx_mock.add_response(
        url=f"{FILES}/daily/{site}/{site}.parquet",
        content=_parquet(
            [
                {"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"},
                {
                    "ts": "a",
                    "site": site,
                    "time": date(2020, 1, 2),
                    "value": "2",
                    "qualifier": ["ICE"],
                },
            ]
        ),
    )
    # Day 2 was revised after the snapshot, and day 3 is new.
    ogc.answers.append(
        _api_frame(
            [
                {"ts": "a", "site": site, "time": "2020-01-02", "value": "20"},
                {"ts": "a", "site": site, "time": "2020-01-03", "value": "3"},
            ]
        )
    )

    df, md = waterdata.get_daily(
        monitoring_location_id=site,
        parameter_code="00060",
        skip_geometry=True,
    )

    assert df["value"].tolist() == [1.0, 20.0, 3.0]
    assert df["time"].dtype == "datetime64[us]"
    # The files carry no record id, so the result has none.
    assert "daily_id" not in df.columns
    assert md == "md"
    (query,) = ogc.queries
    assert query["parameter_code"] == "00060"
    # The snapshot time, floored to the day so the window has a margin.
    assert query["filter"] == (
        "(monitoring_location_id IN ('USGS-01') AND "
        "last_modified >= '2026-11-21T00:00:00Z')"
    )
    assert query["filter_lang"] == "cql-text"


def test_a_filter_on_a_changeable_column_is_applied_after_the_merge(httpx_mock, ogc):
    """A record Provisional in the file and Approved since must not be
    returned for approval_status='Provisional'."""
    site = "USGS-01"
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item(site)]})
    httpx_mock.add_response(
        url=f"{FILES}/daily/{site}/{site}.parquet",
        content=_parquet(
            [
                {
                    "ts": "a",
                    "site": site,
                    "time": date(2020, 1, 1),
                    "value": "1",
                    "approval": "Provisional",
                },
                {
                    "ts": "a",
                    "site": site,
                    "time": date(2020, 1, 2),
                    "value": "2",
                    "approval": "Provisional",
                },
            ]
        ),
    )
    ogc.answers.append(
        _api_frame([{"ts": "a", "site": site, "time": "2020-01-02", "value": "2"}])
    )

    df, _ = waterdata.get_daily(
        monitoring_location_id=site,
        approval_status="Provisional",
        skip_geometry=True,
    )

    assert df["time"].tolist() == [pd.Timestamp("2020-01-01")]
    (query,) = ogc.queries
    assert "approval_status" not in query
    assert "approval_status" not in query["filter"]


def test_locations_without_a_file_get_the_whole_query(httpx_mock, ogc):
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item("USGS-01")]})
    httpx_mock.add_response(
        url=f"{FILES}/daily/USGS-01/USGS-01.parquet",
        content=_parquet(
            [{"ts": "a", "site": "USGS-01", "time": date(2020, 1, 1), "value": "1"}]
        ),
    )

    waterdata.get_daily(
        monitoring_location_id=["USGS-01", "USGS-02"],
        approval_status="Approved",
        skip_geometry=True,
    )

    (query,) = ogc.queries
    # One query: a change clause for USGS-01, the whole query for USGS-02.
    # The changeable filter goes only into the whole-query clause.
    assert query["filter"] == (
        "(monitoring_location_id IN ('USGS-01') AND "
        "last_modified >= '2026-11-21T00:00:00Z') OR "
        "(monitoring_location_id IN ('USGS-02') AND approval_status IN ('Approved'))"
    )
    assert "approval_status" not in query


def test_a_missing_file_falls_back_to_the_api(httpx_mock, ogc):
    site = "USGS-01"
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item(site)]})
    httpx_mock.add_response(url=f"{FILES}/daily/{site}/{site}.parquet", status_code=404)

    waterdata.get_daily(monitoring_location_id=site, skip_geometry=True)

    (query,) = ogc.queries
    assert query["filter"] == "(monitoring_location_id IN ('USGS-01'))"


def test_file_rows_are_filtered_by_time_and_code(httpx_mock, ogc):
    site = "USGS-01"
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item(site)]})
    httpx_mock.add_response(
        url=f"{FILES}/daily/{site}/{site}.parquet",
        content=_parquet(
            [
                {"ts": "a", "site": site, "time": date(2019, 12, 31), "value": "0"},
                {"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"},
                {
                    "ts": "b",
                    "site": site,
                    "time": date(2020, 1, 1),
                    "value": "9",
                    "pc": "00010",
                },
            ]
        ),
    )

    df, _ = waterdata.get_daily(
        monitoring_location_id=site,
        parameter_code="00060",
        time=["2020-01-01", None],
        skip_geometry=True,
    )

    assert df["value"].tolist() == [1.0]
    assert "daily_id" not in df.columns


def test_daily_geometry_comes_from_the_catalog(httpx_mock, ogc):
    site = "USGS-01"
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item(site)]})
    httpx_mock.add_response(
        url=f"{FILES}/daily/{site}/{site}.parquet",
        content=_parquet(
            [{"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"}]
        ),
    )
    geopandas = pytest.importorskip("geopandas")
    ogc.answers.append(
        geopandas.GeoDataFrame(_api_frame([]), geometry=[], crs="EPSG:4326")
    )

    df, _ = waterdata.get_daily(monitoring_location_id=site)

    assert isinstance(df, geopandas.GeoDataFrame)
    assert (df.geometry.x.iloc[0], df.geometry.y.iloc[0]) == (-74.5, 41.9)


@pytest.mark.parametrize(
    ("days_ago", "reads_historical"), [(7, False), (5000, True), (None, True)]
)
def test_continuous_reads_historical_only_for_an_old_or_open_start(
    httpx_mock, ogc, days_ago, reads_historical
):
    """``current`` covers about 150 days, so a recent start skips the
    much larger ``historical`` file."""
    site = "USGS-01"
    base = f"{FILES}/continuous/{site}/{site}"
    item = _item(site, "continuous", assets=("current", "historical"))
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [item]})
    row = {
        "ts": "a",
        "site": site,
        "value": "1",
        "time": datetime(2026, 11, 1, tzinfo=UTC),
    }
    for url in (f"{base}-hot.parquet", f"{base}.parquet"):
        httpx_mock.add_response(
            url=url, content=_parquet([row], "continuous"), is_optional=True
        )
    time = None
    if days_ago is not None:
        start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days_ago)
        time = [start.isoformat(), None]

    waterdata.get_continuous(monitoring_location_id=site, time=time)

    requested = {str(r.url) for r in httpx_mock.get_requests()}
    assert f"{base}-hot.parquet" in requested
    assert (f"{base}.parquet" in requested) is reads_historical


def test_continuous_cutoff_is_the_current_files(httpx_mock, ogc):
    site = "USGS-01"
    base = f"{FILES}/continuous/{site}/{site}"
    item = _item(site, "continuous", assets=("current", "historical"))
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [item]})
    row = {"ts": "a", "site": site, "value": "1"}
    httpx_mock.add_response(
        url=f"{base}-hot.parquet",
        content=_parquet(
            [{**row, "time": datetime(2026, 11, 1, tzinfo=UTC)}], "continuous"
        ),
    )
    httpx_mock.add_response(
        url=f"{base}.parquet",
        content=_parquet(
            [
                {
                    **row,
                    "time": datetime(2020, 1, 1, tzinfo=UTC),
                    "modified": datetime(2026, 6, 1, 12, tzinfo=UTC),
                }
            ],
            "continuous",
        ),
    )

    df, _ = waterdata.get_continuous(monitoring_location_id=site)

    assert len(df) == 2
    # ``historical`` holds only old records, so its newest change is months
    # older than the night both files were written.
    assert "'2026-11-21T00:00:00Z'" in ogc.queries[0]["filter"]


def _continuous_pair(httpx_mock, site, *, historical, current):
    """Mock a location's two files, each with the given row overrides."""
    base = f"{FILES}/continuous/{site}/{site}"
    item = _item(site, "continuous", assets=("current", "historical"))
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [item]})
    row = {"ts": "a", "site": site, "value": "1"}
    for url, rows in (
        (f"{base}.parquet", historical),
        (f"{base}-hot.parquet", current),
    ):
        httpx_mock.add_response(
            url=url, content=_parquet([{**row, **r} for r in rows], "continuous")
        )


def test_a_stale_current_file_sets_the_cutoff(httpx_mock, ogc):
    """A ``current`` file older than its ``historical`` (a cached copy) lacks
    the records changed since it was written, so its cutoff is the one sent."""
    _continuous_pair(
        httpx_mock,
        "USGS-01",
        historical=[
            {
                "time": datetime(2020, 1, 1, tzinfo=UTC),
                "modified": datetime(2026, 11, 22, 4, tzinfo=UTC),
            }
        ],
        current=[
            {
                "time": datetime(2020, 1, 1, 0, 15, tzinfo=UTC),
                "modified": datetime(2026, 11, 20, 4, tzinfo=UTC),
            }
        ],
    )

    waterdata.get_continuous(monitoring_location_id="USGS-01")

    assert "last_modified >= '2026-11-20T00:00:00Z'" in ogc.queries[0]["filter"]


def test_a_gap_between_the_two_files_is_asked_of_the_api(httpx_mock, ogc):
    """Files written on different nights leave the span between them in
    neither, and a change query does not return records that have not changed."""
    _continuous_pair(
        httpx_mock,
        "USGS-01",
        historical=[{"time": datetime(2026, 5, 29, 23, 45, tzinfo=UTC)}],
        current=[{"time": datetime(2026, 6, 27, tzinfo=UTC)}],
    )
    ogc.answers.append(
        _api_frame(
            [{"ts": "a", "site": "USGS-01", "time": "2026-06-10", "value": "2"}],
            "continuous",
        ).assign(time=lambda d: d["time"].dt.tz_localize("UTC"))
    )

    df, _ = waterdata.get_continuous(monitoring_location_id="USGS-01")

    assert (
        "(monitoring_location_id IN ('USGS-01') AND "
        "time > '2026-05-29T23:45:00Z' AND time < '2026-06-27T00:00:00Z')"
    ) in ogc.queries[0]["filter"]
    assert len(df) == 3


def test_adjoining_files_add_no_gap_clause(httpx_mock, ogc):
    _continuous_pair(
        httpx_mock,
        "USGS-01",
        historical=[{"time": datetime(2026, 6, 26, 23, 45, tzinfo=UTC)}],
        current=[{"time": datetime(2026, 6, 27, tzinfo=UTC)}],
    )

    waterdata.get_continuous(monitoring_location_id="USGS-01")

    assert "time >" not in ogc.queries[0]["filter"]


@pytest.mark.parametrize("time", ["2000-01-01/..", "../2020-01-01"])
def test_a_time_the_api_refuses_is_not_sent_with_the_change_query(
    httpx_mock, ogc, time
):
    """The continuous collection refuses a ``time`` over 1,100 days or with an
    open start (HTTP 400), after every file was downloaded. A change clause
    needs no ``time``, and the merge applies the bounds to its rows."""
    _continuous_pair(
        httpx_mock,
        "USGS-01",
        historical=[{"time": datetime(2010, 1, 1, tzinfo=UTC)}],
        current=[{"time": datetime(2010, 1, 1, 0, 15, tzinfo=UTC)}],
    )
    # Changed records on both sides of each interval; only one is inside both.
    ogc.answers.append(
        _api_frame(
            [
                {"ts": "a", "site": "USGS-01", "time": t, "value": "2"}
                for t in ("1999-06-01", "2015-06-01", "2026-11-22")
            ],
            "continuous",
        ).assign(time=lambda d: d["time"].dt.tz_localize("UTC"))
    )

    df, _ = waterdata.get_continuous(monitoring_location_id="USGS-01", time=time)

    assert "time" not in ogc.queries[0]
    start, _, end = time.partition("/")
    inside = df["time"].dt.strftime("%Y-%m-%d")
    assert start == ".." or (inside >= start).all()
    assert end == ".." or (inside <= end).all()
    assert "2015-06-01" in set(inside)


def test_locations_without_a_file_carry_the_time_the_request_dropped(ogc, httpx_mock):
    httpx_mock.add_response(url=SEARCH_RE, json={"features": []})

    waterdata.get_continuous(
        monitoring_location_id="USGS-01", time="2000-01-01/2010-01-01"
    )

    assert "time" not in ogc.queries[0]
    assert ogc.queries[0]["filter"] == (
        "(monitoring_location_id IN ('USGS-01') AND "
        "time >= '2000-01-01T00:00:00Z' AND time <= '2010-01-01T00:00:00Z')"
    )


@pytest.mark.parametrize(
    ("getter", "time"),
    [
        (waterdata.get_continuous, "2026-01-01/2026-06-01"),
        (waterdata.get_continuous, None),
        # ``daily`` has no limit on ``time``.
        (waterdata.get_daily, "1950-01-01/.."),
    ],
)
def test_a_time_the_api_accepts_is_sent_as_before(ogc, httpx_mock, getter, time):
    httpx_mock.add_response(url=SEARCH_RE, json={"features": []})

    getter(monitoring_location_id="USGS-01", time=time, skip_geometry=None)

    assert ogc.queries[0].get("time") == time
    assert "time >=" not in ogc.queries[0]["filter"]


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({}, "monitoring_location_id is required"),
        ({"monitoring_location_id": "USGS-01", "filter": "value > 1"}, "filter"),
        ({"monitoring_location_id": "USGS-01", "properties": ["value"]}, "properties"),
        ({"monitoring_location_id": "USGS-01", "max_rows": 5}, "max_rows"),
        ({"monitoring_location_id": "USGS-01", "convert_type": False}, "convert_type"),
        ({"monitoring_location_id": "USGS-01", "time": "P7D"}, "durations"),
        # ``daily`` has no method_category column.
        ({"monitoring_location_id": "USGS-01", "method_category": "STNRD"}, "method"),
    ],
)
def test_rejects_what_bulk_cannot_honor_before_any_request(kwargs, match):
    with pytest.raises(ValueError, match=match):
        waterdata.get_daily(**kwargs)


def test_file_rows_alone_have_the_api_column_types(httpx_mock, ogc):
    """Nothing changed since the snapshot: the API returns no rows, so its
    frame has placeholder types that must not leak into the result."""
    site = "USGS-01"
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item(site)]})
    httpx_mock.add_response(
        url=f"{FILES}/daily/{site}/{site}.parquet",
        content=_parquet(
            [{"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"}]
        ),
    )
    # As built from the collection schema: no ``daily_id``, second resolution.
    empty = _api_frame([]).drop(columns="daily_id")
    empty = empty.astype({"last_modified": "datetime64[s]"})
    ogc.answers.append(empty.astype(dict.fromkeys(["time_series_id"], object)))

    df, _ = waterdata.get_daily(monitoring_location_id=site, skip_geometry=True)

    assert str(df["last_modified"].dtype) == "datetime64[us, UTC]"
    assert df["time"].dtype == "datetime64[us]"
    # The type pandas gives a string column: ``str`` on pandas 3, ``object``
    # on pandas 2, as in an API frame with rows.
    assert df["time_series_id"].dtype == pd.Series(["a"]).dtype
    assert df["value"].tolist() == [1.0]
    assert "daily_id" not in df.columns


@pytest.mark.live
@pytest.mark.parametrize(
    ("getter", "kwargs"),
    [
        (
            waterdata.get_daily,
            {"parameter_code": "00060", "time": ["2015-01-01", None]},
        ),
        (
            waterdata.get_continuous,
            {"parameter_code": "00060", "time": ["2026-10-01", None]},
        ),
    ],
)
def test_live_bulk_matches_the_api(getter, kwargs):
    """The two paths return the same rows, values, columns, and types."""
    kwargs = {"monitoring_location_id": "USGS-01435000", **kwargs}
    with dataretrieval.configure(OFF):
        api, _ = getter(**kwargs)
    bulk, _ = getter(**kwargs)

    id_col = "daily_id" if getter is waterdata.get_daily else "continuous_id"
    api = api.drop(columns=id_col)
    assert list(bulk.columns) == list(api.columns)
    assert list(bulk.dtypes.astype(str)) == list(api.dtypes.astype(str))
    key = ["time_series_id", "time"]
    columns = [c for c in api.columns if c != "geometry"]
    pd.testing.assert_frame_equal(
        pd.DataFrame(api[columns]).sort_values(key, ignore_index=True),
        pd.DataFrame(bulk[columns]).sort_values(key, ignore_index=True),
    )


def test_catalog_searches_for_many_locations_are_chunked(httpx_mock, ogc):
    """Each search URL stays well under the edge limit; all chunks are sent
    as one fan-out rather than one after another."""
    # The STAC edge refuses a URL over 2,094 characters, so long ids (21
    # characters here) need more, smaller searches than short ones.
    sites = [f"OH015-{i:015d}" for i in range(250)]
    httpx_mock.add_response(url=SEARCH_RE, json={"features": []}, is_reusable=True)

    waterdata.get_daily(monitoring_location_id=sites, skip_geometry=True)

    searches = [r for r in httpx_mock.get_requests() if "/stac/" in str(r.url)]
    assert max(len(str(r.url)) for r in searches) <= 2094
    sent = [
        s
        for r in searches
        for s in re.findall(r"OH015-\d+", urllib.parse.unquote_plus(str(r.url)))
    ]
    assert sorted(sent) == sites


def test_change_clauses_name_at_most_100_locations_each():
    """The OGC chunker splits a filter only at top-level OR, so one clause
    with every same-day location would be too long for any URL."""
    day = pd.Timestamp("2026-11-21 04:00", tz="UTC")
    cutoffs = {f"USGS-{i:08d}": day for i in range(250)}
    missing = {f"USGS-9{i:07d}" for i in range(150)}

    with pytest.MonkeyPatch.context() as mp:
        sent = []
        mp.setattr(_bulk, "get_ogc_data", lambda q, c: sent.append(q) or (None, None))
        query = _bulk._parse("daily", {"monitoring_location_id": [*cutoffs]}, None)
        _bulk._query_api(query, cutoffs, missing)

    clauses = sent[0]["filter"].split(" OR ")
    assert len(clauses) == 3 + 2
    assert all(clause.count("'USGS-") <= 100 for clause in clauses)
    assert max(len(clause) for clause in clauses) < 2000


def test_a_refused_file_raises_instead_of_falling_back(httpx_mock, ogc):
    """Only a 404 means the file is gone. A 403 (a firewall, a refused key)
    would otherwise turn the call into the full API pull without a word."""
    site = "USGS-01"
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item(site)]})
    httpx_mock.add_response(url=f"{FILES}/daily/{site}/{site}.parquet", status_code=403)

    with pytest.raises(HTTPError) as excinfo:
        waterdata.get_daily(monitoring_location_id=site, skip_geometry=True)

    assert excinfo.value.status_code == 403
    assert ogc.queries == []


def test_resuming_an_interrupted_download_returns_the_whole_result(httpx_mock, ogc):
    site = "USGS-01"
    url = f"{FILES}/daily/{site}/{site}.parquet"
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item(site)]})
    httpx_mock.add_response(url=url, status_code=503)
    ogc.answers.append(
        _api_frame([{"ts": "a", "site": site, "time": "2020-01-02", "value": "2"}])
    )

    with pytest.raises(FanOutInterrupted) as excinfo:
        waterdata.get_daily(monitoring_location_id=site, skip_geometry=True)
    httpx_mock.add_response(
        url=url,
        content=_parquet(
            [{"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"}]
        ),
    )
    df, md = excinfo.value.call.resume()

    assert df["value"].tolist() == [1.0, 2.0]
    assert md == "md"


def test_resuming_an_interrupted_change_query_returns_the_whole_result(
    httpx_mock, ogc, monkeypatch
):
    site = "USGS-01"
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item(site)]})
    httpx_mock.add_response(
        url=f"{FILES}/daily/{site}/{site}.parquet",
        content=_parquet(
            [{"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"}]
        ),
    )
    calls = []

    def flaky(args, collection):
        calls.append(args)
        if len(calls) == 1:
            raise ServiceInterrupted(completed_chunks=0, total_chunks=1)
        return _api_frame(
            [{"ts": "a", "site": site, "time": "2020-01-02", "value": "2"}]
        ), "md"

    monkeypatch.setattr(_bulk, "get_ogc_data", flaky)

    with pytest.raises(ServiceInterrupted) as excinfo:
        waterdata.get_daily(monitoring_location_id=site, skip_geometry=True)
    df, _ = excinfo.value.call.resume()

    assert df["value"].tolist() == [1.0, 2.0]


def test_without_geopandas_file_rows_get_the_coordinate_list(monkeypatch):
    """The API's plain-pandas geometry is the GeoJSON coordinate list."""
    monkeypatch.setitem(__import__("sys").modules, "geopandas", None)
    api = pd.DataFrame({"geometry": [[-74.5, 41.9]], "monitoring_location_id": ["A"]})
    rows = pd.DataFrame({"monitoring_location_id": ["A", "B"]})
    points = {"A": {"type": "Point", "coordinates": [-74.5, 41.9]}}

    out = _bulk._with_geometry(rows, api, points)

    assert type(out) is pd.DataFrame
    assert out["geometry"].iloc[0] == [-74.5, 41.9]
    assert pd.isna(out["geometry"].iloc[1])


@pytest.mark.live
@pytest.mark.parametrize(
    "path",
    [
        "daily/USGS-01435000/USGS-01435000",
        "continuous/USGS-01435000/USGS-01435000",
        "continuous/USGS-01435000/USGS-01435000-hot",
    ],
)
def test_live_file_layout_is_unchanged(path):
    """Pin the layout USGS has not documented but a reader could exploit.

    Rows are sorted by ``time`` in row groups of 50,000 (shorter at the
    ends), so a time window maps to one contiguous byte range of row groups.
    Reads only the footer, with one range request (two for a long file). A
    failure here means the layout changed. Nothing in the current read path
    depends on it, but a range-request reader would.
    """
    url = f"{FILES}/{path}.parquet"
    # The scheduled job may run without a key; a few small requests fit the
    # anonymous quota.
    key = os.environ.get("API_USGS_PAT")
    headers = {"X-Api-Key": key} if key else {}
    # One suffix range holds the footer (a few KB) and its 8-byte trailer.
    tail = httpx.get(url, headers={**headers, "Range": "bytes=-65536"}, timeout=30)
    tail.raise_for_status()  # a 429 or 5xx is not a layout change
    assert tail.status_code == 206, "the file host no longer serves byte ranges"
    assert tail.content[-4:] == b"PAR1"
    footer_size = int.from_bytes(tail.content[-8:-4], "little")
    if footer_size + 8 > len(tail.content):
        # A long file's footer (92 row groups: ~100 KB) needs a second range.
        tail = httpx.get(
            url, headers={**headers, "Range": f"bytes=-{footer_size + 8}"}, timeout=30
        )
        tail.raise_for_status()
    footer = tail.content[-8 - footer_size : -8]
    meta = pq.read_metadata(pa.BufferReader(footer + tail.content[-8:]))

    schema = meta.schema.to_arrow_schema()
    assert schema.field("last_modified").type == pa.timestamp("us", "UTC")
    time_index = schema.get_field_index("time")
    groups = [meta.row_group(i) for i in range(meta.num_row_groups)]
    # Every group but the ends holds 50,000 rows. The last holds the rest;
    # in ``current`` the first is short too, as the file's start rolls
    # forward each night (46,540 rows on 2026-11-22).
    assert all(g.num_rows == 50_000 for g in groups[1:-1])
    assert all(0 < g.num_rows <= 50_000 for g in (groups[0], groups[-1]))
    bounds = [
        (g.column(time_index).statistics.min, g.column(time_index).statistics.max)
        for g in groups
    ]
    # Sorted by time: each group starts where the previous one ended.
    assert all(lo <= hi for lo, hi in bounds)
    assert all(prev[1] <= cur[0] for prev, cur in zip(bounds, bounds[1:], strict=False))


_HUNDRED = [f"USGS-{i:02d}" for i in range(100)]


@pytest.mark.parametrize(
    ("collection", "kwargs", "max_rows", "expected"),
    [
        # Small: one location, one series, a year.
        (
            "daily",
            {
                "monitoring_location_id": "USGS-01",
                "parameter_code": "00060",
                "time": ["2025-01-01", "2026-01-01"],
            },
            None,
            False,
        ),
        # Large: a hundred locations' whole record.
        (
            "daily",
            {"monitoring_location_id": _HUNDRED, "parameter_code": "00060"},
            None,
            True,
        ),
        # Each named statistic is a series: 3 x a whole record is large.
        (
            "daily",
            {
                "monitoring_location_id": "USGS-01",
                "parameter_code": "00060",
                "statistic_id": ["00001", "00002", "00003"],
            },
            None,
            True,
        ),
        # ``time_series_id`` counts series directly: one series, one year.
        (
            "daily",
            {
                "monitoring_location_id": "USGS-01",
                "time_series_id": "abc",
                "time": ["2025-11-01", "2026-11-01"],
            },
            None,
            False,
        ),
        # A month of one continuous series is served by ``current`` alone
        # and is small: the API.
        (
            "continuous",
            {
                "monitoring_location_id": "USGS-01",
                "parameter_code": "00060",
                "time": ["2026-10-22", None],
            },
            None,
            False,
        ),
        # Five years needs ``historical``, but 175,000 rows outweigh its cost.
        (
            "continuous",
            {
                "monitoring_location_id": "USGS-01",
                "parameter_code": "00060",
                "time": ["2021-11-01", "2026-11-01"],
            },
            None,
            True,
        ),
        # Large, but only the API path honors these.
        (
            "daily",
            {"monitoring_location_id": _HUNDRED, "filter": "value > 1"},
            None,
            False,
        ),
        ("daily", {"monitoring_location_id": _HUNDRED, "time": "P7D"}, None, False),
        (
            "daily",
            {"monitoring_location_id": _HUNDRED, "convert_type": False},
            None,
            False,
        ),
        ("daily", {"monitoring_location_id": _HUNDRED}, 10, False),
        # No locations: the files are per location.
        ("daily", {"parameter_code": "00060"}, None, False),
    ],
)
def test_auto_routes_by_eligibility_and_size(collection, kwargs, max_rows, expected):
    args = _get_args({"convert_type": True, **kwargs}, exclude=set())
    try:
        query = _bulk._parse(collection, args, max_rows)
    except ValueError:  # only the API accepts it
        assert expected is False
    else:
        assert _bulk._prefers_files(query) is expected


def _route_recorder(monkeypatch):
    """Record which path each call takes; the API answers with an id column."""
    sent = []

    def api(args, collection, max_rows=None):
        sent.append("api")
        return _api_frame([{"ts": "a", "site": "s", "time": "2020-01-01", "value": 1}])

    monkeypatch.setattr(_bulk, "get_ogc_data", lambda *a, **k: (api(*a, **k), "md"))
    monkeypatch.setattr(
        _bulk, "_get_bulk", lambda query: sent.append("bulk") or (None, None)
    )
    return sent


def test_bulk_auto_reaches_the_api_for_a_small_query(monkeypatch):
    sent = _route_recorder(monkeypatch)
    with dataretrieval.configure(AUTO):
        df, _ = waterdata.get_daily(monitoring_location_id="USGS-01", time="2026-01-01")
    assert sent == ["api"]
    # The same columns as the files would give: adding locations to a query
    # can move it to the files, and must not change the result's shape.
    assert "daily_id" not in df.columns


def test_auto_returns_the_api_answer_whole_for_a_query_only_the_api_accepts(
    monkeypatch,
):
    sent = _route_recorder(monkeypatch)
    with dataretrieval.configure(AUTO):
        df, _ = waterdata.get_daily(monitoring_location_id=_HUNDRED, filter="value > 1")
    assert sent == ["api"]
    assert "daily_id" in df.columns


def test_off_pages_the_api_for_a_large_query(monkeypatch):
    sent = _route_recorder(monkeypatch)
    with dataretrieval.configure(OFF):
        df, _ = waterdata.get_daily(monitoring_location_id=_HUNDRED)
    assert sent == ["api"]
    assert "daily_id" in df.columns


@pytest.mark.parametrize("mode", [AUTO, ALWAYS], ids=["auto", "always"])
def test_without_pyarrow_the_files_path_pages_the_api_and_warns(monkeypatch, mode):
    sent = _route_recorder(monkeypatch)
    find_spec = _bulk.importlib.util.find_spec
    monkeypatch.setattr(
        _bulk.importlib.util,
        "find_spec",
        lambda name, *a: None if name == "pyarrow" else find_spec(name, *a),
    )
    with (
        dataretrieval.configure(mode),
        pytest.warns(UserWarning, match=r"dataretrieval\[bulk\]") as caught,
    ):
        waterdata.get_daily(monitoring_location_id=_HUNDRED)
    assert sent == ["api"]
    # Attributed to the caller's line, not to the package.
    assert caught[0].filename == __file__
