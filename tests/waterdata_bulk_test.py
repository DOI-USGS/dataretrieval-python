"""``waterdata.bulk.get_daily`` / ``get_continuous``: the files route.

The STAC search and the file downloads are mocked over HTTP. The OGC query
is replaced with a recorder: its own tests live with the OGC engine, and what
matters here is which query the bulk path sends and how it merges the answer.
"""

import inspect
import io
import os
import re
import urllib.parse
from datetime import date, datetime, timedelta, timezone

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import dataretrieval
from dataretrieval import waterdata
from dataretrieval._response_metadata import BaseMetadata
from dataretrieval.exceptions import HTTPError
from dataretrieval.interruptions import FanOutInterrupted, ServiceInterrupted
from dataretrieval.waterdata import WaterdataConfiguration, bulk
from dataretrieval.waterdata.bulk import _retrieval

SEARCH_RE = re.compile(r"^https://api\.waterdata\.usgs\.gov/stac/v0/search(\?.*)?$")
FILES = "https://api.waterdata.usgs.gov/stac-files"
ITEMS_RE = re.compile(
    r"^https://api\.waterdata\.usgs\.gov/ogcapi/v\d+/collections/daily/items(\?.*)?$"
)
UTC = timezone.utc
SNAPSHOT = datetime(2026, 11, 21, 4, 0, tzinfo=UTC)
OFF = WaterdataConfiguration(bulk="off")


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
    if collection == "continuous":
        df["time"] = df["time"].dt.tz_localize("UTC")
    return df


def _response(seconds=0.0, remaining=None, url="https://api.waterdata.usgs.gov/"):
    """A sent response: its elapsed time and remaining quota."""
    headers = {} if remaining is None else {"x-ratelimit-remaining": str(remaining)}
    response = httpx.Response(200, headers=headers, request=httpx.Request("GET", url))
    response.elapsed = timedelta(seconds=seconds)
    return response


@pytest.fixture
def ogc(monkeypatch):
    """Record the OGC queries and answer each from ``ogc.answers``."""

    class Recorder:
        def __init__(self):
            self.queries = []
            self.answers = []

        def __call__(self, args, collection, *, chunks=1):
            self.queries.append(dict(args))
            answer = self.answers.pop(0) if self.answers else _api_frame([])
            return answer, BaseMetadata(_response())

    recorder = Recorder()
    monkeypatch.setattr(_retrieval, "get_ogc_data", recorder)
    return recorder


def test_file_rows_merge_with_api_changes_and_the_api_wins(httpx_mock, ogc):
    site = "USGS-01"
    _daily_file(
        httpx_mock,
        site,
        [
            {"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"},
            {
                "ts": "a",
                "site": site,
                "time": date(2020, 1, 2),
                "value": "2",
                "qualifier": ["ICE"],
            },
        ],
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

    df, md = bulk.get_daily(
        monitoring_location_id=site,
        parameter_code="00060",
        skip_geometry=True,
    )

    assert df["value"].tolist() == [1.0, 20.0, 3.0]
    assert df["time"].dtype == "datetime64[us]"
    # The files carry no record id, so the result has none.
    assert "daily_id" not in df.columns
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
    _daily_file(
        httpx_mock,
        site,
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
        ],
    )
    ogc.answers.append(
        _api_frame([{"ts": "a", "site": site, "time": "2020-01-02", "value": "2"}])
    )

    df, _ = bulk.get_daily(
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

    bulk.get_daily(
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
    _daily_file(httpx_mock, site, status_code=404)

    bulk.get_daily(monitoring_location_id=site, skip_geometry=True)

    (query,) = ogc.queries
    assert query["filter"] == "(monitoring_location_id IN ('USGS-01'))"


def test_file_rows_are_filtered_by_time_and_code(httpx_mock, ogc):
    site = "USGS-01"
    _daily_file(
        httpx_mock,
        site,
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
        ],
    )

    df, _ = bulk.get_daily(
        monitoring_location_id=site,
        parameter_code="00060",
        time=["2020-01-01", None],
        skip_geometry=True,
    )

    assert df["value"].tolist() == [1.0]
    assert "daily_id" not in df.columns


def test_continuous_geometry_comes_from_the_catalog(httpx_mock, ogc):
    """The API returns each continuous record's point, so the files path must."""
    geopandas = pytest.importorskip("geopandas")
    _continuous_pair(
        httpx_mock,
        "USGS-01",
        historical=[{"time": datetime(2020, 1, 1, tzinfo=UTC)}],
        current=[{"time": datetime(2020, 1, 1, 0, 15, tzinfo=UTC)}],
    )
    ogc.answers.append(
        geopandas.GeoDataFrame(
            _api_frame([], "continuous"), geometry=[], crs="EPSG:4326"
        )
    )

    df, _ = bulk.get_continuous(monitoring_location_id="USGS-01")

    assert isinstance(df, geopandas.GeoDataFrame)
    assert df.crs == "EPSG:4326"
    assert set(zip(df.geometry.x, df.geometry.y, strict=True)) == {(-74.5, 41.9)}


def test_daily_geometry_comes_from_the_catalog(httpx_mock, ogc):
    site = "USGS-01"
    _daily_file(
        httpx_mock,
        site,
        [{"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"}],
    )
    geopandas = pytest.importorskip("geopandas")
    ogc.answers.append(
        geopandas.GeoDataFrame(_api_frame([]), geometry=[], crs="EPSG:4326")
    )

    df, _ = bulk.get_daily(monitoring_location_id=site)

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

    bulk.get_continuous(monitoring_location_id=site, time=time)

    requested = {str(r.url) for r in httpx_mock.get_requests()}
    assert f"{base}-hot.parquet" in requested
    assert (f"{base}.parquet" in requested) is reads_historical


def test_continuous_cutoff_is_the_current_files(httpx_mock, ogc):
    _continuous_pair(
        httpx_mock,
        "USGS-01",
        historical=[
            {
                "time": datetime(2020, 1, 1, tzinfo=UTC),
                "modified": datetime(2026, 6, 1, 12, tzinfo=UTC),
            }
        ],
        current=[{"time": datetime(2026, 11, 1, tzinfo=UTC)}],
    )

    df, _ = bulk.get_continuous(monitoring_location_id="USGS-01")

    assert len(df) == 2
    # ``historical`` holds only old records, so its newest change is months
    # older than the night both files were written.
    assert "'2026-11-21T00:00:00Z'" in ogc.queries[0]["filter"]


def _daily_file(httpx_mock, site, rows=None, **response):
    """Mock a location's catalog item and its daily file: ``rows``, or ``response``."""
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item(site)]})
    if rows is not None:
        response["content"] = _parquet(rows)
    httpx_mock.add_response(url=f"{FILES}/daily/{site}/{site}.parquet", **response)


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

    bulk.get_continuous(monitoring_location_id="USGS-01")

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
        )
    )

    df, _ = bulk.get_continuous(monitoring_location_id="USGS-01")

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

    bulk.get_continuous(monitoring_location_id="USGS-01")

    assert "time >" not in ogc.queries[0]["filter"]


@pytest.mark.parametrize(
    ("time", "bound"),
    [
        ("2000-01-01/..", "time >= '2000-01-01T00:00:00Z'"),
        ("../2020-01-01", "time <= '2020-01-01T00:00:00Z'"),
    ],
)
def test_a_time_the_api_refuses_is_sent_in_every_clause_instead(
    httpx_mock, ogc, time, bound
):
    """The continuous collection refuses a ``time`` over 1,100 days or with an
    open start (HTTP 400), after every file was downloaded. A filter can
    compare ``time`` itself, so every clause carries the bounds instead."""
    _continuous_pair(
        httpx_mock,
        "USGS-01",
        historical=[{"time": datetime(2010, 1, 1, tzinfo=UTC)}],
        # Two months later: a gap clause as well as a change clause.
        current=[{"time": datetime(2010, 3, 1, tzinfo=UTC)}],
    )

    bulk.get_continuous(monitoring_location_id="USGS-01", time=time)

    assert "time" not in ogc.queries[0]
    clauses = ogc.queries[0]["filter"].split(" OR ")
    assert len(clauses) == 2
    assert all(clause.endswith(f" AND {bound})") for clause in clauses)


def test_locations_without_a_file_carry_the_time_the_request_dropped(ogc, httpx_mock):
    httpx_mock.add_response(url=SEARCH_RE, json={"features": []})

    bulk.get_continuous(monitoring_location_id="USGS-01", time="2000-01-01/2010-01-01")

    assert "time" not in ogc.queries[0]
    assert ogc.queries[0]["filter"] == (
        "(monitoring_location_id IN ('USGS-01') AND "
        "time >= '2000-01-01T00:00:00Z' AND time <= '2010-01-01T00:00:00Z')"
    )


@pytest.mark.parametrize("time", ["2026-01-01/2026-06-01", None])
def test_a_continuous_time_the_api_accepts_is_sent_as_before(ogc, httpx_mock, time):
    """Without it, a continuous change query is far slower (ADR 0014)."""
    httpx_mock.add_response(url=SEARCH_RE, json={"features": []})

    bulk.get_continuous(monitoring_location_id="USGS-01", time=time)

    assert ogc.queries[0].get("time") == time
    assert "time >=" not in ogc.queries[0]["filter"]


@pytest.mark.parametrize(
    ("time", "bounds"),
    [
        ("1950-01-01/..", " AND time >= '1950-01-01T00:00:00Z'"),
        (
            "2020-01-01/2020-12-31",
            " AND time >= '2020-01-01T00:00:00Z' AND time <= '2020-12-31T00:00:00Z'",
        ),
    ],
)
def test_a_daily_change_query_sends_no_time(httpx_mock, ogc, time, bounds):
    """Without a time window, a daily change query is faster (ADR 0014). Only
    the clause for a location without a file carries the bounds."""
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item("USGS-01")]})
    httpx_mock.add_response(
        url=f"{FILES}/daily/USGS-01/USGS-01.parquet",
        content=_parquet(
            [{"ts": "a", "site": "USGS-01", "time": date(2020, 1, 1), "value": "1"}]
        ),
    )

    bulk.get_daily(
        monitoring_location_id=["USGS-01", "USGS-02"], time=time, skip_geometry=True
    )

    (query,) = ogc.queries
    assert "time" not in query
    assert query["filter"] == (
        "(monitoring_location_id IN ('USGS-01') AND "
        "last_modified >= '2026-11-21T00:00:00Z') OR "
        f"(monitoring_location_id IN ('USGS-02'){bounds})"
    )


def test_changed_records_outside_the_time_are_dropped(httpx_mock, ogc):
    """The change clause has no time bounds, so the API also returns records
    outside the interval that changed since the snapshot. The merge keeps the
    API's rows, so they must not reach it."""
    site = "USGS-01"
    _daily_file(
        httpx_mock,
        site,
        [
            {"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"},
            {"ts": "a", "site": site, "time": date(2020, 1, 2), "value": "2"},
        ],
    )
    ogc.answers.append(
        _api_frame(
            [
                {"ts": "a", "site": site, "time": "2019-12-31", "value": "0"},
                {"ts": "a", "site": site, "time": "2020-01-02", "value": "20"},
                {"ts": "a", "site": site, "time": "2020-01-03", "value": "3"},
            ]
        )
    )

    df, _ = bulk.get_daily(
        monitoring_location_id=site, time="2020-01-01/2020-01-02", skip_geometry=True
    )

    assert df["time"].tolist() == [
        pd.Timestamp("2020-01-01"),
        pd.Timestamp("2020-01-02"),
    ]
    assert df["value"].tolist() == [1.0, 20.0]


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"monitoring_location_id": []}, "monitoring_location_id is required"),
        ({"monitoring_location_id": "USGS-01", "time": "P7D"}, "durations"),
    ],
)
def test_rejects_what_the_files_cannot_answer_before_any_request(kwargs, match):
    with pytest.raises(ValueError, match=match):
        bulk.get_daily(**kwargs)


@pytest.mark.parametrize("collection", ["daily", "continuous"])
def test_a_bulk_getter_takes_the_arguments_the_files_route_accepts(collection):
    """The signature is the files route's whole vocabulary, so any other
    argument is a TypeError at the call, not a ValueError after a request."""
    getter = getattr(bulk, f"get_{collection}")
    names = set(inspect.signature(getter).parameters)
    assert names == _retrieval.accepts(collection)


def test_without_pyarrow_a_bulk_getter_raises_before_any_request(monkeypatch, ogc):
    find_spec = _retrieval.importlib.util.find_spec
    monkeypatch.setattr(
        _retrieval.importlib.util,
        "find_spec",
        lambda name, *a: None if name == "pyarrow" else find_spec(name, *a),
    )
    with pytest.raises(ImportError, match=r"dataretrieval\[pyarrow\]"):
        bulk.get_daily("USGS-01")
    assert ogc.queries == []


def test_a_text_column_null_in_the_changed_rows_keeps_its_type(httpx_mock, ogc):
    """The changed rows are few, so a column can be all null there (an
    ``object`` column) while the files have values for it."""
    site = "USGS-01"
    _daily_file(
        httpx_mock,
        site,
        [{"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"}],
    )
    changed = _api_frame([{"ts": "a", "site": site, "time": "2020-01-02", "value": 2}])
    ogc.answers.append(changed.assign(unit_of_measure=None))

    df, _ = bulk.get_daily(monitoring_location_id=site, skip_geometry=True)

    assert df["unit_of_measure"].dtype == pd.Series(["a"]).dtype
    assert df["unit_of_measure"].isna().tolist() == [False, True]


def test_file_rows_alone_have_the_api_column_types(httpx_mock, ogc):
    """Nothing changed since the snapshot: the API returns no rows, so its
    frame has placeholder types that must not leak into the result."""
    site = "USGS-01"
    _daily_file(
        httpx_mock,
        site,
        [{"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"}],
    )
    # As built from the collection schema: no ``daily_id``, second resolution.
    empty = _api_frame([]).drop(columns="daily_id")
    empty = empty.astype({"last_modified": "datetime64[s]"})
    ogc.answers.append(empty.astype(dict.fromkeys(["time_series_id"], object)))

    df, _ = bulk.get_daily(monitoring_location_id=site, skip_geometry=True)

    assert str(df["last_modified"].dtype) == "datetime64[us, UTC]"
    assert df["time"].dtype == "datetime64[us]"
    # The type pandas gives a string column: ``str`` on pandas 3, ``object``
    # on pandas 2, as in an API frame with rows.
    assert df["time_series_id"].dtype == pd.Series(["a"]).dtype
    assert df["value"].tolist() == [1.0]
    assert "daily_id" not in df.columns


@pytest.mark.live
@pytest.mark.parametrize(
    ("collection", "kwargs"),
    [
        ("daily", {"parameter_code": "00060", "time": ["2015-01-01", None]}),
        ("continuous", {"parameter_code": "00060", "time": ["2026-10-01", None]}),
    ],
)
def test_live_bulk_matches_the_api(collection, kwargs):
    """The two paths return the same rows, values, columns, and types."""
    kwargs = {"monitoring_location_id": "USGS-01435000", **kwargs}
    with dataretrieval.configure(OFF):
        api, _ = getattr(waterdata, f"get_{collection}")(**kwargs)
    files, _ = getattr(bulk, f"get_{collection}")(**kwargs)

    api = api.drop(columns=f"{collection}_id")
    assert list(files.columns) == list(api.columns)
    assert list(files.dtypes.astype(str)) == list(api.dtypes.astype(str))
    key = ["time_series_id", "time"]
    columns = [c for c in api.columns if c != "geometry"]
    pd.testing.assert_frame_equal(
        pd.DataFrame(api[columns]).sort_values(key, ignore_index=True),
        pd.DataFrame(files[columns]).sort_values(key, ignore_index=True),
    )


def test_catalog_searches_for_many_locations_are_chunked(httpx_mock, ogc):
    """Each search URL stays well under the edge limit; all chunks are sent
    as one fan-out rather than one after another."""
    # The STAC edge refuses a URL over 2,094 characters, so long ids (21
    # characters here) need more, smaller searches than short ones.
    sites = [f"OH015-{i:015d}" for i in range(250)]
    httpx_mock.add_response(url=SEARCH_RE, json={"features": []}, is_reusable=True)

    bulk.get_daily(monitoring_location_id=sites, skip_geometry=True)

    searches = [r for r in httpx_mock.get_requests() if "/stac/" in str(r.url)]
    assert max(len(str(r.url)) for r in searches) <= 2094
    sent = [
        s
        for r in searches
        for s in re.findall(r"OH015-\d+", urllib.parse.unquote_plus(str(r.url)))
    ]
    assert sorted(sent) == sites


def _change_query(mp, n_changing, n_missing=0):
    """The request ``_query_api`` sends, and the chunk count it asks for."""
    day = pd.Timestamp("2026-11-21 04:00", tz="UTC")
    cutoffs = {f"USGS-{i:08d}": day for i in range(n_changing)}
    missing = {f"USGS-9{i:07d}" for i in range(n_missing)}
    sent = []

    def get(request, collection, *, chunks=1):
        sent.append((request, chunks))
        return pd.DataFrame(), None

    mp.setattr(_retrieval, "get_ogc_data", get)
    query = _retrieval.parse("daily", {"monitoring_location_id": [*cutoffs]})
    _retrieval._query_api(query, cutoffs, missing, [])
    ((request, chunks),) = sent
    return request["filter"].split(" OR "), chunks


@pytest.mark.parametrize(
    ("locations", "concurrency", "sizes"),
    [
        # Up to 8 requests, because the API's time grows with the locations
        # in one request (ADR 0014).
        (4, 32, [1, 1, 1, 1]),
        (30, 32, [4] * 7 + [2]),
        # Then 10 locations a request.
        (250, 32, [10] * 25),
        # But no more requests than run at once: the rest would add only quota.
        (400, 32, [13] * 30 + [10]),
        (40, 2, [20, 20]),
    ],
)
def test_a_change_query_is_one_request_per_clause(
    monkeypatch, locations, concurrency, sizes
):
    monkeypatch.setenv("API_USGS_CONCURRENT", str(concurrency))

    clauses, chunks = _change_query(monkeypatch, locations)

    assert [clause.count("'USGS-") for clause in clauses] == sizes
    assert chunks == len(sizes)


def test_clauses_for_locations_without_a_file_name_at_most_100_each(monkeypatch):
    """The OGC chunker splits a filter only at top-level OR, so one clause
    with every such location would be too long for any URL."""
    clauses, _ = _change_query(monkeypatch, 250, n_missing=150)

    assert [clause.count("'USGS-") for clause in clauses[-2:]] == [100, 50]
    assert max(len(clause) for clause in clauses) < 2000


def test_a_value_no_float_spells_is_nan(httpx_mock, ogc):
    """As the API path's ``to_numeric`` would make it, beside readable ones."""
    site = "USGS-01"
    rows = [
        {"ts": "a", "site": site, "time": date(2020, 1, d), "value": v}
        for d, v in ((1, "1.5"), (2, ""))
    ]
    _daily_file(httpx_mock, site, rows)

    df, _ = bulk.get_daily(monitoring_location_id=site, skip_geometry=True)

    assert df["value"].iloc[0] == 1.5
    assert pd.isna(df["value"].iloc[1])


def test_a_location_whose_file_is_empty_gets_the_whole_query(httpx_mock, ogc):
    """An empty file has no ``T``, so it says nothing about the location."""
    site = "USGS-01"
    _daily_file(httpx_mock, site, [])

    bulk.get_daily(monitoring_location_id=site, skip_geometry=True)

    assert ogc.queries[0]["filter"] == "(monitoring_location_id IN ('USGS-01'))"


def test_a_refused_catalog_search_names_the_call_that_avoids_it(httpx_mock, ogc):
    """A bad ``files_api_key`` is refused at the search, before any download."""
    httpx_mock.add_response(url=SEARCH_RE, status_code=403)

    with pytest.raises(HTTPError, match="bulk='off'") as excinfo:
        bulk.get_daily(monitoring_location_id="USGS-01", skip_geometry=True)

    assert excinfo.value.status_code == 403
    assert ogc.queries == []


def test_a_refused_file_raises_instead_of_falling_back(httpx_mock, ogc):
    """Only a 404 means the file is gone. A 403 (a firewall, a refused key)
    would otherwise turn the call into the full API pull without a word."""
    site = "USGS-01"
    _daily_file(httpx_mock, site, status_code=403)

    with pytest.raises(HTTPError) as excinfo:
        bulk.get_daily(monitoring_location_id=site, skip_geometry=True)

    assert excinfo.value.status_code == 403
    # The message names the call that pages the API instead.
    assert "bulk='off'" in str(excinfo.value)
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
        bulk.get_daily(monitoring_location_id=site, skip_geometry=True)
    httpx_mock.add_response(
        url=url,
        content=_parquet(
            [{"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"}]
        ),
    )
    df, md = excinfo.value.call.resume()

    assert df["value"].tolist() == [1.0, 2.0]
    assert isinstance(md, BaseMetadata)


def test_resuming_an_interrupted_change_query_returns_the_whole_result(
    httpx_mock, ogc, monkeypatch
):
    site = "USGS-01"
    _daily_file(
        httpx_mock,
        site,
        [{"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"}],
    )
    calls = []

    def flaky(args, collection, **kwargs):
        calls.append(args)
        if len(calls) == 1:
            raise ServiceInterrupted(completed_chunks=0, total_chunks=1)
        return _api_frame(
            [{"ts": "a", "site": site, "time": "2020-01-02", "value": "2"}]
        ), BaseMetadata(_response())

    monkeypatch.setattr(_retrieval, "get_ogc_data", flaky)

    with pytest.raises(ServiceInterrupted) as excinfo:
        bulk.get_daily(monitoring_location_id=site, skip_geometry=True)
    # The partial rows are the downloads', as on the handle that resumes.
    assert excinfo.value.partial_frame["value"].tolist() == [1.0]
    assert excinfo.value.partial_frame.equals(excinfo.value.call.partial_frame)
    assert excinfo.value.partial_response is not None
    df, _ = excinfo.value.call.resume()

    assert df["value"].tolist() == [1.0, 2.0]


def test_without_geopandas_file_rows_get_the_coordinate_list(monkeypatch):
    """The API's plain-pandas geometry is the GeoJSON coordinate list."""
    monkeypatch.setattr(_retrieval, "GEOPANDAS", False)
    api = pd.DataFrame({"geometry": [[-74.5, 41.9]], "monitoring_location_id": ["A"]})
    rows = pd.DataFrame({"monitoring_location_id": ["A", "B"]})
    points = {"A": {"type": "Point", "coordinates": [-74.5, 41.9]}}

    out = _retrieval._with_geometry(rows, api, points)

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


def test_md_reports_the_url_the_api_getter_reports(httpx_mock):
    site = "USGS-01"
    httpx_mock.add_response(
        url=SEARCH_RE,
        json={"features": [_item(site)]},
        headers={"x-ratelimit-remaining": "900"},
    )
    httpx_mock.add_response(
        url=f"{FILES}/daily/{site}/{site}.parquet",
        content=_parquet(
            [{"ts": "a", "site": site, "time": date(2020, 1, 1), "value": "1"}]
        ),
        headers={"x-ratelimit-remaining": "800"},
    )
    change = {
        "time_series_id": "b",
        "monitoring_location_id": "USGS-02",
        "parameter_code": "00060",
        "statistic_id": "00003",
        "time": "2020-01-02",
        "value": "2",
        "unit_of_measure": "ft^3/s",
        "approval_status": "Approved",
        "qualifier": None,
        "last_modified": "2026-11-21T04:00:00+00:00",
    }
    httpx_mock.add_response(
        url=ITEMS_RE,
        json={
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "uuid-1",
                    "geometry": None,
                    "properties": change,
                }
            ],
        },
        headers={"x-ratelimit-remaining": "950"},
    )
    kwargs = {
        "monitoring_location_id": [site, "USGS-02"],
        "parameter_code": "00060",
        "time": "2020-01-01/2020-12-31",
        "skip_geometry": True,
    }

    _, md = bulk.get_daily(**kwargs)
    with dataretrieval.configure(OFF):
        _, api = waterdata.get_daily(**kwargs)

    assert md.url == api.url
    # The file download reported the least quota left.
    assert md.header["x-ratelimit-remaining"] == "800"


def test_md_query_time_sums_the_search_the_files_and_the_changes():
    query = _retrieval.parse("daily", {"monitoring_location_id": "USGS-01"})
    changes = BaseMetadata(_response(3, remaining=950))

    md = _retrieval._metadata(
        query, changes, _response(1, 900), _response(2, 800), None
    )

    assert md.query_time == timedelta(seconds=6)
    assert md.header["x-ratelimit-remaining"] == "800"
