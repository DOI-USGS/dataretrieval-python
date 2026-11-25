"""The router of the Water Data ``bulk`` setting, behind ``waterdata.get_daily``
and ``waterdata.get_continuous``.

The files route itself is tested through the bulk getters in
``waterdata_bulk_test.py``; here it is replaced with a recorder, and what
matters is which path each query takes and the columns it returns.
"""

import importlib.util

import pandas as pd
import pytest

import dataretrieval
from dataretrieval import waterdata
from dataretrieval.waterdata import WaterdataConfiguration, _routing
from dataretrieval.waterdata.utils import _get_args

AUTO = WaterdataConfiguration(bulk="auto")
OFF = WaterdataConfiguration(bulk="off")


def _api_frame():
    """One API row, with the record id the files do not carry."""
    return pd.DataFrame({"time_series_id": ["a"], "value": [1.0], "daily_id": ["u"]})


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
        # No parameter: 4 x 2 series x 731 days, over the daily base of 5,000
        # plus 100 a file. Locations with many series return severalfold more.
        (
            "daily",
            {
                "monitoring_location_id": [f"USGS-{i}" for i in range(4)],
                "time": ["2024-11-01", "2026-11-01"],
            },
            None,
            True,
        ),
        # One gage, five years: 3,650, under the base.
        (
            "daily",
            {"monitoring_location_id": "USGS-01", "time": ["2021-11-01", "2026-11-01"]},
            None,
            False,
        ),
        # A continuous query the daily base would send to the files: 1 x 3
        # series x 96 x 25 days is 7,200, under the continuous base of 10,000.
        (
            "continuous",
            {"monitoring_location_id": "USGS-01", "time": ["2026-10-20", "2026-11-14"]},
            None,
            False,
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
    query = _routing._files_query(args, collection, max_rows)
    if query is None:  # only the API accepts it
        assert expected is False
    else:
        assert _routing._prefers_files(query) is expected


def _route_recorder(monkeypatch):
    """Record which path each call takes; the API answers with an id column."""
    sent = []

    def api(args, collection, max_rows=None):
        sent.append("api")
        return _api_frame()

    monkeypatch.setattr(_routing, "get_ogc_data", lambda *a, **k: (api(*a, **k), "md"))
    monkeypatch.setattr(
        _routing, "read", lambda query: sent.append("files") or (None, None)
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


@pytest.mark.parametrize(
    "extra", [{"filter": "value > 1"}, {"max_rows": 10}], ids=["filter", "max_rows"]
)
def test_auto_sends_a_query_only_the_api_accepts_there_without_the_id(
    monkeypatch, extra
):
    """The id column does not come and go with the arguments a query passes."""
    sent = _route_recorder(monkeypatch)
    with dataretrieval.configure(AUTO):
        df, _ = waterdata.get_daily(monitoring_location_id=_HUNDRED, **extra)
    assert sent == ["api"]
    assert "daily_id" not in df.columns


@pytest.mark.parametrize("name", ["id", "daily_id"])
def test_auto_keeps_the_id_that_properties_names(monkeypatch, name):
    sent = _route_recorder(monkeypatch)
    with dataretrieval.configure(AUTO):
        df, _ = waterdata.get_daily(
            monitoring_location_id=_HUNDRED, properties=[name, "value"]
        )
    assert sent == ["api"]
    assert "daily_id" in df.columns


def test_off_pages_the_api_for_a_large_query(monkeypatch):
    sent = _route_recorder(monkeypatch)
    with dataretrieval.configure(OFF):
        df, _ = waterdata.get_daily(monitoring_location_id=_HUNDRED)
    assert sent == ["api"]
    assert "daily_id" in df.columns


def _without_pyarrow(monkeypatch):
    find_spec = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *a: None if name == "pyarrow" else find_spec(name, *a),
    )


@pytest.mark.parametrize("time", ["2000-01-01/..", "../2026-01-01"])
def test_without_pyarrow_a_time_only_the_files_answer_raises_before_any_request(
    monkeypatch, time
):
    """The API refuses a continuous time over 1,100 days or with an open start,
    so paging it instead would end in HTTP 400 with no remedy."""
    sent = _route_recorder(monkeypatch)
    _without_pyarrow(monkeypatch)
    with dataretrieval.configure(AUTO), pytest.raises(ValueError, match="pyarrow"):
        waterdata.get_continuous(monitoring_location_id="USGS-01", time=time)
    assert sent == []


def test_without_pyarrow_the_files_path_pages_the_api_and_warns(monkeypatch):
    sent = _route_recorder(monkeypatch)
    _without_pyarrow(monkeypatch)
    with (
        dataretrieval.configure(AUTO),
        pytest.warns(UserWarning, match=r"dataretrieval\[pyarrow\]") as caught,
    ):
        waterdata.get_daily(monitoring_location_id=_HUNDRED)
    assert sent == ["api"]
    # Attributed to the caller's line, not to the package.
    assert caught[0].filename == __file__


def test_the_default_is_auto(monkeypatch):
    sent = _route_recorder(monkeypatch)
    waterdata.get_daily(monitoring_location_id=_HUNDRED, parameter_code="00060")
    assert sent == ["files"]
