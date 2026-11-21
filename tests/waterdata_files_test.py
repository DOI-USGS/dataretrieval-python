"""``waterdata.get_parquet_files``: listing the STAC-indexed Parquet files."""

import re
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest

from dataretrieval.waterdata import get_parquet_files, get_ratings
from dataretrieval.waterdata.files import _build_filter

SEARCH = "https://api.waterdata.usgs.gov/stac/v0/search"
SEARCH_RE = re.compile(rf"^{re.escape(SEARCH)}(\?.*)?$")
FILES = "https://api.waterdata.usgs.gov/stac-files"


def _item(site, assets, start="2000-01-01T00:00:00Z", end="2026-11-20T00:00:00Z"):
    return {
        "id": site,
        "properties": {
            "monitoring_location_id": site,
            "start_datetime": start,
            "end_datetime": end,
        },
        "assets": assets,
    }


def _daily_item(site, **kwargs):
    href = f"{FILES}/daily/{site}/{site}.parquet"
    return _item(site, {"data": {"href": href, "roles": ["data"]}}, **kwargs)


def _sent_query(httpx_mock, index=0):
    return parse_qs(urlsplit(str(httpx_mock.get_requests()[index].url)).query)


def test_daily_files_one_row_per_location_sorted(httpx_mock):
    httpx_mock.add_response(
        url=SEARCH_RE,
        json={"features": [_daily_item("USGS-02"), _daily_item("USGS-01")]},
    )

    df, md = get_parquet_files("daily", monitoring_location_id=["USGS-01", "USGS-02"])

    assert list(df.columns) == [
        "monitoring_location_id",
        "asset",
        "url",
        "start_datetime",
        "end_datetime",
    ]
    assert df["monitoring_location_id"].tolist() == ["USGS-01", "USGS-02"]
    assert df["url"].tolist() == [
        f"{FILES}/daily/USGS-01/USGS-01.parquet",
        f"{FILES}/daily/USGS-02/USGS-02.parquet",
    ]
    assert (df["asset"] == "data").all()
    assert df["start_datetime"].iloc[0] == pd.Timestamp("2000-01-01", tz="UTC")
    assert md.url.startswith(SEARCH)


def test_search_is_scoped_to_the_collection_and_sends_every_filter(httpx_mock):
    httpx_mock.add_response(url=SEARCH_RE, json={"features": []})

    get_parquet_files(
        "continuous",
        monitoring_location_id="USGS-01435000",
        time=["2004-01-01", None],
        bbox=[-85.0, 38.0, -80.0, 42.0],
        filter="state_code = '39' OR state_code = '18'",
    )

    query = _sent_query(httpx_mock)
    assert query["collections"] == ["continuous"]
    # The caller's OR stays inside its own parentheses.
    assert query["filter"] == [
        "(state_code = '39' OR state_code = '18') AND "
        "monitoring_location_id IN ('USGS-01435000')"
    ]
    assert query["datetime"] == ["2004-01-01T00:00:00Z/.."]
    assert query["bbox"] == ["-85.0,38.0,-80.0,42.0"]


def test_continuous_lists_current_and_historical_but_no_other_asset(httpx_mock):
    site = "USGS-01435000"
    base = f"{FILES}/continuous/{site}/{site}"
    assets = {
        "current": {"href": f"{base}-hot.parquet", "roles": ["data", "current"]},
        "historical": {"href": f"{base}.parquet", "roles": ["data", "historical"]},
        "thumbnail": {"href": f"{base}.png", "roles": ["thumbnail"]},
        "broken": {"roles": ["data"]},
    }
    httpx_mock.add_response(url=SEARCH_RE, json={"features": [_item(site, assets)]})

    df, _ = get_parquet_files("continuous", monitoring_location_id=site)

    assert df["asset"].tolist() == ["current", "historical"]
    assert df["url"].tolist() == [f"{base}-hot.parquet", f"{base}.parquet"]


def test_every_search_page_is_followed(httpx_mock):
    next_url = f"{SEARCH}?collections=daily&limit=1&token=next%3Adaily%3AUSGS-02"
    httpx_mock.add_response(
        url=SEARCH_RE,
        json={
            "features": [_daily_item("USGS-01")],
            "links": [{"rel": "next", "href": next_url}],
        },
    )
    httpx_mock.add_response(url=next_url, json={"features": [_daily_item("USGS-02")]})

    df, _ = get_parquet_files("daily", limit=1)

    assert df["monitoring_location_id"].tolist() == ["USGS-01", "USGS-02"]


def test_no_match_returns_an_empty_frame_with_the_columns(httpx_mock):
    httpx_mock.add_response(url=SEARCH_RE, json={"features": []})

    df, _ = get_parquet_files("daily", monitoring_location_id="USGS-00000000")

    assert df.empty
    assert "url" in df.columns
    assert str(df["start_datetime"].dtype).startswith("datetime64")


def test_rejects_a_collection_without_parquet_files():
    with pytest.raises(ValueError, match="collection"):
        get_parquet_files("ratings")  # type: ignore[arg-type]


def test_rejects_a_duration_before_any_request():
    with pytest.raises(ValueError, match=r"durations.*not supported"):
        get_parquet_files("daily", time="P7D")


@pytest.mark.parametrize("blank", [None, "", "  "])
def test_build_filter_ignores_a_blank_filter(blank):
    assert _build_filter("USGS-01", blank) == "monitoring_location_id IN ('USGS-01')"
    assert _build_filter(None, blank) is None


def test_ratings_search_is_scoped_to_the_ratings_collection(httpx_mock):
    """``/search`` spans every collection. Unscoped, a ratings search for one
    location also returned its ``daily`` and ``continuous`` items."""
    httpx_mock.add_response(url=SEARCH_RE, json={"features": []})

    get_ratings(
        monitoring_location_id="USGS-01104475",
        file_type=["exsa", "corr"],
        download_and_parse=False,
    )

    assert _sent_query(httpx_mock)["collections"] == ["ratings"]
