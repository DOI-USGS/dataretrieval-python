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


# --- URL-length chunking -------------------------------------------------
# The catalog edge answers HTTP 403 to a search URL over 2,094 characters.

EDGE_LIMIT = 2094


def _long_ids(n):
    return [f"OH015-{383638000000 + i:015d}" for i in range(n)]


def _ids_in(request):
    filter_str = parse_qs(urlsplit(str(request.url)).query)["filter"][0]
    in_clause = filter_str.split("monitoring_location_id IN (")[1].split(")")[0]
    return re.findall(r"'([^']+)'", in_clause)


def _echo_ids(request):
    import httpx

    features = [_daily_item(i) for i in _ids_in(request)]
    return httpx.Response(200, json={"features": features})


def test_many_long_ids_are_split_across_searches_that_fit_the_edge(httpx_mock):
    ids = _long_ids(200)
    assert len(ids[0]) == 21
    httpx_mock.add_callback(_echo_ids, url=SEARCH_RE, is_reusable=True)

    df, _ = get_parquet_files("daily", monitoring_location_id=ids[::-1])

    requests = httpx_mock.get_requests()
    assert len(requests) > 1
    assert all(len(str(r.url)) <= EDGE_LIMIT for r in requests)
    sent = [i for r in requests for i in _ids_in(r)]
    assert sorted(sent) == ids
    assert df["monitoring_location_id"].tolist() == ids


def test_a_short_id_list_makes_one_search(httpx_mock):
    httpx_mock.add_callback(_echo_ids, url=SEARCH_RE)

    df, _ = get_parquet_files("daily", monitoring_location_id=["USGS-02", "USGS-01"])

    assert len(httpx_mock.get_requests()) == 1
    assert df["monitoring_location_id"].tolist() == ["USGS-01", "USGS-02"]


def test_the_user_filter_and_dates_are_kept_in_every_chunk(httpx_mock):
    httpx_mock.add_callback(_echo_ids, url=SEARCH_RE, is_reusable=True)

    get_parquet_files(
        "daily",
        monitoring_location_id=_long_ids(120),
        filter="state_code = '39'",
        time=["2004-01-01", None],
        bbox=[-85.0, 38.0, -80.0, 42.0],
    )

    requests = httpx_mock.get_requests()
    assert len(requests) > 1
    for i in range(len(requests)):
        query = _sent_query(httpx_mock, i)
        assert query["filter"][0].startswith("(state_code = '39') AND ")
        assert query["datetime"] == ["2004-01-01T00:00:00Z/.."]
        assert query["bbox"] == ["-85.0,38.0,-80.0,42.0"]
        assert len(str(requests[i].url)) <= EDGE_LIMIT


def test_a_repeated_id_is_sent_once(httpx_mock):
    httpx_mock.add_callback(_echo_ids, url=SEARCH_RE)

    df, _ = get_parquet_files("daily", monitoring_location_id=["USGS-01"] * 3)

    assert _ids_in(httpx_mock.get_requests()[0]) == ["USGS-01"]
    assert len(df) == 1


def test_a_user_filter_over_the_url_limit_raises_before_any_request(httpx_mock):
    too_long = " OR ".join(f"state_code = '{i:02d}'" for i in range(200))

    with pytest.raises(ValueError, match="2,094-character") as exc:
        get_parquet_files("daily", filter=too_long)
    with pytest.raises(ValueError, match="2,094-character"):
        get_parquet_files("daily", filter=too_long, monitoring_location_id=["USGS-01"])

    assert "split the call" in str(exc.value)
    assert httpx_mock.get_requests() == []


def test_ratings_with_many_ids_are_chunked(httpx_mock):
    ids = [f"USGS-{1000000 + i:08d}" for i in range(300)]
    httpx_mock.add_callback(_echo_ids, url=SEARCH_RE, is_reusable=True)

    features = get_ratings(
        monitoring_location_id=ids, file_type="exsa", download_and_parse=False
    )

    requests = httpx_mock.get_requests()
    assert len(requests) > 1
    assert all(len(str(r.url)) <= EDGE_LIMIT for r in requests)
    assert all(
        parse_qs(urlsplit(str(r.url)).query)["collections"] == ["ratings"]
        for r in requests
    )
    assert all(
        parse_qs(urlsplit(str(r.url)).query)["filter"][0].endswith(
            " AND file_type = 'exsa'"
        )
        for r in requests
    )
    assert sorted(i for r in requests for i in _ids_in(r)) == ids
    assert len(features) == len(ids)


def test_ratings_without_ids_makes_one_search_and_checks_the_url_length(httpx_mock):
    httpx_mock.add_response(url=SEARCH_RE, json={"features": []})
    get_ratings(bbox=[-95.0, 40.0, -92.0, 42.0], download_and_parse=False)
    assert len(httpx_mock.get_requests()) == 1

    with pytest.raises(ValueError, match="2,094-character"):
        get_ratings(bbox=[0.123456789] * 4 * 60, download_and_parse=False)
    assert len(httpx_mock.get_requests()) == 1
