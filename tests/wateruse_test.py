"""Offline tests for :mod:`dataretrieval.wateruse`.

All HTTP is mocked with ``pytest-httpx``; no live calls (per AGENTS.md).
"""

import re
from urllib.parse import parse_qs, urlsplit

import httpx
import pandas as pd
import pytest

import dataretrieval
from dataretrieval import wateruse
from dataretrieval.utils import BaseMetadata
from dataretrieval.wateruse import _next_page_url, _resolve_locations, get_wateruse

# Match the NWDC endpoint regardless of query string, so assertions can drill
# into the captured params without coupling registration to param order.
WU_RE = re.compile(r"^https://api\.water\.usgs\.gov/nwaa-data/data(\?.*)?$")

# A single-page monthly CSV: two HUC12s (one with a leading zero), three months.
_CSV_PAGE = """\
huc12_id,year_month,pswdgw_mgd,pswdsw_mgd,pswdtot_mgd
010900020502,2020-01,0.0,0.8313625,0.8313625
010900020502,2020-02,0.0,0.8977986,0.8977986
180600060101,2020-01,1.5,0.5,2.0
"""

# Two pages used for pagination tests; each page is its own CSV (own header).
_CSV_P1 = """\
huc12_id,year_month,pswdtot_mgd
010900020502,2020-01,0.8313625
010900020503,2020-01,0.0
"""
_CSV_P2 = """\
huc12_id,year_month,pswdtot_mgd
010900020504,2020-01,1.25
"""


def test_get_wateruse_single_page(httpx_mock):
    """Happy path: CSV parsed to a long frame; returns (df, BaseMetadata)."""
    httpx_mock.add_response(method="GET", url=WU_RE, text=_CSV_PAGE)

    df, md = get_wateruse(
        model="wu-public-supply-wd",
        variable=["pswdtot", "pswdgw", "pswdsw"],
        state="RI",
        start_date="2020-01",
        time_resolution="monthly",
    )

    assert isinstance(df, pd.DataFrame)
    assert isinstance(md, BaseMetadata)
    assert list(df.columns) == [
        "huc12_id",
        "year_month",
        "pswdgw_mgd",
        "pswdsw_mgd",
        "pswdtot_mgd",
    ]
    assert len(df) == 3


def test_huc12_id_kept_as_string_with_leading_zero(httpx_mock):
    """The HUC12 identifier must not be coerced to int (leading zeros matter)."""
    httpx_mock.add_response(method="GET", url=WU_RE, text=_CSV_PAGE)

    df, _ = get_wateruse(model="wu-public-supply-wd", state="RI")

    # String-typed (object or the pandas StringDtype, depending on version),
    # never coerced to int — the leading zero must survive.
    assert pd.api.types.is_string_dtype(df["huc12_id"])
    assert df["huc12_id"].iloc[0] == "010900020502"


def test_variables_are_comma_joined(httpx_mock):
    """A list of variables is sent as one comma-joined query parameter."""
    httpx_mock.add_response(method="GET", url=WU_RE, text=_CSV_PAGE)

    get_wateruse(
        model="wu-public-supply-wd",
        variable=["pswdtot", "pswdgw", "pswdsw"],
        state="RI",
    )

    qs = parse_qs(urlsplit(str(httpx_mock.get_requests()[0].url)).query)
    assert qs["variable"] == ["pswdtot,pswdgw,pswdsw"]
    assert qs["format"] == ["csv"]


def test_unset_params_are_dropped(httpx_mock):
    """Params left as None are omitted (the service rejects empty values)."""
    httpx_mock.add_response(method="GET", url=WU_RE, text=_CSV_PAGE)

    get_wateruse(model="wu-public-supply-wd", state="RI")

    qs = parse_qs(urlsplit(str(httpx_mock.get_requests()[0].url)).query)
    assert "enddate" not in qs
    assert "variable" not in qs
    assert "timeres" not in qs
    # Defaulted params are still present.
    assert qs["intersection"] == ["overlap"]
    assert qs["limit"] == ["600"]


def test_snake_case_date_params_map_to_nwdc_wire_names(httpx_mock):
    """The public snake_case params (``start_date`` / ``end_date`` /
    ``time_resolution``) are sent under the NWDC's compact wire names
    (``startdate`` / ``enddate`` / ``timeres``)."""
    httpx_mock.add_response(method="GET", url=WU_RE, text=_CSV_PAGE)

    get_wateruse(
        model="wu-public-supply-wd",
        state="RI",
        start_date="2020-01",
        end_date="2020-12",
        time_resolution="monthly",
    )

    qs = parse_qs(urlsplit(str(httpx_mock.get_requests()[0].url)).query)
    assert qs["startdate"] == ["2020-01"]
    assert qs["enddate"] == ["2020-12"]
    assert qs["timeres"] == ["monthly"]


def test_pagination_follows_link_header_and_concatenates(httpx_mock):
    """Pages are followed via the ``rel="next"`` Link header and concatenated."""
    httpx_mock.add_response(
        method="GET",
        url=WU_RE,
        text=_CSV_P1,
        headers={
            "link": (
                "<https://api.water.usgs.gov/nwaa-data/data"
                '?model=wu-public-supply-wd&skip=2>; rel="next"'
            )
        },
    )
    httpx_mock.add_response(method="GET", url=WU_RE, text=_CSV_P2)

    df, _ = get_wateruse(model="wu-public-supply-wd", state="RI")

    # 2 rows from page 1 + 1 row from page 2, reindexed.
    assert len(df) == 3
    assert df["huc12_id"].tolist() == [
        "010900020502",
        "010900020503",
        "010900020504",
    ]
    assert list(df.index) == [0, 1, 2]
    assert len(httpx_mock.get_requests()) == 2
    # The second request carries the Link's ``skip`` offset, not the originals.
    second_qs = parse_qs(urlsplit(str(httpx_mock.get_requests()[1].url)).query)
    assert second_qs["skip"] == ["2"]


def test_pagination_rewrites_bare_host(httpx_mock):
    """A next link on the bare ``water.usgs.gov`` host is routed to the API."""
    httpx_mock.add_response(
        method="GET",
        url=WU_RE,
        text=_CSV_P1,
        headers={
            "link": (
                "<https://water.usgs.gov/nwaa-data/data"
                '?model=wu-public-supply-wd&skip=2>; rel="next"'
            )
        },
    )
    httpx_mock.add_response(method="GET", url=WU_RE, text=_CSV_P2)

    get_wateruse(model="wu-public-supply-wd", state="RI")

    second = httpx_mock.get_requests()[1]
    assert second.url.host == "api.water.usgs.gov"


def test_http_error_raises_typed_exception_with_detail(httpx_mock):
    """A 4xx response surfaces as a typed error carrying the NWDC ``detail``."""
    httpx_mock.add_response(
        method="GET",
        url=WU_RE,
        status_code=400,
        json={"detail": "Invalid model name: bad-model"},
    )

    with pytest.raises(dataretrieval.DataRetrievalError, match="Invalid model name"):
        get_wateruse(model="bad-model", state="RI")


def test_empty_response_body_raises_typed_error(httpx_mock):
    """An empty 200 body becomes a typed error, not a bare pandas EmptyDataError."""
    httpx_mock.add_response(method="GET", url=WU_RE, text="")

    with pytest.raises(dataretrieval.DataRetrievalError, match="empty response"):
        get_wateruse(model="wu-public-supply-wd", state="RI")


def test_cyclic_next_link_terminates(httpx_mock):
    """A non-advancing/cyclic ``next`` cursor must not loop forever."""
    # Page 1 points to a "next" URL; page 2 points back to that SAME URL.
    cyclic = (
        "<https://api.water.usgs.gov/nwaa-data/data"
        '?model=wu-public-supply-wd&skip=2>; rel="next"'
    )
    httpx_mock.add_response(
        method="GET", url=WU_RE, text=_CSV_P1, headers={"link": cyclic}
    )
    httpx_mock.add_response(
        method="GET", url=WU_RE, text=_CSV_P2, headers={"link": cyclic}
    )

    df, _ = get_wateruse(model="wu-public-supply-wd", state="RI")

    # Fetches page 1 + the cyclic page once, then breaks on the repeat — it must
    # return (not hang) with the two pages collected.
    assert len(df) == 3
    assert len(httpx_mock.get_requests()) == 2


def test_uses_shared_default_headers(httpx_mock):
    """Requests carry the shared dataretrieval User-Agent (per _default_headers)."""
    httpx_mock.add_response(method="GET", url=WU_RE, text=_CSV_PAGE)

    get_wateruse(model="wu-public-supply-wd", state="RI")

    sent = httpx_mock.get_requests()[0]
    assert sent.headers["User-Agent"].startswith("python-dataretrieval/")


def test_state_selector_builds_location_query(httpx_mock):
    """``state=`` is resolved to the wire ``location=stateCd:<postal>`` param."""
    httpx_mock.add_response(method="GET", url=WU_RE, text=_CSV_PAGE)

    get_wateruse(model="wu-public-supply-wd", state="Rhode Island")

    qs = parse_qs(urlsplit(str(httpx_mock.get_requests()[0].url)).query)
    assert qs["location"] == ["stateCd:RI"]


def test_multiple_states_fan_out_preserves_input_order(httpx_mock):
    """A list selector fans out one request per location and concatenates the
    results in the order given — even though the requests run concurrently and
    may reach the server out of order. Each location is routed to its own
    response so attribution is deterministic regardless of arrival order."""
    httpx_mock.add_response(
        method="GET", url=re.compile(r".*location=stateCd%3ARI.*"), text=_CSV_P1
    )
    httpx_mock.add_response(
        method="GET", url=re.compile(r".*location=stateCd%3AWI.*"), text=_CSV_P2
    )

    df, _ = get_wateruse(model="wu-public-supply-wd", state=["RI", "Wisconsin"])

    # RI's rows (_CSV_P1) precede WI's (_CSV_P2) regardless of which request the
    # thread pool dispatched first.
    assert df["huc12_id"].tolist() == [
        "010900020502",
        "010900020503",
        "010900020504",
    ]
    reqs = httpx_mock.get_requests()
    assert len(reqs) == 2
    assert {parse_qs(urlsplit(str(r.url)).query)["location"][0] for r in reqs} == {
        "stateCd:RI",
        "stateCd:WI",
    }


def test_fan_out_is_serial_when_concurrency_is_one(httpx_mock, monkeypatch):
    """``MAX_CONCURRENT_REQUESTS = 1`` still fans out correctly (serial path)."""
    monkeypatch.setattr(wateruse, "MAX_CONCURRENT_REQUESTS", 1)
    httpx_mock.add_response(
        method="GET", url=re.compile(r".*location=stateCd%3ARI.*"), text=_CSV_P1
    )
    httpx_mock.add_response(
        method="GET", url=re.compile(r".*location=stateCd%3AWI.*"), text=_CSV_P2
    )

    df, _ = get_wateruse(model="wu-public-supply-wd", state=["RI", "WI"])

    assert len(df) == 3
    assert len(httpx_mock.get_requests()) == 2


def test_fan_out_surfaces_final_rate_limit_header(httpx_mock):
    """``md.header`` reports the lowest (latest) remaining quota across the fan-out,
    not the first request's value."""
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*location=stateCd%3ARI.*"),
        text=_CSV_P1,
        headers={"x-ratelimit-remaining": "900"},
    )
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*location=stateCd%3AWI.*"),
        text=_CSV_P2,
        headers={"x-ratelimit-remaining": "850"},
    )

    _, md = get_wateruse(model="wu-public-supply-wd", state=["RI", "WI"])

    assert md.header["x-ratelimit-remaining"] == "850"


# (response aggregation now reuses ogc.planning._combine_chunk_responses; the
# integration test above pins the rate-limit-header behavior end-to-end.)


# --- _resolve_locations unit tests (no HTTP) -------------------------------


def test_resolve_locations_state_accepts_name_postal_fips():
    # All three encodings normalize to the two-letter postal code stateCd wants.
    assert _resolve_locations("Rhode Island", None, None) == ["stateCd:RI"]
    assert _resolve_locations("ri", None, None) == ["stateCd:RI"]
    assert _resolve_locations("44", None, None) == ["stateCd:RI"]
    assert _resolve_locations(44, None, None) == ["stateCd:RI"]


def test_resolve_locations_county_five_digit_fips():
    assert _resolve_locations(None, "55025", None) == ["countyCd:55025"]


@pytest.mark.parametrize(
    "code,expected",
    [
        ("04", "huc2:04"),
        ("0109", "huc4:0109"),
        ("07070005", "huc8:07070005"),
        ("010900020502", "huc12:010900020502"),
    ],
)
def test_resolve_locations_huc_level_from_length(code, expected):
    assert _resolve_locations(None, None, code) == [expected]


def test_resolve_locations_accepts_lists():
    assert _resolve_locations(["RI", "Wisconsin"], None, None) == [
        "stateCd:RI",
        "stateCd:WI",
    ]
    assert _resolve_locations(None, ["55025", "55021"], None) == [
        "countyCd:55025",
        "countyCd:55021",
    ]
    assert _resolve_locations(None, None, ["04", "070700"]) == [
        "huc2:04",
        "huc6:070700",
    ]


def test_resolve_locations_requires_exactly_one():
    with pytest.raises(ValueError, match="exactly one"):
        _resolve_locations(None, None, None)
    with pytest.raises(ValueError, match="exactly one"):
        _resolve_locations("RI", "55025", None)


def test_resolve_locations_empty_list_rejected():
    with pytest.raises(ValueError, match="empty"):
        _resolve_locations([], None, None)


def test_resolve_locations_rejects_malformed_selectors():
    with pytest.raises(ValueError):  # unrecognized state
        _resolve_locations("Atlantis", None, None)
    with pytest.raises(ValueError, match="five-digit"):  # county not 5 digits
        _resolve_locations(None, "025", None)
    with pytest.raises(ValueError, match="hydrologic unit"):  # odd-length huc
        _resolve_locations(None, None, "123")


# --- _next_page_url unit tests (no HTTP) -----------------------------------


def test_next_page_url_none_when_no_link():
    resp = httpx.Response(200, text="")
    assert _next_page_url(resp) is None


def test_next_page_url_none_when_link_has_no_next():
    resp = httpx.Response(
        200,
        text="",
        headers={"link": '<https://api.water.usgs.gov/x>; rel="prev"'},
    )
    assert _next_page_url(resp) is None


def test_next_page_url_rewrites_bare_host():
    resp = httpx.Response(
        200,
        text="",
        headers={
            "link": '<https://water.usgs.gov/nwaa-data/data?skip=600>; rel="next"'
        },
    )
    assert _next_page_url(resp) == (
        "https://api.water.usgs.gov/nwaa-data/data?skip=600"
    )


def test_next_page_url_leaves_api_host_untouched():
    url = "https://api.water.usgs.gov/nwaa-data/data?skip=600"
    resp = httpx.Response(200, text="", headers={"link": f'<{url}>; rel="next"'})
    # Must not double-prefix into ``api.api.water.usgs.gov``.
    assert _next_page_url(resp) == url


def test_module_exposes_catalog_constants():
    assert "wu-public-supply-wd" in wateruse.MODELS
    assert set(wateruse.TIME_RESOLUTIONS) == {"monthly", "annualcy", "annualwy"}
