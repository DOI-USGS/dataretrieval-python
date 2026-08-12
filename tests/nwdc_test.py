"""Offline tests for :mod:`dataretrieval.nwdc`.

All HTTP is mocked with ``pytest-httpx``; no live calls (per AGENTS.md).
"""

import re
import socket
import warnings
from urllib.parse import parse_qs, urlsplit

import httpx
import pandas as pd
import pytest

import dataretrieval
from dataretrieval import nwdc, settings
from dataretrieval import progress as _progress
from dataretrieval.exceptions import DataRetrievalError
from dataretrieval.nwdc import _next_page_url, _resolve_locations, get_wateruse
from dataretrieval.transport import fanout as _fanout
from dataretrieval.utils import BaseMetadata

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
    """``API_USGS_CONCURRENT=1`` still fans out correctly (serial path)."""
    monkeypatch.setenv("API_USGS_CONCURRENT", "1")
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


# (response aggregation uses combining._combine_chunk_responses; the
# integration test above pins the rate-limit-header behavior end-to-end.)


def test_fan_out_failure_never_returns_partial_data(httpx_mock):
    """A failed location aborts the call even when another location succeeded.

    The completed sibling is not returned as though the call had succeeded --
    it is carried on the raised interruption for ``resume()`` instead. Water Use
    reports ``ServiceInterrupted`` rather than the bare ``ServiceUnavailable``
    it raised before sharing the fan-out executor: the same upstream 503, now
    resumable.
    """
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*location=stateCd%3ARI.*"),
        text=_CSV_P1,
    )
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*location=stateCd%3AWI.*"),
        status_code=503,
        json={"detail": "temporarily unavailable"},
    )

    with pytest.raises(dataretrieval.ServiceInterrupted) as excinfo:
        get_wateruse(model="wu-public-supply-wd", state=["RI", "WI"])

    # The 503 is still the reported cause, and the successful location survives
    # on the exception rather than being passed off as the whole answer.
    assert isinstance(excinfo.value.__cause__, dataretrieval.ServiceUnavailable)
    assert excinfo.value.status_code == 503
    assert excinfo.value.retryable
    assert excinfo.value.completed_chunks == 1
    assert excinfo.value.total_chunks == 2
    assert len(excinfo.value.partial_frame) == 2


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


def test_next_page_url_normalizes_other_spellings_of_the_same_service():
    """The cursor is normalized by host, not by one literal prefix.

    A plain-http or relative ``next`` link is the same service; refusing it
    would throw away every page already collected for that location.
    """
    plain_http = httpx.Response(
        200,
        text="",
        headers={"link": '<http://water.usgs.gov/nwaa-data/data?skip=600>; rel="next"'},
    )
    assert _next_page_url(plain_http) == (
        "https://api.water.usgs.gov/nwaa-data/data?skip=600"
    )

    relative = httpx.Response(
        200,
        text="",
        headers={"link": '</nwaa-data/data?skip=600>; rel="next"'},
        request=httpx.Request("GET", "https://api.water.usgs.gov/nwaa-data/data"),
    )
    assert _next_page_url(relative) == (
        "https://api.water.usgs.gov/nwaa-data/data?skip=600"
    )


def test_next_page_url_strips_credentials_from_the_cursor():
    """Userinfo on a cursor must not become an Authorization header.

    httpx derives ``Authorization: Basic ...`` from a URL's userinfo, so a
    cursor spelled ``http://user:pass@water.usgs.gov/...`` would send a
    credential the caller never configured to the rewritten host -- exactly what
    the host check exists to prevent, arriving through the host check's own
    normalization. The port is dropped for the same reason.
    """
    response = httpx.Response(
        200,
        text="",
        headers={
            "link": (
                "<http://attacker:s3cret@water.usgs.gov:8080"
                '/nwaa-data/data?skip=600>; rel="next"'
            )
        },
    )

    cursor = _next_page_url(response)

    assert cursor == "https://api.water.usgs.gov/nwaa-data/data?skip=600"
    assert "s3cret" not in cursor
    assert httpx.URL(cursor).userinfo == b""

    # Assert at the layer that actually synthesizes the header: ``httpx.Request``
    # never derives Basic auth from userinfo (so asserting there would pass for
    # any URL) -- the ``Client`` does it at send time.
    sent: dict[str, str | None] = {}

    def capture(request: httpx.Request) -> httpx.Response:
        sent["auth"] = request.headers.get("Authorization")
        return httpx.Response(200, text="")

    with httpx.Client(transport=httpx.MockTransport(capture)) as client:
        client.get(cursor)
    assert sent["auth"] is None


def test_a_configured_base_url_redirects_the_request(httpx_mock):
    """The whole call moves, page walk included, or the redirect is a half-truth.

    The page-two mock is served from the mirror and its cursor names the mirror:
    if either the request or the ``rel="next"`` walk had stayed on the NWDC's
    host, one of them would go unmocked and this would fail rather than quietly
    talk to the service the block redirected away from.
    """
    mirror = re.compile(r"^https://mirror\.example/data")
    httpx_mock.add_response(
        method="GET",
        url=mirror,
        text=_CSV_P1,
        headers={"link": '<https://mirror.example/data?skip=2>; rel="next"'},
    )
    httpx_mock.add_response(method="GET", url=mirror, text=_CSV_P2)

    with dataretrieval.configure(
        nwdc.NwdcSettings(base_url="https://mirror.example/data")
    ):
        df, _ = get_wateruse(model="wu-public-supply-wd", state="RI")

    assert len(df) == 3
    assert [urlsplit(str(r.url)).netloc for r in httpx_mock.get_requests()] == [
        "mirror.example",
        "mirror.example",
    ]


def test_next_page_url_drops_the_service_rewrite_when_redirected():
    """The alias list and the rewrite are facts about the NWDC, not about URLs.

    Nothing but the NWDC answers for ``water.usgs.gov``, so a call an
    ``NwdcSettings(base_url=...)`` pointed elsewhere gets the general rule
    instead: follow a link only back to the host that served the page. Keeping
    the rewrite would send page two of a mirrored query to the USGS -- and
    refusing the mirror's own cursor would throw away page one.
    """
    mirrored = httpx.Response(
        200,
        text="",
        headers={"link": '<https://mirror.example/data?skip=600>; rel="next"'},
        request=httpx.Request("GET", "https://mirror.example/data"),
    )

    assert _next_page_url(mirrored, host="mirror.example") == (
        "https://mirror.example/data?skip=600"
    )

    # A cursor back to the real service is now the cross-host case, refused for
    # the same reason a foreign link is refused on an ordinary call.
    strayed = httpx.Response(
        200,
        text="",
        headers={"link": '<https://api.water.usgs.gov/nwaa-data/data>; rel="next"'},
        request=httpx.Request("GET", "https://mirror.example/data"),
    )
    with pytest.raises(DataRetrievalError, match="cross-host"):
        _next_page_url(strayed, host="mirror.example")


def test_module_exposes_catalog_constants():
    assert "wu-public-supply-wd" in nwdc.MODELS
    assert set(nwdc.TIME_RESOLUTIONS) == {"monthly", "annualcy", "annualwy"}


def test_initial_transient_is_retried(httpx_mock, monkeypatch):
    """Water Use retries an initial transient without holding its semaphore."""
    import dataretrieval.transport.retry as retry

    url = re.compile(r".*location=stateCd%3ARI.*")
    httpx_mock.add_response(method="GET", url=url, status_code=503)
    httpx_mock.add_response(method="GET", url=url, text=_CSV_P1)
    monkeypatch.setenv("API_USGS_RETRIES", "1")
    monkeypatch.setattr(retry, "_RETRY_BASE_BACKOFF", 0.0)
    monkeypatch.setattr(retry, "_RETRY_MAX_BACKOFF", 0.0)

    df, _ = get_wateruse(model="wu-public-supply-wd", state="RI")

    assert len(df) == 2
    assert len(httpx_mock.get_requests()) == 2


def test_fatal_failure_waits_for_siblings_before_closing_the_client(monkeypatch):
    """A fan-out failure must not close the client under its own siblings.

    Every location shares one ``httpx.AsyncClient`` scoped to the fan-out. When
    the first failure propagated straight out of the ``gather``, that block
    exited while siblings were still walking pages, and the next page they asked
    for failed with "Cannot send a request, as the client has been closed" -- on
    a task nobody was awaiting any more, so it also surfaced as an unretrieved
    exception. Both are artifacts of our own teardown, not of the service.
    """
    import asyncio
    from contextlib import asynccontextmanager

    pages = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("location") == "stateCd:AA":
            return httpx.Response(400, json={"detail": "Invalid model name: bad"})
        pages["n"] += 1
        # A real suspension window, deliberately: the sibling has to still be
        # mid-walk when the failure propagates, and an ``Event`` set by the
        # failing branch is already set by the time this runs -- it returns
        # without suspending, and the test then passes against the old code too.
        await asyncio.sleep(0.05)
        if pages["n"] == 1:
            return httpx.Response(
                200,
                text=_CSV_P1,
                headers={
                    "link": (
                        "<https://api.water.usgs.gov/nwaa-data/data"
                        '?location=stateCd%3ABB&skip=2>; rel="next"'
                    )
                },
            )
        return httpx.Response(200, text=_CSV_P2)

    @asynccontextmanager
    async def open_mock_client(**overrides):
        overrides.pop("verify", None)
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), **overrides
        ) as client:
            yield client

    monkeypatch.setattr(_fanout, "open_async_client", open_mock_client)

    requests = [
        httpx.Request("GET", nwdc.WATERUSE_URL, params={"location": location})
        for location in ("stateCd:AA", "stateCd:BB")
    ]

    with pytest.raises(dataretrieval.DataRetrievalError, match="Invalid model"):
        nwdc._fan_out(requests, {}, True)
    assert pages["n"] == 2, "the sibling finished its walk rather than being abandoned"


def test_next_page_url_rejects_cross_host_link():
    response = httpx.Response(
        200,
        headers={"link": '<https://outside.example/next>; rel="next"'},
    )
    # Typed, so a caller's ``except DataRetrievalError`` catches it like any
    # other failure rather than seeing a bare RuntimeError.
    with pytest.raises(dataretrieval.DataRetrievalError, match="outside.example"):
        _next_page_url(response)


# --- capabilities Water Use gained by sharing the fan-out executor ----------


def test_interrupted_fan_out_resumes_only_the_unfinished_locations(httpx_mock):
    """A rate-limited location is resumable; completed siblings are not re-fetched.

    Before Water Use shared the executor, a 429 anywhere in the fan-out
    discarded every location that had already succeeded. That is the whole
    reason a multi-location pull needed re-running from scratch against an
    hourly quota.
    """
    httpx_mock.add_response(
        method="GET", url=re.compile(r".*location=stateCd%3ARI.*"), text=_CSV_P1
    )
    # WI is rate-limited on the first pass, then succeeds once resumed.
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*location=stateCd%3AWI.*"),
        status_code=429,
        json={"detail": "rate limited"},
        is_reusable=False,
    )
    httpx_mock.add_response(
        method="GET", url=re.compile(r".*location=stateCd%3AWI.*"), text=_CSV_P2
    )

    with pytest.raises(dataretrieval.QuotaExhausted) as excinfo:
        get_wateruse(model="wu-public-supply-wd", state=["RI", "WI"])

    interrupted = excinfo.value
    assert interrupted.status_code == 429
    assert interrupted.retryable
    assert interrupted.completed_chunks == 1
    assert interrupted.total_chunks == 2
    requests_before = len(httpx_mock.get_requests())

    df, md = interrupted.call.resume()

    # Only WI was re-issued; RI's completed frame carried across the resume.
    assert len(httpx_mock.get_requests()) == requests_before + 1
    assert len(df) == 3
    assert isinstance(md, BaseMetadata)


def test_fan_out_honors_the_general_concurrency_setting(monkeypatch):
    """``API_USGS_CONCURRENT`` outranks this service's default.

    A user dialing concurrency down to be polite must not find Water Use
    quietly ignoring them -- the defect that motivated consolidating the knob.
    """
    monkeypatch.setenv("API_USGS_CONCURRENT", "7")
    assert settings.concurrency(nwdc.DEFAULT_CONCURRENT_REQUESTS) == 7

    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    assert (
        settings.concurrency(nwdc.DEFAULT_CONCURRENT_REQUESTS)
        == nwdc.DEFAULT_CONCURRENT_REQUESTS
    )
    # The service default is deliberately below the package-wide 32.
    assert nwdc.DEFAULT_CONCURRENT_REQUESTS < settings.DEFAULT_CONCURRENCY


def test_fan_out_reports_progress(httpx_mock, monkeypatch):
    """The fan-out ticks the progress reporter, which it never did standalone."""
    seen = []

    class _Recorder:
        def set_chunks(self, total):
            seen.append(("chunks", total))

        def start_chunk(self, completed):
            seen.append(("chunk", completed))

        def set_rate_remaining(self, remaining, limit=None):
            pass

        def add_page(self, rows):
            seen.append(("page", rows))

    monkeypatch.setattr(_fanout._progress, "current", lambda: _Recorder())
    httpx_mock.add_response(
        method="GET", url=re.compile(r".*location=stateCd%3ARI.*"), text=_CSV_P1
    )
    httpx_mock.add_response(
        method="GET", url=re.compile(r".*location=stateCd%3AWI.*"), text=_CSV_P2
    )

    get_wateruse(model="wu-public-supply-wd", state=["RI", "WI"])

    assert ("chunks", 2) in seen
    assert ("chunk", 1) in seen and ("chunk", 2) in seen


def test_resume_uses_the_current_progress_reporter(httpx_mock, monkeypatch):
    """Resume must not resurrect the reporter closed by the interrupted call."""
    created = []

    class _Recorder:
        def __init__(self, **_kwargs):
            self.closed = False
            self.events = []
            created.append(self)

        def _record(self, event):
            assert not self.closed, "fan-out updated a closed progress reporter"
            self.events.append(event)

        def set_chunks(self, total):
            self._record(("chunks", total))

        def start_chunk(self, completed):
            self._record(("chunk", completed))

        def set_rate_remaining(self, remaining, limit=None):
            self._record(("remaining", remaining, limit))

        def add_page(self, rows):
            self._record(("page", rows))

        def close(self):
            self.closed = True

    monkeypatch.setattr(_progress, "ProgressReporter", _Recorder)
    httpx_mock.add_response(
        method="GET", url=re.compile(r".*location=stateCd%3ARI.*"), text=_CSV_P1
    )
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*location=stateCd%3AWI.*"),
        status_code=429,
        is_reusable=False,
    )
    httpx_mock.add_response(
        method="GET", url=re.compile(r".*location=stateCd%3AWI.*"), text=_CSV_P2
    )

    with pytest.raises(dataretrieval.QuotaExhausted) as excinfo:
        get_wateruse(model="wu-public-supply-wd", state=["RI", "WI"])

    assert created[0].closed
    with _progress.progress_context(service="resume") as resumed_reporter:
        _, md = excinfo.value.call.resume()

    assert isinstance(md, BaseMetadata)
    assert resumed_reporter is created[1]
    assert ("chunks", 2) in resumed_reporter.events
    assert ("chunk", 2) in resumed_reporter.events


def test_permanent_transport_failure_remains_a_network_error(monkeypatch):
    """A deterministic connection failure stays inside the public taxonomy."""

    async def fail(*_args, **_kwargs):
        resolution = socket.gaierror(socket.EAI_NONAME, "name not known")
        failure = httpx.ConnectError("name not known")
        failure.__context__ = resolution
        raise failure

    monkeypatch.setattr(nwdc, "paginate", fail)

    with pytest.raises(dataretrieval.NetworkError) as excinfo:
        get_wateruse(model="wu-public-supply-wd", state="RI")

    assert isinstance(excinfo.value.__cause__, httpx.ConnectError)


def test_permanent_later_page_failure_remains_a_network_error(httpx_mock):
    """Normalization finds transport failures nested by pagination."""
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*location=stateCd%3ARI(?!.*cursor).*"),
        text=_CSV_P1,
        headers={
            "Link": '<https://api.water.usgs.gov/nwaa-data/data?cursor=x>; rel="next"'
        },
    )
    resolution = socket.gaierror(socket.EAI_NONAME, "name not known")
    failure = httpx.ConnectError("name not known")
    failure.__context__ = resolution
    httpx_mock.add_exception(
        failure,
        method="GET",
        url=re.compile(r".*cursor=x.*"),
    )

    with pytest.raises(dataretrieval.NetworkError) as excinfo:
        get_wateruse(model="wu-public-supply-wd", state="RI")

    assert isinstance(excinfo.value.__cause__, httpx.ConnectError)


def test_mid_page_walk_transient_is_still_resumable(httpx_mock):
    """A 429 on page 2+ of a location must still be a resumable interruption.

    ``paginate`` re-wraps a later-page failure as a plain ``DataRetrievalError``
    (page 1's status check sits outside its ``try``), so the typed cause is only
    reachable through ``__cause__``. ``_classify_chunk_error`` walks that chain
    for exactly this reason; were it a single ``isinstance`` check, a mid-walk
    rate limit would escape as a bare error and lose ``.call.resume()`` --
    inconsistently, since page 1 would still be resumable.
    """
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*location=stateCd%3ARI(?!.*cursor).*"),
        text=_CSV_P1,
        headers={
            "Link": '<https://api.water.usgs.gov/nwaa-data/data?cursor=x>; rel="next"'
        },
    )
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*cursor=x.*"),
        status_code=429,
        json={"detail": "rate limited"},
    )
    httpx_mock.add_response(
        method="GET", url=re.compile(r".*location=stateCd%3AWI.*"), text=_CSV_P2
    )

    with pytest.raises(dataretrieval.QuotaExhausted) as excinfo:
        get_wateruse(model="wu-public-supply-wd", state=["RI", "WI"])

    assert excinfo.value.call is not None
    assert excinfo.value.completed_chunks == 1
    assert excinfo.value.total_chunks == 2


# ---------------------------------------------------------------------------
# Deprecated ``wateruse`` alias
# ---------------------------------------------------------------------------


def _reimport_wateruse():
    """Import the alias fresh, so its module-level warning fires again."""
    import importlib
    import sys

    sys.modules.pop("dataretrieval.wateruse", None)
    return importlib.import_module("dataretrieval.wateruse")


def test_wateruse_alias_warns_and_names_the_replacement():
    """Importing the old name is deprecated, dated, and points at ``nwdc``."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _reimport_wateruse()

    deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert len(deprecations) == 1, [str(w.message) for w in caught]
    message = str(deprecations[0].message)
    assert "`dataretrieval.wateruse` is deprecated" in message
    assert "`dataretrieval.nwdc`" in message
    # Dated removal, per the convention nwis follows.
    assert nwdc_alias_removal_date() in message


def nwdc_alias_removal_date() -> str:
    from dataretrieval.wateruse import NWDC_RENAME_REMOVAL_DATE

    return NWDC_RENAME_REMOVAL_DATE


def test_wateruse_alias_re_exports_the_same_objects():
    """The alias forwards, it does not copy: identity must survive it.

    A caller monkeypatching through one spelling and asserting through the
    other would otherwise see two different objects.
    """
    alias = _reimport_wateruse()

    assert alias.get_wateruse is nwdc.get_wateruse
    assert alias.MODELS is nwdc.MODELS
    assert alias.TIME_RESOLUTIONS is nwdc.TIME_RESOLUTIONS
    assert alias.DEFAULT_CONCURRENT_REQUESTS == nwdc.DEFAULT_CONCURRENT_REQUESTS
    assert alias.__all__ == nwdc.__all__


def test_importing_dataretrieval_does_not_warn():
    """``import dataretrieval`` must stay silent.

    The package imports ``nwdc`` directly; only code naming ``wateruse``
    itself should see the warning. If ``__init__`` ever imports the alias,
    every user of the library gets a DeprecationWarning they cannot act on.

    Runs in a subprocess: a fresh interpreter is the only honest way to test
    an import side effect, and clearing ``sys.modules`` in-process would hand
    every later test a second copy of the package.
    """
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-W",
            "error::DeprecationWarning",
            "-c",
            "import dataretrieval",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
