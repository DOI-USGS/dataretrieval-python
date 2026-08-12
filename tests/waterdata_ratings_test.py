import re
import warnings
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest

from dataretrieval.exceptions import DataRetrievalError, SkippedRatingWarning
from dataretrieval.interruptions import QuotaExhausted
from dataretrieval.waterdata import get_ratings
from dataretrieval.waterdata.ratings import _build_filter

# pytest-httpx matches URL strings exactly (including query). For the
# ratings tests we want a "match this endpoint, ignore the params"
# fixture so the assertions can drill into the captured params
# afterwards without coupling the registration to the implementation's
# parameter order. ``url=STAC_SEARCH_RE`` does that.
STAC_SEARCH_RE = re.compile(
    r"^https://api\.waterdata\.usgs\.gov/stac/v0/search(\?.*)?$"
)


def test_build_filter_single_site_single_type():
    f = _build_filter("USGS-01104475", "exsa")
    assert f == "monitoring_location_id IN ('USGS-01104475') AND file_type = 'exsa'"


def test_build_filter_multi_site_no_type():
    f = _build_filter(["USGS-A", "USGS-B"], None)
    assert f == "monitoring_location_id IN ('USGS-A', 'USGS-B')"


def test_build_filter_no_site_single_type():
    f = _build_filter(None, "corr")
    assert f == "file_type = 'corr'"


def test_build_filter_empty_returns_none():
    assert _build_filter(None, None) is None


def test_get_ratings_rejects_invalid_file_type():
    with pytest.raises(ValueError, match="Invalid file_type"):
        get_ratings(monitoring_location_id="USGS-01104475", file_type="bogus")


def test_get_ratings_rejects_iso_8601_duration_in_time():
    """STAC ratings doesn't accept ISO 8601 durations; surface a clear error."""
    with pytest.raises(ValueError, match=r"durations.*not supported"):
        get_ratings(
            monitoring_location_id="USGS-01104475",
            time="P7D",
        )


def test_build_filter_escapes_quotes():
    """Defends against malformed CQL or injection if an ID contains a quote."""
    f = _build_filter("USGS-x'-y", None)
    assert f == "monitoring_location_id IN ('USGS-x''-y')"


_SAMPLE_RDB = """\
# header line one
# header line two
agency_cd\tsite_no\tINDEP\tDEP
5s\t15s\t10n\t10n
USGS\t01104475\t0.10\t0.0
USGS\t01104475\t0.20\t0.5
USGS\t01104475\t0.30\t1.2
"""


_GOOD_ASSET = "https://api.waterdata.usgs.gov/stac-files/ratings/USGS.01104475.exsa.rdb"
_BAD_ASSET = "https://api.waterdata.usgs.gov/stac-files/ratings/USGS.99999999.exsa.rdb"


def _stub_search_response():
    return {
        "features": [
            {
                "id": "USGS-01104475.exsa.rdb",
                "properties": {"file_type": "exsa"},
                "assets": {"data": {"href": _GOOD_ASSET}},
            }
        ]
    }


def test_get_ratings_mocked_search_and_download(httpx_mock, tmp_path):
    """End-to-end happy path with mocked STAC search + RDB download."""
    httpx_mock.add_response(
        method="GET",
        url=STAC_SEARCH_RE,
        json=_stub_search_response(),
    )
    httpx_mock.add_response(method="GET", url=_GOOD_ASSET, text=_SAMPLE_RDB)

    out = get_ratings(
        monitoring_location_id="USGS-01104475",
        file_type="exsa",
        file_path=str(tmp_path),
    )
    assert "USGS-01104475.exsa.rdb" in out
    df = out["USGS-01104475.exsa.rdb"]
    assert isinstance(df, pd.DataFrame)
    assert {"INDEP", "DEP"}.issubset(df.columns)
    assert len(df) == 3

    # Server-side filter should pin the single requested file_type.
    sent = httpx_mock.get_requests()[0]
    qs = parse_qs(urlsplit(str(sent.url)).query)
    assert "file_type = 'exsa'" in qs["filter"][0]
    assert "monitoring_location_id IN ('USGS-01104475')" in qs["filter"][0]


def test_get_ratings_attaches_rdb_comment_and_url(httpx_mock, tmp_path):
    """Each parsed frame should carry its RDB header + source URL in df.attrs."""
    httpx_mock.add_response(
        method="GET",
        url=STAC_SEARCH_RE,
        json=_stub_search_response(),
    )
    httpx_mock.add_response(method="GET", url=_GOOD_ASSET, text=_SAMPLE_RDB)

    out = get_ratings(
        monitoring_location_id="USGS-01104475",
        file_type="exsa",
        file_path=str(tmp_path),
    )
    df = out["USGS-01104475.exsa.rdb"]
    # The fixture has two `# ...` lines at the top; both should land in attrs.
    assert df.attrs["comment"] == [
        "# header line one",
        "# header line two",
    ]
    assert df.attrs["url"] == _GOOD_ASSET


def test_get_ratings_download_and_parse_false_returns_features(httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url=STAC_SEARCH_RE,
        json=_stub_search_response(),
    )
    features = get_ratings(
        monitoring_location_id="USGS-01104475",
        download_and_parse=False,
    )
    assert isinstance(features, list)
    assert features[0]["id"] == "USGS-01104475.exsa.rdb"


def test_get_ratings_multi_type_filters_via_property(httpx_mock, tmp_path):
    """File_type list: server filter omits it; local filter reads the property."""
    httpx_mock.add_response(
        method="GET",
        url=STAC_SEARCH_RE,
        json={
            "features": [
                {
                    "id": "USGS-X.exsa.rdb",
                    "properties": {"file_type": "exsa"},
                    "assets": {"data": {"href": "https://x.example/X.exsa.rdb"}},
                },
                {
                    "id": "USGS-X.base.rdb",
                    "properties": {"file_type": "base"},
                    "assets": {"data": {"href": "https://x.example/X.base.rdb"}},
                },
                {
                    "id": "USGS-X.corr.rdb",
                    "properties": {"file_type": "corr"},
                    "assets": {"data": {"href": "https://x.example/X.corr.rdb"}},
                },
            ]
        },
    )
    # Only mock the two URLs we expect to be downloaded.
    httpx_mock.add_response(
        method="GET", url="https://x.example/X.exsa.rdb", text=_SAMPLE_RDB
    )
    httpx_mock.add_response(
        method="GET", url="https://x.example/X.corr.rdb", text=_SAMPLE_RDB
    )

    out = get_ratings(
        monitoring_location_id="USGS-X",
        file_type=["exsa", "corr"],
        file_path=str(tmp_path),
    )
    assert set(out) == {"USGS-X.exsa.rdb", "USGS-X.corr.rdb"}

    # Server-side filter must NOT include file_type for multi-type requests.
    search_req = httpx_mock.get_requests()[0]
    qs = parse_qs(urlsplit(str(search_req.url)).query)
    assert "file_type" not in qs["filter"][0]


def test_get_ratings_search_429_is_resumable(httpx_mock):
    """A rate-limited search surfaces as a resumable interruption — parity
    with the other getters, which drive the same executor — instead of a raw
    ``RateLimited``; resuming finishes the interrupted stage."""
    httpx_mock.add_response(method="GET", url=STAC_SEARCH_RE, status_code=429)
    httpx_mock.add_response(
        method="GET", url=STAC_SEARCH_RE, json=_stub_search_response()
    )

    with pytest.raises(QuotaExhausted) as excinfo:
        get_ratings(monitoring_location_id="USGS-01104475", download_and_parse=False)

    df, _ = excinfo.value.call.resume()
    assert list(df["feature"])[0]["id"] == "USGS-01104475.exsa.rdb"


def _two_feature_search_response():
    """One feature whose asset will fail, one that will succeed."""
    return {
        "features": [
            {
                "id": "USGS-99999999.exsa.rdb",
                "properties": {"file_type": "exsa"},
                "assets": {"data": {"href": _BAD_ASSET}},
            },
            {
                "id": "USGS-01104475.exsa.rdb",
                "properties": {"file_type": "exsa"},
                "assets": {"data": {"href": _GOOD_ASSET}},
            },
        ]
    }


def test_get_ratings_deterministic_download_failure_warns_and_skips(httpx_mock):
    """A stale catalog entry (404 on its asset) costs only that feature: the
    skip is announced with ``SkippedRatingWarning`` naming the feature, and
    every other rating in the batch is still returned."""
    httpx_mock.add_response(
        method="GET", url=STAC_SEARCH_RE, json=_two_feature_search_response()
    )
    httpx_mock.add_response(method="GET", url=_BAD_ASSET, status_code=404)
    httpx_mock.add_response(method="GET", url=_GOOD_ASSET, text=_SAMPLE_RDB)

    with pytest.warns(SkippedRatingWarning, match="USGS-99999999"):
        out = get_ratings(monitoring_location_id=["USGS-99999999", "USGS-01104475"])

    assert sorted(out) == ["USGS-01104475.exsa.rdb"]
    assert len(out["USGS-01104475.exsa.rdb"]) == 3


def test_get_ratings_feature_without_asset_warns_and_skips(httpx_mock):
    """A catalog feature carrying no data asset is a per-feature data problem:
    skipped with a warning, without costing the rest of the batch."""
    body = _two_feature_search_response()
    body["features"][0]["assets"] = {}

    httpx_mock.add_response(method="GET", url=STAC_SEARCH_RE, json=body)
    httpx_mock.add_response(method="GET", url=_GOOD_ASSET, text=_SAMPLE_RDB)

    with pytest.warns(SkippedRatingWarning, match="no data asset"):
        out = get_ratings(monitoring_location_id=["USGS-99999999", "USGS-01104475"])

    assert sorted(out) == ["USGS-01104475.exsa.rdb"]


def test_get_ratings_skip_warning_escalates_to_error(httpx_mock):
    """``filterwarnings("error", ...)`` restores strict all-or-nothing: the
    escalated skip surfaces as an exception instead of a silent gap."""
    httpx_mock.add_response(
        method="GET", url=STAC_SEARCH_RE, json=_two_feature_search_response()
    )
    httpx_mock.add_response(method="GET", url=_BAD_ASSET, status_code=404)
    httpx_mock.add_response(
        method="GET", url=_GOOD_ASSET, text=_SAMPLE_RDB, is_optional=True
    )

    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=SkippedRatingWarning)
        with pytest.raises(SkippedRatingWarning):
            get_ratings(monitoring_location_id=["USGS-99999999", "USGS-01104475"])


def test_get_ratings_download_429_is_resumable_not_skipped(httpx_mock):
    """A rate-limited download must never be skipped -- it is raised as a
    resumable interruption, and resuming completes the batch. The escalation
    filter proves no ``SkippedRatingWarning`` fires along the way."""
    httpx_mock.add_response(
        method="GET", url=STAC_SEARCH_RE, json=_stub_search_response()
    )
    httpx_mock.add_response(method="GET", url=_GOOD_ASSET, status_code=429)
    httpx_mock.add_response(method="GET", url=_GOOD_ASSET, text=_SAMPLE_RDB)

    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=SkippedRatingWarning)
        with pytest.raises(QuotaExhausted) as excinfo:
            get_ratings(monitoring_location_id="USGS-01104475")

        df, _ = excinfo.value.call.resume()
    assert len(df) == 3


def test_stac_next_link_refuses_another_host(httpx_mock):
    """The STAC page walk must not follow a link off the ratings host.

    The search request carries the Water Data API key; a ``next`` href naming
    another host would take it somewhere the caller never asked for. Unlike the
    OGC engine, this walk had no host check at all.
    """
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*/stac/v0/search.*"),
        json={
            "features": [{"id": "a", "properties": {}, "assets": {}}],
            "links": [{"rel": "next", "href": "https://evil.example/page2"}],
        },
    )
    with pytest.raises(DataRetrievalError, match="rather than"):
        get_ratings(monitoring_location_id="USGS-X")


def test_stac_next_link_strips_embedded_credentials(httpx_mock):
    """A same-host ``next`` href must not smuggle in ``user:pass@``.

    The host check passes by construction here, so only the strip catches it.
    """
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r"^https://api\.waterdata\.usgs\.gov/stac/v0/search\?.*"),
        json={
            "features": [],
            "links": [
                {
                    "rel": "next",
                    "href": "https://u:p@api.waterdata.usgs.gov/stac/v0/search?page=2",
                }
            ],
        },
    )
    # Second page: no ``next``, so the walk terminates.
    httpx_mock.add_response(
        method="GET",
        url="https://api.waterdata.usgs.gov/stac/v0/search?page=2",
        json={"features": [], "links": []},
    )

    assert get_ratings(monitoring_location_id="USGS-X") == {}

    followed = httpx_mock.get_requests()[1]
    assert followed.url.userinfo == b""
    assert followed.headers.get("Authorization") is None
