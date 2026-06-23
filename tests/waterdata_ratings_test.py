import re
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest

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


def _stub_search_response():
    return {
        "features": [
            {
                "id": "USGS-01104475.exsa.rdb",
                "properties": {"file_type": "exsa"},
                "assets": {
                    "data": {
                        "href": "https://api.waterdata.usgs.gov/stac-files/ratings/USGS.01104475.exsa.rdb"
                    }
                },
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
    httpx_mock.add_response(
        method="GET",
        url="https://api.waterdata.usgs.gov/stac-files/ratings/USGS.01104475.exsa.rdb",
        text=_SAMPLE_RDB,
    )

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
    asset_url = (
        "https://api.waterdata.usgs.gov/stac-files/ratings/USGS.01104475.exsa.rdb"
    )
    httpx_mock.add_response(method="GET", url=asset_url, text=_SAMPLE_RDB)

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
    assert df.attrs["url"] == asset_url


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
