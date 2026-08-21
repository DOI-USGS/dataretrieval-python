from unittest import mock

import pytest
from geopandas import GeoDataFrame

import dataretrieval
import dataretrieval.nldi as nldi
from dataretrieval.nldi import (
    NLDI_API_BASE_URL,
    _validate_feature_source_comid,
    _validate_navigation_mode,
    get_basin,
    get_features,
    get_features_by_data_source,
    get_flowlines,
    search,
)


@pytest.fixture(autouse=True)
def _reset_data_source_cache(monkeypatch):
    """Reset the module-level cache between tests."""
    monkeypatch.setattr(nldi, "_AVAILABLE_DATA_SOURCES", None)


def mock_request_data_sources(httpx_mock):
    request_url = f"{NLDI_API_BASE_URL}/"
    available_data_sources = [
        {"source": "ca_gages"},
        {"source": "census2020-nhdpv2"},
        {"source": "epa_nrsa"},
        {"source": "geoconnex-demo"},
        {"source": "gfv11_pois"},
        {"source": "huc12pp"},
        {"source": "huc12pp_102020"},
        {"source": "nmwdi-st"},
        {"source": "npdes"},
        {"source": "nwisgw"},
        {"source": "nwissite"},
        {"source": "ref_gage"},
        {"source": "vigil"},
        {"source": "wade"},
        {"source": "WQP"},
        {"source": "comid"},
    ]
    httpx_mock.add_response(
        method="GET",
        url=request_url,
        json=available_data_sources,
        headers={"mock_header": "value"},
    )


def test_query_nldi_opts_into_retry(monkeypatch):
    """NLDI explicitly enables shared retry while NWIS remains unchanged."""
    response = mock.Mock()
    response.json.return_value = {}
    query = mock.Mock(return_value=response)
    monkeypatch.setattr(nldi, "_query_with_retry", query)

    assert nldi._query_nldi("https://example.test", {}) == {}
    # ``adapter`` names whose settings the retry resolves, so a ``[nldi]``
    # table reaches these calls and no others.
    query.assert_called_once_with("https://example.test", payload={}, adapter="nldi")


def mock_request(httpx_mock, request_url, file_path):
    with open(file_path) as text:
        httpx_mock.add_response(
            method="GET",
            url=request_url,
            text=text.read(),
            headers={"mock_header": "value"},
        )


def test_get_basin(httpx_mock):
    """Tests NLDI get basin query"""
    request_url = (
        f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/basin"
        f"?simplified=true&splitCatchment=false"
    )
    response_file_path = "tests/data/nldi_get_basin.json"
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    gdf = get_basin(feature_source="WQP", feature_id="USGS-054279485")
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 1


def test_get_flowlines(httpx_mock):
    """Tests NLDI get flowlines query using feature source as the origin"""
    request_url = (
        f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/navigation/UM/flowlines"
        f"?distance=5&trimStart=false"
    )
    response_file_path = "tests/data/nldi_get_flowlines.json"
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    gdf = get_flowlines(
        feature_source="WQP", feature_id="USGS-054279485", navigation_mode="UM"
    )
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 2


def test_get_flowlines_by_comid(httpx_mock):
    """Tests NLDI get flowlines query using comid as the origin"""
    request_url = (
        f"{NLDI_API_BASE_URL}/comid/13294314/navigation/UM/flowlines"
        "?distance=50&trimStart=false"
    )
    response_file_path = "tests/data/nldi_get_flowlines_by_comid.json"
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    gdf = get_flowlines(navigation_mode="UM", comid=13294314, distance=50)
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 16


def test_features_by_feature_source_with_navigation(httpx_mock):
    """Tests NLDI get features query using feature source as the origin
    with navigation mode
    """
    request_url = (
        f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/navigation/UM/nwissite?distance=50"
    )
    response_file_path = (
        "tests/data/nldi_get_features_by_feature_source_with_nav_mode.json"
    )
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    gdf = get_features(
        feature_source="WQP",
        feature_id="USGS-054279485",
        data_source="nwissite",
        navigation_mode="UM",
        distance=50,
    )
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 108


def test_features_by_feature_source_without_navigation(httpx_mock):
    """Tests NLDI get features query using feature source as the origin
    without navigation mode
    """
    request_url = f"{NLDI_API_BASE_URL}/WQP/USGS-054279485"
    response_file_path = (
        "tests/data/nldi_get_features_by_feature_source_without_nav_mode.json"
    )
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    gdf = get_features(feature_source="WQP", feature_id="USGS-054279485")
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 10


def test_get_features_by_comid(httpx_mock):
    """Tests NLDI get features query using comid as the origin"""
    request_url = f"{NLDI_API_BASE_URL}/comid/13294314/navigation/UM/WQP?distance=5"
    response_file_path = "tests/data/nldi_get_features_by_comid.json"
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    gdf = get_features(
        comid=13294314, data_source="WQP", navigation_mode="UM", distance=5
    )
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 405


def test_get_features_by_lat_long(httpx_mock):
    """Tests NLDI get features query using lat/long as the origin"""
    request_url = (
        f"{NLDI_API_BASE_URL}/comid/position?coords=POINT%28-89.509%2043.087%29"
    )
    response_file_path = "tests/data/nldi_get_features_by_lat_long.json"
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    gdf = get_features(lat=43.087, long=-89.509)
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 6


@pytest.mark.parametrize(
    ("kwargs", "problem", "remedy"),
    [
        (
            {"lat": 43.087},
            "lat and long must be given together",
            "Pass both, e.g. lat=43.087, long=-89.509.",
        ),
        (
            {"lat": 43.087, "long": -89.509, "comid": 13294314},
            "lat/long and comid cannot be combined",
            "one origin per call",
        ),
        (
            {"lat": 43.087, "long": -89.509, "feature_source": "WQP"},
            "lat/long and feature_source cannot be combined",
            "one origin per call",
        ),
        (
            {"comid": 13294314},
            "navigation_mode is required",
            "Pass one of 'UM', 'DM', 'UT', 'DD'.",
        ),
    ],
)
def test_get_features_rejects_ambiguous_origins(kwargs, problem, remedy):
    """Origin validation runs ahead of the request, and names the way out.

    Both halves are asserted because the caller is usually a program: the
    problem alone tells it something is wrong, and only the remedy tells it
    what to send instead.
    """
    with pytest.raises(ValueError) as excinfo:
        get_features(**kwargs)
    assert problem in str(excinfo.value)
    assert remedy in str(excinfo.value)


def test_get_features_includes_stop_comid(httpx_mock):
    """The extracted request builder preserves optional navigation bounds."""
    request_url = (
        f"{NLDI_API_BASE_URL}/comid/13294314/navigation/UM/WQP"
        "?distance=5&stopComid=13294315"
    )
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, "tests/data/nldi_get_features_by_comid.json")

    result = get_features(
        comid=13294314,
        data_source="WQP",
        navigation_mode="UM",
        distance=5,
        stop_comid=13294315,
        as_json=True,
    )

    assert isinstance(result, dict)


def test_search_for_basin(httpx_mock):
    """Tests NLDI search query for basin"""
    request_url = (
        f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/basin"
        "?simplified=true&splitCatchment=false"
    )
    response_file_path = "tests/data/nldi_get_basin.json"
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    search_results = search(
        feature_source="WQP", feature_id="USGS-054279485", find="basin"
    )
    assert isinstance(search_results, dict)
    assert search_results["features"][0]["type"] == "Feature"
    assert search_results["features"][0]["geometry"]["type"] == "Polygon"
    assert len(search_results["features"][0]["geometry"]["coordinates"][0]) == 122


def test_search_for_flowlines(httpx_mock):
    """Tests NLDI search query for flowlines"""
    request_url = (
        f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/navigation/UM/flowlines"
        "?distance=50&trimStart=false"
    )
    response_file_path = "tests/data/nldi_get_flowlines.json"
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    search_results = search(
        feature_source="WQP",
        feature_id="USGS-054279485",
        navigation_mode="UM",
        find="flowlines",
    )
    assert isinstance(search_results, dict)
    assert search_results["features"][0]["type"] == "Feature"
    assert search_results["features"][0]["geometry"]["type"] == "LineString"
    assert len(search_results["features"][0]["geometry"]["coordinates"]) == 27


def test_search_for_flowlines_by_comid(httpx_mock):
    """Tests NLDI search query for flowlines by comid"""
    request_url = (
        f"{NLDI_API_BASE_URL}/comid/13294314/navigation/UM/flowlines"
        "?distance=50&trimStart=false"
    )
    response_file_path = "tests/data/nldi_get_flowlines_by_comid.json"
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    search_results = search(comid=13294314, navigation_mode="UM", find="flowlines")
    assert isinstance(search_results, dict)
    assert search_results["features"][0]["type"] == "Feature"
    assert search_results["features"][0]["geometry"]["type"] == "LineString"
    assert len(search_results["features"][0]["geometry"]["coordinates"]) == 27


def test_search_for_features_by_feature_source_with_navigation(httpx_mock):
    """Tests NLDI search query for features by feature source"""
    request_url = (
        f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/navigation/UM/nwissite?distance=50"
    )
    response_file_path = (
        "tests/data/nldi_get_features_by_feature_source_with_nav_mode.json"
    )
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    search_results = search(
        feature_source="WQP",
        feature_id="USGS-054279485",
        data_source="nwissite",
        navigation_mode="UM",
        find="features",
    )
    assert isinstance(search_results, dict)
    assert search_results["features"][0]["type"] == "Feature"
    assert search_results["features"][0]["geometry"]["type"] == "Point"
    assert len(search_results["features"]) == 9


def test_search_for_features_by_feature_source_without_navigation(httpx_mock):
    """Tests NLDI search query for features by feature source"""
    request_url = f"{NLDI_API_BASE_URL}/WQP/USGS-054279485"
    response_file_path = (
        "tests/data/nldi_get_features_by_feature_source_without_nav_mode.json"
    )
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    search_results = search(
        feature_source="WQP", feature_id="USGS-054279485", find="features"
    )
    assert isinstance(search_results, dict)
    assert search_results["features"][0]["type"] == "Feature"
    assert search_results["features"][0]["geometry"]["type"] == "Point"
    assert len(search_results["features"]) == 1


def test_search_for_features_by_comid(httpx_mock):
    """Tests NLDI search query for features by comid"""
    request_url = f"{NLDI_API_BASE_URL}/comid/13294314/navigation/UM/WQP?distance=5"
    response_file_path = "tests/data/nldi_get_features_by_comid.json"
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    search_results = search(
        comid=13294314,
        data_source="WQP",
        navigation_mode="UM",
        find="features",
        distance=5,
    )
    assert isinstance(search_results, dict)
    assert search_results["features"][0]["type"] == "Feature"
    assert search_results["features"][0]["geometry"]["type"] == "Point"
    assert len(search_results["features"]) == 45


def test_search_for_features_by_lat_long(httpx_mock):
    """Tests NLDI search query for features by lat/long"""
    request_url = (
        f"{NLDI_API_BASE_URL}/comid/position?coords=POINT%28-89.509%2043.087%29"
    )
    response_file_path = "tests/data/nldi_get_features_by_lat_long.json"
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, response_file_path)

    search_results = search(lat=43.087, long=-89.509, find="features")
    assert isinstance(search_results, dict)
    assert search_results["features"][0]["type"] == "Feature"
    assert search_results["features"][0]["geometry"]["type"] == "LineString"
    assert len(search_results["features"][0]["geometry"]["coordinates"]) == 27


def test_validate_data_source_rejects_invalid_after_cache_populated(httpx_mock):
    """Once the cache is warm, invalid data sources must still raise ValueError.

    Regression: previously the validation check was nested inside the
    cache-population branch, so all calls after the first silently passed.
    """
    mock_request_data_sources(httpx_mock)

    nldi._validate_data_source("WQP")

    with pytest.raises(ValueError, match="Invalid data source: 'not_a_real_source'"):
        nldi._validate_data_source("not_a_real_source")


# --- regression tests for nldi cleanup batch ---


def test_search_flowlines_without_navigation_mode_raises_value_error():
    """Regression: previously crashed with AttributeError on None.upper()."""
    with pytest.raises(ValueError, match="navigation_mode is required"):
        search(comid=13294314, find="flowlines")


@pytest.mark.parametrize(
    ("kwargs", "problem"),
    [
        ({}, "feature_source is required for find='basin'"),
        (
            {"feature_source": "WQP"},
            "feature_source and feature_id must be given together",
        ),
        ({"feature_id": "USGS-01031500"}, "must be given together"),
    ],
)
def test_search_for_basin_names_the_missing_half(kwargs, problem):
    """An incomplete basin origin says which argument to add, and shows one.

    Covers both ways the pair can be incomplete -- neither supplied, and one
    of the two -- because a caller that has to guess which it hit cannot
    correct the call from the message alone.
    """
    with pytest.raises(ValueError) as excinfo:
        search(find="basin", **kwargs)
    message = str(excinfo.value)
    assert problem in message
    assert "feature_source='WQP', feature_id='USGS-01031500'" in message


@pytest.mark.parametrize(
    ("half", "supplied"),
    [
        ("feature_id", {"feature_source": None, "feature_id": "USGS-01031500"}),
        ("feature_source", {"feature_source": "WQP", "feature_id": None}),
    ],
)
def test_half_a_feature_pair_beside_a_comid_is_reported_as_a_conflict(half, supplied):
    """Completing the pair would only raise the origin conflict next."""
    with pytest.raises(ValueError) as excinfo:
        _validate_feature_source_comid(comid=13294314, **supplied)
    message = str(excinfo.value)
    assert f"comid and {half} cannot be combined" in message
    assert "Pass both" not in message


def test_validate_navigation_mode_raises_value_error_for_invalid():
    """Regression: previously raised TypeError; should be ValueError."""
    with pytest.raises(ValueError, match="Invalid navigation_mode"):
        _validate_navigation_mode("XX")


def test_validate_navigation_mode_normalizes_lowercase():
    """Regression: lowercase values used to validate but be sent unchanged."""
    assert _validate_navigation_mode("um") == "UM"


def test_query_nldi_non_200_raises_typed_error(httpx_mock):
    """A non-200 NLDI response surfaces a typed ``DataRetrievalError`` (here a
    429 → ``RateLimited``, raised by the shared ``query`` path)."""
    from dataretrieval.exceptions import RateLimited

    httpx_mock.add_response(
        method="GET",
        url=f"{NLDI_API_BASE_URL}/WQP/USGS-MISSING/basin"
        "?simplified=true&splitCatchment=false",
        status_code=429,
    )
    mock_request_data_sources(httpx_mock)
    with pytest.raises(RateLimited, match="429"):
        nldi.get_basin(feature_source="WQP", feature_id="USGS-MISSING")


def test_validate_data_source_rejects_malformed_catalog(httpx_mock, monkeypatch):
    """``_validate_data_source`` should raise ``ValueError`` with an
    informative message if the NLDI base URL returns a non-list shape
    (or a list whose entries don't carry ``source`` keys), instead of
    crashing with ``TypeError: string indices must be integers``."""
    monkeypatch.setattr(nldi, "_AVAILABLE_DATA_SOURCES", None)
    httpx_mock.add_response(
        method="GET",
        url=f"{NLDI_API_BASE_URL}/",
        json={"error": "upstream maintenance"},
    )
    with pytest.raises(ValueError, match="unexpected shape"):
        nldi._validate_data_source("WQP")


def test_query_504_raises_service_unavailable(httpx_mock):
    """``utils.query`` classifies any 5xx (here 504 Gateway Timeout) as the
    transient ``ServiceUnavailable`` -- the whole 5xx range, not an enumerated
    subset of codes."""
    from dataretrieval.exceptions import ServiceUnavailable
    from dataretrieval.utils import query

    url = "https://example.invalid/x"
    httpx_mock.add_response(method="GET", url=f"{url}?a=1", status_code=504)
    # Match on the status number — robust against the exact message, which the
    # legacy query path renders verbatim as "HTTP 504 <reason> (URL: ...)".
    with pytest.raises(ServiceUnavailable, match="504"):
        query(url, {"a": "1"})


def test_a_configured_base_url_redirects_every_nldi_request(httpx_mock):
    """The block moves the catalog probe and the query alike.

    NLDI validates a feature source against a catalog it fetches itself, so a
    redirect that reached only the getter's own URL would leave the library
    asking the real service whether the mirror's sources exist -- and the mirror
    exists precisely because the caller cannot or should not reach the service.
    Both mocks are on the mirror, so either one straying fails this.
    """
    mirror = "https://mirror.example/nldi"
    httpx_mock.add_response(
        method="GET", url=f"{mirror}/", json=[{"source": "WQP"}, {"source": "comid"}]
    )
    with open("tests/data/nldi_get_basin.json") as body:
        httpx_mock.add_response(
            method="GET",
            url=(
                f"{mirror}/WQP/USGS-054279485/basin"
                "?simplified=true&splitCatchment=false"
            ),
            text=body.read(),
        )

    with dataretrieval.configure(nldi.NldiConfiguration(base_url=mirror)):
        gdf = get_basin(feature_source="WQP", feature_id="USGS-054279485")

    assert isinstance(gdf, GeoDataFrame)
    assert {str(r.url).startswith(mirror) for r in httpx_mock.get_requests()} == {True}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"comid": 13294314, "navigation_mode": "UM"},
        {
            "feature_source": "WQP",
            "feature_id": "USGS-054279485",
            "navigation_mode": "UM",
        },
    ],
)
def test_navigation_without_a_data_source_says_what_to_add(kwargs, monkeypatch):
    """A navigation needs the source naming which features to return.

    Without this the missing source was interpolated into the path as the
    literal string 'None'; the service answered 200 with an empty
    FeatureCollection and the caller got an empty GeoDataFrame with no way to
    tell it apart from a navigation that really has nothing on it.
    """
    # Seed the catalog: the feature_source case validates it on the way past,
    # and the autouse fixture clears it, so an unseeded run reaches the network
    # for a failure that is purely local.
    monkeypatch.setattr(nldi, "_AVAILABLE_DATA_SOURCES", ["WQP", "nwissite"])
    with pytest.raises(ValueError) as excinfo:
        get_features(**kwargs)
    message = str(excinfo.value)
    assert "data_source is required" in message
    assert "data_source='nwissite'" in message


def test_a_bad_navigation_mode_is_reported_before_the_missing_data_source():
    """Both arguments are wrong; the mode is the one the caller typed.

    Requiring ``data_source`` ahead of validating the mode would answer a
    mistyped ``navigation_mode`` with a message about a different argument,
    so the caller fixes that, re-runs, and only then learns about the typo.
    """
    with pytest.raises(ValueError) as excinfo:
        get_features(comid=13294314, navigation_mode="XX")
    assert "Invalid navigation_mode" in str(excinfo.value)


def test_get_features_by_data_source_returns_the_whole_catalog(httpx_mock):
    """The one getter that takes no origin: every feature of a source."""
    mock_request_data_sources(httpx_mock)
    mock_request(
        httpx_mock,
        f"{NLDI_API_BASE_URL}/WQP",
        "tests/data/nldi_get_features_by_comid.json",
    )

    gdf = get_features_by_data_source("WQP")

    assert isinstance(gdf, GeoDataFrame)
    assert not gdf.empty


def test_get_features_by_data_source_validates_the_source(httpx_mock):
    mock_request_data_sources(httpx_mock)
    with pytest.raises(ValueError, match="Invalid data source"):
        get_features_by_data_source("not_a_real_source")


def test_a_200_with_a_non_json_body_becomes_an_empty_frame(httpx_mock):
    """NLDI answers some queries 200 with an empty body, and that is not an
    error condition -- a feature with nothing upstream is a real answer.

    This is the one place the package returns an empty frame rather than
    raising on a malformed response. Pinned because it is deliberate: the
    swallow is easy to mistake for an oversight and 'fix' into a raise, which
    would turn a legitimate empty navigation into a crash.
    """
    mock_request_data_sources(httpx_mock)
    httpx_mock.add_response(
        method="GET",
        url=f"{NLDI_API_BASE_URL}/WQP",
        text="",
        headers={"Content-Type": "text/plain"},
    )

    gdf = get_features_by_data_source("WQP")

    assert isinstance(gdf, GeoDataFrame)
    assert gdf.empty
    assert gdf.crs is not None  # the CRS survives the empty path


def test_get_flowlines_forwards_stop_comid(httpx_mock):
    """``stop_comid`` bounds a navigation and must reach the query string."""
    request_url = (
        f"{NLDI_API_BASE_URL}/comid/13294314/navigation/UM/flowlines"
        "?distance=50&trimStart=false&stopComid=13294312"
    )
    mock_request_data_sources(httpx_mock)
    mock_request(httpx_mock, request_url, "tests/data/nldi_get_flowlines_by_comid.json")

    gdf = get_flowlines(
        navigation_mode="UM", comid=13294314, distance=50, stop_comid=13294312
    )

    assert isinstance(gdf, GeoDataFrame)
    sent = httpx_mock.get_requests()[-1].url
    assert "stopComid=13294312" in str(sent)


def test_search_rejects_a_basin_lookup_by_comid():
    """A basin is looked up by feature, not by flowline; the message must
    offer both ways forward rather than only naming the conflict."""
    with pytest.raises(ValueError) as excinfo:
        search(find="basin", comid=13294314)
    message = str(excinfo.value)
    assert "find='basin' cannot be combined with comid" in message
    assert "feature_source" in message
    assert "find='flowlines'" in message
