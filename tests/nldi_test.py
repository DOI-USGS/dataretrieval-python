import pytest
from geopandas import GeoDataFrame

import dataretrieval.nldi as nldi
from dataretrieval.nldi import (
    NLDI_API_BASE_URL,
    _validate_navigation_mode,
    get_basin,
    get_features,
    get_flowlines,
    search,
)


@pytest.fixture(autouse=True)
def _reset_data_source_cache(monkeypatch):
    """Reset the module-level cache between tests."""
    monkeypatch.setattr(nldi, "_AVAILABLE_DATA_SOURCES", None)


def mock_request_data_sources(requests_mock):
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
    requests_mock.get(
        request_url, json=available_data_sources, headers={"mock_header": "value"}
    )


def mock_request(requests_mock, request_url, file_path):
    with open(file_path) as text:
        requests_mock.get(
            request_url, text=text.read(), headers={"mock_header": "value"}
        )


def test_get_basin(requests_mock):
    """Tests NLDI get basin query"""
    request_url = (
        f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/basin"
        f"?simplified=true&splitCatchment=false"
    )
    response_file_path = "tests/data/nldi_get_basin.json"
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

    gdf = get_basin(feature_source="WQP", feature_id="USGS-054279485")
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 1


def test_get_flowlines(requests_mock):
    """Tests NLDI get flowlines query using feature source as the origin"""
    request_url = (
        f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/navigation/UM/flowlines"
        f"?distance=5&trimStart=false"
    )
    response_file_path = "tests/data/nldi_get_flowlines.json"
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

    gdf = get_flowlines(
        feature_source="WQP", feature_id="USGS-054279485", navigation_mode="UM"
    )
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 2


def test_get_flowlines_by_comid(requests_mock):
    """Tests NLDI get flowlines query using comid as the origin"""
    request_url = (
        f"{NLDI_API_BASE_URL}/comid/13294314/navigation/UM/flowlines?distance=50"
    )
    response_file_path = "tests/data/nldi_get_flowlines_by_comid.json"
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

    gdf = get_flowlines(navigation_mode="UM", comid=13294314, distance=50)
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 16


def test_features_by_feature_source_with_navigation(requests_mock):
    """Tests NLDI get features query using feature source as the origin
    with navigation mode
    """
    request_url = (
        f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/navigation/UM/nwissite?distance=50"
    )
    response_file_path = (
        "tests/data/nldi_get_features_by_feature_source_with_nav_mode.json"
    )
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

    gdf = get_features(
        feature_source="WQP",
        feature_id="USGS-054279485",
        data_source="nwissite",
        navigation_mode="UM",
        distance=50,
    )
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 108


def test_features_by_feature_source_without_navigation(requests_mock):
    """Tests NLDI get features query using feature source as the origin
    without navigation mode
    """
    request_url = f"{NLDI_API_BASE_URL}/WQP/USGS-054279485"
    response_file_path = (
        "tests/data/nldi_get_features_by_feature_source_without_nav_mode.json"
    )
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

    gdf = get_features(feature_source="WQP", feature_id="USGS-054279485")
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 10


def test_get_features_by_comid(requests_mock):
    """Tests NLDI get features query using comid as the origin"""
    request_url = f"{NLDI_API_BASE_URL}/comid/13294314/navigation/UM/WQP?distance=5"
    response_file_path = "tests/data/nldi_get_features_by_comid.json"
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

    gdf = get_features(
        comid=13294314, data_source="WQP", navigation_mode="UM", distance=5
    )
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 405


def test_get_features_by_lat_long(requests_mock):
    """Tests NLDI get features query using lat/long as the origin"""
    request_url = (
        f"{NLDI_API_BASE_URL}/comid/position?coords=POINT%28-89.509%2043.087%29"
    )
    response_file_path = "tests/data/nldi_get_features_by_lat_long.json"
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

    gdf = get_features(lat=43.087, long=-89.509)
    assert isinstance(gdf, GeoDataFrame)
    assert gdf.size == 6


def test_search_for_basin(requests_mock):
    """Tests NLDI search query for basin"""
    request_url = f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/basin"
    response_file_path = "tests/data/nldi_get_basin.json"
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

    search_results = search(
        feature_source="WQP", feature_id="USGS-054279485", find="basin"
    )
    assert isinstance(search_results, dict)
    assert search_results["features"][0]["type"] == "Feature"
    assert search_results["features"][0]["geometry"]["type"] == "Polygon"
    assert len(search_results["features"][0]["geometry"]["coordinates"][0]) == 122


def test_search_for_flowlines(requests_mock):
    """Tests NLDI search query for flowlines"""
    request_url = f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/navigation/UM/flowlines"
    response_file_path = "tests/data/nldi_get_flowlines.json"
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

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


def test_search_for_flowlines_by_comid(requests_mock):
    """Tests NLDI search query for flowlines by comid"""
    request_url = f"{NLDI_API_BASE_URL}/comid/13294314/navigation/UM/flowlines"
    response_file_path = "tests/data/nldi_get_flowlines_by_comid.json"
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

    search_results = search(comid=13294314, navigation_mode="UM", find="flowlines")
    assert isinstance(search_results, dict)
    assert search_results["features"][0]["type"] == "Feature"
    assert search_results["features"][0]["geometry"]["type"] == "LineString"
    assert len(search_results["features"][0]["geometry"]["coordinates"]) == 27


def test_search_for_features_by_feature_source_with_navigation(requests_mock):
    """Tests NLDI search query for features by feature source"""
    request_url = (
        f"{NLDI_API_BASE_URL}/WQP/USGS-054279485/navigation/UM/nwissite?distance=50"
    )
    response_file_path = (
        "tests/data/nldi_get_features_by_feature_source_with_nav_mode.json"
    )
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

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


def test_search_for_features_by_feature_source_without_navigation(requests_mock):
    """Tests NLDI search query for features by feature source"""
    request_url = f"{NLDI_API_BASE_URL}/WQP/USGS-054279485"
    response_file_path = (
        "tests/data/nldi_get_features_by_feature_source_without_nav_mode.json"
    )
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

    search_results = search(
        feature_source="WQP", feature_id="USGS-054279485", find="features"
    )
    assert isinstance(search_results, dict)
    assert search_results["features"][0]["type"] == "Feature"
    assert search_results["features"][0]["geometry"]["type"] == "Point"
    assert len(search_results["features"]) == 1


def test_search_for_features_by_comid(requests_mock):
    """Tests NLDI search query for features by comid"""
    request_url = f"{NLDI_API_BASE_URL}/comid/13294314/navigation/UM/WQP?distance=5"
    response_file_path = "tests/data/nldi_get_features_by_comid.json"
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

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


def test_search_for_features_by_lat_long(requests_mock):
    """Tests NLDI search query for features by lat/long"""
    request_url = (
        f"{NLDI_API_BASE_URL}/comid/position?coords=POINT%28-89.509%2043.087%29"
    )
    response_file_path = "tests/data/nldi_get_features_by_lat_long.json"
    mock_request_data_sources(requests_mock)
    mock_request(requests_mock, request_url, response_file_path)

    search_results = search(lat=43.087, long=-89.509, find="features")
    assert isinstance(search_results, dict)
    assert search_results["features"][0]["type"] == "Feature"
    assert search_results["features"][0]["geometry"]["type"] == "LineString"
    assert len(search_results["features"][0]["geometry"]["coordinates"]) == 27


def test_validate_data_source_rejects_invalid_after_cache_populated(requests_mock):
    """Once the cache is warm, invalid data sources must still raise ValueError.

    Regression: previously the validation check was nested inside the
    cache-population branch, so all calls after the first silently passed.
    """
    mock_request_data_sources(requests_mock)

    nldi._validate_data_source("WQP")

    with pytest.raises(ValueError, match="Invalid data source 'not_a_real_source'"):
        nldi._validate_data_source("not_a_real_source")


# --- regression tests for nldi cleanup batch ---


def test_search_flowlines_without_navigation_mode_raises_value_error():
    """Regression: previously crashed with AttributeError on None.upper()."""
    with pytest.raises(ValueError, match="navigation_mode is required"):
        search(comid=13294314, find="flowlines")


def test_validate_navigation_mode_raises_value_error_for_invalid():
    """Regression: previously raised TypeError; should be ValueError."""
    with pytest.raises(ValueError, match="Invalid navigation mode"):
        _validate_navigation_mode("XX")


def test_validate_navigation_mode_normalizes_lowercase():
    """Regression: lowercase values used to validate but be sent unchanged."""
    assert _validate_navigation_mode("um") == "UM"
