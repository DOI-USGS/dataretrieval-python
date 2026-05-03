"""Tests for the streamstats module."""

import json

from dataretrieval.streamstats import Watershed, get_watershed

SAMPLE_JSON = {
    "featurecollection": [
        {"name": "globalwatershedpoint", "feature": {"type": "Feature", "id": "pt-1"}},
        {"name": "globalwatershed", "feature": {"type": "Feature", "id": "poly-1"}},
    ],
    "parameters": [{"code": "DRNAREA", "value": 41.2}],
    "workspaceID": "NY20240101000000000",
}


def test_from_streamstats_json_returns_instance():
    """Watershed.from_streamstats_json must return an instance, not the class."""
    w = Watershed.from_streamstats_json(SAMPLE_JSON)
    assert isinstance(w, Watershed)


def test_from_streamstats_json_does_not_mutate_class():
    """Two watersheds must not share state via class-level attributes."""
    other_json = {
        "featurecollection": [
            {"feature": {"id": "pt-2"}},
            {"feature": {"id": "poly-2"}},
        ],
        "parameters": [{"code": "OTHER", "value": 1.0}],
        "workspaceID": "VT20240101000000000",
    }
    w1 = Watershed.from_streamstats_json(SAMPLE_JSON)
    w2 = Watershed.from_streamstats_json(other_json)

    assert w1.workspace_id == "NY20240101000000000"
    assert w2.workspace_id == "VT20240101000000000"
    assert w1.parameters[0]["code"] == "DRNAREA"
    assert w2.parameters[0]["code"] == "OTHER"
    assert w1.watershed_point["id"] == "pt-1"
    assert w2.watershed_point["id"] == "pt-2"


def test_get_watershed_object_returns_dict(requests_mock):
    """get_watershed(format='object') must return parsed JSON, not None."""
    url = "https://streamstats.usgs.gov/streamstatsservices/watershed.geojson"
    requests_mock.get(url, text=json.dumps(SAMPLE_JSON))

    result = get_watershed("NY", -74.524, 43.939, format="object")
    assert isinstance(result, dict)
    assert result["workspaceID"] == "NY20240101000000000"


def test_watershed_init_populates_instance(requests_mock):
    """Watershed(...) must populate the instance (regression: previously discarded)."""
    url = "https://streamstats.usgs.gov/streamstatsservices/watershed.geojson"
    requests_mock.get(url, text=json.dumps(SAMPLE_JSON))

    w = Watershed("NY", -74.524, 43.939)
    assert w.workspace_id == "NY20240101000000000"
    assert w.parameters[0]["code"] == "DRNAREA"
    assert w.watershed_point["id"] == "pt-1"
    assert w.watershed_polygon["id"] == "poly-1"


def test_workspace_id_back_compat_alias():
    """Legacy `_workspaceID` attribute should still resolve."""
    w = Watershed.from_streamstats_json(SAMPLE_JSON)
    assert w._workspaceID == w.workspace_id == "NY20240101000000000"
