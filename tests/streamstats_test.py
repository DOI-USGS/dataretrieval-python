"""Tests for the streamstats module."""

from dataretrieval.streamstats import Watershed

SAMPLE_JSON = {
    "featurecollection": [
        {"name": "globalwatershedpoint", "feature": {"type": "Feature", "id": "pt-1"}},
        {"name": "globalwatershed", "feature": {"type": "Feature", "id": "poly-1"}},
    ],
    "parameters": [{"code": "DRNAREA", "value": 41.2}],
    "workspaceID": "NY20240101000000000",
}


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
