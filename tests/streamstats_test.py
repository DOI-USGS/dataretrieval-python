"""Tests for the streamstats module."""

from unittest import mock

import pytest

from dataretrieval import streamstats
from dataretrieval.streamstats import Watershed, get_watershed

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


def test_from_streamstats_json_returns_watershed_instance():
    """Regression: previously returned the class itself, not an instance."""
    w = Watershed.from_streamstats_json(SAMPLE_JSON)
    assert isinstance(w, Watershed)


def test_workspaceID_alias_emits_deprecation_warning():
    """`_workspaceID` mirrors `workspace_id` but signals migration."""
    w = Watershed.from_streamstats_json(SAMPLE_JSON)
    with pytest.warns(DeprecationWarning, match="workspace_id"):
        assert w._workspaceID == w.workspace_id


@pytest.fixture
def patched_get(monkeypatch):
    """Stub ``requests.get`` so format-dispatch tests stay offline."""
    response = mock.MagicMock()
    response.json.return_value = SAMPLE_JSON
    response.raise_for_status.return_value = None
    monkeypatch.setattr(streamstats.requests, "get", lambda *a, **kw: response)
    return response


def test_get_watershed_object_returns_dict(patched_get):
    """Regression: pre-fix this branch was a `pass` and returned None."""
    result = get_watershed("NY", -74.524, 43.939, format="object")
    assert result == SAMPLE_JSON


def test_get_watershed_watershed_returns_instance(patched_get):
    """`format='watershed'` builds a Watershed from the parsed JSON."""
    result = get_watershed("NY", -74.524, 43.939, format="watershed")
    assert isinstance(result, Watershed)
    assert result.workspace_id == SAMPLE_JSON["workspaceID"]


def test_get_watershed_unknown_format_raises(patched_get):
    """Unknown `format` is rejected; pre-fix it silently fell through."""
    with pytest.raises(ValueError, match="Invalid format"):
        get_watershed("NY", -74.524, 43.939, format="bogus")
