"""Tests for ``dataretrieval.streamstats``."""

import json

import pytest

from dataretrieval.streamstats import Watershed, get_watershed

# Minimal StreamStats watershed payload shaped like the service response
# (two-element featurecollection: point + delineated basin polygon).
_SAMPLE = {
    "featurecollection": [
        {"name": "globalwatershedpoint", "feature": {"type": "Feature", "id": "pt"}},
        {"name": "globalwatershed", "feature": {"type": "Feature", "id": "poly"}},
    ],
    "parameters": [{"code": "DRNAREA", "value": 12.3}],
    "workspaceID": "WS-ABC",
}


def test_watershed_from_streamstats_json_builds_independent_instances():
    """B3 regression: ``from_streamstats_json`` previously wrote *class*
    attributes and returned the class object, so it produced no real
    instance and a second parse clobbered the first. It must now return
    an independent, populated ``Watershed`` instance."""
    w1 = Watershed.from_streamstats_json(_SAMPLE)
    assert isinstance(w1, Watershed)  # was the class object pre-fix
    assert w1.watershed_point == {"type": "Feature", "id": "pt"}
    assert w1.watershed_polygon == {"type": "Feature", "id": "poly"}
    assert w1.parameters == [{"code": "DRNAREA", "value": 12.3}]
    assert w1._workspaceID == "WS-ABC"

    w2 = Watershed.from_streamstats_json(dict(_SAMPLE, workspaceID="WS-XYZ"))
    assert w1 is not w2
    assert w1._workspaceID == "WS-ABC"  # not clobbered by w2 (was shared class state)
    assert w2._workspaceID == "WS-XYZ"


def test_get_watershed_object_returns_instance(httpx_mock):
    """``get_watershed(format='object')`` parses the response into a
    populated ``Watershed`` instance."""
    httpx_mock.add_response(text=json.dumps(_SAMPLE))
    w = get_watershed("NY", -74.524, 43.939, format="object")
    assert isinstance(w, Watershed)
    assert w._workspaceID == "WS-ABC"
    assert w.parameters == [{"code": "DRNAREA", "value": 12.3}]


def test_get_watershed_geojson_returns_raw_response(httpx_mock):
    """The default ``format='geojson'`` returns the raw httpx response."""
    httpx_mock.add_response(text=json.dumps(_SAMPLE))
    r = get_watershed("NY", -74.524, 43.939)
    assert r.status_code == 200
    assert json.loads(r.text)["workspaceID"] == "WS-ABC"


def test_get_watershed_shape_raises_not_implemented(httpx_mock):
    """B3: the unimplemented ``format='shape'`` must fail loudly rather
    than silently falling through to a (previously broken) ``Watershed``."""
    httpx_mock.add_response(text=json.dumps(_SAMPLE))
    with pytest.raises(NotImplementedError):
        get_watershed("NY", -74.524, 43.939, format="shape")
