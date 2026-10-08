"""Tests for :func:`dataretrieval.waterdata.get_queryables`, plus a live monitor
that flags upstream changes to the Water Data API's queryable sets.

The committed snapshot (``tests/data/waterdata_queryables.json``) records, for
each collection, every queryable's ``type``, ``title``, and ``description`` as the
service publishes them. The live monitor (:func:`test_queryables_match_snapshot`)
compares the queryables each collection publishes against the snapshot's keys.
When it fails, the upstream API has added / removed / renamed a queryable:
regenerate the snapshot and enable any new queryables on the matching getter.
Regenerate with::

    python - <<'PY'
    import json
    from typing import get_args
    from dataretrieval.ogc.schema import queryables_frame
    from dataretrieval.waterdata.endpoints import ogc_api_url
    from dataretrieval.waterdata.types import WATERDATA_COLLECTIONS
    snap = {}
    for c in get_args(WATERDATA_COLLECTIONS):
        df, _ = queryables_frame(c, base_url=ogc_api_url())
        df = df.astype(object).where(df.notna(), None)
        snap[c] = df.set_index("queryable").to_dict("index")
    path = "tests/data/waterdata_queryables.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(snap, f, indent=2, sort_keys=True, ensure_ascii=False)
        f.write("\\n")
    PY
"""

import json
import re
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

import dataretrieval
from dataretrieval import waterdata
from dataretrieval.utils import BaseMetadata

# The OGC queryables endpoint for any Water Data collection.
QUERYABLES_RE = re.compile(
    r"^https://api\.waterdata\.usgs\.gov/ogcapi/v1/collections/[^/]+/queryables$"
)

# A minimal queryables document (the JSON Schema shape the real endpoint returns).
_FAKE_QUERYABLES = {
    "type": "object",
    "title": "Daily",
    "$schema": "https://json-schema.org/draft/2019-09/schema",
    "properties": {
        "state_name": {
            "title": "State name",
            "type": "string",
            "description": "The name of the state.\n",
        },
        "parameter_code": {
            "title": "Parameter code",
            "type": "string",
            "description": "5-digit codes.\n",
        },
    },
}

_SNAPSHOT_PATH = Path(__file__).parent / "data" / "waterdata_queryables.json"
_SNAPSHOT = json.loads(_SNAPSHOT_PATH.read_text(encoding="utf-8"))


# --- get_queryables unit tests (mocked) ------------------------------------


def test_get_queryables_parses_properties(httpx_mock):
    """Properties become one row each, sorted by name, with the
    description whitespace-stripped; returns ``(DataFrame, BaseMetadata)``."""
    httpx_mock.add_response(method="GET", url=QUERYABLES_RE, json=_FAKE_QUERYABLES)

    df, md = waterdata.get_queryables("daily")

    assert isinstance(md, BaseMetadata)
    assert list(df.columns) == ["queryable", "type", "title", "description"]
    # Sorted by name (parameter_code before state_name).
    assert df["queryable"].tolist() == ["parameter_code", "state_name"]
    row = df.set_index("queryable").loc["state_name"]
    assert row["type"] == "string"
    assert row["title"] == "State name"
    assert row["description"] == "The name of the state."  # trailing \n stripped


def test_get_queryables_unknown_collection_raises(httpx_mock):
    """An HTTP error (e.g. a 404 for an unknown collection) is raised as the
    typed ``DataRetrievalError``, not a bare DataFrame."""
    httpx_mock.add_response(
        method="GET",
        url=QUERYABLES_RE,
        status_code=404,
        json={"code": "404", "description": "Collection not found"},
    )

    with pytest.raises(dataretrieval.DataRetrievalError):
        waterdata.get_queryables("not-a-collection")


# --- passthrough queryables (mocked) ---------------------------------------

_DAILY_ITEMS_RE = re.compile(
    r"^https://api\.waterdata\.usgs\.gov/ogcapi/v1/collections/daily/items"
)
_DAILY_SCHEMA_RE = re.compile(
    r"^https://api\.waterdata\.usgs\.gov/ogcapi/v1/collections/daily/schema$"
)
_EMPTY_FEATURES = {
    "type": "FeatureCollection",
    "features": [],
    "numberReturned": 0,
    "numberMatched": 0,
    "links": [],
}


def _mock_daily(httpx_mock):
    """Mock the two endpoints a ``get_daily`` call requests: the items query and
    the schema fetch (used for output typing)."""
    httpx_mock.add_response(method="GET", url=_DAILY_SCHEMA_RE, json={"properties": {}})
    httpx_mock.add_response(method="GET", url=_DAILY_ITEMS_RE, json=_EMPTY_FEATURES)


def _items_query(httpx_mock):
    """Parsed query string of the ``/items`` request the getter sent."""
    req = next(r for r in httpx_mock.get_requests() if "/items" in str(r.url))
    return parse_qs(urlsplit(str(req.url)).query)


def test_passthrough_queryables_sent_as_filters(httpx_mock):
    """An OGC getter forwards queryables that aren't in its explicit signature
    (e.g. ``state_name``, ``site_type_code``) to the collection as query filters,
    alongside the named params."""
    _mock_daily(httpx_mock)

    waterdata.get_daily(
        monitoring_location_id="USGS-05427718",
        state_name="Wisconsin",
        site_type_code="ST",
    )

    qs = _items_query(httpx_mock)
    assert qs["state_name"] == ["Wisconsin"]
    assert qs["site_type_code"] == ["ST"]
    assert qs["monitoring_location_id"] == ["USGS-05427718"]


def test_passthrough_list_queryable_is_comma_joined(httpx_mock):
    """A list-valued passthrough queryable is normalized and comma-joined like a
    named multi-value param."""
    _mock_daily(httpx_mock)

    waterdata.get_daily(
        monitoring_location_id="USGS-05427718",
        site_type_code=["ST", "LK"],
    )

    assert _items_query(httpx_mock)["site_type_code"] == ["ST,LK"]


# --- the committed snapshot ------------------------------------------------


def test_snapshot_records_each_queryables_definition():
    """Each queryable maps to the ``type``, ``title``, and ``description`` the
    service publishes, written as sorted, indented JSON so that a revised
    description reads as a small diff in review."""
    for collection, queryables in _SNAPSHOT.items():
        for queryable, definition in queryables.items():
            assert set(definition) == {"description", "title", "type"}, (
                f"{collection}: {queryable} is not a queryable definition"
            )
    canonical = json.dumps(_SNAPSHOT, indent=2, sort_keys=True, ensure_ascii=False)
    assert _SNAPSHOT_PATH.read_text(encoding="utf-8") == canonical + "\n", (
        f"{_SNAPSHOT_PATH.name} is not sorted, indented JSON; regenerate it."
    )


# --- live queryables monitor -----------------------------------------------


@pytest.mark.live
@pytest.mark.parametrize("collection", sorted(_SNAPSHOT))
def test_queryables_match_snapshot(collection):
    """Each collection's live queryables match the committed snapshot.

    A failure means the upstream API changed a collection's queryables.
    Regenerate ``tests/data/waterdata_queryables.json`` (see this module's
    docstring) and enable any newly added queryables on the matching getter.
    """
    df, _ = waterdata.get_queryables(collection)
    live = set(df["queryable"])
    expected = set(_SNAPSHOT[collection])
    assert live == expected, (
        f"{collection} queryables changed upstream: "
        f"added={sorted(live - expected)}, removed={sorted(expected - live)}. "
        f"Regenerate {_SNAPSHOT_PATH.name} and enable any new queryables."
    )
