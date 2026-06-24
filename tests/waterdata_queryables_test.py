"""Tests for :func:`dataretrieval.waterdata.get_queryables`, plus a live monitor
that flags upstream changes to the Water Data API's queryable sets.

The live monitor (:func:`test_queryables_match_snapshot`) compares the
queryables each collection advertises against a committed snapshot
(``tests/data/waterdata_queryables.json``). When it fails, the upstream API has
added / removed / renamed a queryable: regenerate the snapshot and enable any
new queryables on the matching getter. Regenerate with::

    python - <<'PY'
    import httpx, json
    from typing import get_args
    from dataretrieval.waterdata.types import WATERDATA_SERVICES
    base = "https://api.waterdata.usgs.gov/ogcapi/v0"
    snap = {}
    for c in get_args(WATERDATA_SERVICES):
        r = httpx.get(f"{base}/collections/{c}/queryables", timeout=30)
        r.raise_for_status()
        snap[c] = sorted(r.json().get("properties", {}))
    json.dump(snap, open("tests/data/waterdata_queryables.json", "w"),
              indent=2, sort_keys=True)
    open("tests/data/waterdata_queryables.json", "a").write("\\n")
    PY
"""

import json
import re
from pathlib import Path

import pytest

import dataretrieval
from dataretrieval import waterdata
from dataretrieval.utils import BaseMetadata
from tests.conftest import flaky_api

# The OGC queryables endpoint for any Water Data collection.
QUERYABLES_RE = re.compile(
    r"^https://api\.waterdata\.usgs\.gov/ogcapi/v0/collections/[^/]+/queryables$"
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
_SNAPSHOT = json.loads(_SNAPSHOT_PATH.read_text())


# --- get_queryables unit tests (mocked) ------------------------------------


def test_get_queryables_parses_properties(httpx_mock):
    """Properties become one tidy row each, sorted by name, with the
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
    """An HTTP error (e.g. a 404 for an unknown collection) is surfaced as the
    typed ``DataRetrievalError``, not a bare DataFrame."""
    httpx_mock.add_response(
        method="GET",
        url=QUERYABLES_RE,
        status_code=404,
        json={"code": "404", "description": "Collection not found"},
    )

    with pytest.raises(dataretrieval.DataRetrievalError):
        waterdata.get_queryables("not-a-collection")


# --- live queryables monitor -----------------------------------------------


@flaky_api
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
