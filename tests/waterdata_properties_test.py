"""The columns a Water Data collection has, against its getter's parameters and
documentation.

A column of a collection can be filtered by a named parameter of its getter,
and some getters also list their columns in the ``properties`` docstring, under
"Available options are:". Both are hand-written, so they go stale when USGS adds
or removes a field. The live tests check them against the collection's schema
rather than generating them, because generated docs would make the docs build
depend on the live service and would not appear in ``help()``.
"""

import inspect
import re

import pandas as pd
import pytest

from dataretrieval import waterdata
from dataretrieval.ogc.schema import _check_ogc_requests
from dataretrieval.waterdata import utils
from dataretrieval.waterdata.endpoints import ogc_api_url
from dataretrieval.waterdata.utils import _OUTPUT_ID_BY_COLLECTION

#: Each OGC collection's getter. Written out rather than discovered, so a
#: getter that is renamed or removed fails instead of being skipped.
_GETTERS = {
    "channel-measurements": waterdata.get_channel,
    "combined-metadata": waterdata.get_combined_metadata,
    "continuous": waterdata.get_continuous,
    "daily": waterdata.get_daily,
    "field-measurements": waterdata.get_field_measurements,
    "field-measurements-metadata": waterdata.get_field_measurements_metadata,
    "latest-continuous": waterdata.get_latest_continuous,
    "latest-daily": waterdata.get_latest_daily,
    "monitoring-locations": waterdata.get_monitoring_locations,
    "peaks": waterdata.get_peaks,
    "time-series-metadata": waterdata.get_time_series_metadata,
}

#: The collections whose getter lists its columns in the docstring.
_DOCUMENTED = (
    "channel-measurements",
    "continuous",
    "daily",
    "latest-continuous",
    "latest-daily",
    "monitoring-locations",
    "time-series-metadata",
)

#: Parameters the engine reads itself rather than sending as a filter.
_NOT_FORWARDED = {"max_rows"}

#: Values a parameter must have to pass its own validation.
_VALID = {"monitoring_location_id": "USGS-05427718", "state": "Wisconsin"}

#: Parameters sent under the API's name rather than their own.
_SENT_AS = {"state": "state_name"}

#: The label may be split across two lines. The list ends at the next unindented
#: line of the dedented docstring (the next numpydoc parameter) or at the first
#: sentence after it.
_COLUMNS_RE = re.compile(r"Available\s+options\s+are:(.*?)(?=\.\s|\n\S|\Z)", re.S)


def _ids(collection: str) -> set[str]:
    """The schema's ``id`` and the name the getter returns it under. Some
    schemas list ``id`` and some do not, so both are left out of each check."""
    return {"id", _OUTPUT_ID_BY_COLLECTION[collection]}


def _documented_properties(getter) -> set[str]:
    """The column names *getter* lists in its ``properties`` docstring."""
    match = _COLUMNS_RE.search(inspect.getdoc(getter) or "")
    assert match, f"{getter.__name__} no longer documents its columns"
    return {n.strip().rstrip(".") for n in match.group(1).split(",") if n.strip()}


def _schema_properties(collection: str) -> set[str]:
    """The columns *collection* publishes in its OGC schema document."""
    body, _ = _check_ogc_requests(collection, "schema", base_url=ogc_api_url())
    properties = body.get("properties")
    assert properties, f"{collection} published no schema properties"
    return set(properties)


@pytest.mark.parametrize(
    ("getter", "parameter"),
    [
        pytest.param(getter, name, id=f"{getter.__name__}-{name}")
        for getter in _GETTERS.values()
        for name, param in inspect.signature(getter).parameters.items()
        if name not in _NOT_FORWARDED and param.kind is not param.VAR_KEYWORD
    ],
)
def test_every_named_parameter_reaches_the_engine(monkeypatch, getter, parameter):
    """A named parameter that a getter accepts is passed on to the OGC engine,
    rather than accepted and dropped, which would return unfiltered rows."""
    sent = {}

    def capture(args, *_, **__):
        sent.update(args)
        return pd.DataFrame(), None

    monkeypatch.setattr(utils, "_facade_get_ogc_data", capture)
    getter(**{parameter: _VALID.get(parameter, "2024-01-01")})

    assert _SENT_AS.get(parameter, parameter) in sent, sent


@pytest.mark.live
@pytest.mark.parametrize("collection", sorted(_GETTERS))
def test_every_schema_column_is_a_named_parameter(collection):
    """A column the collection publishes can be filtered by a named parameter of
    its getter, not only through ``**queryables``."""
    getter = _GETTERS[collection]
    named = set(inspect.signature(getter).parameters) | {"geometry"}
    unnamed = _schema_properties(collection) - _ids(collection) - named

    assert not unnamed, (
        f"{getter.__name__} has no named parameter for the {collection} columns "
        f"{sorted(unnamed)}. Add each as a keyword argument with the service's "
        "description, and to its 'Available options are:' list if it has one."
    )


@pytest.mark.live
@pytest.mark.parametrize("collection", _DOCUMENTED)
def test_documented_columns_match_the_collection_schema(collection):
    """A getter's documented column list matches what the collection publishes.

    A removed field is a breaking change and belongs in NEWS.
    """
    getter = _GETTERS[collection]
    documented = _documented_properties(getter) - _ids(collection)
    published = _schema_properties(collection) - _ids(collection)

    assert documented == published, (
        f"{getter.__name__} documents the wrong columns for {collection}: "
        f"missing={sorted(published - documented)}, "
        f"stale={sorted(documented - published)}. Edit the 'Available options "
        "are:' list in its properties docstring."
    )
