"""Live monitor: the columns a getter documents are the ones its collection has.

Some getters list their returned columns in the ``properties`` docstring, under
"Available options are:". The list is hand-written, so it goes stale when USGS
adds or removes a field. It is checked here rather than generated, because a
generated list would make the docs build depend on the live service and would
not appear in ``help()``.
"""

import inspect
import re

import pytest

from dataretrieval import waterdata
from dataretrieval.ogc.schema import _check_ogc_requests
from dataretrieval.waterdata.endpoints import ogc_api_url

#: Getters that list their returned columns, by collection. Written out rather
#: than discovered, so a getter that loses its list fails instead of being
#: skipped. get_channel is left out: its list names the output column
#: channel_measurements_id where the schema has id.
_DOCUMENTED = {
    "daily": waterdata.get_daily,
    "continuous": waterdata.get_continuous,
    "latest-continuous": waterdata.get_latest_continuous,
    "latest-daily": waterdata.get_latest_daily,
    "monitoring-locations": waterdata.get_monitoring_locations,
    "time-series-metadata": waterdata.get_time_series_metadata,
}

#: The label may be split across two lines. The list ends at the next unindented
#: line of the dedented docstring, which is the next numpydoc parameter.
_COLUMNS_RE = re.compile(r"Available\s+options\s+are:(.*?)(?=\n\S|\Z)", re.S)

#: Some schemas list ``id`` and some do not, but every getter accepts it, so it
#: is excluded from both sides.
_ALWAYS_REQUESTABLE = {"id"}


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


@pytest.mark.live
@pytest.mark.parametrize("collection", sorted(_DOCUMENTED))
def test_documented_columns_match_the_collection_schema(collection):
    """A getter's documented column list matches what the collection publishes.

    For an added field, consider a named parameter too; a removed field is a
    breaking change and belongs in NEWS.
    """
    getter = _DOCUMENTED[collection]
    documented = _documented_properties(getter) - _ALWAYS_REQUESTABLE
    published = _schema_properties(collection) - _ALWAYS_REQUESTABLE

    assert documented == published, (
        f"{getter.__name__} documents the wrong columns for {collection}: "
        f"missing={sorted(published - documented)}, "
        f"stale={sorted(documented - published)}. Edit the 'Available options "
        "are:' list in its properties docstring."
    )
