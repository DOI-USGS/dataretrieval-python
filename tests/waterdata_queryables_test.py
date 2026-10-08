"""Tests for :func:`dataretrieval.waterdata.get_queryables`, the committed
queryables snapshot, and the check that getter docstrings agree with it.

The snapshot (``tests/data/waterdata_queryables.json``) records, for every Water
Data collection, each queryable's ``type``, ``title``, and ``description`` as
the service publishes them. Three checks read it, and only the first two block
a merge:

* :func:`test_documented_descriptions_contain_the_service_text` -- offline. Each
  typed argument of a Water Data getter that is a queryable of the getter's
  collection must have a documented description containing the service's
  description, after whitespace is normalised. Our own usage notes may surround
  the service's text; they may not replace it.
* :func:`test_typed_arguments_are_queryables_or_package_owned` -- offline. A
  typed argument that is neither a queryable nor declared package-owned means
  :data:`_GETTER_TARGETS` or :data:`_PACKAGE_OWNED` is wrong.
* :func:`test_queryables_match_snapshot` -- ``live``, on the nightly schedule.
  A failure means the service added or removed a queryable.

A queryable the service publishes but a getter does not take as a typed
argument is reported as a warning, not a failure: whether to promote it is a
design decision.

Regenerate the snapshot with::

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

from __future__ import annotations

import configparser
import importlib
import inspect
import json
import re
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, get_args
from urllib.parse import parse_qs, urlsplit

import pytest

import dataretrieval
from dataretrieval import waterdata
from dataretrieval.utils import BaseMetadata
from dataretrieval.waterdata.types import WATERDATA_COLLECTIONS

#: ``{collection: {queryable: {"type", "title", "description"}}}``.
Snapshot = dict[str, dict[str, dict[str, Any]]]

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

_REPO_ROOT = Path(__file__).parents[1]
_SNAPSHOT_PATH = Path(__file__).parent / "data" / "waterdata_queryables.json"


def _load_snapshot(path: Path) -> Snapshot:
    return json.loads(path.read_text(encoding="utf-8"))


_SNAPSHOT = _load_snapshot(_SNAPSHOT_PATH)


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


# --- comparing docstrings to a snapshot ------------------------------------
#
# Nothing in this section names a service, a base URL, or a collection: an
# OGC adapter adopts the check by supplying its own getters, targets, exempt
# arguments, and snapshot, as the Water Data section below does.


@dataclass(frozen=True)
class Target:
    """The collection a getter queries, and how its arguments name queryables.

    ``id_argument`` is the typed argument the getter sends as the collection's
    ``id`` queryable, if any. ``deprecated`` lists typed arguments the getter
    still accepts although they are not queryables of the collection, so they
    are neither compared nor reported as unknown.
    """

    collection: str
    id_argument: str | None = None
    deprecated: frozenset[str] = frozenset()

    def queryable(self, argument: str) -> str:
        """The queryable a typed argument filters on."""
        return "id" if argument == self.id_argument else argument


#: A section underline in a numpydoc docstring.
_SECTION_RULE = re.compile(r"^-{3,}\s*$")


def _normalise(text: str) -> str:
    """Collapse whitespace runs, wrapping, and trailing newlines; keep case."""
    return " ".join(text.split())


def _is_section_header(lines: list[str], index: int) -> bool:
    return (
        index + 1 < len(lines)
        and bool(lines[index].strip())
        and not lines[index][0].isspace()
        and bool(_SECTION_RULE.match(lines[index + 1]))
    )


def parameter_descriptions(doc: str | None) -> dict[str, str]:
    """Map each name in a numpydoc ``Parameters`` section to its description.

    A shared entry (``filter, filter_lang : optional``) maps every name it lists
    to the same text, and a ``**name`` entry maps ``name``. A docstring with no
    ``Parameters`` section documents nothing.
    """
    lines = inspect.cleandoc(doc or "").splitlines()
    starts = [
        i + 2
        for i in range(len(lines))
        if lines[i].strip() == "Parameters" and _is_section_header(lines, i)
    ]
    if not starts:
        return {}
    descriptions: dict[str, str] = {}
    names: list[str] = []
    body: list[str] = []
    for index in range(starts[0], len(lines)):
        line = lines[index]
        if _is_section_header(lines, index):
            break
        if line and not line[0].isspace():
            descriptions.update(dict.fromkeys(names, "\n".join(body)))
            entry = line.split(":", 1)[0]
            names = [name.strip().lstrip("*") for name in entry.split(",")]
            body = []
        else:
            body.append(line)
    descriptions.update(dict.fromkeys(names, "\n".join(body)))
    return descriptions


def typed_arguments(function: Callable[..., Any]) -> list[str]:
    """The keyword arguments a getter declares, in signature order.

    A ``**queryables`` catch-all is not a typed argument.
    """
    variadic = (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    return [
        name
        for name, parameter in inspect.signature(function).parameters.items()
        if parameter.kind not in variadic
    ]


def _compared_arguments(
    function: Callable[..., Any], target: Target, exempt: frozenset[str]
) -> list[str]:
    return [
        argument
        for argument in typed_arguments(function)
        if argument not in exempt and argument not in target.deprecated
    ]


def description_mismatches(
    name: str,
    function: Callable[..., Any],
    target: Target,
    snapshot: Snapshot,
    exempt: frozenset[str],
) -> list[str]:
    """Failures: a queryable argument whose description lacks the service's text.

    Each message names the collection, the queryable, and the getter, and prints
    the service's description beside ours, so a CI log is enough to make the fix.
    """
    queryables = snapshot[target.collection]
    documented = parameter_descriptions(function.__doc__)
    failures = []
    for argument in _compared_arguments(function, target, exempt):
        queryable = target.queryable(argument)
        if queryable not in queryables:
            continue  # an unknown argument, reported by unknown_arguments
        service = _normalise(queryables[queryable].get("description") or "")
        ours = _normalise(documented.get(argument, ""))
        if service in ours:
            continue
        failures.append(
            f"{target.collection}: queryable {queryable!r} -- the docstring of "
            f"{name}({argument}=...) does not contain the service's "
            "description. Make the parameter's description include the "
            "service's text; a usage note may surround it.\n"
            f"    service: {service}\n"
            f"    ours:    {ours or '(not documented)'}"
        )
    return failures


def unknown_arguments(
    name: str,
    function: Callable[..., Any],
    target: Target,
    snapshot: Snapshot,
    exempt: frozenset[str],
) -> list[str]:
    """Failures: a typed argument that is neither a queryable nor exempt."""
    queryables = snapshot[target.collection]
    return [
        f"{name}({argument}=...) is not a queryable of {target.collection!r} "
        "and is not declared package-owned. If it filters on a queryable "
        "under another name, declare that name as the target's id_argument; "
        "if the package owns it, add it to the exempt arguments; if the "
        "getter targets another collection, correct its target."
        for argument in _compared_arguments(function, target, exempt)
        if target.queryable(argument) not in queryables
    ]


def unexposed_queryables(
    function: Callable[..., Any], target: Target, snapshot: Snapshot
) -> list[str]:
    """Reports: queryables the getter takes only through ``**queryables``."""
    exposed = {target.queryable(argument) for argument in typed_arguments(function)}
    return sorted(set(snapshot[target.collection]) - exposed)


# --- the Water Data mapping layer ------------------------------------------

#: Typed arguments the package owns rather than the service: request controls
#: from the OGC standard, result shaping, and ``state``, which accepts several
#: spellings of a state and is sent as a queryable it chooses. None is compared
#: with a queryable, and their consistency across getters is not checked here.
_PACKAGE_OWNED = frozenset(
    {
        "bbox",
        "convert_type",
        "filter",
        "filter_lang",
        "limit",
        "max_rows",
        "properties",
        "skip_geometry",
        "ssl_check",
        "state",
    }
)

#: The collection each Water Data getter queries. Every getter the facade
#: exports from a collection family must appear here, so a new getter fails the
#: check until its collection is declared. ``None`` declares that the getter
#: reads no OGC queryables document: the untyped ``get_cql`` path (ADR 0007),
#: the reference tables, which the snapshot does not record, and the Samples
#: and Statistics services, which publish none.
_GETTER_TARGETS: dict[str, Target | None] = {
    "get_channel": Target("channel-measurements"),
    "get_codes": None,
    "get_combined_metadata": Target("combined-metadata"),
    "get_continuous": Target("continuous", id_argument="continuous_id"),
    "get_cql": None,
    "get_daily": Target("daily", id_argument="daily_id"),
    "get_field_measurements": Target("field-measurements"),
    "get_field_measurements_metadata": Target("field-measurements-metadata"),
    "get_latest_continuous": Target(
        "latest-continuous", id_argument="latest_continuous_id"
    ),
    "get_latest_daily": Target("latest-daily", id_argument="latest_daily_id"),
    "get_monitoring_locations": Target(
        "monitoring-locations", id_argument="monitoring_location_id"
    ),
    "get_peaks": Target("peaks"),
    "get_queryables": None,
    "get_reference_table": None,
    "get_samples": None,
    "get_samples_summary": None,
    "get_stats_date_range": None,
    "get_stats_por": None,
    # v1 dropped these filters; the getter still accepts them and sends the
    # query to v0 with a deprecation warning.
    "get_time_series_metadata": Target(
        "time-series-metadata",
        id_argument="time_series_id",
        deprecated=frozenset(
            {"begin_utc", "end_utc", "hydrologic_unit_code", "state_name"}
        ),
    ),
}


def _family_modules() -> list[str]:
    """The collection-family inventory, read from its dependency contract."""
    parser = configparser.ConfigParser()
    parser.read(_REPO_ROOT / ".importlinter", encoding="utf-8")
    section = parser["importlinter:contract:waterdata-families"]
    return section["modules"].split()


def _waterdata_getters() -> dict[str, Callable[..., Any]]:
    """Every family getter, resolved through the ``waterdata`` facade."""
    return {
        name: getattr(waterdata, name)
        for module in _family_modules()
        for name in importlib.import_module(module).__all__
    }


def _checked_getters() -> list[tuple[str, Callable[..., Any], Target]]:
    getters = _waterdata_getters()
    return [
        (name, getters[name], target)
        for name, target in sorted(_GETTER_TARGETS.items())
        if target is not None and name in getters
    ]


def _waterdata_collections() -> list[str]:
    return sorted(get_args(WATERDATA_COLLECTIONS))


# --- docstrings against the snapshot (offline gate) ------------------------


def test_every_getter_declares_its_collection():
    """Every family getter has a declared target, and every target is real.

    The getter set is derived from the collection-family inventory, so a new
    getter is covered the day it is exported and fails here until its target
    is declared in ``_GETTER_TARGETS``.
    """
    getters = set(_waterdata_getters())
    undeclared = sorted(getters - set(_GETTER_TARGETS))
    stale = sorted(set(_GETTER_TARGETS) - getters)
    assert not undeclared, (
        f"Getters with no declared collection: {undeclared}. Add each to "
        "_GETTER_TARGETS, with None if it reads no queryables document."
    )
    assert not stale, f"_GETTER_TARGETS names getters that do not exist: {stale}"


def test_snapshot_covers_every_collection_and_every_collection_is_checked():
    """No Water Data collection can be quietly exempted from the check."""
    collections = set(_waterdata_collections())
    assert set(_SNAPSHOT) == collections, (
        f"The snapshot's collections differ from WATERDATA_COLLECTIONS: "
        f"missing={sorted(collections - set(_SNAPSHOT))}, "
        f"extra={sorted(set(_SNAPSHOT) - collections)}. Regenerate the snapshot."
    )
    targeted = {target.collection for _, _, target in _checked_getters()}
    assert targeted == collections, (
        f"Collections no getter targets: {sorted(collections - targeted)}; "
        f"targets naming no collection: {sorted(targeted - collections)}."
    )


def test_typed_arguments_are_queryables_or_package_owned():
    """A typed argument the service does not know means a declaration is wrong."""
    failures = [
        failure
        for name, function, target in _checked_getters()
        for failure in unknown_arguments(
            name, function, target, _SNAPSHOT, _PACKAGE_OWNED
        )
    ]
    assert not failures, "\n".join(failures)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "#379: the getter docstrings have not yet been revised to contain the "
        "service's descriptions; the sweep that does so removes this marker."
    ),
)
def test_documented_descriptions_contain_the_service_text():
    """Each documented queryable argument contains the service's description.

    A failure names the collection, the queryable, and the getter, and prints
    both texts. Fix the docstring; if the service's text is what changed,
    regenerate the snapshot first (see this module's docstring).
    """
    failures = [
        failure
        for name, function, target in _checked_getters()
        for failure in description_mismatches(
            name, function, target, _SNAPSHOT, _PACKAGE_OWNED
        )
    ]
    assert not failures, (
        f"{len(failures)} documented descriptions disagree with the snapshot:\n\n"
        + "\n\n".join(failures)
    )


def test_unexposed_queryables_are_reported():
    """Queryables a getter takes only through ``**queryables`` are reported.

    Promoting one to a typed argument is a design decision, so this warns
    rather than fails.
    """
    reports = [
        f"{name} ({target.collection}): {', '.join(unexposed)}"
        for name, function, target in _checked_getters()
        if (unexposed := unexposed_queryables(function, target, _SNAPSHOT))
    ]
    if reports:
        warnings.warn(
            "Queryables with no typed argument:\n" + "\n".join(reports),
            UserWarning,
            stacklevel=1,
        )


# --- the check itself, on fakes --------------------------------------------

_FAKE_SNAPSHOT: Snapshot = {
    "things": {
        "id": {"type": "string", "title": "Id", "description": "The record id."},
        "colour": {
            "type": "string",
            "title": "Colour",
            "description": "The colour of the thing,\nas the service records it.",
        },
        "size": {"type": "number", "title": "Size", "description": "Its size."},
    }
}
_FAKE_TARGET = Target("things", id_argument="thing_id")


def _fake_getter(
    thing_id: str | None = None,
    colour: str | None = None,
    limit: int | None = None,
    **queryables: Any,
) -> None:
    """Get things.

    Parameters
    ----------
    thing_id : string, optional
        The record id.
    colour : string, optional
        The colour of the thing, as the
        service records it. Accepts a list.
    limit, other : int, optional
        Page size.
    **queryables : optional
        Anything else.

    Returns
    -------
    None
        Nothing.
    """


def test_parameter_descriptions_reads_the_parameters_section():
    descriptions = parameter_descriptions(_fake_getter.__doc__)
    assert set(descriptions) == {"thing_id", "colour", "limit", "other", "queryables"}
    assert descriptions["limit"] == descriptions["other"]
    assert "Nothing" not in "".join(descriptions.values())
    assert parameter_descriptions("No sections.") == {}
    assert parameter_descriptions(None) == {}


def test_containment_ignores_wrapping_and_allows_added_prose():
    assert not description_mismatches(
        "get_things", _fake_getter, _FAKE_TARGET, _FAKE_SNAPSHOT, frozenset({"limit"})
    )


def test_a_dropped_description_fails_naming_collection_queryable_and_getter():
    snapshot = json.loads(json.dumps(_FAKE_SNAPSHOT))
    snapshot["things"]["colour"]["description"] = "The hue, as revised."
    (failure,) = description_mismatches(
        "get_things", _fake_getter, _FAKE_TARGET, snapshot, frozenset({"limit"})
    )
    assert "things" in failure
    assert "'colour'" in failure
    assert "get_things(colour=...)" in failure
    assert "service: The hue, as revised." in failure
    assert "ours:    The colour of the thing, as the service records it." in failure


def test_an_undocumented_queryable_argument_fails():
    def bare(colour: str | None = None) -> None:
        """No parameters documented."""

    (failure,) = description_mismatches(
        "bare", bare, _FAKE_TARGET, _FAKE_SNAPSHOT, frozenset()
    )
    assert "(not documented)" in failure


def test_an_argument_neither_queryable_nor_exempt_is_unknown():
    assert not unknown_arguments(
        "get_things", _fake_getter, _FAKE_TARGET, _FAKE_SNAPSHOT, frozenset({"limit"})
    )
    (failure,) = unknown_arguments(
        "get_things", _fake_getter, _FAKE_TARGET, _FAKE_SNAPSHOT, frozenset()
    )
    assert "get_things(limit=...)" in failure
    assert "'things'" in failure
    deprecated = Target(
        "things", id_argument="thing_id", deprecated=frozenset({"limit"})
    )
    assert not unknown_arguments(
        "get_things", _fake_getter, deprecated, _FAKE_SNAPSHOT, frozenset()
    )


def test_queryables_without_a_typed_argument_are_reported():
    assert unexposed_queryables(_fake_getter, _FAKE_TARGET, _FAKE_SNAPSHOT) == ["size"]


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
