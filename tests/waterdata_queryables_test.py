"""Tests for :func:`dataretrieval.waterdata.get_queryables`, plus checks that
flag drift between the Water Data API's queryables and this package.

The committed snapshot (``tests/data/waterdata_queryables.json``) records each
collection's queryables with the ``type``, ``title``, and ``description`` the
service publishes. Offline, each typed argument of a getter that is a queryable
of its collection must have a documented description containing the service's
description once both are reduced to plain text: the service's Markdown and the
docstring's reStructuredText lose their markup, and whitespace is collapsed.
Our own notes may surround the service's text. The snapshot keeps the service's
raw text, so a change of markup alone still shows as drift. The live monitor
(:func:`test_queryables_match_snapshot`) compares the queryables each collection
publishes against the snapshot. When the service has changed, regenerate the
snapshot with ``python -m tests.waterdata_queryables_test``: it rewrites the
file and reports which queryables appeared, disappeared, or changed, and which
getters document them. ``--check`` reports without rewriting.
"""

from __future__ import annotations

import argparse
import configparser
import importlib
import inspect
import json
import re
import sys
import warnings
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, get_args
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest

import dataretrieval
from dataretrieval import waterdata
from dataretrieval.ogc.schema import queryables_frame
from dataretrieval.utils import BaseMetadata
from dataretrieval.waterdata.endpoints import ogc_api_url
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
    httpx_mock.add_response(method=None, url=_DAILY_ITEMS_RE, json=_EMPTY_FEATURES)


def _items_request(httpx_mock):
    """The ``/items`` request the getter sent."""
    return next(r for r in httpx_mock.get_requests() if "/items" in str(r.url))


def _items_query(httpx_mock):
    """Parsed query string of the ``/items`` request the getter sent."""
    return parse_qs(urlsplit(str(_items_request(httpx_mock).url)).query)


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


def test_passthrough_list_queryable_is_a_cql2_predicate(httpx_mock):
    """A list-valued passthrough queryable is normalized and sent in the CQL2
    body like a named multi-value param."""
    _mock_daily(httpx_mock)

    waterdata.get_daily(
        monitoring_location_id="USGS-05427718",
        site_type_code=["ST", "LK"],
    )

    body = json.loads(_items_request(httpx_mock).content)
    assert body["args"] == [
        {"op": "in", "args": [{"property": "site_type_code"}, ["ST", "LK"]]}
    ]


# --- comparing docstrings to a snapshot ------------------------------------


@dataclass(frozen=True)
class Target:
    """The collection a getter queries, and how its arguments name queryables.

    ``id_argument`` is the typed argument the getter sends as the collection's
    ``id`` queryable, if any. ``deprecated`` lists typed arguments the getter
    still accepts although they are not queryables of the collection, so they
    are neither compared nor reported as unknown. ``owned`` lists typed
    arguments this getter alone owns, exempt like the shared package-owned ones.
    """

    collection: str
    id_argument: str | None = None
    deprecated: frozenset[str] = frozenset()
    owned: frozenset[str] = frozenset()

    def queryable(self, argument: str) -> str:
        """The queryable a typed argument filters on."""
        return "id" if argument == self.id_argument else argument


_SECTION_RULE = re.compile(r"^-{3,}\s*$")


_BULLET = re.compile(r"^[ \t]*[-*][ \t]+", re.MULTILINE)

# Inline markup of either syntax, one alternative per construct. The service
# writes Markdown and the docstrings reStructuredText; scanning both with one
# pattern, left to right, consumes each construct once, so a literal's
# contents are never read as emphasis or a link.
_MARKUP = re.compile(
    r"`(?P<rst_text>[^`<]*?)\s*<(?P<rst_url>[^`<>]+)>`__?"  # `text <url>`_
    r"|:[\w.+-]+(?::[\w.+-]+)*:`(?P<role>[^`]+)`"  # :role:`x`
    r"|(?P<ticks>`+)(?P<literal>.+?)(?P=ticks)"  # `x` and ``x``
    r"|\[(?P<md_text>[^\[\]]*)\]\((?P<md_url>[^()\s]+)\)"  # [text](url)
    r"|\*\*(?P<strong>\S(?:.*?\S)?)\*\*"  # **x**
    r"|\*(?P<emphasis>\S(?:.*?\S)?)\*"  # *x*
)


def _link(text: str, url: str) -> str:
    """A link as plain text: its text then its URL, or the URL once if equal."""
    text = text.strip()
    return url if text in ("", url) else f"{text} {url}"


def _unmarked(match: re.Match[str]) -> str:
    if match["rst_url"] is not None:
        return _link(match["rst_text"], match["rst_url"])
    if match["md_url"] is not None:
        return _link(match["md_text"], match["md_url"])
    return match["role"] or match["literal"] or match["strong"] or match["emphasis"]


def _normalise(text: str) -> str:
    """Reduce Markdown (the service's) or reStructuredText (a docstring's) to
    plain text, so the two compare by what they say rather than how it is
    marked up.

    Literals and emphasis keep their contents, a role keeps its target, a link
    becomes its text followed by its URL (the URL once when the two are the
    same), and a bullet becomes a plain line. Whitespace runs, wrapping, and
    trailing newlines then collapse to single spaces. Case is kept.
    """
    flat = " ".join(_BULLET.sub("", text).split())
    return " ".join(_MARKUP.sub(_unmarked, flat).split())


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
        if argument not in exempt | target.deprecated | target.owned
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
        if ours and service in ours:
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


def stale_declarations(
    name: str, function: Callable[..., Any], target: Target
) -> list[str]:
    """Failures: an ``id_argument``, ``deprecated`` or ``owned`` name the getter
    lacks."""
    declared = set(target.deprecated | target.owned)
    if target.id_argument is not None:
        declared.add(target.id_argument)
    return [
        f"{name} declares {argument!r} in its target but takes no such typed "
        "argument. Remove the stale entry, or rename it to match the signature."
        for argument in sorted(declared - set(typed_arguments(function)))
    ]


def unexposed_queryables(
    function: Callable[..., Any], target: Target, snapshot: Snapshot
) -> list[str]:
    """Reports: queryables the getter takes only through ``**queryables``."""
    exposed = {target.queryable(argument) for argument in typed_arguments(function)}
    return sorted(set(snapshot[target.collection]) - exposed)


def documenting_getters(
    collection: str,
    queryable: str,
    getters: Mapping[str, Callable[..., Any]],
    targets: Mapping[str, Target | None],
) -> list[str]:
    """The getters with a typed argument for one queryable of one collection."""
    names = []
    for name, function in getters.items():
        target = targets.get(name)
        if target is None or target.collection != collection:
            continue
        arguments = typed_arguments(function)
        if any(target.queryable(argument) == queryable for argument in arguments):
            names.append(name)
    return sorted(names)


# --- refreshing a snapshot from the service --------------------------------


def fetch_snapshot(collections: Iterable[str], *, base_url: str) -> Snapshot:
    """Read every collection's queryables from the service at ``base_url``."""
    snapshot: Snapshot = {}
    for collection in collections:
        frame, _ = queryables_frame(collection, base_url=base_url)
        assert not frame.empty, f"{collection} published no queryables"
        # A field the service omits is missing in the frame; JSON spells it null.
        snapshot[collection] = {
            row["queryable"]: {
                field: None if pd.isna(row[field]) else row[field]
                for field in ("description", "title", "type")
            }
            for row in frame.to_dict("records")
        }
    return snapshot


def write_snapshot(snapshot: Snapshot, path: Path) -> None:
    """Write a snapshot as sorted, indented JSON, so a change reads as a diff."""
    text = json.dumps(snapshot, indent=2, sort_keys=True, ensure_ascii=False)
    path.write_text(text + "\n", encoding="utf-8", newline="\n")


def drift_report(
    old: Snapshot,
    new: Snapshot,
    documenting: Callable[[str, str], list[str]],
) -> list[str]:
    """Describe how ``new`` differs from ``old``, one finding per line group.

    An empty list means the two agree. For a changed description, the getters
    returned by ``documenting`` are the docstrings to revise.
    """
    lines: list[str] = []
    for collection in sorted(set(old) | set(new)):
        before = old.get(collection, {})
        after = new.get(collection, {})
        appeared = sorted(set(after) - set(before))
        disappeared = sorted(set(before) - set(after))
        if appeared:
            lines.append(f"{collection}: appeared: {', '.join(appeared)}")
        if disappeared:
            lines.append(f"{collection}: disappeared: {', '.join(disappeared)}")
        for queryable in disappeared:
            if getters := ", ".join(documenting(collection, queryable)):
                lines.append(
                    f"{collection}: {queryable}: disappeared; documented by {getters}"
                )
        for queryable in sorted(set(before) & set(after)):
            fields = [
                field
                for field in ("type", "title", "description")
                if before[queryable].get(field) != after[queryable].get(field)
            ]
            if not fields:
                continue
            getters = ", ".join(documenting(collection, queryable)) or "no getter"
            lines.append(
                f"{collection}: {queryable}: changed {', '.join(fields)}; "
                f"documented by {getters}"
            )
            for field in fields:
                lines.append(f"    {field} was: {before[queryable].get(field)}")
                lines.append(f"    {field} now: {after[queryable].get(field)}")
    return lines


def refresh(
    snapshot_path: Path,
    fetch: Callable[[], Snapshot],
    documenting: Callable[[str, str], list[str]],
    *,
    write: bool = True,
) -> list[str]:
    """Fetch the service's queryables, report drift, and optionally rewrite."""
    new = fetch()
    report = drift_report(_load_snapshot(snapshot_path), new, documenting)
    if write:
        write_snapshot(new, snapshot_path)
    return report


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
#: exports must appear here, so a new getter fails the check until its
#: collection is declared. ``None`` declares that the getter reads no OGC
#: queryables document: the untyped ``get_cql`` path (ADR 0007), the reference
#: tables, which the snapshot does not record, the Samples and Statistics
#: services, which publish none, and ``get_ratings``, which searches the STAC
#: ratings catalog.
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
    "get_nearest_continuous": Target(
        "continuous", owned=frozenset({"on_tie", "targets", "window"})
    ),
    "get_peaks": Target("peaks"),
    "get_queryables": None,
    "get_ratings": None,
    "get_reference_table": None,
    "get_samples": None,
    "get_samples_summary": None,
    "get_stats_date_range": None,
    "get_stats_por": None,
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
    """Every getter, resolved through the ``waterdata`` facade.

    The family modules' exports, plus every ``get_*`` the facade exports from
    outside the families (``nearest``, ``ratings``), so a getter is covered the
    day it is exported, wherever it is defined.
    """
    names = {
        name
        for module in _family_modules()
        for name in importlib.import_module(module).__all__
    }
    names |= {name for name in waterdata.__all__ if name.startswith("get_")}
    return {name: getattr(waterdata, name) for name in sorted(names)}


def _checked_getters() -> list[tuple[str, Callable[..., Any], Target]]:
    getters = _waterdata_getters()
    return [
        (name, getters[name], target)
        for name, target in sorted(_GETTER_TARGETS.items())
        if target is not None and name in getters
    ]


def _waterdata_documenting(collection: str, queryable: str) -> list[str]:
    return documenting_getters(
        collection, queryable, _waterdata_getters(), _GETTER_TARGETS
    )


def _waterdata_collections() -> list[str]:
    return sorted(get_args(WATERDATA_COLLECTIONS))


def main(argv: list[str] | None = None) -> int:
    """Refresh the Water Data queryables snapshot from the live service."""
    parser = argparse.ArgumentParser(
        prog="python -m tests.waterdata_queryables_test",
        description=(
            "Fetch every Water Data collection's queryables, rewrite "
            f"{_SNAPSHOT_PATH.relative_to(_REPO_ROOT)}, and report what changed."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="report drift without rewriting the snapshot; exit 1 if any",
    )
    options = parser.parse_args(argv)
    report = refresh(
        _SNAPSHOT_PATH,
        lambda: fetch_snapshot(_waterdata_collections(), base_url=ogc_api_url()),
        _waterdata_documenting,
        write=not options.check,
    )
    if not report:
        sys.stdout.write("The snapshot matches the service.\n")
        return 0
    sys.stdout.write("\n".join(report) + "\n")
    return 1 if options.check else 0


# --- docstrings against the snapshot (offline gate) ------------------------


def test_every_getter_declares_its_collection():
    """Every exported getter has a declared target, and every target is real.

    The getter set is derived from the collection-family inventory and the
    facade's exports, so a new getter is covered the day it is exported and
    fails here until its target is declared in ``_GETTER_TARGETS``.
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
        f"extra={sorted(set(_SNAPSHOT) - collections)}. Refresh the snapshot."
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


def test_target_declarations_name_real_arguments():
    """A renamed id argument or a removed deprecated one leaves no stale entry."""
    failures = [
        failure
        for name, function, target in _checked_getters()
        for failure in stale_declarations(name, function, target)
    ]
    assert not failures, "\n".join(failures)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "#379: the getter docstrings have not yet been revised to contain the "
        "service's descriptions; the sweep that does so removes this marker."
    ),
)
def test_documented_descriptions_contain_the_service_text():
    """Each documented queryable argument contains the service's description.

    A failure names the collection, the queryable, and the getter, and prints
    both texts. Fix the docstring; if the service's text is what changed,
    refresh the snapshot first (see this module's docstring).
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


@pytest.mark.parametrize(
    ("markdown", "plain"),
    [
        ("compare the `time` field", "compare the time field"),
        ("at [https://a.gov/x](https://a.gov/x).", "at https://a.gov/x."),
        ("undergo [review](https://a.gov/r).", "undergo review https://a.gov/r."),
        ("Examples:\n\n  - one\n  - two", "Examples: one two"),
        ("Examples:\n\n* one\n* two", "Examples: one two"),
        ("a **strong** word", "a strong word"),
        ("an *emphasised* word", "an emphasised word"),
    ],
    ids=[
        "literal",
        "link-once",
        "link",
        "dash-bullets",
        "star-bullets",
        "strong",
        "em",
    ],
)
def test_normalise_reduces_the_services_markdown(markdown, plain):
    assert _normalise(markdown) == plain


@pytest.mark.parametrize(
    ("rst", "plain"),
    [
        ("compare the ``time`` field", "compare the time field"),
        ("at `https://a.gov/x <https://a.gov/x>`_.", "at https://a.gov/x."),
        ("undergo `review <https://a.gov/r>`_.", "undergo review https://a.gov/r."),
        ("undergo `review <https://a.gov/r>`__.", "undergo review https://a.gov/r."),
        ("call :func:`get_queryables` first", "call get_queryables first"),
        ("Examples:\n\n- one\n- two", "Examples: one two"),
        ("Examples:\n\n    * one\n    * two", "Examples: one two"),
        ("a **strong** word", "a strong word"),
        ("an *emphasised* word", "an emphasised word"),
    ],
    ids=[
        "literal",
        "link-once",
        "link",
        "anonymous-link",
        "role",
        "dash-bullets",
        "star-bullets",
        "strong",
        "em",
    ],
)
def test_normalise_reduces_a_docstrings_restructuredtext(rst, plain):
    assert _normalise(rst) == plain


def test_normalise_keeps_case_and_what_is_not_markup():
    assert _normalise("  Keep  CASE,\n2 * 3, and -1.\n") == "Keep CASE, 2 * 3, and -1."
    # A literal's contents are not read as markup.
    assert _normalise("``*args*`` and ``[a](b)``") == "*args* and [a](b)"
    # A stray bracket before a link stays; only the link is reduced.
    assert _normalise("[A list at [https://a.gov](https://a.gov).") == (
        "[A list at https://a.gov."
    )


def test_an_rst_docstring_contains_the_services_markdown():
    """A docstring may use RST links and literals for the service's Markdown."""
    snapshot: Snapshot = {
        "things": {
            "colour": {
                "type": "string",
                "title": "Colour",
                "description": (
                    "The `colour` field. Codes are listed at\n"
                    "[https://a.gov/codes](https://a.gov/codes); see the\n"
                    "[style guide](https://a.gov/style)."
                ),
            }
        }
    }

    def get_things(colour: str | None = None) -> None:
        """Get things.

        Parameters
        ----------
        colour : string, optional
            The ``colour`` field. Codes are listed at
            `https://a.gov/codes <https://a.gov/codes>`_; see the
            `style guide <https://a.gov/style>`_. Accepts a list.
        """

    target = Target("things")
    assert not description_mismatches(
        "get_things", get_things, target, snapshot, frozenset()
    )
    snapshot["things"]["colour"]["description"] = "The `hue` field."
    (failure,) = description_mismatches(
        "get_things", get_things, target, snapshot, frozenset()
    )
    assert "service: The hue field." in failure


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

    snapshot = json.loads(json.dumps(_FAKE_SNAPSHOT))
    snapshot["things"]["colour"]["description"] = ""
    (failure,) = description_mismatches(
        "bare", bare, _FAKE_TARGET, snapshot, frozenset()
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
    owned = Target("things", id_argument="thing_id", owned=frozenset({"limit"}))
    assert not unknown_arguments(
        "get_things", _fake_getter, owned, _FAKE_SNAPSHOT, frozenset()
    )


def test_a_declaration_the_signature_lacks_is_stale():
    assert not stale_declarations("get_things", _fake_getter, _FAKE_TARGET)
    stale = Target("things", id_argument="item_id", deprecated=frozenset({"old"}))
    failures = stale_declarations("get_things", _fake_getter, stale)
    assert len(failures) == 2
    assert all(failure.startswith("get_things declares") for failure in failures)


def test_queryables_without_a_typed_argument_are_reported():
    assert unexposed_queryables(_fake_getter, _FAKE_TARGET, _FAKE_SNAPSHOT) == ["size"]


def test_drift_report_names_what_moved_and_who_documents_it():
    new = json.loads(json.dumps(_FAKE_SNAPSHOT))
    del new["things"]["size"]
    del new["things"]["id"]
    new["things"]["weight"] = {"type": "number", "title": "W", "description": "W."}
    new["things"]["colour"]["description"] = "The hue."

    def documenting(collection: str, queryable: str) -> list[str]:
        return documenting_getters(
            collection,
            queryable,
            {"get_things": _fake_getter},
            {"get_things": _FAKE_TARGET},
        )

    report = drift_report(_FAKE_SNAPSHOT, new, documenting)
    assert "things: appeared: weight" in report
    assert "things: disappeared: id, size" in report
    # A disappeared queryable a getter still takes names that getter.
    assert "things: id: disappeared; documented by get_things" in report
    assert not any(line.startswith("things: size:") for line in report)
    assert "things: colour: changed description; documented by get_things" in report
    assert "    description now: The hue." in report
    assert drift_report(_FAKE_SNAPSHOT, _FAKE_SNAPSHOT, documenting) == []


def test_refresh_never_writes_a_collection_with_no_queryables(httpx_mock, tmp_path):
    httpx_mock.add_response(method="GET", url=QUERYABLES_RE, json={"type": "object"})
    path = tmp_path / "snapshot.json"
    write_snapshot({}, path)
    before = path.read_text(encoding="utf-8")

    with pytest.raises(AssertionError, match="daily published no queryables"):
        refresh(
            path,
            lambda: fetch_snapshot(["daily"], base_url=ogc_api_url()),
            lambda collection, queryable: [],
        )

    assert path.read_text(encoding="utf-8") == before


def test_refresh_rewrites_the_snapshot_from_the_schema_reader(httpx_mock, tmp_path):
    """Offline: the refresh path reads queryables through ``ogc.schema`` and
    writes sorted, indented JSON ending in a newline."""
    document = json.loads(json.dumps(_FAKE_QUERYABLES))
    document["properties"]["geometry"] = {"$ref": "https://geojson.org/geometry"}
    httpx_mock.add_response(method="GET", url=QUERYABLES_RE, json=document)
    path = tmp_path / "snapshot.json"
    write_snapshot({"daily": {"state_name": _FAKE_SNAPSHOT["things"]["id"]}}, path)

    report = refresh(
        path,
        lambda: fetch_snapshot(["daily"], base_url=ogc_api_url()),
        lambda collection, queryable: [],
    )

    assert "daily: appeared: geometry, parameter_code" in report
    written = path.read_text(encoding="utf-8")
    assert written.endswith("}\n")
    # A field the service omits is written as null, never as NaN.
    assert json.loads(written)["daily"]["geometry"] == {
        "description": "",
        "title": None,
        "type": None,
    }
    assert json.loads(written)["daily"]["state_name"] == {
        "description": "The name of the state.",
        "title": "State name",
        "type": "string",
    }
    assert (
        written
        == json.dumps(json.loads(written), indent=2, sort_keys=True, ensure_ascii=False)
        + "\n"
    )


def test_the_refresh_command_checks_every_collection_offline(httpx_mock, capsys):
    """``--check`` reads every collection, prints the report, and exits 1 on
    drift, leaving the committed snapshot as it was."""
    httpx_mock.add_response(method="GET", url=QUERYABLES_RE, json=_FAKE_QUERYABLES)
    before = _SNAPSHOT_PATH.read_text(encoding="utf-8")

    assert main(["--check"]) == 1

    requested = {str(request.url) for request in httpx_mock.get_requests()}
    assert len(requested) == len(_waterdata_collections())
    out = capsys.readouterr().out
    assert "daily: disappeared:" in out
    assert "daily: parameter_code: changed description; documented by get_daily" in out
    assert _SNAPSHOT_PATH.read_text(encoding="utf-8") == before


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
        f"{_SNAPSHOT_PATH.name} is not sorted, indented JSON; refresh it."
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


if __name__ == "__main__":
    raise SystemExit(main())
