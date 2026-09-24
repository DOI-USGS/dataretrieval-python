"""Public import, export, and signature contracts for Water Data.

These assert *properties* of the signatures rather than their rendered text. A
character-exact ``str(inspect.signature(f))`` snapshot fails on every annotation
reflow and on a new keyword-only parameter -- changes that break no caller. The
properties below fail on the changes that do: a new required argument, and a
new parameter that can be passed by position, which shifts a positional call.
"""

from __future__ import annotations

import inspect

import pytest

from dataretrieval import waterdata
from dataretrieval.waterdata import api

_EXPECTED_WATERDATA_ALL = [
    "CODE_SERVICES",
    "FILTER_LANG",
    "WaterdataConfiguration",
    "PROFILES",
    "PROFILE_LOOKUP",
    "SERVICES",
    "WATERDATA_SERVICES",
    "parallel_chunks",
    "get_channel",
    "get_codes",
    "get_combined_metadata",
    "get_continuous",
    "get_cql",
    "get_daily",
    "get_field_measurements",
    "get_field_measurements_metadata",
    "get_latest_continuous",
    "get_latest_daily",
    "get_monitoring_locations",
    "get_nearest_continuous",
    "get_peaks",
    "get_queryables",
    "get_ratings",
    "get_reference_table",
    "get_samples",
    "get_samples_summary",
    "get_stats_date_range",
    "get_stats_por",
    "get_time_series_metadata",
]

#: Arguments a caller must supply positionally or by keyword. Adding an entry
#: here is a breaking change to every existing call; that is why
#: this mapping is written out instead of derived.
_REQUIRED_ARGUMENTS = {
    "get_channel": (),
    "get_codes": ("code_service",),
    "get_combined_metadata": (),
    "get_continuous": (),
    "get_cql": ("collection", "cql"),
    "get_daily": (),
    "get_field_measurements": (),
    "get_field_measurements_metadata": (),
    "get_latest_continuous": (),
    "get_latest_daily": (),
    "get_monitoring_locations": (),
    "get_peaks": (),
    "get_queryables": ("collection",),
    "get_reference_table": ("collection",),
    "get_samples": (),
    "get_samples_summary": ("monitoring_location_id",),
    "get_stats_date_range": (),
    "get_stats_por": (),
    "get_time_series_metadata": (),
}

#: Defaults that are deliberately not ``None``. Every other optional parameter defaults
#: to ``None``, which is how the request builder distinguishes an omitted argument from
#: a caller-supplied value -- a non-``None`` default adds a filter to every query
#: without the caller seeing it.
_INTENTIONAL_DEFAULTS = {
    "convert_type": True,
    "expand_percentiles": True,
    "page_size": 1000,
    "profile": "fullphyschem",
    "service": "results",
    "ssl_check": True,
}

#: The parameters shared by every collection getter, independent of which
#: queryables its collection happens to publish.
_CROSS_CUTTING_PARAMETERS = (
    "bbox",
    "convert_type",
    "filter",
    "filter_lang",
    "limit",
    "max_rows",
    "properties",
    "skip_geometry",
)

#: The continuous endpoint returns no geometries, so it exposes neither the
#: spatial filter nor the switch for dropping a geometry it never sends.
_NO_GEOMETRY_PARAMETERS = {"get_continuous": {"bbox", "skip_geometry"}}


def _collection_getters() -> list[str]:
    """The getters that query an OGC collection, identified by the ``**queryables``
    passthrough that only they have."""
    return [
        name
        for name in api.__all__
        if any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in inspect.signature(getattr(api, name)).parameters.values()
        )
    ]


def test_waterdata_exports_are_stable() -> None:
    assert waterdata.__all__ == _EXPECTED_WATERDATA_ALL
    assert all(hasattr(waterdata, name) for name in waterdata.__all__)


def test_facade_names_are_the_package_names() -> None:
    """Both import paths must reach the same objects, or a caller that switched
    paths gets different behavior from the same call."""
    assert set(api.__all__) <= set(waterdata.__all__)
    for name in api.__all__:
        assert getattr(waterdata, name) is getattr(api, name)


@pytest.mark.parametrize("name", api.__all__)
def test_getter_returns_frame_and_metadata(name: str) -> None:
    annotation = inspect.signature(getattr(api, name)).return_annotation
    assert annotation == "tuple[pd.DataFrame, BaseMetadata]"


@pytest.mark.parametrize("name", api.__all__)
def test_every_parameter_is_annotated_and_keyword_callable(name: str) -> None:
    """Positional-only parameters would make the keyword-argument style the
    documentation and notebooks use a TypeError."""
    for parameter in inspect.signature(getattr(api, name)).parameters.values():
        assert parameter.annotation is not inspect.Parameter.empty, parameter.name
        assert parameter.kind is not inspect.Parameter.POSITIONAL_ONLY, parameter.name


@pytest.mark.parametrize("name", api.__all__)
def test_required_arguments_have_not_grown(name: str) -> None:
    parameters = inspect.signature(getattr(api, name)).parameters
    required = tuple(
        parameter.name
        for parameter in parameters.values()
        if parameter.default is inspect.Parameter.empty
        and parameter.kind is not inspect.Parameter.VAR_KEYWORD
    )
    assert required == _REQUIRED_ARGUMENTS[name]


#: How many parameters each getter accepts by position, as released. A positional
#: call binds each argument by its position, so a parameter inserted or appended
#: without ``*`` changes what an existing call sends without raising. Change a
#: count only in a major release, with the deprecation notice ADR 0012 requires.
_POSITIONAL_COUNT = {
    "get_channel": 32,
    "get_codes": 1,
    "get_combined_metadata": 64,
    "get_continuous": 17,
    "get_cql": 2,
    "get_daily": 19,
    "get_field_measurements": 21,
    "get_field_measurements_metadata": 15,
    "get_latest_continuous": 19,
    "get_latest_daily": 19,
    "get_monitoring_locations": 49,
    "get_nearest_continuous": 3,
    "get_peaks": 19,
    "get_queryables": 1,
    "get_ratings": 8,
    "get_reference_table": 4,
    "get_samples": 25,
    "get_samples_summary": 2,
    "get_stats_date_range": 16,
    "get_stats_por": 16,
    "get_time_series_metadata": 29,
}


@pytest.mark.parametrize("name", sorted(_POSITIONAL_COUNT))
def test_no_parameter_is_added_by_position(name: str) -> None:
    """A new parameter goes after ``*``, so it cannot shift a positional call."""
    parameters = inspect.signature(getattr(waterdata, name)).parameters.values()
    positional = [p.name for p in parameters if p.kind is p.POSITIONAL_OR_KEYWORD]
    assert len(positional) == _POSITIONAL_COUNT[name], (
        f"{name} accepts {len(positional)} parameters by position, not "
        f"{_POSITIONAL_COUNT[name]}. Move the new parameter after the bare * in "
        "the signature."
    )


@pytest.mark.parametrize("name", api.__all__)
def test_optional_parameters_default_to_none(name: str) -> None:
    for parameter in inspect.signature(getattr(api, name)).parameters.values():
        if parameter.default is inspect.Parameter.empty or parameter.default is None:
            continue
        assert parameter.name in _INTENTIONAL_DEFAULTS, parameter.name
        assert parameter.default == _INTENTIONAL_DEFAULTS[parameter.name]


def test_collection_getters_share_the_cross_cutting_parameters() -> None:
    getters = _collection_getters()
    assert getters, "no collection getters found; the **queryables probe is stale"
    for name in getters:
        parameters = inspect.signature(getattr(api, name)).parameters
        expected = set(_CROSS_CUTTING_PARAMETERS) - _NO_GEOMETRY_PARAMETERS.get(
            name, set()
        )
        assert expected <= set(parameters), (
            f"{name} is missing {sorted(expected - set(parameters))}"
        )


def test_api_private_samples_compatibility_names_remain() -> None:
    assert isinstance(api._SAMPLES_PARAM_TO_API, dict)
    assert isinstance(api._SAMPLES_LEGACY_KWARGS, dict)
