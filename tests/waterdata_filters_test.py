from datetime import timedelta
from types import SimpleNamespace
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest

from dataretrieval.waterdata import get_continuous
from dataretrieval.waterdata.filters import (
    _check_numeric_filter_pitfall,
    _split_top_level_or,
)
from dataretrieval.waterdata.utils import _construct_api_requests


def _query_params(prepared_request):
    return parse_qs(urlsplit(str(prepared_request.url)).query)


def _fake_prepared_request(url="https://example.test"):
    """Stand-in for the object ``_construct_api_requests`` returns."""
    return SimpleNamespace(url=url, method="GET", headers={})


def _fake_response(url="https://example.test", elapsed_ms=1):
    """Stand-in for the response object ``_walk_pages`` returns."""
    return SimpleNamespace(
        url=url,
        elapsed=timedelta(milliseconds=elapsed_ms),
        headers={},
    )


def test_construct_filter_passthrough():
    """`filter` is forwarded verbatim as a query parameter."""
    expr = (
        "(time >= '2023-01-06T16:00:00Z' AND time <= '2023-01-06T18:00:00Z') "
        "OR (time >= '2023-01-10T18:00:00Z' AND time <= '2023-01-10T20:00:00Z')"
    )
    req = _construct_api_requests(
        service="continuous",
        monitoring_location_id="USGS-07374525",
        parameter_code="72255",
        filter=expr,
    )
    qs = _query_params(req)
    assert qs["filter"] == [expr]


def test_construct_filter_lang_hyphenated():
    """The Python kwarg `filter_lang` is sent as URL key `filter-lang`."""
    req = _construct_api_requests(
        service="continuous",
        monitoring_location_id="USGS-07374525",
        parameter_code="72255",
        filter="time >= '2023-01-01T00:00:00Z'",
        filter_lang="cql-text",
    )
    qs = _query_params(req)
    assert qs["filter-lang"] == ["cql-text"]
    assert "filter_lang" not in qs


def test_split_top_level_or_simple():
    parts = _split_top_level_or("A OR B OR C")
    assert parts == ["A", "B", "C"]


def test_split_top_level_or_case_insensitive():
    assert _split_top_level_or("A or B Or C") == ["A", "B", "C"]


def test_split_top_level_or_respects_parens():
    assert _split_top_level_or("(A OR B) OR (C OR D)") == ["(A OR B)", "(C OR D)"]


def test_split_top_level_or_respects_quotes():
    expr = "name = 'foo OR bar' OR id = 1"
    assert _split_top_level_or(expr) == ["name = 'foo OR bar'", "id = 1"]


def test_split_top_level_or_handles_doubled_quote_escape():
    """CQL text escapes a single quote inside a literal as ``''``. The
    two quotes are adjacent, so the scanner's naive toggle-on-quote logic
    happens to land back in the correct state with nothing between the
    toggles to misclassify. Lock that behavior in so a future refactor
    can't regress it."""
    cases = [
        ("name = 'O''Reilly OR Co' OR id = 1", ["name = 'O''Reilly OR Co'", "id = 1"]),
        ("name = 'It''s' OR id = 1", ["name = 'It''s'", "id = 1"]),
        (
            "name = 'alpha ''or'' beta' OR id = 1",
            ["name = 'alpha ''or'' beta'", "id = 1"],
        ),
        ("'x'' OR ''y' OR id = 1", ["'x'' OR ''y'", "id = 1"]),
    ]
    for expr, expected in cases:
        assert _split_top_level_or(expr) == expected, expr


def test_split_top_level_or_single_clause():
    assert _split_top_level_or("time >= '2023-01-01T00:00:00Z'") == [
        "time >= '2023-01-01T00:00:00Z'"
    ]


@pytest.mark.parametrize(
    "service",
    [
        "daily",
        "continuous",
        "monitoring-locations",
        "time-series-metadata",
        "latest-continuous",
        "latest-daily",
        "field-measurements",
        "channel-measurements",
    ],
)
def test_construct_filter_on_all_ogc_services(service):
    """Filter passthrough works uniformly for every OGC collection endpoint."""
    req = _construct_api_requests(
        service=service,
        filter="value > 0",
        filter_lang="cql-text",
    )
    qs = _query_params(req)
    assert qs["filter"] == ["value > 0"]
    assert qs["filter-lang"] == ["cql-text"]


def _filter_chunking_clauses(n: int = 300) -> str:
    """Stock long filter used by the end-to-end fan-out tests below."""
    clause = (
        "(time >= '2023-01-{day:02d}T00:00:00Z' "
        "AND time <= '2023-01-{day:02d}T00:30:00Z')"
    )
    return " OR ".join(clause.format(day=(i % 28) + 1) for i in range(n))


def _filter_size_aware_build(**kwargs):
    """Fake ``_construct_api_requests`` whose returned URL length scales
    with the request's ``filter`` value, so the joint planner naturally
    triggers chunking on long filters."""
    return _fake_prepared_request(
        url=f"https://example.test/?filter={kwargs.get('filter', '')}",
    )


def test_long_filter_fans_out_into_multiple_requests():
    """An oversized top-level OR filter triggers multiple HTTP
    sub-requests via the joint planner; every original clause is
    preserved across sub-requests; results concatenate to one row per
    sub-request given the one-row-per-chunk mock."""
    expr = _filter_chunking_clauses()
    sent_filters: list[str] = []

    async def fake_walk_pages(*, geopd, req):
        idx = len(sent_filters)
        sent_filters.append(_query_params(req).get("filter", [None])[0])
        return pd.DataFrame({"id": [f"chunk-{idx}"], "value": [idx]}), _fake_response()

    with (
        mock.patch(
            "dataretrieval.waterdata.utils._construct_api_requests",
            side_effect=_filter_size_aware_build,
        ),
        mock.patch(
            "dataretrieval.waterdata.utils._walk_pages",
            side_effect=fake_walk_pages,
        ),
    ):
        df, _ = get_continuous(
            monitoring_location_id="USGS-07374525",
            parameter_code="72255",
            filter=expr,
            filter_lang="cql-text",
        )

    expected_parts = _split_top_level_or(expr)
    assert len(sent_filters) > 1
    rejoined_parts: list[str] = []
    for chunk in sent_filters:
        rejoined_parts.extend(_split_top_level_or(chunk))
    assert rejoined_parts == expected_parts
    assert len(df) == len(sent_filters)


def test_long_filter_deduplicates_cross_chunk_overlap():
    """Features returned by multiple sub-requests with the same ``id``
    are deduplicated in the concatenated result."""
    expr = _filter_chunking_clauses()
    call_count = {"n": 0}

    async def fake_walk_pages(*_args, **_kwargs):
        call_count["n"] += 1
        return (
            pd.DataFrame({"id": ["shared-feature"], "value": [1]}),
            _fake_response(),
        )

    with (
        mock.patch(
            "dataretrieval.waterdata.utils._construct_api_requests",
            side_effect=_filter_size_aware_build,
        ),
        mock.patch(
            "dataretrieval.waterdata.utils._walk_pages",
            side_effect=fake_walk_pages,
        ),
    ):
        df, _ = get_continuous(
            monitoring_location_id="USGS-07374525",
            parameter_code="72255",
            filter=expr,
            filter_lang="cql-text",
        )

    assert call_count["n"] > 1  # chunking must have happened
    assert len(df) == 1  # dedup by ``id`` collapses the duplicates


def test_empty_chunks_do_not_downgrade_geodataframe():
    """A mix of empty and non-empty sub-request responses must not
    downgrade a GeoDataFrame-typed result to a plain DataFrame.
    ``_get_resp_data`` returns ``pd.DataFrame()`` on empty responses,
    which would otherwise strip geometry/CRS from the concatenated
    output."""
    pytest.importorskip("geopandas")
    import geopandas as gpd
    from shapely.geometry import Point

    expr = _filter_chunking_clauses()
    call_count = {"n": 0}

    async def fake_walk_pages(*_args, **_kwargs):
        call_count["n"] += 1
        if call_count["n"] == 2:
            return pd.DataFrame(), _fake_response()
        return (
            gpd.GeoDataFrame(
                {"id": [f"feat-{call_count['n']}"], "value": [call_count["n"]]},
                geometry=[Point(call_count["n"], call_count["n"])],
                crs="EPSG:4326",
            ),
            _fake_response(),
        )

    with (
        mock.patch(
            "dataretrieval.waterdata.utils._construct_api_requests",
            side_effect=_filter_size_aware_build,
        ),
        mock.patch(
            "dataretrieval.waterdata.utils._walk_pages",
            side_effect=fake_walk_pages,
        ),
    ):
        df, _ = get_continuous(
            monitoring_location_id="USGS-07374525",
            parameter_code="72255",
            filter=expr,
            filter_lang="cql-text",
        )

    assert isinstance(df, gpd.GeoDataFrame)
    assert "geometry" in df.columns
    assert df.crs is not None


def test_cql_json_filter_is_not_chunked():
    """Chunking applies only to cql-text; cql-json is passed through unchanged."""
    clause = "(time >= '2023-01-01T00:00:00Z' AND time <= '2023-01-01T00:30:00Z')"
    expr = " OR ".join([clause] * 300)
    sent_filters = []

    def fake_construct_api_requests(**kwargs):
        sent_filters.append(kwargs.get("filter"))
        return _fake_prepared_request()

    with (
        mock.patch(
            "dataretrieval.waterdata.utils._construct_api_requests",
            side_effect=fake_construct_api_requests,
        ),
        mock.patch(
            "dataretrieval.waterdata.utils._walk_pages",
            new=mock.AsyncMock(
                return_value=(
                    pd.DataFrame({"id": ["row-1"], "value": [1]}),
                    _fake_response(),
                )
            ),
        ),
    ):
        get_continuous(
            monitoring_location_id="USGS-07374525",
            parameter_code="72255",
            filter=expr,
            filter_lang="cql-json",
        )

    assert sent_filters == [expr]


@pytest.mark.parametrize(
    "expr",
    [
        # The motivating case — numeric-valued string field
        "value >= 1000",
        "value > 1000",
        "value <= 1000",
        "value < 1000",
        "value = 1000",
        "value != 1000",
        "value >= 1000.5",
        "value >= -50",
        # Zero-padded codes: `parameter_code = 60` matches nothing
        # because the real values are all `'00060'`-shaped
        "parameter_code = 60",
        "statistic_id = 11",
        "district_code = 1",
        "county_code != 0",
        "hydrologic_unit_code = 20301030401",
        # Channel-measurements numeric-looking string fields
        "channel_flow > 500",
        "channel_velocity >= 1.5",
        # Scientific notation — floats expressed as 1e5, 1.5e-3
        "value > 1e5",
        "value >= 2.5E+3",
        "value < 1.5e-3",
        # Leading-dot decimals (``.5`` is a fraction, not a typo)
        "value > .5",
        "value >= -.5",
        "value < .5e-3",
        # ``IN`` list form — same footgun, common pattern for codes
        "parameter_code IN (60, 61)",
        "value IN (10, 20, 30)",
        "statistic_id in (11)",  # case-insensitive, single-element
        # ``NOT IN`` with numbers — same footgun via negation
        "value NOT IN (1, 2, 3)",
        "parameter_code not in (60, 61)",
        # ``BETWEEN`` range form — same footgun
        "value BETWEEN 5 AND 10",
        "channel_flow between 100 and 500",
        # ``NOT BETWEEN`` with numbers
        "value NOT BETWEEN 0 AND 100",
        "channel_flow not between 50 and 150",
        # Composite expressions
        "time >= '2023-01-01T00:00:00Z' AND value >= 1000",
        "value > 1000 OR value < 0",
        "parameter_code = 60 AND statistic_id = 11",
        # Reverse (literal on the left)
        "1000 <= value",
        "60 = parameter_code",
    ],
)
def test_check_numeric_filter_pitfall_raises(expr):
    """Unquoted numeric comparisons against any field resolve
    lexicographically on this API — every queryable is string-typed —
    so reject them with a clear message before the request is sent."""
    with pytest.raises(ValueError, match="lexicographic"):
        _check_numeric_filter_pitfall(expr)


@pytest.mark.parametrize(
    "expr",
    [
        # Quoted literals — caller has opted into string comparison
        "value >= '1000'",
        "value = '42.5'",
        "parameter_code = '00060'",
        "district_code = '01'",
        "hydrologic_unit_code = '020301030401'",
        # Pure string comparisons
        "time >= '2023-01-01T00:00:00Z' AND time <= '2023-01-02T00:00:00Z'",
        "monitoring_location_id = 'USGS-02238500'",
        "approval_status = 'Approved'",
        "qualifier IN ('A', 'P')",
        "parameter_code IN ('00060', '00065')",
        "value BETWEEN '1' AND '9'",
        # Footgun identifiers appearing only inside string literals
        "monitoring_location_id = 'USGS-value >= 1000'",
        "name = 'why I care about parameter_code = 60'",
        "note = 'see district_code = 1 in docs'",
        "note = 'quoted: value IN (10, 20) within literal'",
        # Multi-clause where every comparison is quoted
        "parameter_code = '00060' AND statistic_id = '00011'",
        # CQL escape-quote (``O''Reilly``) within a quoted literal
        "name = 'O''Reilly 1000'",
        # Identifiers that start with "NOT" (e.g. ``NOTES``) must not be
        # mistakenly treated as the CQL negation keyword
        "NOTES = 'hello'",
        "NOTE_VAL LIKE 'anything%'",
    ],
)
def test_check_numeric_filter_pitfall_allows(expr):
    """Quoted literals and comparisons that don't pair a field with an
    unquoted numeric literal must not trigger the check."""
    _check_numeric_filter_pitfall(expr)  # must not raise


@pytest.mark.parametrize(
    "expr,field,op",
    [
        ("value NOT IN (1, 2)", "value", "NOT IN"),
        ("parameter_code NOT IN (60, 61)", "parameter_code", "NOT IN"),
        ("value IN (1, 2)", "value", "IN"),
        ("value NOT BETWEEN 0 AND 10", "value", "NOT BETWEEN"),
        ("channel_flow between 100 and 500", "channel_flow", "BETWEEN"),
    ],
)
def test_pitfall_error_names_real_field_not_NOT_keyword(expr, field, op):
    """The CQL keyword ``NOT`` must not be reported as the offending field
    — the error should identify the actual column and include ``NOT`` as
    part of the operator form so the caller knows what to quote."""
    with pytest.raises(ValueError) as exc:
        _check_numeric_filter_pitfall(expr)
    msg = str(exc.value)
    assert f"against {field!r}" in msg, msg
    assert op.upper() in msg.upper(), msg


def test_get_continuous_surfaces_pitfall_to_caller():
    """End-to-end: the check runs at the ``get_continuous`` boundary,
    not as a deep internal-only protection, so callers see the error
    before any HTTP traffic."""
    with mock.patch("dataretrieval.waterdata.utils._construct_api_requests") as build:
        with pytest.raises(ValueError, match="lexicographic"):
            get_continuous(
                monitoring_location_id="USGS-02238500",
                parameter_code="00060",
                filter="value >= 1000",
                filter_lang="cql-text",
            )
        build.assert_not_called()
