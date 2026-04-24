from datetime import timedelta
from types import SimpleNamespace
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest
import requests

from dataretrieval.waterdata.utils import (
    _CQL_FILTER_CHUNK_LEN,
    _WATERDATA_URL_BYTE_LIMIT,
    _check_numeric_filter_pitfall,
    _chunk_cql_or,
    _construct_api_requests,
    _effective_filter_budget,
    _get_args,
    _split_top_level_or,
    _walk_pages,
)


def _query_params(prepared_request):
    return parse_qs(urlsplit(prepared_request.url).query)


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


def test_get_args_basic():
    local_vars = {
        "monitoring_location_id": "123",
        "service": "daily",
        "output_id": "daily_id",
        "none_val": None,
        "other": "val",
    }
    result = _get_args(local_vars)
    assert result == {"monitoring_location_id": "123", "other": "val"}


def test_get_args_with_exclude():
    local_vars = {
        "monitoring_location_id": "123",
        "service": "daily",
        "output_id": "daily_id",
        "to_exclude": "secret",
        "other": "val",
    }
    result = _get_args(local_vars, exclude={"to_exclude"})
    assert result == {"monitoring_location_id": "123", "other": "val"}


def test_get_args_empty():
    assert _get_args({}) == {}


def test_walk_pages_multiple_mocked():
    # Setup mock responses
    resp1 = mock.MagicMock()
    resp1.json.return_value = {
        "numberReturned": 1,
        "features": [{"id": "1", "properties": {"val": "a"}}],
        "links": [{"rel": "next", "href": "https://example.com/page2"}],
    }
    # Mock headers and links
    resp1.headers = {}
    resp1.links = {"next": {"url": "https://example.com/page2"}}
    resp1.status_code = 200

    resp2 = mock.MagicMock()
    resp2.json.return_value = {
        "numberReturned": 1,
        "features": [{"id": "2", "properties": {"val": "b"}}],
        "links": [],
    }
    resp2.headers = {}
    resp2.links = {}
    resp2.status_code = 200

    # Mock client (Session)
    mock_client = mock.MagicMock(spec=requests.Session)
    # First call to send() returns resp1, then call to request() in loop returns resp2
    mock_client.send.return_value = resp1
    mock_client.request.return_value = resp2

    # Mock request (PreparedRequest)
    mock_req = mock.MagicMock(spec=requests.PreparedRequest)
    mock_req.method = "GET"
    mock_req.headers = {}
    mock_req.url = "https://example.com/page1"

    # Call _walk_pages
    df, final_resp = _walk_pages(geopd=False, req=mock_req, client=mock_client)

    assert len(df) == 2
    assert list(df["val"]) == ["a", "b"]
    assert list(df["id"]) == ["1", "2"]
    assert mock_client.send.called
    assert mock_client.request.called
    assert mock_client.request.call_args[0][1] == "https://example.com/page2"


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


def test_chunk_cql_or_short_passthrough():
    expr = "time >= '2023-01-01T00:00:00Z'"
    assert _chunk_cql_or(expr, max_len=1000) == [expr]


def test_chunk_cql_or_splits_into_multiple():
    clause = "(time >= '2023-01-01T00:00:00Z' AND time <= '2023-01-01T00:30:00Z')"
    expr = " OR ".join([clause] * 200)
    chunks = _chunk_cql_or(expr, max_len=1000)
    # each chunk must be under the budget
    assert all(len(c) <= 1000 for c in chunks)
    # rejoined chunks must cover every clause
    rejoined_clauses = sum(len(c.split(" OR ")) for c in chunks)
    assert rejoined_clauses == 200
    # and must be a valid OR chain (each chunk is itself a top-level OR of clauses)
    assert len(chunks) > 1


def test_chunk_cql_or_unsplittable_returns_input():
    big = "value > 0 AND " + ("A " * 4000)
    assert _chunk_cql_or(big, max_len=1000) == [big]


def test_chunk_cql_or_single_clause_over_budget_returns_input():
    huge_clause = "(value > " + "9" * 6000 + ")"
    expr = f"{huge_clause} OR (value > 0)"
    assert _chunk_cql_or(expr, max_len=1000) == [expr]


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


def test_long_filter_fans_out_into_multiple_requests():
    """An oversized top-level OR filter triggers multiple HTTP requests
    whose results are concatenated."""
    from dataretrieval.waterdata import get_continuous

    clause = (
        "(time >= '2023-01-{day:02d}T00:00:00Z' "
        "AND time <= '2023-01-{day:02d}T00:30:00Z')"
    )
    expr = " OR ".join(clause.format(day=(i % 28) + 1) for i in range(300))
    assert len(expr) > _CQL_FILTER_CHUNK_LEN

    sent_filters = []

    def fake_construct_api_requests(**kwargs):
        sent_filters.append(kwargs.get("filter"))
        return _fake_prepared_request()

    def fake_walk_pages(*_args, **_kwargs):
        idx = len(sent_filters)
        frame = pd.DataFrame({"id": [f"chunk-{idx}"], "value": [idx]})
        return frame, _fake_response()

    with mock.patch(
        "dataretrieval.waterdata.utils._construct_api_requests",
        side_effect=fake_construct_api_requests,
    ), mock.patch(
        "dataretrieval.waterdata.utils._walk_pages", side_effect=fake_walk_pages
    ), mock.patch(
        "dataretrieval.waterdata.utils._effective_filter_budget",
        return_value=_CQL_FILTER_CHUNK_LEN,
    ):
        df, _ = get_continuous(
            monitoring_location_id="USGS-07374525",
            parameter_code="72255",
            filter=expr,
            filter_lang="cql-text",
        )

    # Mocking _effective_filter_budget bypasses the URL-length probe, so
    # sent_filters contains only real chunk requests. Assert invariants:
    # chunking happened, every original clause is preserved exactly once
    # in order, each chunk stays under the budget, and the mock's
    # one-row-per-chunk responses concatenate to a row per chunk.
    expected_parts = _split_top_level_or(expr)
    assert len(sent_filters) > 1
    rejoined_parts = []
    for chunk in sent_filters:
        rejoined_parts.extend(_split_top_level_or(chunk))
    assert rejoined_parts == expected_parts
    assert len(df) == len(sent_filters)
    assert all(len(chunk) <= _CQL_FILTER_CHUNK_LEN for chunk in sent_filters)


def test_long_filter_deduplicates_cross_chunk_overlap():
    """Features returned by multiple chunks (same feature `id`) are
    deduplicated in the concatenated result."""
    from dataretrieval.waterdata import get_continuous

    clause = (
        "(time >= '2023-01-{day:02d}T00:00:00Z' "
        "AND time <= '2023-01-{day:02d}T00:30:00Z')"
    )
    expr = " OR ".join(clause.format(day=(i % 28) + 1) for i in range(300))

    call_count = {"n": 0}

    def fake_walk_pages(*_args, **_kwargs):
        call_count["n"] += 1
        frame = pd.DataFrame({"id": ["shared-feature"], "value": [1]})
        return frame, _fake_response()

    with mock.patch(
        "dataretrieval.waterdata.utils._construct_api_requests",
        return_value=_fake_prepared_request(),
    ), mock.patch(
        "dataretrieval.waterdata.utils._walk_pages", side_effect=fake_walk_pages
    ), mock.patch(
        "dataretrieval.waterdata.utils._effective_filter_budget",
        return_value=_CQL_FILTER_CHUNK_LEN,
    ):
        df, _ = get_continuous(
            monitoring_location_id="USGS-07374525",
            parameter_code="72255",
            filter=expr,
            filter_lang="cql-text",
        )

    # Chunking must have happened (otherwise dedup wouldn't be exercised).
    assert call_count["n"] > 1
    # Even though each chunk returned a feature, dedup by id collapses them.
    assert len(df) == 1


def test_empty_chunks_do_not_downgrade_geodataframe():
    """A mix of empty and non-empty chunk responses must not downgrade a
    GeoDataFrame-typed result to a plain DataFrame. ``_get_resp_data``
    returns ``pd.DataFrame()`` on empty responses, which would otherwise
    strip geometry/CRS from the concatenated output."""
    pytest.importorskip("geopandas")
    import geopandas as gpd
    from shapely.geometry import Point

    from dataretrieval.waterdata import get_continuous

    clause = (
        "(time >= '2023-01-{day:02d}T00:00:00Z' "
        "AND time <= '2023-01-{day:02d}T00:30:00Z')"
    )
    expr = " OR ".join(clause.format(day=(i % 28) + 1) for i in range(300))

    call_count = {"n": 0}

    def fake_walk_pages(*_args, **_kwargs):
        call_count["n"] += 1
        # Chunk 2 returns empty; chunks 1 and 3 return GeoDataFrames.
        if call_count["n"] == 2:
            frame = pd.DataFrame()
        else:
            frame = gpd.GeoDataFrame(
                {"id": [f"feat-{call_count['n']}"], "value": [call_count["n"]]},
                geometry=[Point(call_count["n"], call_count["n"])],
                crs="EPSG:4326",
            )
        return frame, _fake_response()

    with mock.patch(
        "dataretrieval.waterdata.utils._construct_api_requests",
        return_value=_fake_prepared_request(),
    ), mock.patch(
        "dataretrieval.waterdata.utils._walk_pages", side_effect=fake_walk_pages
    ), mock.patch(
        "dataretrieval.waterdata.utils._effective_filter_budget",
        return_value=_CQL_FILTER_CHUNK_LEN,
    ):
        df, _ = get_continuous(
            monitoring_location_id="USGS-07374525",
            parameter_code="72255",
            filter=expr,
            filter_lang="cql-text",
        )

    # The empty chunk must not have stripped the GeoDataFrame type.
    assert isinstance(df, gpd.GeoDataFrame)
    assert "geometry" in df.columns
    assert df.crs is not None


def test_effective_filter_budget_respects_url_limit():
    """The computed budget, once encoded, fits within the URL byte limit
    alongside the other query params."""
    from urllib.parse import quote_plus

    filter_expr = "(time >= '2023-01-15T00:00:00Z' AND time <= '2023-01-15T00:30:00Z')"
    args = {
        "service": "continuous",
        "monitoring_location_id": "USGS-02238500",
        "parameter_code": "00060",
        "filter": filter_expr,
        "filter_lang": "cql-text",
    }
    raw_budget = _effective_filter_budget(args, filter_expr)

    # Build a chunk exactly at the raw budget (padded with the clause repeated)
    # and confirm the full URL it produces stays under the URL byte limit.
    padded = (" OR ".join([filter_expr] * 200))[:raw_budget]
    req = _construct_api_requests(**{**args, "filter": padded})
    assert len(req.url) <= _WATERDATA_URL_BYTE_LIMIT
    # And the budget scales inversely with encoding ratio (sanity).
    assert raw_budget < _WATERDATA_URL_BYTE_LIMIT
    # Quick sanity on the encoding math itself.
    assert len(quote_plus(padded)) <= _WATERDATA_URL_BYTE_LIMIT


def test_effective_filter_budget_uses_max_clause_ratio():
    """Heavy clauses clustered in one part of the filter must not be able
    to push any chunk over the URL limit. The budget is computed against
    the max per-clause encoding ratio, not the whole-filter average, so
    a chunk of only-heaviest-clauses still fits."""
    from urllib.parse import quote_plus

    heavy = (
        "(time >= '2023-01-15T00:00:00Z' AND time <= '2023-01-15T00:30:00Z' "
        "AND approval_status IN ('Approved','Provisional','Revised'))"
    )
    light = "(time >= '2023-01-15T00:00:00Z' AND time <= '2023-01-15T00:30:00Z')"
    # Heavy ratio < light ratio for these shapes; cluster them at opposite
    # ends so the chunker must produce at least one light-only chunk.
    clauses = [heavy] * 100 + [light] * 400
    expr = " OR ".join(clauses)
    args = {
        "service": "continuous",
        "monitoring_location_id": "USGS-02238500",
        "filter": expr,
        "filter_lang": "cql-text",
    }
    budget = _effective_filter_budget(args, expr)
    chunks = _chunk_cql_or(expr, max_len=budget)
    assert len(chunks) > 1

    # Every chunk, once built into a full request, fits under the URL byte
    # limit — even the all-light chunks that have a higher-than-average ratio.
    for chunk in chunks:
        req = _construct_api_requests(**{**args, "filter": chunk})
        assert len(req.url) <= _WATERDATA_URL_BYTE_LIMIT, (
            f"chunk url {len(req.url)} exceeds {_WATERDATA_URL_BYTE_LIMIT}"
        )

    # Budget should be tight enough that a chunk of only-light clauses
    # (the heavier-encoding shape here) still fits.
    assert len(quote_plus(light)) * (budget // len(light)) < _WATERDATA_URL_BYTE_LIMIT


def test_effective_filter_budget_passes_through_when_no_url_space():
    """If the non-filter URL already exceeds the byte limit, chunking
    cannot make the request succeed. The budget helper should signal
    pass-through (return a budget larger than the filter) so
    ``_chunk_cql_or`` emits one chunk — one 414 from the server is
    clearer than a burst of N guaranteed-414 sub-requests."""
    expr = " OR ".join(
        ["(time >= '2023-01-15T00:00:00Z' AND time <= '2023-01-15T00:30:00Z')"] * 50
    )
    with mock.patch(
        "dataretrieval.waterdata.utils._construct_api_requests",
        return_value=_fake_prepared_request(url="https://example.test/" + "A" * 9000),
    ):
        budget = _effective_filter_budget({"filter": expr}, expr)
    # Budget is large enough that _chunk_cql_or returns the expression
    # unchanged (passthrough) rather than producing many small chunks.
    assert budget > len(expr)
    assert _chunk_cql_or(expr, max_len=budget) == [expr]


def test_effective_filter_budget_shrinks_with_more_url_params():
    """Adding more scalar query params consumes URL bytes and should
    shrink the raw filter budget accordingly. Use a filter large enough
    to skip the short-circuit fast path so the probe actually runs."""
    clause = "(time >= '2023-01-15T00:00:00Z' AND time <= '2023-01-15T00:30:00Z')"
    expr = " OR ".join([clause] * 100)
    sparse_args = {
        "service": "continuous",
        "monitoring_location_id": "USGS-02238500",
        "filter": expr,
        "filter_lang": "cql-text",
    }
    dense_args = {
        **sparse_args,
        "parameter_code": "00060",
        "statistic_id": "00003",
        "last_modified": "2023-01-01T00:00:00Z/2023-12-31T23:59:59Z",
    }
    sparse_budget = _effective_filter_budget(sparse_args, expr)
    dense_budget = _effective_filter_budget(dense_args, expr)
    assert dense_budget < sparse_budget


def test_cql_json_filter_is_not_chunked():
    """Chunking applies only to cql-text; cql-json is passed through unchanged."""
    from dataretrieval.waterdata import get_continuous

    clause = "(time >= '2023-01-01T00:00:00Z' AND time <= '2023-01-01T00:30:00Z')"
    expr = " OR ".join([clause] * 300)
    sent_filters = []

    def fake_construct_api_requests(**kwargs):
        sent_filters.append(kwargs.get("filter"))
        return _fake_prepared_request()

    with mock.patch(
        "dataretrieval.waterdata.utils._construct_api_requests",
        side_effect=fake_construct_api_requests,
    ), mock.patch(
        "dataretrieval.waterdata.utils._walk_pages",
        return_value=(
            pd.DataFrame({"id": ["row-1"], "value": [1]}),
            _fake_response(),
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
        # Footgun identifiers appearing only inside string literals
        "monitoring_location_id = 'USGS-value >= 1000'",
        "name = 'why I care about parameter_code = 60'",
        "note = 'see district_code = 1 in docs'",
        # Multi-clause where every comparison is quoted
        "parameter_code = '00060' AND statistic_id = '00011'",
    ],
)
def test_check_numeric_filter_pitfall_allows(expr):
    """Quoted literals and comparisons that don't pair a field with an
    unquoted numeric literal must not trigger the check."""
    _check_numeric_filter_pitfall(expr)  # must not raise


def test_get_continuous_surfaces_pitfall_to_caller():
    """End-to-end: the check runs at the ``get_continuous`` boundary,
    not as a deep internal-only protection, so callers see the error
    before any HTTP traffic."""
    from dataretrieval.waterdata import get_continuous

    with mock.patch("dataretrieval.waterdata.utils._construct_api_requests") as build:
        with pytest.raises(ValueError, match="lexicographic"):
            get_continuous(
                monitoring_location_id="USGS-02238500",
                parameter_code="00060",
                filter="value >= 1000",
                filter_lang="cql-text",
            )
        build.assert_not_called()
