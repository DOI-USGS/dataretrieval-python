import copy
import datetime
import functools
import json
import re
import warnings
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import numpy as np
import pandas as pd
import pytest
from pandas import DataFrame

from dataretrieval.ogc.requests import (
    _check_monitoring_location_id,
    _normalize_str_iterable,
)
from dataretrieval.ogc.requests import (
    _construct_api_requests as _construct_api_requests_explicit,
)
from dataretrieval.ogc.requests import (
    _construct_cql_request as _construct_cql_request_explicit,
)
from dataretrieval.waterdata import (
    get_channel,
    get_combined_metadata,
    get_continuous,
    get_cql,
    get_daily,
    get_field_measurements,
    get_field_measurements_metadata,
    get_latest_continuous,
    get_latest_daily,
    get_monitoring_locations,
    get_peaks,
    get_reference_table,
    get_samples,
    get_samples_summary,
    get_stats_date_range,
    get_stats_por,
    get_time_series_metadata,
)
from dataretrieval.waterdata.types import _check_profiles
from dataretrieval.waterdata.utils import (
    OGC_API_URL,
    WATERDATA_DIALECT,
    _get_args,
)

_OGC_BASE = "https://api.waterdata.usgs.gov/ogcapi/v0"
_STATS_BASE = "https://api.waterdata.usgs.gov/statistics/v0"

#: Two real features per collection, captured from the live collection and trimmed.
#: Property names, nesting, and value types (including the numeric-looking
#: strings the API really sends) are verbatim; only the row count is reduced.
#: Regenerate a collection by re-querying it with ``limit=2`` and replacing that
#: key -- the getters' behavior depends on the shape, not the row count.
_OGC_FIXTURES = json.loads(
    (Path(__file__).parent / "data" / "waterdata_ogc_fixtures.json").read_text()
)


# The direct request-construction unit tests below bypass ``get_ogc_data``,
# which is what normally binds the Water Data base URL and dialect (the OGC
# package names no collection of its own). Bind them here the same way the
# engine does — explicitly, via ``functools.partial`` — so the tests exercise
# the real Water Data behavior (monitoring-locations -> POST/CQL2; daily ->
# date-only time args).
_construct_api_requests = functools.partial(
    _construct_api_requests_explicit, base_url=OGC_API_URL, dialect=WATERDATA_DIALECT
)
_construct_cql_request = functools.partial(
    _construct_cql_request_explicit, base_url=OGC_API_URL
)


def mock_request(httpx_mock, request_url, file_path):
    """Mock request code"""
    with open(file_path) as text:
        httpx_mock.add_response(
            method="GET",
            url=request_url,
            text=text.read(),
            headers={"mock_header": "value"},
        )


def test_mock_get_samples(httpx_mock):
    """Tests USGS Samples query"""
    request_url = (
        "https://api.waterdata.usgs.gov/samples-data/results/fullphyschem?"
        "activityMediaName=Water&activityStartDateLower=2020-01-01"
        "&activityStartDateUpper=2024-12-31&monitoringLocationIdentifier=USGS-05406500&mimeType=text%2Fcsv"
    )
    response_file_path = "tests/data/samples_results.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = get_samples(
        service="results",
        profile="fullphyschem",
        activity_media_name="Water",
        activity_start_date_lower="2020-01-01",
        activity_start_date_upper="2024-12-31",
        monitoring_location_id="USGS-05406500",
    )
    assert type(df) is DataFrame
    # 181 source columns + 6 derived <prefix>DateTime columns
    assert df.shape == (67, 187)
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None
    assert df["Activity_StartDateTime"].notna().any()


def test_mock_get_samples_summary(httpx_mock):
    """Tests USGS Samples summary query"""
    request_url = (
        "https://api.waterdata.usgs.gov/samples-data/summary/USGS-04183500"
        "?mimeType=text%2Fcsv"
    )
    response_file_path = "tests/data/samples_summary.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = get_samples_summary(monitoring_location_id="USGS-04183500")
    assert type(df) is DataFrame
    expected_columns = {
        "monitoringLocationIdentifier",
        "characteristicGroup",
        "characteristic",
        "characteristicUserSupplied",
        "resultCount",
        "activityCount",
        "firstActivity",
        "mostRecentActivity",
    }
    assert expected_columns.issubset(df.columns)
    assert (df["monitoringLocationIdentifier"] == "USGS-04183500").all()
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None


def test_get_samples_summary_rejects_list():
    """The summary endpoint accepts only one site; a list must raise TypeError."""
    with pytest.raises(TypeError, match="exactly one monitoring location"):
        get_samples_summary(monitoring_location_id=["USGS-04183500"])


def test_get_samples_raises_typed_error_on_429(httpx_mock):
    """Non-200 from the Samples endpoint now raises the module's typed error
    (RateLimited on 429) — consistent with the OGC/stats path — instead of a
    bare httpx.HTTPStatusError."""
    from dataretrieval.exceptions import RateLimited

    httpx_mock.add_response(status_code=429, headers={"Retry-After": "30"})
    with pytest.raises(RateLimited):
        get_samples(
            service="results",
            profile="fullphyschem",
            monitoring_location_id="USGS-05406500",
        )


def test_get_samples_summary_raises_typed_error_on_5xx(httpx_mock):
    """A 5xx from the Samples summary endpoint raises ServiceUnavailable."""
    from dataretrieval.exceptions import ServiceUnavailable

    httpx_mock.add_response(status_code=503)
    with pytest.raises(ServiceUnavailable):
        get_samples_summary(monitoring_location_id="USGS-04183500")


def test_get_samples_legacy_camelcase_kwargs_warn(httpx_mock):
    """Legacy camelCase kwargs still work but emit a DeprecationWarning that
    names the new snake_case parameter, and produce the same request URL as
    the snake_case call."""
    request_url = (
        "https://api.waterdata.usgs.gov/samples-data/results/fullphyschem?"
        "activityMediaName=Water&activityStartDateLower=2020-01-01"
        "&activityStartDateUpper=2024-12-31&monitoringLocationIdentifier=USGS-05406500&mimeType=text%2Fcsv"
    )
    response_file_path = "tests/data/samples_results.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    with pytest.warns(DeprecationWarning, match="monitoring_location_id"):
        df, md = get_samples(
            service="results",
            profile="fullphyschem",
            activityMediaName="Water",
            activityStartDateLower="2020-01-01",
            activityStartDateUpper="2024-12-31",
            monitoringLocationIdentifier="USGS-05406500",
        )
    assert type(df) is DataFrame
    # The deprecated names map to the same camelCase wire params: same URL.
    assert md.url == request_url


def test_get_samples_summary_legacy_camelcase_kwarg_warns(httpx_mock):
    """The deprecated ``monitoringLocationIdentifier`` keyword still works for
    the summary endpoint and warns, naming the new snake_case parameter."""
    request_url = (
        "https://api.waterdata.usgs.gov/samples-data/summary/USGS-04183500"
        "?mimeType=text%2Fcsv"
    )
    response_file_path = "tests/data/samples_summary.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    with pytest.warns(DeprecationWarning, match="monitoring_location_id"):
        df, md = get_samples_summary(monitoringLocationIdentifier="USGS-04183500")
    assert type(df) is DataFrame
    assert md.url == request_url


def test_get_samples_rejects_both_legacy_and_new_kwarg():
    """Passing both the deprecated camelCase name and its snake_case
    replacement is ambiguous and must raise TypeError."""
    with pytest.raises(TypeError, match="monitoring_location_id"):
        get_samples(
            monitoringLocationIdentifier="USGS-05406500",
            monitoring_location_id="USGS-05406500",
        )


def test_accept_legacy_kwargs_passthrough_no_warning(recwarn):
    """Using only the new names emits no DeprecationWarning."""
    from dataretrieval.waterdata.utils import _accept_legacy_kwargs

    @_accept_legacy_kwargs({"oldName": "new_name"})
    def f(new_name=None):
        return new_name

    assert f(new_name="x") == "x"
    assert not [w for w in recwarn.list if w.category is DeprecationWarning]


def test_every_legacy_camelcase_samples_kwarg_is_backward_compatible():
    """Every deprecated camelCase ``get_samples`` parameter stays backward
    compatible: it is still accepted, is renamed to its snake_case replacement
    with a ``DeprecationWarning``, and resolves to the exact same Samples-API
    wire parameter it always did — so existing camelCase call sites keep
    producing identical requests after the rename. Covers the whole mapping, so
    a future param renamed without a legacy alias fails here."""
    from dataretrieval.waterdata.api import (
        _SAMPLES_LEGACY_KWARGS,
        _SAMPLES_PARAM_TO_API,
    )
    from dataretrieval.waterdata.utils import _accept_legacy_kwargs

    assert _SAMPLES_LEGACY_KWARGS, "expected a non-empty legacy-kwarg mapping"

    received = {}

    @_accept_legacy_kwargs(_SAMPLES_LEGACY_KWARGS)
    def spy(**kwargs):
        received.clear()
        received.update(kwargs)

    for old_camel, new_snake in _SAMPLES_LEGACY_KWARGS.items():
        # The deprecated camelCase name is accepted, warns, and is translated to
        # the snake_case parameter the function now expects ...
        with pytest.warns(DeprecationWarning, match=new_snake):
            spy(**{old_camel: "sentinel"})
        assert received == {new_snake: "sentinel"}, (
            f"legacy {old_camel!r} did not map to {new_snake!r}"
        )
        # ... and that snake_case parameter resolves back to the same camelCase
        # wire name, so the request is byte-identical to the pre-rename behavior.
        assert _SAMPLES_PARAM_TO_API[new_snake] == old_camel


def test_legacy_camelcase_kwargs_return_identical_to_snake_case(httpx_mock):
    """End-to-end: a legacy camelCase ``get_samples`` call returns results
    byte-identical to the equivalent snake_case call — same request URL and same
    DataFrame — for every renamed parameter at once. The camelCase shim changes
    nothing the caller sees but the parameter names."""
    import warnings

    from dataretrieval.waterdata.api import _SAMPLES_LEGACY_KWARGS

    def value_for(snake):
        if snake == "monitoring_location_id":
            return "USGS-01646500"  # must satisfy the AGENCY-ID format check
        if snake in {
            "point_location_latitude",
            "point_location_longitude",
            "point_location_within_miles",
        }:
            return 42.5
        if snake == "bbox":
            return [-90.0, 30.0, -89.0, 31.0]
        return "x"

    with open("tests/data/samples_results.txt") as fh:
        body = fh.read()
    # one mocked response per call; match any URL so both requests are captured.
    httpx_mock.add_response(text=body, headers={"mock_header": "v"})
    httpx_mock.add_response(text=body, headers={"mock_header": "v"})

    new_kwargs = {snake: value_for(snake) for snake in _SAMPLES_LEGACY_KWARGS.values()}
    legacy_kwargs = {
        camel: value_for(snake) for camel, snake in _SAMPLES_LEGACY_KWARGS.items()
    }

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        df_legacy, md_legacy = get_samples(
            service="results", profile="fullphyschem", **legacy_kwargs
        )
    df_new, md_new = get_samples(
        service="results", profile="fullphyschem", **new_kwargs
    )

    legacy_req, new_req = httpx_mock.get_requests()
    assert str(legacy_req.url) == str(new_req.url)
    assert md_legacy.url == md_new.url
    assert df_legacy.equals(df_new)


def test_check_profiles():
    """Tests that correct errors are raised for invalid profiles."""
    with pytest.raises(ValueError):
        _check_profiles(service="foo", profile="bar")
    with pytest.raises(ValueError):
        _check_profiles(service="results", profile="foo")


def test_construct_api_requests_multivalue_get():
    """Multi-value params use GET with comma-separated values for daily collection."""
    req = _construct_api_requests(
        "daily",
        monitoring_location_id=["USGS-05427718", "USGS-05427719"],
        parameter_code=["00060", "00065"],
    )
    assert req.method == "GET"
    assert "monitoring_location_id=USGS-05427718%2CUSGS-05427719" in str(req.url)
    assert "parameter_code=00060%2C00065" in str(req.url)


def test_construct_api_requests_omits_empty_list():
    """An empty list value is omitted from the URL, not emitted as a filterless
    ``&parameter_code=`` (which the server reads as 'match empty')."""
    req = _construct_api_requests(
        "daily", monitoring_location_id="USGS-05427718", parameter_code=[]
    )
    assert "parameter_code" not in str(req.url)
    assert "monitoring_location_id=USGS-05427718" in str(req.url)


def test_construct_api_requests_monitoring_locations_post():
    """monitoring-locations uses POST+CQL2 for multi-value params (API limitation)."""
    req = _construct_api_requests(
        "monitoring-locations",
        hydrologic_unit_code=["010802050102", "010802050103"],
    )
    assert req.method == "POST"
    assert req.headers["Content-Type"] == "application/query-cql-json"

    # Body is serialized compactly (tight separators, no whitespace): the
    # body counts against the server's ~8 KB request-size cap and the
    # chunk planner's byte budget, so pretty-printing would needlessly
    # halve how many ids fit per chunk and double the chunk count.
    raw = req.content.decode()
    assert "\n" not in raw and ", " not in raw and ": " not in raw

    body = json.loads(req.content)
    # Top-level shape: AND over a list of per-param predicates.
    assert body["op"] == "and"
    assert isinstance(body["args"], list) and len(body["args"]) == 1

    # The single predicate is an IN over hydrologic_unit_code with both values.
    predicate = body["args"][0]
    assert predicate["op"] == "in"
    assert predicate["args"][0] == {"property": "hydrologic_unit_code"}
    assert predicate["args"][1] == ["010802050102", "010802050103"]


def test_construct_cql_request_post_verbatim_body():
    """get_cql's request builder POSTs the CQL2 body verbatim with the
    right content-type, and puts the OGC knobs on the URL."""
    body = json.dumps(
        {"op": "like", "args": [{"property": "hydrologic_unit_code"}, "02070010%"]},
        separators=(",", ":"),
    )
    req = _construct_cql_request(
        "daily",
        body,
        properties=["id", "value"],
        bbox=[-90.0, 40.0, -89.0, 41.0],
        limit=10,
        skip_geometry=True,
    )
    assert req.method == "POST"
    assert req.headers["Content-Type"] == "application/query-cql-json"
    assert str(req.url).startswith(
        "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items"
    )
    # The body is sent through unchanged, not re-serialized.
    assert req.content.decode() == body
    url = str(req.url)
    assert "skipGeometry=true" in url
    assert "limit=10" in url
    assert "bbox=-90.0%2C40.0%2C-89.0%2C41.0" in url
    assert "properties=id%2Cvalue" in url


def test_construct_cql_request_skip_geometry_none_omits_param():
    """skip_geometry=None leaves skipGeometry unset (server default), so it never
    reaches the URL — matching get_cql's default."""
    req = _construct_cql_request("daily", "{}")
    assert "skipGeometry" not in str(req.url)


def test_get_cql_service_keyword_is_deprecated_but_works():
    """``service=`` still resolves to ``collection`` for one deprecation window.

    ``service`` was the published spelling, and OGC API - Features calls the
    value a collection -- it is the ``collectionId`` in ``/collections/{id}``.
    The rename must not silently change behavior for callers using the old name.
    """
    with pytest.warns(DeprecationWarning, match="use 'collection'"):
        with pytest.raises(ValueError, match="Unknown collection"):
            get_cql(service="not-a-collection", cql="a=1")

    # The new spelling emits nothing.
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        with pytest.raises(ValueError, match="Unknown collection"):
            get_cql(collection="not-a-collection", cql="a=1")

    # Passing both spellings is ambiguous and refused, which the hand-rolled
    # shim this replaced did not do -- it silently dropped ``service``.
    with pytest.raises(TypeError, match="received both"):
        get_cql(service="daily", collection="daily", cql="a=1")


def test_get_cql_unknown_service_raises():
    """An unknown collection is rejected before any network call."""
    with pytest.raises(ValueError, match="Unknown collection"):
        get_cql("not-a-collection", {"op": "isNull", "args": [{"property": "x"}]})


def test_waterdata_services_literal_matches_output_id_map():
    """The WATERDATA_SERVICES Literal and _OUTPUT_ID_BY_COLLECTION must enumerate
    the same collections: get_cql validates against the dict while the Literal
    types the public signature, so drift would let one accept a collection the other
    rejects."""
    from typing import get_args

    from dataretrieval.waterdata.types import WATERDATA_SERVICES
    from dataretrieval.waterdata.utils import _OUTPUT_ID_BY_COLLECTION

    assert set(get_args(WATERDATA_SERVICES)) == set(_OUTPUT_ID_BY_COLLECTION)


def test_construct_api_requests_single_value_stays_get():
    """A length-1 list (or scalar) reaches the URL as a plain value, not a
    comma-separated form, so existing single-site callers see no change."""
    req = _construct_api_requests(
        "daily",
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
    )
    assert req.method == "GET"
    assert "monitoring_location_id=USGS-05427718" in str(req.url)
    assert "%2C" not in str(req.url)  # no comma-encoded multi-value


def test_construct_api_requests_numeric_list_joins_with_str():
    """Numeric-list params (e.g. ``water_year=[2020, 2021]`` on get_peaks)
    must reach the URL as a comma-joined string, not crash on ``",".join``
    of ints. The generator-of-``str(x)`` exists exactly for this case."""
    req = _construct_api_requests(
        "peaks",
        monitoring_location_id="USGS-05427718",
        water_year=[2020, 2021],
    )
    assert req.method == "GET"
    assert "water_year=2020%2C2021" in str(req.url)


def test_get_args_materializes_numpy_and_series_numeric_params():
    """Regression: numeric (_NO_NORMALIZE_PARAMS) params given as a numpy array
    or pandas Series must be materialized to a list of native Python scalars so
    they comma-join in the URL (and stay JSON-serializable) — previously the
    array/Series repr leaked into the query string."""
    for value in (np.array([2020, 2021]), pd.Series([2020, 2021])):
        args = _get_args({"water_year": value})
        assert args["water_year"] == [2020, 2021]
        # native Python ints, not numpy scalars (JSON-serializable, no np reprs)
        assert [type(x) for x in args["water_year"]] == [int, int]
        req = _construct_api_requests("peaks", **args)
        assert "water_year=2020%2C2021" in str(req.url)

    # float coordinate arrays (e.g. bbox) likewise materialize to native floats
    args = _get_args({"bbox": np.array([-92.8, 44.2, -88.9, 46.0])})
    assert args["bbox"] == [-92.8, 44.2, -88.9, 46.0]
    assert all(type(x) is float for x in args["bbox"])
    req = _construct_api_requests("daily", **args)
    assert "bbox=-92.8%2C44.2%2C-88.9%2C46.0" in str(req.url)


def test_construct_api_requests_two_element_date_list_becomes_interval():
    """A two-element date list is interpreted as start/end of an OGC datetime
    interval (joined with '/'), NOT as two discrete dates. The OGC `datetime`
    parameter does not support "these N specific dates" — that would require
    a CQL filter. Verifying so this contract is locked in."""
    req = _construct_api_requests(
        "daily",
        monitoring_location_id="USGS-05427718",
        time=["2024-01-01", "2024-01-31"],
    )
    assert req.method == "GET"
    # `/` URL-encodes to %2F. Confirms _format_api_dates ran before the join.
    assert "time=2024-01-01%2F2024-01-31" in str(req.url)


# --- mocked getter smoke tests ------------------------------------------------
# These replace what used to be ~34 live calls to the Water Data API. Each one
# serves a committed fixture (``tests/data/waterdata_ogc_fixtures.json``, two
# real features per collection captured from the collection) and asserts what we
# actually control: that the request we build carries the right params, and that
# the frame we hand back has the right columns, dtypes, and ordering.
#
# The assertions they replaced could not do that. ``len(df) > 0`` passes or fails
# on whether a particular gage reported yesterday; ``df.shape[1] == 97`` breaks
# when USGS adds a column, which is not our bug. Genuine upstream-drift
# detection lives in ``waterdata_queryables_test.py`` (marked ``live``), which
# diffs each collection's queryables against a snapshot and tells us precisely
# what moved.


def _fixture(collection):
    """One collection's committed FeatureCollection, deep-copied.

    Copied because ``httpx_mock`` serializes whatever object it is handed and
    some tests trim the features; a shared mutable body would leak between
    tests.
    """
    return copy.deepcopy(_OGC_FIXTURES[collection])


def _items_url(collection, *, base=_OGC_BASE):
    return re.compile(rf"^{re.escape(base)}/collections/{re.escape(collection)}/items")


def _schema_url(collection, *, base=_OGC_BASE):
    return re.compile(
        rf"^{re.escape(base)}/collections/{re.escape(collection)}/schema$"
    )


def _mock_items(httpx_mock, collection, body=None, **kwargs):
    """Serve ``collection``'s fixture for any ``/items`` request against it."""
    httpx_mock.add_response(
        method=None,
        url=_items_url(collection),
        json=_fixture(collection) if body is None else body,
        **kwargs,
    )


def _sent(httpx_mock, collection=None):
    """Parsed query strings of the ``/items`` requests sent, in order."""
    out = []
    for req in httpx_mock.get_requests():
        url = str(req.url)
        if "/items" not in url:
            continue
        if collection and f"/collections/{collection}/items" not in url:
            continue
        out.append(parse_qs(urlsplit(url).query))
    return out


# --- samples (CSV) -----------------------------------------------------------
# The samples collection is CSV over a different host, so these use small CSV
# bodies rather than the GeoJSON fixtures.

_SAMPLES_RE = re.compile(r"^https://api\.waterdata\.usgs\.gov/samples-data/")

_RESULTS_CSV = (
    "Org_Identifier,Location_Identifier,Activity_ActivityIdentifier,"
    "Result_Characteristic,Result_Measure,Result_MeasureUnit\n"
    "USGS-WI,USGS-05288705,nwiswi.01.2024100112,Temperature water,12.4,deg C\n"
    "USGS-WI,USGS-05288705,nwiswi.01.2024100113,Temperature water,12.9,deg C\n"
)


def test_samples_results(httpx_mock):
    """A results query parses the CSV into the documented column names."""
    httpx_mock.add_response(method="GET", url=_SAMPLES_RE, text=_RESULTS_CSV)

    df, _ = get_samples(
        service="results",
        profile="narrow",
        monitoring_location_id="USGS-05288705",
        activity_start_date_lower="2024-10-01",
        activity_start_date_upper="2025-04-24",
    )

    assert all(
        col in df.columns
        for col in ["Location_Identifier", "Activity_ActivityIdentifier"]
    )
    assert len(df) == 2


@pytest.mark.parametrize(
    ("collection", "profile", "kwargs", "expect_path"),
    [
        (
            "activities",
            "sampact",
            {"monitoring_location_id": "USGS-06719505"},
            "/activities/sampact",
        ),
        (
            "locations",
            "site",
            {"state_code": "US:55", "usgs_pcode": "00010"},
            "/locations/site",
        ),
        (
            "projects",
            "project",
            {"state_code": "US:15"},
            "/projects/project",
        ),
        (
            "organizations",
            "count",
            {"state_code": "US:01"},
            "/organizations/count",
        ),
    ],
)
def test_samples_service_profile_routes_to_its_endpoint(
    httpx_mock, collection, profile, kwargs, expect_path
):
    """Each ``collection``/``profile`` pair addresses ``/<collection>/<profile>``.

    Previously one live test per collection asserted a column count against real
    data (``len(df.columns) == 97``), which broke whenever the collection added a
    field. What is ours to get right is the routing and the parse, so that is
    what this checks.
    """
    httpx_mock.add_response(
        method="GET",
        url=_SAMPLES_RE,
        text="Org_Identifier,Location_Identifier\nUSGS-WI,USGS-06719505\n",
    )

    df, _ = get_samples(service=collection, profile=profile, **kwargs)

    url = str(httpx_mock.get_requests()[0].url)
    assert expect_path in url
    assert len(df) == 1


# --- daily / continuous ------------------------------------------------------


def test_get_daily(httpx_mock):
    """A daily query returns tidy rows with the collection id renamed to
    ``daily_id`` and moved last, dates as ``date`` objects, values numeric."""
    _mock_items(httpx_mock, "daily")

    df, md = get_daily(
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
        time="2025-01-01/..",
    )

    assert "daily_id" in df.columns and "id" not in df.columns
    assert df.columns[-1] == "daily_id"
    assert "geometry" in df.columns
    assert df.parameter_code.unique().tolist() == ["00060"]
    assert df["time"].apply(lambda x: isinstance(x, datetime.date)).all()
    assert df["value"].dtype == "float64"
    assert hasattr(md, "url") and hasattr(md, "query_time")


def test_get_daily_sends_date_only_time_interval(httpx_mock):
    """The Water Data dialect marks ``daily`` date-only, so an open-ended
    interval goes out as ``2025-01-01/..`` with no time component."""
    _mock_items(httpx_mock, "daily")

    get_daily(
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
        time="2025-01-01/..",
    )

    qs = _sent(httpx_mock, "daily")[0]
    assert qs["time"] == ["2025-01-01/.."]
    assert qs["parameter_code"] == ["00060"]


def test_get_daily_properties(httpx_mock):
    """``properties`` selects and orders the output columns, and is forwarded to
    the collection so it does the projection too."""
    requested = [
        "daily_id",
        "monitoring_location_id",
        "parameter_code",
        "time",
        "value",
        "geometry",
    ]
    _mock_items(httpx_mock, "daily")

    df, _ = get_daily(
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
        properties=requested,
    )

    assert df.columns[0] == "daily_id"
    assert df.columns[-1] == "geometry"
    assert df.shape[1] == len(requested)
    # ``daily_id`` is our name for the wire's ``id`` and ``geometry`` is governed
    # by ``skipGeometry``, not by ``properties`` -- neither is a real queryable,
    # so neither may be forwarded or the collection would reject the projection.
    sent = _sent(httpx_mock, "daily")[0]["properties"][0].split(",")
    assert "daily_id" not in sent and "geometry" not in sent
    assert sent == ["monitoring_location_id", "parameter_code", "time", "value"]


def test_get_daily_properties_id(httpx_mock):
    """``'id'`` in ``properties`` resolves to the collection-specific id column
    while keeping the caller's requested position."""
    _mock_items(httpx_mock, "daily")

    df, _ = get_daily(
        monitoring_location_id="USGS-05427718",
        properties=[
            "monitoring_location_id",
            "id",
            "parameter_code",
            "time",
            "value",
            "geometry",
        ],
    )

    assert df.columns[1] == "daily_id"


def test_get_daily_no_geometry(httpx_mock):
    """``skip_geometry=True`` is forwarded and drops the geometry column,
    yielding a plain DataFrame."""
    body = _fixture("daily")
    for feature in body["features"]:
        feature.pop("geometry", None)
    _mock_items(httpx_mock, "daily", body=body)

    df, _ = get_daily(monitoring_location_id="USGS-05427718", skip_geometry=True)

    assert "geometry" not in df.columns
    assert isinstance(df, DataFrame)
    assert _sent(httpx_mock, "daily")[0]["skipGeometry"] == ["true"]


def test_get_continuous(httpx_mock):
    """Continuous observations are timestamped (not date-only), so ``time``
    comes back as a UTC-aware datetime column."""
    _mock_items(httpx_mock, "continuous")

    df, _ = get_continuous(
        monitoring_location_id="USGS-06904500",
        parameter_code="00065",
        time="2025-01-01/2025-12-31",
    )

    assert isinstance(df, DataFrame)
    assert "geometry" in df.columns
    assert "continuous_id" in df.columns
    assert df["time"].dtype.name.startswith("datetime64[")
    assert "UTC" in df["time"].dtype.name


def test_get_latest_continuous(httpx_mock):
    _mock_items(httpx_mock, "latest-continuous")

    df, md = get_latest_continuous(
        monitoring_location_id=["USGS-05427718", "USGS-05427719"],
        parameter_code=["00060", "00065"],
    )

    assert df.columns[-1] == "latest_continuous_id"
    assert df["time"].dtype.name.startswith("datetime64[")
    assert "UTC" in df["time"].dtype.name
    assert hasattr(md, "url")
    # Multi-value params are comma-joined into one request when the URL fits.
    qs = _sent(httpx_mock, "latest-continuous")[0]
    assert qs["monitoring_location_id"] == ["USGS-05427718,USGS-05427719"]
    assert qs["parameter_code"] == ["00060,00065"]


def test_get_latest_daily(httpx_mock):
    _mock_items(httpx_mock, "latest-daily")

    df, md = get_latest_daily(
        monitoring_location_id=["USGS-05427718", "USGS-05427719"],
        parameter_code=["00060", "00065"],
    )

    assert "latest_daily_id" in df.columns
    assert hasattr(md, "url") and hasattr(md, "query_time")


def test_get_latest_daily_properties_geometry(httpx_mock):
    """Geometry survives an explicit ``properties`` list that omits it -- the
    collection returns it regardless unless ``skip_geometry`` is set, so the
    projection must not drop it."""
    _mock_items(httpx_mock, "latest-daily")

    df, _ = get_latest_daily(
        monitoring_location_id=["USGS-05427718", "USGS-05427719"],
        properties=[
            "monitoring_location_id",
            "parameter_code",
            "time",
            "value",
            "unit_of_measure",
        ],
    )

    assert "geometry" in df.columns
    assert df.shape[1] == 6


# --- monitoring locations ----------------------------------------------------


def test_get_monitoring_locations(httpx_mock):
    _mock_items(httpx_mock, "monitoring-locations")

    df, md = get_monitoring_locations(state_name="Connecticut", site_type_code="GW")

    assert "monitoring_location_id" in df.columns
    assert hasattr(md, "url") and hasattr(md, "query_time")


def test_get_monitoring_locations_hucs_uses_post_cql(httpx_mock):
    """``monitoring-locations`` is a POST/CQL2 collection in the Water Data
    dialect, so a multi-value filter goes out as a CQL2 body rather than a
    comma-joined query param."""
    _mock_items(httpx_mock, "monitoring-locations")

    get_monitoring_locations(hydrologic_unit_code=["010802050102", "010802050103"])

    req = next(
        r
        for r in httpx_mock.get_requests()
        if "monitoring-locations/items" in str(r.url)
    )
    assert req.method == "POST"
    body = json.loads(req.content)
    assert "010802050102" in json.dumps(body)
    assert "010802050103" in json.dumps(body)


# --- generalized CQL ---------------------------------------------------------


def test_get_cql_compound_and_in(httpx_mock):
    """A compound AND-of-INs is sent as a CQL2 body and shaped like the typed
    getters: wire ``id`` renamed and ordered last."""
    cql = {
        "op": "and",
        "args": [
            {"op": "in", "args": [{"property": "parameter_code"}, ["00060", "00065"]]},
            {
                "op": "in",
                "args": [{"property": "monitoring_location_id"}, ["USGS-05427718"]],
            },
        ],
    }
    _mock_items(httpx_mock, "latest-daily")

    df, md = get_cql("latest-daily", cql)

    assert "latest_daily_id" in df.columns and "id" not in df.columns
    assert df.columns[-1] == "latest_daily_id"
    assert hasattr(md, "url") and hasattr(md, "query_time")
    req = httpx_mock.get_requests()[0]
    assert req.method == "POST"
    # The CQL2 body is posted verbatim, not wrapped in an envelope.
    assert json.loads(req.content) == cql


def test_get_cql_str_body_sent_verbatim(httpx_mock):
    """A ``str`` CQL2 body is forwarded verbatim and yields the same result as
    the equivalent ``dict``."""
    cql = {
        "op": "in",
        "args": [{"property": "monitoring_location_id"}, ["USGS-05427718"]],
    }
    _mock_items(httpx_mock, "latest-daily")

    df_dict, _ = get_cql("latest-daily", cql)
    df_str, _ = get_cql("latest-daily", json.dumps(cql))

    assert list(df_str.columns) == list(df_dict.columns)
    assert len(df_str) == len(df_dict)
    bodies = [json.loads(r.content) for r in httpx_mock.get_requests()]
    assert bodies[0] == bodies[1] == cql


def test_get_cql_properties_id_translation(httpx_mock):
    """``properties=['id', ...]`` resolves ``id`` to the collection's output id
    column, preserving the requested order."""
    cql = {
        "op": "in",
        "args": [{"property": "monitoring_location_id"}, ["USGS-05427718"]],
    }
    _mock_items(httpx_mock, "latest-daily")

    df, _ = get_cql(
        "latest-daily",
        cql,
        properties=["monitoring_location_id", "id", "parameter_code", "value"],
    )

    assert df.columns[1] == "latest_daily_id"


def test_get_cql_like_wildcard(httpx_mock):
    """``get_cql`` passes through predicates the typed getters cannot express,
    e.g. a LIKE with a ``%`` wildcard -- it must not be escaped or rewritten."""
    cql = {
        "op": "like",
        "args": [{"property": "hydrologic_unit_code"}, "020700100101%"],
    }
    _mock_items(httpx_mock, "monitoring-locations")

    get_cql("monitoring-locations", cql)

    assert json.loads(httpx_mock.get_requests()[0].content) == cql


def test_get_cql_resume_returns_finalized_shape(httpx_mock):
    """A resumed ``get_cql`` returns the same finished ``(df, BaseMetadata)``
    shape as an uninterrupted call. The verbatim-CQL path drives the shared
    executor with the same finalizer as the typed getters, so
    ``exc.call.resume()`` yields the shaped result, not a raw
    ``(frame, response)`` pair."""
    from dataretrieval.interruptions import QuotaExhausted
    from dataretrieval.utils import BaseMetadata

    cql = {
        "op": "in",
        "args": [{"property": "monitoring_location_id"}, ["USGS-05427718"]],
    }
    httpx_mock.add_response(
        method=None, url=_items_url("latest-daily"), status_code=429
    )
    _mock_items(httpx_mock, "latest-daily")

    with pytest.raises(QuotaExhausted) as excinfo:
        get_cql("latest-daily", cql)

    df, md = excinfo.value.call.resume()

    assert isinstance(md, BaseMetadata)
    assert "latest_daily_id" in df.columns and "id" not in df.columns


# --- field measurements ------------------------------------------------------


def test_get_field_measurements(httpx_mock):
    body = _fixture("field-measurements")
    for feature in body["features"]:
        feature.pop("geometry", None)
    _mock_items(httpx_mock, "field-measurements", body=body)

    df, md = get_field_measurements(
        monitoring_location_id="USGS-05427718",
        unit_of_measure="ft^3/s",
        time="2025-01-01/2025-10-01",
        skip_geometry=True,
    )

    assert "field_measurement_id" in df.columns
    assert "geometry" not in df.columns
    assert hasattr(md, "url") and hasattr(md, "query_time")
    qs = _sent(httpx_mock, "field-measurements")[0]
    assert qs["unit_of_measure"] == ["ft^3/s"]


def test_get_field_measurements_metadata(httpx_mock):
    _mock_items(httpx_mock, "field-measurements-metadata")

    df, md = get_field_measurements_metadata(
        monitoring_location_id="USGS-05427718", skip_geometry=True
    )

    assert "field_series_id" in df.columns
    assert "begin" in df.columns and "end" in df.columns
    assert hasattr(md, "url") and hasattr(md, "query_time")


def test_get_field_measurements_metadata_multi_site(httpx_mock):
    """Multiple sites plus a parameter filter reach the collection in one
    request."""
    sites = ["USGS-07069000", "USGS-07064000", "USGS-07068000"]
    _mock_items(httpx_mock, "field-measurements-metadata")

    get_field_measurements_metadata(
        monitoring_location_id=sites, parameter_code="00060", skip_geometry=True
    )

    qs = _sent(httpx_mock, "field-measurements-metadata")[0]
    assert qs["monitoring_location_id"] == [",".join(sites)]
    assert qs["parameter_code"] == ["00060"]


# --- metadata collections ----------------------------------------------------


def test_get_time_series_metadata(httpx_mock):
    _mock_items(httpx_mock, "time-series-metadata")

    df, md = get_time_series_metadata(
        bbox=[-89.840355, 42.853411, -88.818626, 43.422598],
        parameter_code=["00060", "00065", "72019"],
        skip_geometry=True,
    )

    assert "parameter_name" in df.columns
    assert hasattr(md, "url") and hasattr(md, "query_time")
    qs = _sent(httpx_mock, "time-series-metadata")[0]
    # bbox is a fixed 4-coord scalar param, comma-joined and never chunked.
    assert qs["bbox"] == ["-89.840355,42.853411,-88.818626,43.422598"]


def test_get_combined_metadata(httpx_mock):
    _mock_items(httpx_mock, "combined-metadata")

    df, md = get_combined_metadata(
        monitoring_location_id="USGS-05427718", skip_geometry=True
    )

    for col in (
        "monitoring_location_id",
        "parameter_code",
        "data_type",
        "drainage_area",
    ):
        assert col in df.columns, col
    assert hasattr(md, "url") and hasattr(md, "query_time")


def test_get_combined_metadata_multi_site(httpx_mock):
    """Multiple sites are comma-joined into one GET.

    Only ``monitoring-locations`` is a CQL2/POST collection in the Water Data
    dialect; ``combined-metadata`` is not, so this stays a query param.
    """
    sites = ["USGS-07069000", "USGS-07064000", "USGS-07068000"]
    _mock_items(httpx_mock, "combined-metadata")

    get_combined_metadata(
        monitoring_location_id=sites, parameter_code="00060", skip_geometry=True
    )

    req = next(
        r for r in httpx_mock.get_requests() if "combined-metadata/items" in str(r.url)
    )
    assert req.method == "GET"
    qs = _sent(httpx_mock, "combined-metadata")[0]
    assert qs["monitoring_location_id"] == [",".join(sites)]
    assert qs["parameter_code"] == ["00060"]


# --- peaks -------------------------------------------------------------------


def test_get_peaks(httpx_mock):
    _mock_items(httpx_mock, "peaks")

    df, md = get_peaks(monitoring_location_id="USGS-02238500", skip_geometry=True)

    assert "peak_id" in df.columns
    assert "value" in df.columns
    assert "water_year" in df.columns
    assert hasattr(md, "url") and hasattr(md, "query_time")


def test_get_peaks_water_year_filter(httpx_mock):
    """``water_year`` is forwarded as a comma-joined filter.

    The live version of this test asserted only that the returned rows fell
    inside the requested years -- which an empty frame satisfies, so it could
    not fail. Asserting on the outgoing request is what actually pins the
    behavior.
    """
    _mock_items(httpx_mock, "peaks")

    get_peaks(
        monitoring_location_id="USGS-02238500",
        parameter_code="00060",
        water_year=[2020, 2021, 2022],
        skip_geometry=True,
    )

    qs = _sent(httpx_mock, "peaks")[0]
    assert qs["water_year"] == ["2020,2021,2022"]
    assert qs["parameter_code"] == ["00060"]


# --- channel measurements ----------------------------------------------------


def test_get_channel(httpx_mock):
    _mock_items(httpx_mock, "channel-measurements")

    df, _ = get_channel(monitoring_location_id="USGS-02238500")

    assert "channel_measurements_id" in df.columns
    assert len(df) == 2


# --- reference tables --------------------------------------------------------


def test_get_reference_table(httpx_mock):
    _mock_items(httpx_mock, "agency-codes")

    df, md = get_reference_table("agency-codes")

    assert "agency_code" in df.columns
    assert df.shape[0] == 2
    assert hasattr(md, "url") and hasattr(md, "query_time")


def test_get_reference_table_with_query(httpx_mock):
    """A ``query`` dict is merged into the request's query params."""
    _mock_items(httpx_mock, "agency-codes")

    df, md = get_reference_table("agency-codes", query={"id": "AK001,AK008"})

    assert "agency_code" in df.columns
    assert df.shape[0] == 2
    assert _sent(httpx_mock, "agency-codes")[0]["id"] == ["AK001,AK008"]
    assert hasattr(md, "url") and hasattr(md, "query_time")


def test_get_daily_max_rows_is_excluded_from_request_and_forwarded():
    # ``max_rows`` is a client-side pagination cap, not an OGC query
    # parameter — the server never sees it. So a getter must keep it out of
    # the request ``args`` (which become query params) and instead forward it
    # to ``get_ogc_data`` as the keyword that drives the cap. This pins that
    # wiring; the cap mechanism itself (stop following ``next`` once the cap is
    # met, then truncate the combined frame to exactly N) is covered without a
    # network round-trip by the ``_row_cap`` / ``_finalize_ogc`` tests in
    # tests/waterdata_utils_test.py.
    with mock.patch("dataretrieval.waterdata.time_series.get_ogc_data") as fake:
        fake.return_value = (pd.DataFrame(), mock.MagicMock(spec=[]))
        get_daily(
            monitoring_location_id="USGS-05427718",
            parameter_code="00060",
            max_rows=3,
        )
    args_dict = fake.call_args[0][0]
    assert "max_rows" not in args_dict  # not leaked into the query params
    assert fake.call_args.kwargs["max_rows"] == 3  # forwarded to the cap


def test_get_reference_table_wrong_name():
    with pytest.raises(ValueError):
        get_reference_table("agency-cod")


@pytest.mark.parametrize("bad", [0, -1, 2.5, 10.0, True])
def test_get_reference_table_rejects_bad_max_rows(bad):
    # max_rows must be a genuine positive int; a non-positive value, a float
    # (even integral like 10.0), or a bool must raise ValueError up front —
    # not crash later inside pandas .head(). Raises before any HTTP request.
    with pytest.raises(ValueError, match="positive integer"):
        get_reference_table("agency-codes", max_rows=bad)


def test_get_reference_table_accepts_numpy_int_max_rows(httpx_mock):
    # numpy integers are valid caps: isinstance(np.int64, int) is False, so the
    # validation must accept numbers.Integral (not just int) — otherwise a cap
    # derived from a numpy/pandas computation is wrongly rejected.
    _mock_items(httpx_mock, "agency-codes")

    df, _ = get_reference_table("agency-codes", max_rows=np.int64(2))

    assert len(df) == 2


# --- statistics --------------------------------------------------------------
# The statistics API nests its values two levels deep (feature -> data ->
# values); these pin the flattening, which is the part we own.


def _mock_stats(httpx_mock, collection):
    httpx_mock.add_response(
        method="GET",
        url=re.compile(rf"^{re.escape(_STATS_BASE)}/{collection}"),
        json=_fixture(collection),
    )


def test_get_stats_por(httpx_mock):
    """Period-of-record normals flatten to one row per computation, with
    ``percentile`` expanded into its own column by default."""
    _mock_stats(httpx_mock, "observationNormals")

    df, _ = get_stats_por(
        monitoring_location_id="USGS-12451000",
        parameter_code="00060",
        start_date="01-01",
        end_date="01-01",
    )

    assert "computation" in df.columns
    assert "percentile" in df.columns
    assert df["time_of_year"].isin(["01-01", "01"]).all()
    # The nesting is flattened, not left as objects.
    assert "data" not in df.columns and "values" not in df.columns
    # The single ``percentile`` entry expands into one row per percentile, so the
    # frame is longer than the nested value list: 4 scalar computations + 7
    # percentiles.
    assert set(df["computation"]) == {
        "arithmetic_mean",
        "maximum",
        "median",
        "minimum",
        "percentile",
    }
    assert len(df) == 11
    assert df.loc[df["computation"] == "minimum", "percentile"].tolist() == [0.0]
    assert df.loc[df["computation"] == "arithmetic_mean", "percentile"].isnull().all()


def test_get_stats_por_expanded_false(httpx_mock):
    """``expand_percentiles=False`` keeps the raw ``percentiles`` list column
    instead of exploding it into one row per percentile."""
    _mock_stats(httpx_mock, "observationNormals")

    df, _ = get_stats_por(
        monitoring_location_id="USGS-12451000",
        parameter_code="00060",
        start_date="01-01",
        end_date="01-01",
        expand_percentiles=False,
        computation_type=["minimum", "percentile"],
    )

    assert "percentile" not in df.columns
    assert "percentiles" in df.columns
    # ``expand_percentiles`` is a client-side shaping flag, not a query param.
    url = str(httpx_mock.get_requests()[0].url)
    assert "expand_percentiles" not in url
    # The statistics API takes repeated params, not the comma-joined form the
    # OGC collections use.
    qs = parse_qs(urlsplit(url).query)
    assert qs["computation_type"] == ["minimum", "percentile"]


def test_get_stats_date_range(httpx_mock):
    """Interval statistics carry an ``interval_type`` distinguishing month from
    calendar- and water-year rows."""
    _mock_stats(httpx_mock, "observationIntervals")

    df, _ = get_stats_date_range(
        monitoring_location_id="USGS-12451000",
        parameter_code="00060",
        start_date="2025-01-01",
        end_date="2025-01-01",
        computation_type="maximum",
    )

    assert "interval_type" in df.columns
    assert df["interval_type"].isin(["month", "calendar_year", "water_year"]).all()
    assert "data" not in df.columns and "values" not in df.columns


class TestCheckMonitoringLocationId:
    """Tests for the AGENCY-ID-specific layer over ``_normalize_str_iterable``.

    Generic type/iterable normalization is covered by
    ``TestNormalizeStrIterable`` below; this suite holds only the format
    check (``AGENCY-NUMBER`` shape) and the public-API integration smokes.

    Regression tests for GitHub issue #188.
    """

    def test_valid_string(self):
        """Happy-path smoke: the wrapper still routes through normalization
        for a well-formed AGENCY-ID string."""
        assert _check_monitoring_location_id("USGS-01646500") == "USGS-01646500"

    def test_integer_raises_type_error(self):
        """An integer ID raises TypeError with a helpful AGENCY-ID hint."""
        with pytest.raises(TypeError, match="not int") as exc_info:
            _check_monitoring_location_id(5129115)
        # The wrapper appends the AGENCY-ID format hint that the generic
        # helper alone doesn't carry.
        assert "USGS-01646500" in str(exc_info.value)

    def test_missing_agency_prefix_raises_value_error(self):
        """A string without the AGENCY- prefix raises ValueError."""
        with pytest.raises(ValueError, match="Invalid monitoring_location_id"):
            _check_monitoring_location_id("dog")

    def test_bare_site_number_raises_value_error(self):
        """A bare site number string (no agency prefix) raises ValueError."""
        with pytest.raises(ValueError, match="Invalid monitoring_location_id"):
            _check_monitoring_location_id("01646500")

    def test_get_daily_integer_id_raises(self):
        """get_daily raises TypeError before making any network call."""
        with pytest.raises(TypeError):
            get_daily(monitoring_location_id=5129115, parameter_code="00060")

    def test_get_daily_malformed_id_raises(self):
        """get_daily raises ValueError for a malformed string ID."""
        with pytest.raises(ValueError):
            get_daily(monitoring_location_id="dog", parameter_code="00060")

    def test_per_item_format_check_in_list(self):
        """The AGENCY-ID format check runs on EVERY element of an
        iterable, not just the first. Regression guard against a
        future ``_check_monitoring_location_id`` loop that bails after one
        valid item or only checks the head."""
        with pytest.raises(ValueError, match="Invalid monitoring_location_id"):
            _check_monitoring_location_id(["USGS-01646500", "badformat"])


class TestNormalizeStrIterable:
    """Tests for the generic _normalize_str_iterable helper.

    Mirrors TestCheckMonitoringLocationId for the type/iterable contract;
    the AGENCY-ID format check is monitoring_location_id-specific and lives
    only in the _check_monitoring_location_id wrapper.
    """

    def test_none_passes(self):
        assert _normalize_str_iterable(None, "p") is None

    def test_string_returned_unchanged(self):
        assert _normalize_str_iterable("00060", "parameter_code") == "00060"
        # Note: no hyphen requirement here — that's monitoring_location_id-specific.
        assert _normalize_str_iterable("dog", "parameter_code") == "dog"

    @pytest.mark.parametrize(
        "value",
        [
            ["00060", "00010"],
            ("00060", "00010"),
            pd.Series(["00060", "00010"]),
            np.array(["00060", "00010"]),
        ],
        ids=["list", "tuple", "series", "ndarray"],
    )
    def test_iterable_normalizes_to_list(self, value):
        """Any iterable of strings (list / tuple / Series / ndarray) comes back
        as a plain ``list``."""
        result = _normalize_str_iterable(value, "p")
        assert result == ["00060", "00010"]
        assert isinstance(result, list)

    def test_int_raises_type_error(self):
        with pytest.raises(TypeError, match="parameter_code must be a string"):
            _normalize_str_iterable(5129115, "parameter_code")

    def test_int_in_iterable_raises_type_error(self):
        with pytest.raises(TypeError, match="parameter_code elements must be strings"):
            _normalize_str_iterable(["00060", 5129115], "parameter_code")

    def test_dict_raises_type_error(self):
        with pytest.raises(TypeError, match="not dict"):
            _normalize_str_iterable({"00060": "discharge"}, "parameter_code")

    def test_get_daily_parameter_code_as_series(self):
        """Wiring check: pd.Series for ``parameter_code`` arrives at the inner
        call as a list.

        Regression for the gap PR #229 originally left on every multi-value
        parameter other than ``monitoring_location_id``. Pre-fix, the Series
        was passed through to ``requests`` which str-serialized it into the
        URL (or POST body). Post-fix, ``_normalize_str_iterable`` materializes
        it to ``list`` at the function boundary.
        """
        with mock.patch("dataretrieval.waterdata.time_series.get_ogc_data") as fake:
            fake.return_value = (pd.DataFrame(), mock.MagicMock(spec=[]))
            get_daily(
                monitoring_location_id="USGS-05427718",
                parameter_code=pd.Series(["00060", "00010"]),
            )
        # _get_args(locals()) packs kwargs and passes them as `args` to
        # get_ogc_data; the first positional argument is the args dict.
        args_dict = fake.call_args[0][0]
        assert args_dict["parameter_code"] == ["00060", "00010"]
        assert isinstance(args_dict["parameter_code"], list)

    def test_list_of_ints_rejected_at_boundary(self):
        """List-of-non-strings must be caught client-side, not silently sent.

        Regression: an earlier pass through ``_get_args`` had a
        ``list-of-non-str`` fast-path that bypassed normalization, so
        ``parameter_code=[60, 65]`` would reach the OGC API and surface as
        a confusing JSONDecodeError on the malformed response.
        """
        with pytest.raises(TypeError, match="parameter_code elements must be strings"):
            get_daily(
                monitoring_location_id="USGS-05427718",
                parameter_code=[60, 65],
            )
