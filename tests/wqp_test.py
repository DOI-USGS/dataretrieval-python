import datetime
from unittest import mock

import pytest
from pandas import DataFrame

import dataretrieval
import dataretrieval.wqp as wqp
from dataretrieval.wqp import (
    WQP_Metadata,
    _check_kwargs,
    get_results,
    what_activities,
    what_activity_metrics,
    what_detection_limits,
    what_habitat_metrics,
    what_organizations,
    what_project_weights,
    what_projects,
    what_sites,
)


def mock_request(httpx_mock, request_url, file_path):
    with open(file_path) as text:
        httpx_mock.add_response(
            method="GET",
            url=request_url,
            text=text.read(),
            headers={"mock_header": "value"},
        )


def _assert_wqp_metadata(md, request_url):
    """The metadata assertions shared by every mocked WQP query."""
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None


def test_get_results_opts_into_retry(monkeypatch):
    """WQP explicitly enables retry at its shared query boundary."""
    response = mock.Mock(
        text="ResultIdentifier,ResultMeasureValue\nA,1.0\n",
        url="https://example.test",
        elapsed=datetime.timedelta(),
        headers={},
    )
    query = mock.Mock(return_value=response)
    monkeypatch.setattr(wqp, "_query_with_retry", query)

    df, _ = wqp.get_results(legacy=True)

    assert len(df) == 1
    assert query.call_count == 1


def test_read_wqp_csv_preserves_leading_zero_codes():
    """Regression: WQP code columns (HUCs, parameter codes, FIPS) carry
    significant leading zeros; a bare ``read_csv`` inferred them as int/float
    and dropped the zeros (``"00060"`` -> ``60``). ``_read_wqp_csv`` reads
    code/identifier columns as ``str`` while leaving value columns numeric."""
    from dataretrieval.wqp import _read_wqp_csv

    csv = (
        "Location_HUCEightDigitCode,USGSpcode,ResultMeasureValue\n07090002,00060,1.5\n"
    )
    df = _read_wqp_csv(csv)
    assert df["Location_HUCEightDigitCode"].iloc[0] == "07090002"
    assert df["USGSpcode"].iloc[0] == "00060"
    assert df["ResultMeasureValue"].iloc[0] == 1.5


def test_get_results(httpx_mock):
    """Tests water quality portal ratings query"""
    request_url = (
        "https://www.waterqualitydata.us/data/Result/Search?siteid=WIDNR_WQX-10032762"
        "&characteristicName=Specific+conductance&startDateLo=05-01-2011&startDateHi=09-30-2011"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_results.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = get_results(
        siteid="WIDNR_WQX-10032762",
        characteristicName="Specific conductance",
        startDateLo="05-01-2011",
        startDateHi="09-30-2011",
    )
    assert type(df) is DataFrame
    assert df.shape == (5, 65)
    _assert_wqp_metadata(md, request_url)
    assert df["ActivityStartDateTime"].notna().all()
    # Regression: the getter must thread the query kwargs into the metadata
    # (it previously built WQP_Metadata(response), dropping them), so that
    # md.site_info has a siteid to look up instead of always returning None.
    assert md._parameters.get("siteid") == "WIDNR_WQX-10032762"


def test_get_results_WQX3(httpx_mock):
    """Tests water quality portal results query with new WQX3.0 profile"""
    request_url = (
        "https://www.waterqualitydata.us/wqx3/Result/search?siteid=WIDNR_WQX-10032762"
        "&characteristicName=Specific+conductance&startDateLo=05-01-2011&startDateHi=09-30-2011"
        "&mimeType=csv"
        "&dataProfile=fullPhysChem"
    )
    response_file_path = "tests/data/wqp3_results.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = get_results(
        legacy=False,
        siteid="WIDNR_WQX-10032762",
        characteristicName="Specific conductance",
        startDateLo="05-01-2011",
        startDateHi="09-30-2011",
    )
    assert type(df) is DataFrame
    assert df.shape == (5, 186)
    _assert_wqp_metadata(md, request_url)
    assert df["Activity_StartDateTime"].notna().all()


@pytest.mark.parametrize(
    ("builder", "service", "expected", "warning"),
    [
        (
            wqp.wqp_url,
            "Result",
            "https://www.waterqualitydata.us/data/Result/Search?",
            DeprecationWarning,
        ),
        (
            wqp.wqx3_url,
            "Result",
            "https://www.waterqualitydata.us/wqx3/Result/search?",
            UserWarning,
        ),
    ],
)
def test_wqp_url_profiles(builder, service, expected, warning):
    """Each profile keeps its warning policy and exact endpoint casing."""
    with pytest.warns(warning):
        assert builder(service) == expected


def test_a_configured_base_url_moves_both_interfaces():
    """One root, both paths: the portal serves legacy and WQX3 from one host.

    Redirecting only the interface a caller happened to use first would leave
    the other pointed at the service they were redirecting away from, which is
    the failure a redirect exists to prevent.
    """
    mirror = "https://mirror.example/wqp"

    with dataretrieval.configure(wqp.WqpSettings(base_url=mirror)):
        with pytest.warns(DeprecationWarning):
            legacy = wqp.wqp_url("Result")
        with pytest.warns(UserWarning):
            wqx3 = wqp.wqx3_url("Result")

    assert legacy == f"{mirror}/data/Result/Search?"
    assert wqx3 == f"{mirror}/wqx3/Result/search?"

    # Outside the block, the portal's own root again -- the redirect is scoped
    # to the ``with`` statement, not latched at import.
    with pytest.warns(DeprecationWarning):
        assert wqp.wqp_url("Result").startswith("https://www.waterqualitydata.us/")


@pytest.mark.parametrize(
    ("builder", "profile", "valid_services", "warning"),
    [
        (wqp.wqp_url, "Legacy", wqp.services_legacy, DeprecationWarning),
        (wqp.wqx3_url, "WQX3.0", wqp.services_wqx3, UserWarning),
    ],
)
def test_wqp_url_profiles_reject_unknown_service(
    builder, profile, valid_services, warning
):
    """Shared validation preserves profile-specific error text and ordering."""
    with pytest.warns(warning):
        with pytest.raises(
            ValueError,
            match=rf"^{profile} service not recognized\. Valid options are ",
        ) as exc_info:
            builder("unknown")

    assert str(valid_services) in str(exc_info.value)


# Every WQP ``what_*`` wrapper issues the same query against its own service
# endpoint and returns the parsed DataFrame + metadata; they differ only by the
# service path segment and the response fixture. Each case names one column that
# is unique to that service's profile, which is what distinguishes "parsed the
# right response" from a row count -- a count only says the fixture has not been
# re-captured, and pinning one made the fixtures grow to tens of megabytes.
_WHAT_CASES = [
    (what_sites, "Station", "wqp_sites.txt", "MonitoringLocationIdentifier"),
    (what_organizations, "Organization", "wqp_organizations.txt", "OrganizationType"),
    (what_projects, "Project", "wqp_projects.txt", "ProjectName"),
    (what_activities, "Activity", "wqp_activities.txt", "ActivityTypeCode"),
    (
        what_detection_limits,
        "ResultDetectionQuantitationLimit",
        "wqp_detection_limits.txt",
        "DetectionQuantitationLimitTypeName",
    ),
    (
        what_habitat_metrics,
        "BiologicalMetric",
        "wqp_habitat_metrics.txt",
        "IndexTypeName",
    ),
    (
        what_project_weights,
        "ProjectMonitoringLocationWeighting",
        "wqp_project_weights.txt",
        "StatisticalStratumText",
    ),
    (
        what_activity_metrics,
        "ActivityMetric",
        "wqp_activity_metrics.txt",
        "ActivityMetricType/MetricTypeName",
    ),
]


@pytest.mark.parametrize(
    "func, service, fixture, profile_column",
    _WHAT_CASES,
    ids=[case[0].__name__ for case in _WHAT_CASES],
)
def test_what_query(httpx_mock, func, service, fixture, profile_column):
    """Each WQP ``what_*`` wrapper hits its own service endpoint and returns the
    parsed DataFrame + metadata."""
    request_url = (
        f"https://www.waterqualitydata.us/data/{service}/Search?"
        "statecode=US%3A34&characteristicName=Chloride&mimeType=csv"
    )
    mock_request(httpx_mock, request_url, f"tests/data/{fixture}")
    df, md = func(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert not df.empty
    assert profile_column in df.columns
    _assert_wqp_metadata(md, request_url)


def test_check_kwargs():
    """Tests that correct errors are raised for invalid mimetypes."""
    kwargs = {"mimeType": "geojson"}
    with pytest.raises(NotImplementedError):
        kwargs = _check_kwargs(kwargs)
    kwargs = {"mimeType": "foo"}
    with pytest.raises(ValueError):
        kwargs = _check_kwargs(kwargs)


@pytest.mark.parametrize(
    "name", ["api_key", "x_api_key", "access_token", "password", "pat", "auth"]
)
def test_credential_shaped_wqp_kwargs_are_rejected(name):
    """WQP has the widest ``**kwargs`` passthrough in the package.

    Its ten getters forward whatever the caller names straight into the query
    string, so ``api_key=`` -- the plausible guess now that ``configure()``
    takes ``Settings(api_key=...)`` -- would put a secret in a URL that
    clients, proxies and logs retain. Same predicate and same message as Water
    Data's ``**queryables`` guard, because it is the same mistake.
    """
    with pytest.raises(TypeError, match="Credentials cannot be passed"):
        _check_kwargs({name: "SECRET"})


@pytest.mark.parametrize(
    "name", ["siteid", "characteristicName", "statecode", "providers", "pCode"]
)
def test_real_wqp_filters_still_pass_through(name):
    """The denylist must not claim names the portal owns."""
    assert _check_kwargs({name: "v"})[name] == "v"


def test_get_results_wqx3_preserves_user_dataProfile(httpx_mock):
    """A valid user-supplied WQX3.0 profile must not be overwritten.

    Regression: previously the `else` branch of the `dataProfile` validation
    triggered whenever the value was *not invalid*, including any valid
    user-supplied profile, silently overwriting it with 'fullPhysChem'.
    """
    request_url = (
        "https://www.waterqualitydata.us/wqx3/Result/search?"
        "siteid=UTAHDWQ_WQX-4993795&mimeType=csv&dataProfile=narrow"
    )
    response_file_path = "tests/data/wqp3_results.txt"
    mock_request(httpx_mock, request_url, response_file_path)

    df, _md = get_results(
        legacy=False, siteid="UTAHDWQ_WQX-4993795", dataProfile="narrow"
    )
    assert isinstance(df, DataFrame)
    sent = httpx_mock.get_requests()[-1]
    assert sent.url.params.get("dataProfile") == "narrow"


def _wqp_metadata(**parameters):
    """Build a ``WQP_Metadata`` from a lightweight mock response."""
    resp = mock.Mock(
        url="https://www.waterqualitydata.us/",
        elapsed=datetime.timedelta(seconds=0.01),
        headers={},
    )
    return WQP_Metadata(resp, **parameters)


def test_wqp_metadata_site_info_is_accessible_property():
    """B2 regression: ``WQP_Metadata.site_info`` was accidentally defined
    *inside* ``__init__`` (a discarded local function), so the attribute
    did not exist and accessing it fell through to
    ``BaseMetadata.site_info``, which raises ``NotImplementedError``. It
    must now be a real property that returns ``None`` (no site param)
    without raising."""
    assert isinstance(type(_wqp_metadata()).site_info, property)
    assert _wqp_metadata().site_info is None  # must NOT raise


def test_wqp_metadata_site_info_routes_to_what_sites(monkeypatch):
    """When the query carried a ``siteid`` (WQP's site identifier),
    ``site_info`` delegates to ``wqp.what_sites`` with that identifier."""
    import dataretrieval.wqp as wqp_mod

    captured = {}

    def fake_what_sites(**kwargs):
        captured.update(kwargs)
        return "SENTINEL"

    monkeypatch.setattr(wqp_mod, "what_sites", fake_what_sites)
    assert _wqp_metadata(siteid="USGS-05427718").site_info == "SENTINEL"
    assert captured == {"siteid": "USGS-05427718"}
