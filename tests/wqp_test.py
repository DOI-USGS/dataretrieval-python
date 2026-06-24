import datetime
from unittest import mock

import pytest
from pandas import DataFrame

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


# Every WQP ``what_*`` wrapper issues the same query against its own service
# endpoint and returns the parsed DataFrame + metadata; they differ only by the
# service path segment, the response fixture, and the expected size.
_WHAT_CASES = [
    (what_sites, "Station", "wqp_sites.txt", 239868),
    (what_organizations, "Organization", "wqp_organizations.txt", 576),
    (what_projects, "Project", "wqp_projects.txt", 530),
    (what_activities, "Activity", "wqp_activities.txt", 5087443),
    (
        what_detection_limits,
        "ResultDetectionQuantitationLimit",
        "wqp_detection_limits.txt",
        98770,
    ),
    (what_habitat_metrics, "BiologicalMetric", "wqp_habitat_metrics.txt", 48114),
    (
        what_project_weights,
        "ProjectMonitoringLocationWeighting",
        "wqp_project_weights.txt",
        33098,
    ),
    (what_activity_metrics, "ActivityMetric", "wqp_activity_metrics.txt", 378),
]


@pytest.mark.parametrize(
    "func, service, fixture, size",
    _WHAT_CASES,
    ids=[case[0].__name__ for case in _WHAT_CASES],
)
def test_what_query(httpx_mock, func, service, fixture, size):
    """Each WQP ``what_*`` wrapper hits its own service endpoint and returns the
    parsed DataFrame + metadata."""
    request_url = (
        f"https://www.waterqualitydata.us/data/{service}/Search?"
        "statecode=US%3A34&characteristicName=Chloride&mimeType=csv"
    )
    mock_request(httpx_mock, request_url, f"tests/data/{fixture}")
    df, md = func(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == size
    _assert_wqp_metadata(md, request_url)


def test_check_kwargs():
    """Tests that correct errors are raised for invalid mimetypes."""
    kwargs = {"mimeType": "geojson"}
    with pytest.raises(NotImplementedError):
        kwargs = _check_kwargs(kwargs)
    kwargs = {"mimeType": "foo"}
    with pytest.raises(ValueError):
        kwargs = _check_kwargs(kwargs)


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
