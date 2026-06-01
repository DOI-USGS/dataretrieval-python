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
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None
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
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None
    assert df["Activity_StartDateTime"].notna().all()


def test_what_sites(httpx_mock):
    """Tests Water quality portal sites query"""
    request_url = (
        "https://www.waterqualitydata.us/data/Station/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_sites.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = what_sites(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 239868
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None


def test_what_organizations(httpx_mock):
    """Tests Water quality portal organizations query"""
    request_url = (
        "https://www.waterqualitydata.us/data/Organization/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_organizations.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = what_organizations(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 576
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None


def test_what_projects(httpx_mock):
    """Tests Water quality portal projects query"""
    request_url = (
        "https://www.waterqualitydata.us/data/Project/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_projects.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = what_projects(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 530
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None


def test_what_activities(httpx_mock):
    """Tests Water quality portal activities query"""
    request_url = (
        "https://www.waterqualitydata.us/data/Activity/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_activities.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = what_activities(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 5087443
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None


def test_what_detection_limits(httpx_mock):
    """Tests Water quality portal detection limits query"""
    request_url = (
        "https://www.waterqualitydata.us/data/ResultDetectionQuantitationLimit/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_detection_limits.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = what_detection_limits(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 98770
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None


def test_what_habitat_metrics(httpx_mock):
    """Tests Water quality portal habitat metrics query"""
    request_url = (
        "https://www.waterqualitydata.us/data/BiologicalMetric/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_habitat_metrics.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = what_habitat_metrics(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 48114
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None


def test_what_project_weights(httpx_mock):
    """Tests Water quality portal project weights query"""
    request_url = (
        "https://www.waterqualitydata.us/data/ProjectMonitoringLocationWeighting/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_project_weights.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = what_project_weights(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 33098
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None


def test_what_activity_metrics(httpx_mock):
    """Tests Water quality portal activity metrics query"""
    request_url = (
        "https://www.waterqualitydata.us/data/ActivityMetric/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_activity_metrics.txt"
    mock_request(httpx_mock, request_url, response_file_path)
    df, md = what_activity_metrics(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 378
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header.get("mock_header") == "value"
    assert md.comment is None


def mock_request(httpx_mock, request_url, file_path):
    with open(file_path) as text:
        httpx_mock.add_response(
            method="GET",
            url=request_url,
            text=text.read(),
            headers={"mock_header": "value"},
        )


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


def test_wqp_metadata_variable_info_is_none():
    """Regression: WQP has no variable catalog, so ``variable_info`` must
    return ``None`` rather than inheriting (and raising) the
    ``BaseMetadata.variable_info`` NotImplementedError stub."""
    assert _wqp_metadata().variable_info is None  # must NOT raise


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
