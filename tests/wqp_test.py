import datetime
import warnings

import pytest
from pandas import DataFrame

from dataretrieval.wqp import (
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


def test_get_results(requests_mock):
    """Tests water quality portal ratings query"""
    request_url = (
        "https://www.waterqualitydata.us/data/Result/Search?siteid=WIDNR_WQX-10032762"
        "&characteristicName=Specific+conductance&startDateLo=05-01-2011&startDateHi=09-30-2011"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_results.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_results(
        siteid="WIDNR_WQX-10032762",
        characteristicName="Specific conductance",
        startDateLo="05-01-2011",
        startDateHi="09-30-2011",
    )
    assert type(df) is DataFrame
    assert df.size == 315
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_get_results_WQX3(requests_mock):
    """Tests water quality portal results query with new WQX3.0 profile"""
    request_url = (
        "https://www.waterqualitydata.us/wqx3/Result/search?siteid=WIDNR_WQX-10032762"
        "&characteristicName=Specific+conductance&startDateLo=05-01-2011&startDateHi=09-30-2011"
        "&mimeType=csv"
        "&dataProfile=fullPhysChem"
    )
    response_file_path = "tests/data/wqp3_results.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_results(
        legacy=False,
        siteid="WIDNR_WQX-10032762",
        characteristicName="Specific conductance",
        startDateLo="05-01-2011",
        startDateHi="09-30-2011",
    )
    assert type(df) is DataFrame
    assert df.size == 900
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_what_sites(requests_mock):
    """Tests Water quality portal sites query"""
    request_url = (
        "https://www.waterqualitydata.us/data/Station/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_sites.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = what_sites(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 239868
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_what_organizations(requests_mock):
    """Tests Water quality portal organizations query"""
    request_url = (
        "https://www.waterqualitydata.us/data/Organization/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_organizations.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = what_organizations(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 576
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_what_projects(requests_mock):
    """Tests Water quality portal projects query"""
    request_url = (
        "https://www.waterqualitydata.us/data/Project/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_projects.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = what_projects(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 530
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_what_activities(requests_mock):
    """Tests Water quality portal activities query"""
    request_url = (
        "https://www.waterqualitydata.us/data/Activity/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_activities.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = what_activities(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 5087443
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_what_detection_limits(requests_mock):
    """Tests Water quality portal detection limits query"""
    request_url = (
        "https://www.waterqualitydata.us/data/ResultDetectionQuantitationLimit/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_detection_limits.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = what_detection_limits(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 98770
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_what_habitat_metrics(requests_mock):
    """Tests Water quality portal habitat metrics query"""
    request_url = (
        "https://www.waterqualitydata.us/data/BiologicalMetric/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_habitat_metrics.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = what_habitat_metrics(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 48114
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_what_project_weights(requests_mock):
    """Tests Water quality portal project weights query"""
    request_url = (
        "https://www.waterqualitydata.us/data/ProjectMonitoringLocationWeighting/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_project_weights.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = what_project_weights(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 33098
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_what_activity_metrics(requests_mock):
    """Tests Water quality portal activity metrics query"""
    request_url = (
        "https://www.waterqualitydata.us/data/ActivityMetric/Search?statecode=US%3A34&characteristicName=Chloride"
        "&mimeType=csv"
    )
    response_file_path = "tests/data/wqp_activity_metrics.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = what_activity_metrics(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 378
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def mock_request(requests_mock, request_url, file_path):
    with open(file_path) as text:
        requests_mock.get(
            request_url, text=text.read(), headers={"mock_header": "value"}
        )


def test_check_kwargs():
    """Tests that correct errors are raised for invalid mimetypes."""
    kwargs = {"mimeType": "geojson"}
    with pytest.raises(NotImplementedError):
        kwargs = _check_kwargs(kwargs)
    kwargs = {"mimeType": "foo"}
    with pytest.raises(ValueError):
        kwargs = _check_kwargs(kwargs)


def test_what_organizations_legacy_false_warns(requests_mock):
    """legacy=False on a legacy-only helper warns and suppresses the
    misleading legacy DeprecationWarning."""
    request_url = (
        "https://www.waterqualitydata.us/data/Organization/Search?statecode=US%3A34"
        "&characteristicName=Chloride&mimeType=csv"
    )
    mock_request(requests_mock, request_url, "tests/data/wqp_organizations.txt")

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        what_organizations(
            statecode="US:34", characteristicName="Chloride", legacy=False
        )

    user_warnings = [w for w in captured if issubclass(w.category, UserWarning)]
    deprecation_warnings = [
        w for w in captured if issubclass(w.category, DeprecationWarning)
    ]
    assert any("WQX3.0 profile not available" in str(w.message) for w in user_warnings)
    assert not any("legacy WQX format" in str(w.message) for w in deprecation_warnings)


def test_get_results_wqx3_preserves_user_dataProfile(requests_mock):
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
    mock_request(requests_mock, request_url, response_file_path)

    df, _md = get_results(
        legacy=False, siteid="UTAHDWQ_WQX-4993795", dataProfile="narrow"
    )
    assert isinstance(df, DataFrame)
    sent = requests_mock.request_history[-1]
    assert sent.qs.get("dataprofile") == ["narrow"]
