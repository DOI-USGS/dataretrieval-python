import pytest
import requests
import datetime

from pandas import DataFrame

from dataretrieval.wqp import (get_results, what_sites, what_organizations,
                               what_projects, what_activities,
                               what_detection_limits, what_habitat_metrics,
                               what_project_weights, what_activity_metrics,
                               _alter_kwargs)


def test_get_ratings(requests_mock):
    """Tests water quality portal ratings query"""
    request_url = "https://www.waterqualitydata.us/data/Result/Search?siteid=WIDNR_WQX-10032762" \
                  "&characteristicName=Specific+conductance&startDateLo=05-01-2011&startDateHi=09-30-2011" \
                  "&zip=no&mimeType=csv"
    response_file_path = 'data/wqp_results.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_results(siteid='WIDNR_WQX-10032762',
                          characteristicName = 'Specific conductance',
                          startDateLo='05-01-2011', startDateHi='09-30-2011')
    assert type(df) is DataFrame
    assert df.size == 315
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_what_sites(requests_mock):
    """Tests Water quality portal sites query"""
    request_url = "https://www.waterqualitydata.us/data/Station/Search?statecode=US%3A34&characteristicName=Chloride&zip=no" \
                  "&mimeType=csv"
    response_file_path = 'data/wqp_sites.txt'
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
    request_url = "https://www.waterqualitydata.us/data/Organization/Search?statecode=US%3A34&characteristicName=Chloride&zip=no" \
                  "&mimeType=csv"
    response_file_path = 'data/wqp_organizations.txt'
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
    request_url = "https://www.waterqualitydata.us/data/Project/Search?statecode=US%3A34&characteristicName=Chloride&zip=no" \
                  "&mimeType=csv"
    response_file_path = 'data/wqp_projects.txt'
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
    request_url = "https://www.waterqualitydata.us/data/Activity/Search?statecode=US%3A34&characteristicName=Chloride&zip=no" \
                  "&mimeType=csv"
    response_file_path = 'data/wqp_activities.txt'
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
    request_url = "https://www.waterqualitydata.us/data/ResultDetectionQuantitationLimit/Search?statecode=US%3A34&characteristicName=Chloride&zip=no" \
                  "&mimeType=csv"
    response_file_path = 'data/wqp_detection_limits.txt'
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
    request_url = "https://www.waterqualitydata.us/data/BiologicalMetric/Search?statecode=US%3A34&characteristicName=Chloride&zip=no" \
                  "&mimeType=csv"
    response_file_path = 'data/wqp_habitat_metrics.txt'
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
    request_url = "https://www.waterqualitydata.us/data/ProjectMonitoringLocationWeighting/Search?statecode=US%3A34&characteristicName=Chloride&zip=no" \
                  "&mimeType=csv"
    response_file_path = 'data/wqp_project_weights.txt'
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
    request_url = "https://www.waterqualitydata.us/data/ActivityMetric/Search?statecode=US%3A34&characteristicName=Chloride&zip=no" \
                  "&mimeType=csv"
    response_file_path = 'data/wqp_activity_metrics.txt'
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
        requests_mock.get(request_url, text=text.read(), headers={"mock_header": "value"})


class TestAlterKwargs:
    """Tests for keyword alteration.
    """
    def test_alter_kwargs_zip(self):
        """Tests that zip kwarg is altered correctly and warning is thrown."""
        kwargs = {"zip": "yes", "mimeType": "csv"}
        with pytest.warns(UserWarning):
            kwargs = _alter_kwargs(kwargs)
        assert kwargs == {"zip": "no", "mimeType": "csv"}

    def test_alter_kwargs_mimetype(self):
        """Tests that mimetype kwarg is altered correctly and warning is thrown."""
        kwargs = {"zip": "no", "mimeType": "geojson"}
        with pytest.warns(UserWarning):
            kwargs = _alter_kwargs(kwargs)
        assert kwargs == {"zip": "no", "mimeType": "csv"}