import datetime

import pytest
from pandas import DataFrame

from dataretrieval.samples import (
    _check_profiles,
    get_USGS_samples
)

def mock_request(requests_mock, request_url, file_path):
    with open(file_path) as text:
        requests_mock.get(
            request_url, text=text.read(), headers={"mock_header": "value"}
        )

def test_mock_get_USGS_samples(requests_mock):
    """Tests USGS Samples query"""
    request_url = (
        "https://api.waterdata.usgs.gov/samples-data/results/fullphyschem?"
        "activityMediaName=Water&activityStartDateLower=2020-01-01"
        "&activityStartDateUpper=2024-12-31&monitoringLocationIdentifier=USGS-05406500&mimeType=text%2Fcsv"
    )
    response_file_path = "data/samples_results.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_USGS_samples(
    service="results",
    profile="fullphyschem",
    activityMediaName="Water",
    activityStartDateLower="2020-01-01",
    activityStartDateUpper="2024-12-31",
    monitoringLocationIdentifier="USGS-05406500"
    )
    assert type(df) is DataFrame
    assert df.size == 12127
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None

def test_check_profiles():
    """Tests that correct errors are raised for invalid profiles."""
    with pytest.raises(TypeError):
        _check_profiles(service="foo", profile="bar")
    with pytest.raises(TypeError):
        _check_profiles(service="results", profile="foo")

def test_samples_activity():
    """Test activity call for proper columns"""
    df,_ = get_USGS_samples(
        service="activities",
        profile="sampact",
        monitoringLocationIdentifier="USGS-06719505"
        )
    assert len(df) > 0
    assert len(df.columns) == 95
    assert "Location_HUCTwelveDigitCode" in df.columns
