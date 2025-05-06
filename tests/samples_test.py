import datetime

import pytest
from pandas import DataFrame

from dataretrieval.samples import (
    _check_profiles,
    get_usgs_samples
)

def mock_request(requests_mock, request_url, file_path):
    """Mock request code"""
    with open(file_path) as text:
        requests_mock.get(
            request_url, text=text.read(), headers={"mock_header": "value"}
        )

def test_mock_get_usgs_samples(requests_mock):
    """Tests USGS Samples query"""
    request_url = (
        "https://api.waterdata.usgs.gov/samples-data/results/fullphyschem?"
        "activityMediaName=Water&activityStartDateLower=2020-01-01"
        "&activityStartDateUpper=2024-12-31&monitoringLocationIdentifier=USGS-05406500&mimeType=text%2Fcsv"
    )
    response_file_path = "data/samples_results.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_usgs_samples(
        service="results",
        profile="fullphyschem",
        activityMediaName="Water",
        activityStartDateLower="2020-01-01",
        activityStartDateUpper="2024-12-31",
        monitoringLocationIdentifier="USGS-05406500",
        )
    assert type(df) is DataFrame
    assert df.size == 12127
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None

def test_check_profiles():
    """Tests that correct errors are raised for invalid profiles."""
    with pytest.raises(ValueError):
        _check_profiles(service="foo", profile="bar")
    with pytest.raises(ValueError):
        _check_profiles(service="results", profile="foo")

def test_samples_results():
    """Test results call for proper columns"""
    df,_ = get_usgs_samples(
        service="results",
        profile="narrow",
        monitoringLocationIdentifier="USGS-05288705",
        activityStartDateLower="2024-10-01",
        activityStartDateUpper="2025-04-24"
        )
    assert all(col in df.columns for col in ["Location_Identifier", "Activity_ActivityIdentifier"])
    assert len(df) > 0

def test_samples_activity():
    """Test activity call for proper columns"""
    df,_ = get_usgs_samples(
        service="activities",
        profile="sampact",
        monitoringLocationIdentifier="USGS-06719505"
        )
    assert len(df) > 0
    assert len(df.columns) == 95
    assert "Location_HUCTwelveDigitCode" in df.columns

def test_samples_locations():
    """Test locations call for proper columns"""
    df,_ = get_usgs_samples(
        service="locations",
        profile="site",
        stateFips="US:55",
        activityStartDateLower="2024-10-01",
        activityStartDateUpper="2025-04-24",
        usgsPCode="00010"
        )
    assert all(col in df.columns for col in ["Location_Identifier", "Location_Latitude"])
    assert len(df) > 0

def test_samples_projects():
    """Test projects call for proper columns"""
    df,_ = get_usgs_samples(
        service="projects",
        profile="project",
        stateFips="US:15",
        activityStartDateLower="2024-10-01",
        activityStartDateUpper="2025-04-24"
        )
    assert all(col in df.columns for col in ["Org_Identifier", "Project_Identifier"])
    assert len(df) > 0

def test_samples_organizations():
    """Test organizations call for proper columns"""
    df,_ = get_usgs_samples(
        service="organizations",
        profile="count",
        stateFips="US:01"
        )
    assert len(df) == 1
    assert df.size == 3
