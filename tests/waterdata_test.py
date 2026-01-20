import datetime
import sys
import pytest
from pandas import DataFrame

if sys.version_info < (3, 10):
    pytest.skip("Skip entire module on Python < 3.10", allow_module_level=True)

from dataretrieval.waterdata.utils import _check_profiles
from dataretrieval.waterdata import (
    get_samples,
    get_daily,
    get_continuous,
    get_monitoring_locations,
    get_latest_continuous,
    get_latest_daily,
    get_field_measurements,
    get_time_series_metadata,
    get_reference_table
)

def mock_request(requests_mock, request_url, file_path):
    """Mock request code"""
    with open(file_path) as text:
        requests_mock.get(
            request_url, text=text.read(), headers={"mock_header": "value"}
        )

def test_mock_get_samples(requests_mock):
    """Tests USGS Samples query"""
    request_url = (
        "https://api.waterdata.usgs.gov/samples-data/results/fullphyschem?"
        "activityMediaName=Water&activityStartDateLower=2020-01-01"
        "&activityStartDateUpper=2024-12-31&monitoringLocationIdentifier=USGS-05406500&mimeType=text%2Fcsv"
    )
    response_file_path = "tests/data/samples_results.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_samples(
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
    df,_ = get_samples(
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
    df,_ = get_samples(
        service="activities",
        profile="sampact",
        monitoringLocationIdentifier="USGS-06719505"
        )
    assert len(df) > 0
    assert len(df.columns) == 95
    assert "Location_HUCTwelveDigitCode" in df.columns

def test_samples_locations():
    """Test locations call for proper columns"""
    df,_ = get_samples(
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
    df,_ = get_samples(
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
    df,_ = get_samples(
        service="organizations",
        profile="count",
        stateFips="US:01"
        )
    assert len(df) == 1
    assert df.size == 3

def test_get_daily():
    df, md = get_daily(
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
        time="2025-01-01/.."
    )
    assert "daily_id" in df.columns
    assert "geometry" in df.columns
    assert df.columns[-1] == "daily_id"
    assert df.shape[1] == 12
    assert df.parameter_code.unique().tolist() == ["00060"]
    assert df.monitoring_location_id.unique().tolist() == ["USGS-05427718"]
    assert df["time"].apply(lambda x: isinstance(x, datetime.date)).all()
    assert df["time"].iloc[0] < df["time"].iloc[-1]
    assert hasattr(md, 'url')
    assert hasattr(md, 'query_time')
    assert df["value"].dtype == "float64"

def test_get_daily_properties():
    df,_ = get_daily(
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
        time="2025-01-01/..",
        properties=["daily_id", "monitoring_location_id", "parameter_code", "time", "value", "geometry"]
    )
    assert "daily_id" == df.columns[0]
    assert "geometry" == df.columns[-1]
    assert df.shape[1] == 6
    assert df.parameter_code.unique().tolist() == ["00060"]

def test_get_daily_properties_id():
    df,_ = get_daily(
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
        time="2025-01-01/..",
        properties=["monitoring_location_id", "id", "parameter_code", "time", "value", "geometry"]
    )
    assert "daily_id" == df.columns[1]

def test_get_daily_no_geometry():
    df,_ = get_daily(
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
        time="2025-01-01/..",
        skip_geometry=True
    )
    assert "geometry" not in df.columns
    assert df.shape[1] == 11
    assert isinstance(df, DataFrame)

def test_get_continuous():
    df,_ = get_continuous(
        monitoring_location_id="USGS-06904500",
        parameter_code="00065",
        time="2025-01-01/2025-12-31"
    )
    assert isinstance(df, DataFrame)
    assert "geometry" not in df.columns
    assert df.shape[1] == 11
    assert df['time'].dtype == 'datetime64[ns, UTC]'
    assert "continuous_id" in df.columns

def test_get_monitoring_locations():
    df, md = get_monitoring_locations(
        state_name="Connecticut",
        site_type_code="GW"
    )
    assert df.site_type_code.unique().tolist() == ["GW"]
    assert hasattr(md, 'url')
    assert hasattr(md, 'query_time')

def test_get_monitoring_locations_hucs():
    df,_ = get_monitoring_locations(
        hydrologic_unit_code=["010802050102", "010802050103"]
    )
    assert set(df.hydrologic_unit_code.unique().tolist()) == {"010802050102", "010802050103"}

def test_get_latest_continuous():
    df, md = get_latest_continuous(
        monitoring_location_id=["USGS-05427718", "USGS-05427719"],
        parameter_code=["00060", "00065"]
    )
    assert "latest_continuous_id" == df.columns[-1]
    assert df.shape[0] <= 4
    assert df.statistic_id.unique().tolist() == ["00011"]
    assert hasattr(md, 'url')
    assert hasattr(md, 'query_time')
    assert df['time'].dtype == 'datetime64[ns, UTC]'

def test_get_latest_daily():
    df, md = get_latest_daily(
        monitoring_location_id=["USGS-05427718", "USGS-05427719"],
        parameter_code=["00060", "00065"]
    )
    assert "latest_daily_id" in df.columns
    assert df.shape[1] == 12
    assert hasattr(md, 'url')
    assert hasattr(md, 'query_time')

def test_get_latest_daily_properties_geometry():
    df, md = get_latest_daily(
        monitoring_location_id=["USGS-05427718", "USGS-05427719"],
        parameter_code=["00060", "00065"],
        properties=['monitoring_location_id', 'parameter_code', 'time', 'value', 'unit_of_measure']
    )
    assert "geometry" in df.columns
    assert df.shape[1] == 6

def test_get_field_measurements():
    df, md = get_field_measurements(
        monitoring_location_id="USGS-05427718",
        unit_of_measure="ft^3/s",
        time="2025-01-01/2025-10-01",
        skip_geometry=True
    )
    assert "field_measurement_id" in df.columns
    assert "geometry" not in df.columns
    assert df.unit_of_measure.unique().tolist() == ["ft^3/s"]
    assert hasattr(md, 'url')
    assert hasattr(md, 'query_time')

def test_get_time_series_metadata():
    df, md = get_time_series_metadata(
        bbox=[-89.840355,42.853411,-88.818626,43.422598],
        parameter_code=["00060", "00065", "72019"],
        skip_geometry=True
    )
    assert set(df['parameter_name'].unique().tolist()) == {"Gage height", "Water level, depth LSD", "Discharge"}
    assert hasattr(md, 'url')
    assert hasattr(md, 'query_time')

def test_get_reference_table():
    df, md = get_reference_table("agency-codes")
    assert "agency_code" in df.columns
    assert df.shape[0] > 0
    assert hasattr(md, 'url')
    assert hasattr(md, 'query_time')

def test_get_reference_table_wrong_name():
    with pytest.raises(ValueError):
        get_reference_table("agency-cod")

