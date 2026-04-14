import datetime
import sys
from unittest import mock

import pandas as pd
import pytest
from pandas import DataFrame

if sys.version_info < (3, 10):
    pytest.skip("Skip entire module on Python < 3.10", allow_module_level=True)

from dataretrieval.waterdata import (
    get_channel,
    get_combined_metadata,
    get_continuous,
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
from dataretrieval.waterdata.utils import (
    _check_monitoring_location_id,
    _check_profiles,
    _construct_api_requests,
    _normalize_str_iterable,
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
    # 181 source columns + 6 derived <prefix>DateTime columns
    assert df.shape == (67, 187)
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None
    assert df["Activity_StartDateTime"].notna().any()


def test_mock_get_samples_summary(requests_mock):
    """Tests USGS Samples summary query"""
    request_url = (
        "https://api.waterdata.usgs.gov/samples-data/summary/USGS-04183500"
        "?mimeType=text%2Fcsv"
    )
    response_file_path = "tests/data/samples_summary.txt"
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_samples_summary(monitoringLocationIdentifier="USGS-04183500")
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
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_get_samples_summary_rejects_list():
    """The summary endpoint accepts only one site; a list must raise TypeError."""
    with pytest.raises(TypeError, match="exactly one monitoring location"):
        get_samples_summary(monitoringLocationIdentifier=["USGS-04183500"])


def test_check_profiles():
    """Tests that correct errors are raised for invalid profiles."""
    with pytest.raises(ValueError):
        _check_profiles(service="foo", profile="bar")
    with pytest.raises(ValueError):
        _check_profiles(service="results", profile="foo")


def test_construct_api_requests_multivalue_get():
    """Multi-value params use GET with comma-separated values for daily service."""
    req = _construct_api_requests(
        "daily",
        monitoring_location_id=["USGS-05427718", "USGS-05427719"],
        parameter_code=["00060", "00065"],
    )
    assert req.method == "GET"
    assert "monitoring_location_id=USGS-05427718%2CUSGS-05427719" in req.url
    assert "parameter_code=00060%2C00065" in req.url


def test_construct_api_requests_monitoring_locations_post():
    """monitoring-locations uses POST+CQL2 for multi-value params (API limitation)."""
    req = _construct_api_requests(
        "monitoring-locations",
        hydrologic_unit_code=["010802050102", "010802050103"],
    )
    assert req.method == "POST"
    assert req.body is not None


def test_samples_results():
    """Test results call for proper columns"""
    df, _ = get_samples(
        service="results",
        profile="narrow",
        monitoringLocationIdentifier="USGS-05288705",
        activityStartDateLower="2024-10-01",
        activityStartDateUpper="2025-04-24",
    )
    assert all(
        col in df.columns
        for col in ["Location_Identifier", "Activity_ActivityIdentifier"]
    )
    assert len(df) > 0


def test_samples_activity():
    """Test activity call for proper columns"""
    df, _ = get_samples(
        service="activities",
        profile="sampact",
        monitoringLocationIdentifier="USGS-06719505",
    )
    assert len(df) > 0
    assert len(df.columns) == 97
    assert "Location_HUCTwelveDigitCode" in df.columns


def test_samples_locations():
    """Test locations call for proper columns"""
    df, _ = get_samples(
        service="locations",
        profile="site",
        stateFips="US:55",
        activityStartDateLower="2024-10-01",
        activityStartDateUpper="2025-04-24",
        usgsPCode="00010",
    )
    assert all(
        col in df.columns for col in ["Location_Identifier", "Location_Latitude"]
    )
    assert len(df) > 0


def test_samples_projects():
    """Test projects call for proper columns"""
    df, _ = get_samples(
        service="projects",
        profile="project",
        stateFips="US:15",
        activityStartDateLower="2024-10-01",
        activityStartDateUpper="2025-04-24",
    )
    assert all(col in df.columns for col in ["Org_Identifier", "Project_Identifier"])
    assert len(df) > 0


def test_samples_organizations():
    """Test organizations call for proper columns"""
    df, _ = get_samples(service="organizations", profile="count", stateFips="US:01")
    assert len(df) == 1
    assert df.size == 3


def test_get_daily():
    df, md = get_daily(
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
        time="2025-01-01/..",
    )
    assert "daily_id" in df.columns
    assert "geometry" in df.columns
    assert df.columns[-1] == "daily_id"
    assert df.shape[1] == 12
    assert df.parameter_code.unique().tolist() == ["00060"]
    assert df.monitoring_location_id.unique().tolist() == ["USGS-05427718"]
    assert df["time"].apply(lambda x: isinstance(x, datetime.date)).all()
    assert df["time"].iloc[0] < df["time"].iloc[-1]
    assert hasattr(md, "url")
    assert hasattr(md, "query_time")
    assert df["value"].dtype == "float64"


def test_get_daily_properties():
    df, _ = get_daily(
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
        time="2025-01-01/..",
        properties=[
            "daily_id",
            "monitoring_location_id",
            "parameter_code",
            "time",
            "value",
            "geometry",
        ],
    )
    assert df.columns[0] == "daily_id"
    assert df.columns[-1] == "geometry"
    assert df.shape[1] == 6
    assert df.parameter_code.unique().tolist() == ["00060"]


def test_get_daily_properties_id():
    df, _ = get_daily(
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
        time="2025-01-01/..",
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


def test_get_daily_no_geometry():
    df, _ = get_daily(
        monitoring_location_id="USGS-05427718",
        parameter_code="00060",
        time="2025-01-01/..",
        skip_geometry=True,
    )
    assert "geometry" not in df.columns
    assert df.shape[1] == 11
    assert isinstance(df, DataFrame)


def test_get_continuous():
    df, _ = get_continuous(
        monitoring_location_id="USGS-06904500",
        parameter_code="00065",
        time="2025-01-01/2025-12-31",
    )
    assert isinstance(df, DataFrame)
    assert "geometry" not in df.columns
    assert (
        df["time"].dtype.name.startswith("datetime64[")
        and "UTC" in df["time"].dtype.name
    )
    assert "continuous_id" in df.columns


def test_get_monitoring_locations():
    df, md = get_monitoring_locations(state_name="Connecticut", site_type_code="GW")
    assert df.site_type_code.unique().tolist() == ["GW"]
    assert hasattr(md, "url")
    assert hasattr(md, "query_time")


def test_get_monitoring_locations_hucs():
    df, _ = get_monitoring_locations(
        hydrologic_unit_code=["010802050102", "010802050103"]
    )
    assert set(df.hydrologic_unit_code.unique().tolist()) == {
        "010802050102",
        "010802050103",
    }


def test_get_latest_continuous():
    df, md = get_latest_continuous(
        monitoring_location_id=["USGS-05427718", "USGS-05427719"],
        parameter_code=["00060", "00065"],
    )
    assert df.columns[-1] == "latest_continuous_id"
    assert df.shape[0] <= 4
    assert df.statistic_id.unique().tolist() == ["00011"]
    assert hasattr(md, "url")
    assert (
        df["time"].dtype.name.startswith("datetime64[")
        and "UTC" in df["time"].dtype.name
    )


def test_get_latest_daily():
    df, md = get_latest_daily(
        monitoring_location_id=["USGS-05427718", "USGS-05427719"],
        parameter_code=["00060", "00065"],
    )
    assert "latest_daily_id" in df.columns
    assert df.shape[1] == 12
    assert hasattr(md, "url")
    assert hasattr(md, "query_time")


def test_get_latest_daily_properties_geometry():
    df, _md = get_latest_daily(
        monitoring_location_id=["USGS-05427718", "USGS-05427719"],
        parameter_code=["00060", "00065"],
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


def test_get_field_measurements():
    df, md = get_field_measurements(
        monitoring_location_id="USGS-05427718",
        unit_of_measure="ft^3/s",
        time="2025-01-01/2025-10-01",
        skip_geometry=True,
    )
    assert "field_measurement_id" in df.columns
    assert "geometry" not in df.columns
    assert df.unit_of_measure.unique().tolist() == ["ft^3/s"]
    assert hasattr(md, "url")
    assert hasattr(md, "query_time")


def test_get_time_series_metadata():
    df, md = get_time_series_metadata(
        bbox=[-89.840355, 42.853411, -88.818626, 43.422598],
        parameter_code=["00060", "00065", "72019"],
        skip_geometry=True,
    )
    assert set(df["parameter_name"].unique().tolist()) == {
        "Gage height",
        "Water level, depth LSD",
        "Discharge",
    }
    assert hasattr(md, "url")
    assert hasattr(md, "query_time")


def test_get_combined_metadata():
    df, md = get_combined_metadata(
        monitoring_location_id="USGS-05407000",
        skip_geometry=True,
    )
    assert "monitoring_location_id" in df.columns
    assert "parameter_code" in df.columns
    assert "data_type" in df.columns
    assert "drainage_area" in df.columns
    assert (df["monitoring_location_id"] == "USGS-05407000").all()
    assert hasattr(md, "url")
    assert hasattr(md, "query_time")


def test_get_combined_metadata_multi_site_post():
    df, _ = get_combined_metadata(
        monitoring_location_id=[
            "USGS-07069000",
            "USGS-07064000",
            "USGS-07068000",
        ],
        parameter_code="00060",
        skip_geometry=True,
    )
    assert set(df["monitoring_location_id"].unique()) == {
        "USGS-07069000",
        "USGS-07064000",
        "USGS-07068000",
    }
    assert (df["parameter_code"] == "00060").all()


def test_get_field_measurements_metadata():
    df, md = get_field_measurements_metadata(
        monitoring_location_id="USGS-02238500", skip_geometry=True
    )
    assert "field_series_id" in df.columns
    assert "begin" in df.columns
    assert "end" in df.columns
    assert (df["monitoring_location_id"] == "USGS-02238500").all()
    assert hasattr(md, "url")
    assert hasattr(md, "query_time")


def test_get_field_measurements_metadata_multi_site():
    df, _ = get_field_measurements_metadata(
        monitoring_location_id=[
            "USGS-07069000",
            "USGS-07064000",
            "USGS-07068000",
        ],
        parameter_code="00060",
        skip_geometry=True,
    )
    assert (df["parameter_code"] == "00060").all()
    assert set(df["monitoring_location_id"].unique()) == {
        "USGS-07069000",
        "USGS-07064000",
        "USGS-07068000",
    }


def test_get_peaks():
    df, md = get_peaks(monitoring_location_id="USGS-02238500", skip_geometry=True)
    assert "peak_id" in df.columns
    assert "value" in df.columns
    assert "water_year" in df.columns
    assert (df["monitoring_location_id"] == "USGS-02238500").all()
    assert set(df["parameter_code"].unique()).issubset({"00060", "00065"})
    assert hasattr(md, "url")
    assert hasattr(md, "query_time")


def test_get_peaks_water_year_filter():
    df, _ = get_peaks(
        monitoring_location_id="USGS-02238500",
        parameter_code="00060",
        water_year=[2020, 2021, 2022],
        skip_geometry=True,
    )
    assert (df["parameter_code"] == "00060").all()
    assert set(df["water_year"].unique()).issubset({2020, 2021, 2022})


def test_get_reference_table():
    df, md = get_reference_table("agency-codes")
    assert "agency_code" in df.columns
    assert df.shape[0] > 0
    assert hasattr(md, "url")
    assert hasattr(md, "query_time")


def test_get_reference_table_with_query():
    query = {"id": "AK001,AK008"}
    df, md = get_reference_table("agency-codes", query=query)
    assert "agency_code" in df.columns
    assert df.shape[0] == 2
    assert hasattr(md, "url")
    assert hasattr(md, "query_time")


def test_get_reference_table_wrong_name():
    with pytest.raises(ValueError):
        get_reference_table("agency-cod")


def test_get_stats_por():
    df, _ = get_stats_por(
        monitoring_location_id="USGS-12451000",
        parameter_code="00060",
        start_date="01-01",
        end_date="01-01",
    )
    assert (
        df["computation"]
        .isin(["median", "maximum", "minimum", "arithmetic_mean", "percentile"])
        .all()
    )
    assert df["time_of_year"].isin(["01-01", "01"]).all()
    assert df.loc[df["computation"] == "minimum", "percentile"].unique().tolist() == [
        0.0
    ]
    assert df.loc[df["computation"] == "arithmetic_mean", "percentile"].isnull().all()


def test_get_stats_por_expanded_false():
    df, _ = get_stats_por(
        monitoring_location_id="USGS-12451000",
        parameter_code="00060",
        start_date="01-01",
        end_date="01-01",
        expand_percentiles=False,
        computation_type=["minimum", "percentile"],
    )
    assert df.shape[0] == 4
    assert df.shape[1] == 20  # if geopandas installed, 21 columns if not
    assert "percentile" not in df.columns
    assert "percentiles" in df.columns
    assert type(df["percentiles"][2]) is list
    assert df.loc[~df["percentiles"].isna(), "value"].isnull().all()


def test_get_stats_date_range():
    df, _ = get_stats_date_range(
        monitoring_location_id="USGS-12451000",
        parameter_code="00060",
        start_date="2025-01-01",
        end_date="2025-01-01",
        computation_type="maximum",
    )

    assert df.shape[0] == 3
    assert df.shape[1] == 20  # if geopandas installed, 21 columns if not
    assert "interval_type" in df.columns
    assert "percentile" in df.columns
    assert df["interval_type"].isin(["month", "calendar_year", "water_year"]).all()


def test_get_channel():
    df, _ = get_channel(monitoring_location_id="USGS-02238500")

    assert df.shape[0] > 470
    assert df.shape[1] == 27  # if geopandas installed, 21 columns if not
    assert "channel_measurements_id" in df.columns


class TestCheckMonitoringLocationId:
    """Tests for _check_monitoring_location_id input validation.

    Regression tests for GitHub issue #188.
    """

    def test_valid_string(self):
        """A correctly formatted string passes and is returned unchanged."""
        assert _check_monitoring_location_id("USGS-01646500") == "USGS-01646500"

    def test_valid_list(self):
        """A list of correctly formatted strings passes without error."""
        ids = ["USGS-01646500", "USGS-02238500"]
        assert _check_monitoring_location_id(ids) == ids

    def test_none_passes(self):
        """None is allowed (optional parameter)."""
        assert _check_monitoring_location_id(None) is None

    def test_integer_raises_type_error(self):
        """An integer ID raises TypeError with a helpful AGENCY-ID hint."""
        with pytest.raises(TypeError, match="not int") as exc_info:
            _check_monitoring_location_id(5129115)
        # The wrapper appends the AGENCY-ID format hint that the generic
        # helper alone doesn't carry.
        assert "USGS-01646500" in str(exc_info.value)

    def test_integer_in_list_raises_type_error(self):
        """An integer inside a list raises TypeError."""
        with pytest.raises(TypeError, match="not int"):
            _check_monitoring_location_id(["USGS-01646500", 5129115])

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

    def test_tuple_normalizes_to_list(self):
        """A tuple of valid strings is accepted and normalized to list."""
        result = _check_monitoring_location_id(("USGS-01646500", "USGS-02238500"))
        assert result == ["USGS-01646500", "USGS-02238500"]
        assert isinstance(result, list)

    def test_pandas_series_normalizes_to_list(self):
        """A pandas.Series of valid strings is accepted and normalized to list."""
        s = pd.Series(["USGS-01646500", "USGS-02238500"])
        result = _check_monitoring_location_id(s)
        assert result == ["USGS-01646500", "USGS-02238500"]
        assert isinstance(result, list)

    def test_pandas_index_normalizes_to_list(self):
        """A pandas.Index of valid strings is accepted and normalized to list."""
        idx = pd.Index(["USGS-01646500", "USGS-02238500"])
        result = _check_monitoring_location_id(idx)
        assert result == ["USGS-01646500", "USGS-02238500"]
        assert isinstance(result, list)

    def test_numpy_array_normalizes_to_list(self):
        """A numpy.ndarray of valid strings is accepted and normalized to list."""
        import numpy as np

        arr = np.array(["USGS-01646500", "USGS-02238500"])
        result = _check_monitoring_location_id(arr)
        assert result == ["USGS-01646500", "USGS-02238500"]
        assert isinstance(result, list)

    def test_numpy_int_array_raises_type_error(self):
        """An iterable whose elements aren't strings (numpy int array) raises."""
        import numpy as np

        with pytest.raises(TypeError, match="elements must be strings"):
            _check_monitoring_location_id(np.array([1, 2, 3]))

    def test_pandas_series_of_ints_raises_type_error(self):
        """An iterable whose elements aren't strings (Series of ints) raises."""
        with pytest.raises(TypeError, match="elements must be strings"):
            _check_monitoring_location_id(pd.Series([1, 2, 3]))

    def test_dict_raises_type_error(self):
        """Mappings are rejected — iterating a dict yields keys, which is a footgun."""
        with pytest.raises(TypeError, match="not dict"):
            _check_monitoring_location_id({"USGS-01646500": "site"})

    def test_get_daily_malformed_id_raises(self):
        """get_daily raises ValueError for a malformed string ID."""
        with pytest.raises(ValueError):
            get_daily(monitoring_location_id="dog", parameter_code="00060")


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

    def test_list_returned_unchanged(self):
        assert _normalize_str_iterable(["00060", "00010"], "p") == ["00060", "00010"]

    def test_tuple_normalizes_to_list(self):
        result = _normalize_str_iterable(("00060", "00010"), "p")
        assert result == ["00060", "00010"]
        assert isinstance(result, list)

    def test_pandas_series_normalizes_to_list(self):
        result = _normalize_str_iterable(pd.Series(["00060", "00010"]), "p")
        assert result == ["00060", "00010"]
        assert isinstance(result, list)

    def test_numpy_array_normalizes_to_list(self):
        import numpy as np

        result = _normalize_str_iterable(np.array(["00060", "00010"]), "p")
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
        with mock.patch("dataretrieval.waterdata.api.get_ogc_data") as fake:
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
