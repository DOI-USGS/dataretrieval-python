"""Unit tests for functions in utils.py"""

from unittest import mock

import pandas as pd
import pytest

from dataretrieval import nwis, utils


class Test_query:
    """Tests of the query function."""

    def test_url_too_long(self):
        """Test to confirm error when query URL too long.

        Test based on GitHub Issue #64.
        The server may respond with a 414 (converted to ValueError by query())
        or abruptly close the connection (ConnectionError). Both are valid
        responses to an excessively long URL.
        """
        import requests as req

        # all sites in MD
        sites, _ = nwis.what_sites(stateCd="MD")
        # raise error by trying to query them all, so URL is way too long
        with pytest.raises((ValueError, req.exceptions.ConnectionError)):
            nwis.get_iv(sites=sites.site_no.values.tolist())

    def test_header(self):
        """Test checking header info with user-agent is part of query."""
        url = "https://waterservices.usgs.gov/nwis/dv"
        payload = {
            "format": "json",
            "startDT": "2010-10-01",
            "endDT": "2010-10-10",
            "sites": "01646500",
            "multi_index": True,
        }
        response = utils.query(url, payload)
        assert response.status_code == 200  # GET was successful
        assert "user-agent" in response.request.headers


class Test_BaseMetadata:
    """Tests of BaseMetadata"""

    def test_init_with_response(self):
        response = mock.MagicMock()
        md = utils.BaseMetadata(response)

        # Test parameters initialized from the API response
        assert md.url is not None
        assert md.query_time is not None
        assert md.header is not None

        # Test NotImplementedError parameters
        with pytest.raises(NotImplementedError):
            _ = md.variable_info


class Test_to_str:
    """Tests of the to_str function."""

    def test_to_str_list(self):
        assert utils.to_str([1, "a", 2]) == "1,a,2"

    def test_to_str_tuple(self):
        assert utils.to_str((1, "b", 3)) == "1,b,3"

    def test_to_str_set(self):
        # Sets are unordered, so we check if elements are present
        result = utils.to_str({1, 2})
        assert "1" in result
        assert "2" in result
        assert "," in result

    def test_to_str_generator(self):
        def gen():
            yield from [1, 2, 3]

        assert utils.to_str(gen()) == "1,2,3"

    def test_to_str_pandas_series(self):
        s = pd.Series([10, 20])
        assert utils.to_str(s) == "10,20"

    def test_to_str_pandas_index(self):
        idx = pd.Index(["x", "y"])
        assert utils.to_str(idx) == "x,y"

    def test_to_str_string(self):
        assert utils.to_str("already a string") == "already a string"

    def test_to_str_custom_delimiter(self):
        assert utils.to_str([1, 2, 3], delimiter="|") == "1|2|3"

    def test_to_str_non_iterable(self):
        assert utils.to_str(123) is None


class Test_attach_datetime_columns:
    """Tests of _attach_datetime_columns, which derives <prefix>DateTime UTC
    columns from Date/Time/TimeZone triplets in Samples and WQP CSVs."""

    def test_wqx3_triplet_resolves_to_utc(self):
        df = pd.DataFrame(
            {
                "Activity_StartDate": ["2024-01-09", "2024-02-15"],
                "Activity_StartTime": ["10:00:00", "14:30:00"],
                "Activity_StartTimeZone": ["PST", "EST"],
            }
        )
        df = utils._attach_datetime_columns(df)
        assert df["Activity_StartDateTime"][0] == pd.Timestamp(
            "2024-01-09 18:00:00", tz="UTC"
        )
        assert df["Activity_StartDateTime"][1] == pd.Timestamp(
            "2024-02-15 19:30:00", tz="UTC"
        )
        assert df["Activity_StartTimeZone"].tolist() == ["PST", "EST"]

    def test_legacy_wqp_triplet_resolves_to_utc(self):
        df = pd.DataFrame(
            {
                "ActivityStartDate": ["2024-01-09"],
                "ActivityStartTime/Time": ["10:00:00"],
                "ActivityStartTime/TimeZoneCode": ["PST"],
            }
        )
        df = utils._attach_datetime_columns(df)
        assert df["ActivityStartDateTime"][0] == pd.Timestamp(
            "2024-01-09 18:00:00", tz="UTC"
        )

    def test_unknown_timezone_is_NaT(self):
        df = pd.DataFrame(
            {
                "Activity_StartDate": ["2024-01-09"],
                "Activity_StartTime": ["10:00:00"],
                "Activity_StartTimeZone": ["BOGUS"],
            }
        )
        df = utils._attach_datetime_columns(df)
        assert df["Activity_StartDateTime"].isna().all()

    def test_missing_time_or_tz_is_NaT(self):
        df = pd.DataFrame(
            {
                "Activity_StartDate": ["2024-01-09", "2024-02-15"],
                "Activity_StartTime": ["10:00:00", None],
                "Activity_StartTimeZone": ["PST", "EST"],
            }
        )
        df = utils._attach_datetime_columns(df)
        assert df["Activity_StartDateTime"][0] == pd.Timestamp(
            "2024-01-09 18:00:00", tz="UTC"
        )
        assert pd.isna(df["Activity_StartDateTime"][1])

    def test_existing_datetime_column_not_overwritten(self):
        df = pd.DataFrame(
            {
                "Activity_StartDate": ["2024-01-09"],
                "Activity_StartTime": ["10:00:00"],
                "Activity_StartTimeZone": ["PST"],
                "Activity_StartDateTime": ["preexisting"],
            }
        )
        df = utils._attach_datetime_columns(df)
        assert df["Activity_StartDateTime"].tolist() == ["preexisting"]

    def test_multiple_triplets_handled(self):
        df = pd.DataFrame(
            {
                "Activity_StartDate": ["2024-01-09"],
                "Activity_StartTime": ["10:00:00"],
                "Activity_StartTimeZone": ["PST"],
                "LabInfo_AnalysisStartDate": ["2024-01-10"],
                "LabInfo_AnalysisStartTime": ["09:00:00"],
                "LabInfo_AnalysisStartTimeZone": ["EST"],
            }
        )
        df = utils._attach_datetime_columns(df)
        assert "Activity_StartDateTime" in df.columns
        assert "LabInfo_AnalysisStartDateTime" in df.columns

    def test_lone_date_column_left_alone(self):
        df = pd.DataFrame({"LastChangeDate": ["2024-01-09"]})
        df = utils._attach_datetime_columns(df)
        assert list(df.columns) == ["LastChangeDate"]

    def test_rows_sorted_by_wqx3_activity_start(self):
        df = pd.DataFrame(
            {
                "Activity_StartDate": ["2024-03-01", "2024-01-09", "2024-02-15"],
                "Activity_StartTime": ["10:00:00", "10:00:00", "10:00:00"],
                "Activity_StartTimeZone": ["UTC", "UTC", "UTC"],
                "marker": ["c", "a", "b"],
            }
        )
        df = utils._attach_datetime_columns(df)
        assert df["marker"].tolist() == ["a", "b", "c"]
        assert df.index.tolist() == [0, 1, 2]

    def test_rows_sorted_by_legacy_activity_start_when_wqx3_absent(self):
        df = pd.DataFrame(
            {
                "ActivityStartDate": ["2024-03-01", "2024-01-09"],
                "ActivityStartTime/Time": ["10:00:00", "10:00:00"],
                "ActivityStartTime/TimeZoneCode": ["UTC", "UTC"],
                "marker": ["b", "a"],
            }
        )
        df = utils._attach_datetime_columns(df)
        assert df["marker"].tolist() == ["a", "b"]

    def test_rows_sorted_by_first_date_column_as_fallback(self):
        # No triplet → no DateTime column added, but rows still sort by the
        # first *Date column found (mirrors R's importWQP.R fallback).
        df = pd.DataFrame(
            {
                "LastChangeDate": ["2024-03-01", "2024-01-09"],
                "marker": ["b", "a"],
            }
        )
        df = utils._attach_datetime_columns(df)
        assert df["marker"].tolist() == ["a", "b"]
