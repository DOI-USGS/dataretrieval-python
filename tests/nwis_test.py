import datetime
import json
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from dataretrieval.nwis import (
    NWIS_Metadata,
    _read_rdb,
    get_discharge_measurements,
    get_gwlevels,
    get_info,
    get_iv,
    get_pmcodes,
    get_qwdata,
    get_record,
    get_water_use,
    preformat_peaks_response,
    what_sites,
)

START_DATE = "2018-01-24"
END_DATE = "2018-01-25"

DATETIME_COL = "datetime"
SITENO_COL = "site_no"


def _load_mock_json(file_name):
    """Helper to load mock JSON from tests/data."""
    path = Path(__file__).parent / "data" / file_name
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _test_iv_service(requests_mock):
    """Mocked test of instantaneous value service"""
    start = START_DATE
    end = END_DATE
    service = "iv"
    site = ["03339000", "05447500", "03346500"]

    # We use a very simple JSON structure just to satisfy the parser
    mock_json = _load_mock_json("nwis_iv_mock.json")

    # Match the base URL and ensure query parameters are correct
    requests_mock.get(
        "https://waterservices.usgs.gov/nwis/iv",
        json=mock_json,
        complete_qs=False,
    )

    return get_record(site, start, end, service=service)


def test_iv_service_answer(requests_mock):
    df = _test_iv_service(requests_mock)
    # check multiindex function
    assert df.index.names == [
        SITENO_COL,
        DATETIME_COL,
    ], f"iv service returned incorrect index: {df.index.names}"


def test_nwis_service_live():
    """Live sanity check of NWIS service, tolerant of transient NWIS outages."""
    site = "01491000"
    try:
        # Minimal query: just most recent record
        get_iv(sites=site)
    except ValueError as e:
        # Catch known transient service failures surfaced as ValueError
        error_text = str(e)
        if any(
            err in error_text
            for err in [
                "500",
                "502",
                "503",
                "Service Unavailable",
                "Received HTML response instead of JSON",
            ]
        ):
            pytest.skip(
                f"Service is currently unavailable (transient NWIS outage): {e}"
            )
        raise
    except Exception as e:
        # Fallback for other potential transient network issues
        if "Expecting value" in str(e) or "JSON" in str(e):
            pytest.skip(
                f"Service returned invalid response (likely transient outage): {e}"
            )
        raise


def test_preformat_peaks_response():
    # make a data frame with a "peak_dt" datetime column
    # it will have some nan and none values
    data = {"peak_dt": ["2000-03-22", np.nan, None], "peak_va": [1000, 2000, 3000]}
    # turn data into dataframe
    df = pd.DataFrame(data)
    # run preformat function
    df = preformat_peaks_response(df)
    # assertions
    assert "datetime" in df.columns
    assert df["datetime"].isna().sum() == 0


# tests using real queries to USGS webservices
# these specific queries represent some edge-cases and the tests to address
# incomplete date-time information


# Removed defunct gwlevels tests.


class TestDefunct:
    """Verify that defunct functions raise NameError."""

    def test_get_qwdata_raises(self):
        with pytest.raises(NameError, match="get_qwdata"):
            get_qwdata()

    def test_get_discharge_measurements_raises(self):
        with pytest.raises(NameError, match="get_discharge_measurements"):
            get_discharge_measurements()

    def test_get_gwlevels_raises(self):
        with pytest.raises(NameError, match="get_gwlevels"):
            get_gwlevels()

    def test_get_pmcodes_raises(self):
        with pytest.raises(NameError, match="get_pmcodes"):
            get_pmcodes()

    def test_get_water_use_raises(self):
        with pytest.raises(NameError, match="get_water_use"):
            get_water_use()

    def test_get_record_defunct_service_measurements(self):
        with pytest.raises(NameError, match="no longer supported by get_record"):
            get_record(service="measurements")

    def test_get_record_defunct_service_gwlevels(self):
        with pytest.raises(NameError, match="no longer supported by get_record"):
            get_record(service="gwlevels")

    def test_get_record_defunct_service_pmcodes(self):
        with pytest.raises(NameError, match="no longer supported by get_record"):
            get_record(service="pmcodes")

    def test_get_record_defunct_service_water_use(self):
        with pytest.raises(NameError, match="no longer supported by get_record"):
            get_record(service="water_use")


class TestTZ:
    """Tests relating to GitHub Issue #60."""

    sites, _ = what_sites(stateCd="MD")

    def test_multiple_tz_01(self):
        """Test based on GitHub Issue #60 - error merging different time zones."""
        # this test fails before issue #60 is fixed
        iv, _ = get_iv(sites=self.sites.site_no.values[:25].tolist())
        # assert that the datetime column exists
        assert "datetime" in iv.index.names
        # assert that it is a datetime type
        assert isinstance(iv.index[0][1], datetime.datetime)

    def test_multiple_tz_02(self):
        """Test based on GitHub Issue #60 - confirm behavior for same tz."""
        # this test passes before issue #60 is fixed
        iv, _ = get_iv(sites=self.sites.site_no.values[:20].tolist())
        # assert that the datetime column exists
        assert "datetime" in iv.index.names
        # assert that it is a datetime type
        assert isinstance(iv.index[0][1], datetime.datetime)


class TestSiteseriesCatalogOutput:
    """Tests relating to GitHub Issue #34."""

    def test_seriesCatalogOutput_get_record(self):
        """Test setting seriesCatalogOutput to true with get_record."""
        data = get_record(
            huc="20", parameterCd="00060", service="site", seriesCatalogOutput="True"
        )
        # assert that expected data columns are present
        assert "begin_date" in data.columns
        assert "end_date" in data.columns
        assert "count_nu" in data.columns

    def test_seriesCatalogOutput_get_info(self):
        """Test setting seriesCatalogOutput to true with get_info."""
        data, _ = get_info(huc="20", parameterCd="00060", seriesCatalogOutput="TRUE")
        # assert that expected data columns are present
        assert "begin_date" in data.columns
        assert "end_date" in data.columns
        assert "count_nu" in data.columns

    def test_seriesCatalogOutput_bool(self):
        """Test setting seriesCatalogOutput with a boolean."""
        data, _ = get_info(huc="20", parameterCd="00060", seriesCatalogOutput=True)
        # assert that expected data columns are present
        assert "begin_date" in data.columns
        assert "end_date" in data.columns
        assert "count_nu" in data.columns

    def test_expandedrdb_get_record(self):
        """Test default expanded_rdb format with get_record."""
        data = get_record(
            huc="20", parameterCd="00060", service="site", seriesCatalogOutput="False"
        )
        # assert that seriesCatalogOutput columns are not present
        assert "begin_date" not in data.columns
        assert "end_date" not in data.columns
        assert "count_nu" not in data.columns

    def test_expandedrdb_get_info(self):
        """Test default expanded_rdb format with get_info."""
        data, _ = get_info(huc="20", parameterCd="00060")
        # assert that seriesCatalogOutput columns are not present
        assert "begin_date" not in data.columns
        assert "end_date" not in data.columns
        assert "count_nu" not in data.columns


def test_empty_timeseries(requests_mock):
    """Test based on empty case from GitHub Issue #26."""
    sites = "011277906"
    start = "2010-07-20"
    end = "2010-07-20"

    mock_json = _load_mock_json("nwis_iv_empty_mock.json")
    # Match the base URL and ensure query parameters are correct
    requests_mock.get(
        "https://waterservices.usgs.gov/nwis/iv",
        json=mock_json,
        complete_qs=False,
    )

    df = get_record(sites=sites, service="iv", start=start, end=end)
    assert df.empty is True


class TestMetaData:
    """Tests of NWIS metadata setting,

    Notes
    -----

    - Originally based on GitHub Issue #73.
    - Modified to site_info and variable_info as properties, not callables.
    """

    def test_set_metadata_info_site(self):
        """Test metadata info is set when site parameter is supplied."""
        # mock the query response
        response = mock.MagicMock()
        # make metadata call
        md = NWIS_Metadata(response, sites="01491000")
        # assert that site_info is implemented
        assert md.site_info

    def test_set_metadata_info_site_no(self):
        """Test metadata info is set when site_no parameter is supplied."""
        # mock the query response
        response = mock.MagicMock()
        # make metadata call
        md = NWIS_Metadata(response, site_no="01491000")
        # assert that site_info is implemented
        assert md.site_info

    def test_set_metadata_info_stateCd(self):
        """Test metadata info is set when stateCd parameter is supplied."""
        # mock the query response
        response = mock.MagicMock()
        # make metadata call
        md = NWIS_Metadata(response, stateCd="RI")
        # assert that site_info is implemented
        assert md.site_info

    def test_set_metadata_info_huc(self):
        """Test metadata info is set when huc parameter is supplied."""
        # mock the query response
        response = mock.MagicMock()
        # make metadata call
        md = NWIS_Metadata(response, huc="01")
        # assert that site_info is implemented
        assert md.site_info

    def test_set_metadata_info_bbox(self):
        """Test metadata info is set when bbox parameter is supplied."""
        # mock the query response
        response = mock.MagicMock()
        # make metadata call
        md = NWIS_Metadata(response, bBox="-92.8,44.2,-88.9,46.0")
        # assert that site_info is implemented
        assert md.site_info

    def test_set_metadata_info_countyCd(self):
        """Test metadata info is set when countyCd parameter is supplied."""
        # mock the query response
        response = mock.MagicMock()
        # make metadata call
        md = NWIS_Metadata(response, countyCd="01001")
        # assert that site_info is implemented
        assert md.site_info

    def test_variable_info_deprecated(self):
        """Test that variable_info raises a DeprecationWarning and returns None."""
        response = mock.MagicMock()
        md = NWIS_Metadata(response)
        with pytest.warns(
            DeprecationWarning,
            match="Accessing variable_info via NWIS_Metadata is deprecated",
        ):
            result = md.variable_info
        assert result is None


class TestReadRdb:
    """Tests for the _read_rdb helper.

    Notes
    -----
    Related to GitHub Issue #171.
    """

    # Minimal valid RDB response with one data row
    _VALID_RDB = "# comment\nsite_no\tvalue\n5s\t10n\n01491000\t42\n"

    # NWIS response when no sites match the query criteria
    _NO_SITES_RDB = (
        "# //Output-Format: RDB\n"
        "# //Response-Status: OK\n"
        "# //Response-Message: No sites found matching all criteria\n"
    )

    def test_valid_rdb_returns_dataframe(self):
        """_read_rdb returns a DataFrame for a well-formed RDB response."""
        df = _read_rdb(self._VALID_RDB)
        assert isinstance(df, pd.DataFrame)
        assert "site_no" in df.columns

    def test_no_sites_returns_empty_dataframe(self):
        """_read_rdb returns an empty DataFrame when NWIS finds no matching sites.

        A "No sites found" response is a legitimate empty result, not an error,
        so callers can check ``df.empty`` rather than catching an exception.
        Regression test for issue #171 (previously raised IndexError).
        """
        df = _read_rdb(self._NO_SITES_RDB)
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_all_comments_returns_empty_dataframe(self):
        """_read_rdb returns an empty DataFrame when the response has only comments."""
        rdb = "# just a comment\n# another comment\n"
        df = _read_rdb(rdb)
        assert isinstance(df, pd.DataFrame)
        assert df.empty
