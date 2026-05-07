import datetime
import json
import warnings
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


class TestDeprecationWarnings:
    """Verify per-function DeprecationWarning fires with the right replacement.

    The module-level "use waterdata instead" warning fires on import; these
    tests pin the function-specific replacements so users see actionable
    migration guidance the first time they call each NWIS getter.
    """

    @pytest.mark.parametrize(
        "func_name, replacement_substring",
        [
            ("get_dv", "waterdata.get_daily"),
            ("get_iv", "waterdata.get_continuous"),
            ("get_info", "waterdata.get_monitoring_locations"),
            ("what_sites", "waterdata.get_monitoring_locations"),
            ("get_stats", "waterdata.get_stats_por"),
            ("get_discharge_peaks", "waterdata.get_peaks"),
            ("get_ratings", "waterdata.get_ratings"),
            ("get_record", "waterdata.get_*"),
            ("query_waterdata", "waterdata.get_*"),
            ("query_waterservices", "waterdata.get_*"),
        ],
    )
    def test_warn_message_includes_replacement(self, func_name, replacement_substring):
        """Each deprecated function emits a warning naming the right replacement."""
        from dataretrieval.nwis import _NWIS_REMOVAL_DATE, _warn_deprecated

        with pytest.warns(DeprecationWarning, match=func_name) as record:
            _warn_deprecated(func_name)
        message = str(record[0].message)
        assert replacement_substring in message
        assert _NWIS_REMOVAL_DATE in message

    def test_get_iv_fires_deprecation_on_call(self, requests_mock):
        """End-to-end: a real call routes through _warn_deprecated."""
        requests_mock.get(
            "https://waterservices.usgs.gov/nwis/iv",
            json={"value": {"timeSeries": []}},
        )
        with pytest.warns(DeprecationWarning, match="get_iv.*waterdata.get_continuous"):
            get_iv(sites="01491000")

    def test_nested_calls_emit_one_warning(self, requests_mock):
        """get_record(service='iv') wraps get_iv -> query_waterservices.

        Without re-entrancy suppression the user would see 3 near-identical
        deprecation warnings for one call; pin the outermost-only contract.
        """
        requests_mock.get(
            "https://waterservices.usgs.gov/nwis/iv",
            json={"value": {"timeSeries": []}},
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            get_record(sites="01491000", service="iv")
        deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert len(deprecations) == 1
        assert "get_record" in str(deprecations[0].message)


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
    """Tests for the NWIS-specific _read_rdb wrapper.

    The format-agnostic parser is exercised in tests/rdb_test.py; this
    class pins the wrapper-specific contract — that an empty parser
    result flows through format_response without crashing (issue #171).
    """

    def test_no_sites_flows_through_format_response(self):
        """A "No sites found" response is a legitimate empty result, not an
        error, so callers can check ``df.empty`` rather than catching an
        exception. Regression for issue #171 (previously raised IndexError),
        which now also covers the empty-frame path through ``format_response``.
        """
        no_sites_rdb = (
            "# //Output-Format: RDB\n"
            "# //Response-Status: OK\n"
            "# //Response-Message: No sites found matching all criteria\n"
        )
        df = _read_rdb(no_sites_rdb)
        assert isinstance(df, pd.DataFrame)
        assert df.empty
