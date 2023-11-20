import datetime
import unittest.mock as mock

import numpy as np
import pandas as pd
import pytest

from dataretrieval.nwis import NWIS_Metadata
from dataretrieval.nwis import get_info, get_record, preformat_peaks_response, get_iv, what_sites

START_DATE = '2018-01-24'
END_DATE = '2018-01-25'

DATETIME_COL = 'datetime'
SITENO_COL = 'site_no'


def test_measurements_service():
    """Test measurement service
    """
    start = '2018-01-24'
    end = '2018-01-25'
    service = 'measurements'
    site = '03339000'
    df = get_record(site, start, end, service=service)
    return df


def test_measurements_service_answer():
    df = test_measurements_service()
    # check parsing
    assert df.iloc[0]['measurement_nu'] == 801


def test_iv_service():
    """Unit test of instantaneous value service
    """
    start = START_DATE
    end = END_DATE
    service = 'iv'
    site = ['03339000', '05447500', '03346500']
    return get_record(site, start, end, service=service)


def test_iv_service_answer():
    df = test_iv_service()
    # check multiindex function
    assert df.index.names == [SITENO_COL, DATETIME_COL], "iv service returned incorrect index: {}".format(df.index.names)


def test_preformat_peaks_response():
    # make a data frame with a "peak_dt" datetime column
    # it will have some nan and none values
    data = {"peak_dt": ["2000-03-22",
                        np.nan,
                        None],
            "peak_va": [1000,
                        2000,
                        3000]
    }
    # turn data into dataframe
    df = pd.DataFrame(data)
    # run preformat function
    df = preformat_peaks_response(df)
    # assertions
    assert 'datetime' in df.columns
    assert df['datetime'].isna().sum() == 0


@pytest.mark.parametrize("site_input_type_list", [True, False])
def test_get_record_site_value_types(site_input_type_list):
    """Test that get_record method for valid input types for the 'sites' parameter."""
    start = '2018-01-24'
    end = '2018-01-25'
    service = 'measurements'
    site = '03339000'
    if site_input_type_list:
        sites = [site]
    else:
        sites = site
    df = get_record(sites=sites, start=start, end=end, service=service)
    assert df.iloc[0]['measurement_nu'] == 801


if __name__ == '__main__':
    test_measurements_service_answer()
    test_iv_service_answer()


# tests using real queries to USGS webservices
# these specific queries represent some edge-cases and the tests to address
# incomplete date-time information

def test_inc_date_01():
    """Test based on GitHub Issue #47 - lack of timestamp for measurement."""
    site = "403451073585601"
    # make call expecting a warning to be thrown due to incomplete dates
    with pytest.warns(UserWarning):
        df = get_record(site, "1980-01-01", "1990-01-01", service='gwlevels')
    # assert that there are indeed incomplete dates
    assert any(pd.isna(df.index) == True)
    # assert that the datetime index is there
    assert df.index.name == 'datetime'
    # make call without defining a datetime index and check that it isn't there
    df2 = get_record(site, "1980-01-01", "1990-01-01", service='gwlevels',
                     datetime_index=False)
    # assert shape of both dataframes is the same (contain the same data)
    assert df.shape == df2.shape
    # assert that the datetime index is not there
    assert df2.index.name != 'datetime'


def test_inc_date_02():
    """Test based on GitHub Issue #47 - lack of month, day, or time."""
    site = "180049066381200"
    # make call expecting a warning to be thrown due to incomplete dates
    with pytest.warns(UserWarning):
        df = get_record(site, "1900-01-01", "2013-01-01", service='gwlevels')
    # assert that there are indeed incomplete dates
    assert any(pd.isna(df.index) == True)
    # assert that the datetime index is there
    assert df.index.name == 'datetime'
    # make call without defining a datetime index and check that it isn't there
    df2 = get_record(site, "1900-01-01", "2013-01-01", service='gwlevels',
                     datetime_index=False)
    # assert shape of both dataframes is the same (contain the same data)
    assert df.shape == df2.shape
    # assert that the datetime index is not there
    assert df2.index.name != 'datetime'


def test_inc_date_03():
    """Test based on GitHub Issue #47 - lack of day, and times."""
    site = "290000095192602"
    # make call expecting a warning to be thrown due to incomplete dates
    with pytest.warns(UserWarning):
        df = get_record(site, "1975-01-01", "2000-01-01", service='gwlevels')
    # assert that there are indeed incomplete dates
    assert any(pd.isna(df.index) == True)
    # assert that the datetime index is there
    assert df.index.name == 'datetime'
    # make call without defining a datetime index and check that it isn't there
    df2 = get_record(site, "1975-01-01", "2000-01-01", service='gwlevels',
                     datetime_index=False)
    # assert shape of both dataframes is the same (contain the same data)
    assert df.shape == df2.shape
    # assert that the datetime index is not there
    assert df2.index.name != 'datetime'


class TestTZ:
    """Tests relating to GitHub Issue #60."""
    sites, _ = what_sites(stateCd='MD')

    def test_multiple_tz_01(self):
        """Test based on GitHub Issue #60 - error merging different time zones."""
        # this test fails before issue #60 is fixed
        iv, _ = get_iv(sites=self.sites.site_no.values[:25].tolist())
        # assert that the datetime column exists
        assert 'datetime' in iv.index.names
        # assert that it is a datetime type
        assert isinstance(iv.index[0][1], datetime.datetime)

    def test_multiple_tz_02(self):
        """Test based on GitHub Issue #60 - confirm behavior for same tz."""
        # this test passes before issue #60 is fixed
        iv, _ = get_iv(sites=self.sites.site_no.values[:20].tolist())
        # assert that the datetime column exists
        assert 'datetime' in iv.index.names
        # assert that it is a datetime type
        assert isinstance(iv.index[0][1], datetime.datetime)


class TestSiteseriesCatalogOutput:
    """Tests relating to GitHub Issue #34."""

    def test_seriesCatalogOutput_get_record(self):
        """Test setting seriesCatalogOutput to true with get_record."""
        data = get_record(huc='20', parameterCd='00060',
                          service='site', seriesCatalogOutput='True')
        # assert that expected data columns are present
        assert 'begin_date' in data.columns
        assert 'end_date' in data.columns
        assert 'count_nu' in data.columns

    def test_seriesCatalogOutput_get_info(self):
        """Test setting seriesCatalogOutput to true with get_info."""
        data, _ = get_info(
            huc='20', parameterCd='00060', seriesCatalogOutput='TRUE')
        # assert that expected data columns are present
        assert 'begin_date' in data.columns
        assert 'end_date' in data.columns
        assert 'count_nu' in data.columns

    def test_seriesCatalogOutput_bool(self):
        """Test setting seriesCatalogOutput with a boolean."""
        data, _ = get_info(
            huc='20', parameterCd='00060', seriesCatalogOutput=True)
        # assert that expected data columns are present
        assert 'begin_date' in data.columns
        assert 'end_date' in data.columns
        assert 'count_nu' in data.columns

    def test_expandedrdb_get_record(self):
        """Test default expanded_rdb format with get_record."""
        data = get_record(huc='20', parameterCd='00060',
                          service='site', seriesCatalogOutput='False')
        # assert that seriesCatalogOutput columns are not present
        assert 'begin_date' not in data.columns
        assert 'end_date' not in data.columns
        assert 'count_nu' not in data.columns

    def test_expandedrdb_get_info(self):
        """Test default expanded_rdb format with get_info."""
        data, _ = get_info(huc='20', parameterCd='00060')
        # assert that seriesCatalogOutput columns are not present
        assert 'begin_date' not in data.columns
        assert 'end_date' not in data.columns
        assert 'count_nu' not in data.columns


def test_empty_timeseries():
    """Test based on empty case from GitHub Issue #26."""
    df = get_record(sites='011277906', service='iv',
                    start='2010-07-20', end='2010-07-20')
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
        md = NWIS_Metadata(response, sites='01491000')
        # assert that site_info is implemented
        assert md.site_info
        
    def test_set_metadata_info_site_no(self):
        """Test metadata info is set when site_no parameter is supplied."""
        # mock the query response
        response = mock.MagicMock()
        # make metadata call
        md = NWIS_Metadata(response, site_no='01491000')
        # assert that site_info is implemented
        assert md.site_info

    def test_set_metadata_info_stateCd(self):
        """Test metadata info is set when stateCd parameter is supplied."""
        # mock the query response
        response = mock.MagicMock()
        # make metadata call
        md = NWIS_Metadata(response, stateCd='RI')
        # assert that site_info is implemented
        assert md.site_info

    def test_set_metadata_info_huc(self):
        """Test metadata info is set when huc parameter is supplied."""
        # mock the query response
        response = mock.MagicMock()
        # make metadata call
        md = NWIS_Metadata(response, huc='01')
        # assert that site_info is implemented
        assert md.site_info

    def test_set_metadata_info_bbox(self):
        """Test metadata info is set when bbox parameter is supplied."""
        # mock the query response
        response = mock.MagicMock()
        # make metadata call
        md = NWIS_Metadata(response, bBox='-92.8,44.2,-88.9,46.0')
        # assert that site_info is implemented
        assert md.site_info

    def test_set_metadata_info_countyCd(self):
        """Test metadata info is set when countyCd parameter is supplied."""
        # mock the query response
        response = mock.MagicMock()
        # make metadata call
        md = NWIS_Metadata(response, countyCd='01001')
        # assert that site_info is implemented
        assert md.site_info
