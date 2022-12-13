import numpy as np
import pandas as pd
import pytest
from dataretrieval.nwis import get_record, preformat_peaks_response
from dataretrieval.nwis import what_sites, get_iv

START_DATE = '2018-01-24'
END_DATE   = '2018-01-25'

DATETIME_COL = 'datetime'
SITENO_COL = 'site_no'


def test_measurements_service():
    """Test measurement service
    """
    start = '2018-01-24'
    end   = '2018-01-25'
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
    end   = END_DATE
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

if __name__=='__main__':
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


def test_multiple_tz_01():
    """Test based on GitHub Issue #60 - error merging different time zones."""
    sites, sites_md = what_sites(stateCd='MD')
    iv, iv_md = get_iv(sites=sites.site_no.values[:25].tolist())
    import pdb; pdb.set_trace()