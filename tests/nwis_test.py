import numpy as np
import pandas as pd
import pytest
from dataretrieval.nwis import get_record, preformat_peaks_response

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
    site = ['03339000','05447500','03346500']
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
