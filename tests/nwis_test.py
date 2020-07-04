import pytest

from dataretrieval.exceptions import EmptyQueryResultError
from dataretrieval.nwis import get_record, read_rdb

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


def test_read_rdb_raises_error():
    with pytest.raises(EmptyQueryResultError):
        read_rdb("Error report")


if __name__=='__main__':
     test_measurements_service_answer()
     test_iv_service_answer()
