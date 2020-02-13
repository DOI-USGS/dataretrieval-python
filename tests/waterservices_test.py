import pytest
import requests

from pandas import DataFrame

from dataretrieval.nwis import query_waterservices, get_dv, get_iv

def test_query_waterservices_validation():
    """Tests the validation parameters of the query_waterservices method"""
    with pytest.raises(TypeError) as type_error:
        query_waterservices(service='dv', format='rdb')
    assert 'Query must specify a major filter: sites, stateCd, bBox' == str(type_error.value)

    with pytest.raises(TypeError) as type_error:
        query_waterservices(service=None, sites='sites')
    assert 'Service not recognized' == str(type_error.value)

def test_get_dv(requests_mock):
    """Tests get_dv method correctly generates the request url and returns the result in a DataFrame"""
    params = {'sites': ["01491000","01645000"]}
    with open('data/waterservices_dv.json') as text:
        requests_mock.get('https://waterservices.usgs.gov/nwis/dv?sites=01491000%2C01645000&format=json',
                          text=text.read())
    dv = get_dv(**params)
    assert type(dv) is DataFrame

def test_get_iv(requests_mock):
    """Tests get_dv method correctly generates the request url and returns the result in a DataFrame"""
    params = {'sites': ["01491000","01645000"]}
    with open('data/waterservices_iv.json') as text:
        requests_mock.get('https://waterservices.usgs.gov/nwis/iv?sites=01491000%2C01645000&format=json',
                          text=text.read())
    iv = get_iv(**params)
    assert type(iv) is DataFrame

