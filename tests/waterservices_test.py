import pytest
import requests

from pandas import DataFrame

from dataretrieval.nwis import query_waterservices, get_record, query_waterdata

def test_query_waterservices_validation():
    """Tests the validation parameters of the query_waterservices method"""
    with pytest.raises(TypeError) as type_error:
        query_waterdata(service='pmcodes', format='rdb')
    assert 'Query must specify a major filter: site_no, stateCd, bBox' == str(type_error.value)

    with pytest.raises(TypeError) as type_error:
        query_waterdata(service=None, site_no='sites')
    assert 'Service not recognized' == str(type_error.value)

    with pytest.raises(TypeError) as type_error:
        query_waterdata(service='pmcodes', nw_longitude_va='something')
    assert 'One or more lat/long coordinates missing or invalid.' == str(type_error.value)

def test_query_waterdata_validation():
    """Tests the validation parameters of the query_waterservices method"""
    with pytest.raises(TypeError) as type_error:
        query_waterservices(service='dv', format='rdb')
    assert 'Query must specify a major filter: sites, stateCd, bBox' == str(type_error.value)

    with pytest.raises(TypeError) as type_error:
        query_waterservices(service=None, sites='sites')
    assert 'Service not recognized' == str(type_error.value)

def test_get_record_validation():
    """Tests the validation parameters of the get_record method"""
    with pytest.raises(TypeError) as type_error:
        get_record(sites=['01491000'], service='not_a_service')
    assert 'Unrecognized service: not_a_service' == str(type_error.value)

    with pytest.raises(TypeError) as type_error:
        get_record(sites=['01491000'], service='stat')
    assert 'stat service not yet implemented' == str(type_error.value)

def test_get_dv(requests_mock):
    """Tests get_dv method correctly generates the request url and returns the result in a DataFrame"""
    with open('data/waterservices_dv.txt') as text:
        requests_mock.get('https://waterservices.usgs.gov/nwis/dv?format=json&sites=01491000%2C01645000'
                          '&startDT=2020-02-14&endDT=2020-02-15',
                          text=text.read())
    dv = get_record(sites=["01491000","01645000"], start='2020-02-14', end='2020-02-15', service='dv')
    assert type(dv) is DataFrame
    assert dv.size == 8

def test_get_iv(requests_mock):
    """Tests get_dv method correctly generates the request url and returns the result in a DataFrame"""
    with open('data/waterservices_iv.txt') as text:
        requests_mock.get('https://waterservices.usgs.gov/nwis/iv?sites=01491000%2C01645000&format=json'
                          '&startDT=2019-02-14&endDT=2020-02-15',
                          text=text.read())
    iv = get_record(sites=["01491000","01645000"], start='2019-02-14', end='2020-02-15', service='iv')
    assert type(iv) is DataFrame
    assert iv.size == 563380

def test_get_info(requests_mock):
    """
    Tests get_info method correctly generates the request url and returns the result in a DataFrame.
    Note that only sites and format are passed as query params
    """
    with open('data/waterservices_site.txt') as text:
        requests_mock.get('https://waterservices.usgs.gov/nwis/site?sites=01491000%2C01645000&format=rdb',
                          text=text.read())
    info = get_record(sites=["01491000","01645000"], start='2020-02-14', end='2020-02-15', service='site')
    assert type(info) is DataFrame
    assert info.size == 24

def test_get_qwdata(requests_mock):
    """Tests get_qwdata method correctly generates the request url and returns the result in a DataFrame"""
    with open('data/waterdata_qwdata.txt') as text:
        requests_mock.get('https://nwis.waterdata.usgs.gov/nwis/qwdata?agency_cd=USGS&format=rdb'
                          '&pm_cd_compare=Greater+than&inventory_output=0&rdb_inventory_output=file&TZoutput=0'
                          '&radio_parm_cds=all_parm_cds&rdb_qw_attributes=expanded&date_format=YYYY-MM-DD'
                          '&rdb_compression=value&submmitted_form=brief_list&site_no=01491000%2C01645000'
                          '&qw_sample_wide=separated_wide',
                          text=text.read())
    qwdata = get_record(sites=["01491000","01645000"], service='qwdata')
    assert type(qwdata) is DataFrame
    assert qwdata.size == 1389300

def test_get_gwlevels(requests_mock):
    """Tests get_gwlevels method correctly generates the request url and returns the result in a DataFrame."""
    with open('data/waterservices_gwlevels.txt') as text:
        requests_mock.get('https://waterservices.usgs.gov/nwis/gwlevels?sites=434400121275801&format=rdb',
                          text=text.read())

    gwlevels = get_record(sites=["434400121275801"], service='gwlevels')
    assert type(gwlevels) is DataFrame
    assert gwlevels.size == 13

def test_get_discharge_peaks(requests_mock):
    """Tests get_discharge_peaks method correctly generates the request url and returns the result in a DataFrame"""
    with open('data/waterservices_peaks.txt') as text:
        requests_mock.get('https://nwis.waterdata.usgs.gov/nwis/peaks?format=rdb&site_no=01594440'
                          '&begin_date=2000-02-14&end_date=2020-02-15',
                          text=text.read())
    info = get_record(sites=["01594440"], service='peaks', start='2000-02-14', end='2020-02-15')
    assert type(info) is DataFrame
    assert info.size == 240

def test_get_discharge_measurements(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a DataFrame"""
    with open('data/waterdata_measurements.txt') as text:
        requests_mock.get('https://nwis.waterdata.usgs.gov/nwis/measurements?format=rdb&site_no=01594440'
                          '&begin_date=2000-02-14&end_date=2020-02-15',
                          text=text.read())
    info = get_record(sites=["01594440"], service='measurements', start='2000-02-14', end='2020-02-15')
    assert type(info) is DataFrame
    assert info.size == 2130

def test_get_pmcodes(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a DataFrame"""
    with open('data/waterdata_pmcodes.txt') as text:
        requests_mock.get('https://nwis.waterdata.usgs.gov/nwis/pmcodes/pmcodes?radio_pm_search=pm_search'
                          '&pm_group=All%2B--%2Binclude%2Ball%2Bparameter%2Bgroups&pm_search=00618'
                          '&show=parameter_group_nm&show=casrn&show=srsname&show=parameter_units&format=rdb',
                          text=text.read())
    info = get_record(sites=None, service='pmcodes', parameterCd='00618')
    assert type(info) is DataFrame
    assert info.size == 5
