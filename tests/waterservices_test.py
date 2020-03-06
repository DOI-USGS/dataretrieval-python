import pytest
import requests
import datetime

from pandas import DataFrame

from dataretrieval.nwis import query_waterservices, get_record, query_waterdata, what_sites


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
    request_url = 'https://waterservices.usgs.gov/nwis/dv?format=json&sites=01491000%2C01645000' \
                  '&startDT=2020-02-14&endDT=2020-02-15'
    with open('data/waterservices_dv.txt') as text:
        requests_mock.get(request_url, text=text.read())
    dv, md = get_record(sites=["01491000", "01645000"], start='2020-02-14', end='2020-02-15', service='dv')
    assert type(dv) is DataFrame
    assert dv.size == 8
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_get_iv(requests_mock):
    """Tests get_dv method correctly generates the request url and returns the result in a DataFrame"""
    request_url = 'https://waterservices.usgs.gov/nwis/iv?format=json&sites=01491000%2C01645000' \
                  '&startDT=2019-02-14&endDT=2020-02-15'
    with open('data/waterservices_iv.txt') as text:
        requests_mock.get(request_url, text=text.read())
    iv, md = get_record(sites=["01491000", "01645000"], start='2019-02-14', end='2020-02-15', service='iv')
    assert type(iv) is DataFrame
    assert iv.size == 563380
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_get_info(requests_mock):
    """
    Tests get_info method correctly generates the request url and returns the result in a DataFrame.
    Note that only sites and format are passed as query params
    """
    request_url = 'https://waterservices.usgs.gov/nwis/site?sites=01491000%2C01645000&siteOutput=Expanded&format=rdb'
    with open('data/waterservices_site.txt') as text:
        requests_mock.get(request_url, text=text.read())
    info, md = get_record(sites=["01491000", "01645000"], start='2020-02-14', end='2020-02-15', service='site')
    assert type(info) is DataFrame
    assert info.size == 24
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_get_qwdata(requests_mock):
    """Tests get_qwdata method correctly generates the request url and returns the result in a DataFrame"""
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/qwdata?agency_cd=USGS&format=rdb' \
                  '&pm_cd_compare=Greater+than&inventory_output=0&rdb_inventory_output=file&TZoutput=0' \
                  '&radio_parm_cds=all_parm_cds&rdb_qw_attributes=expanded&date_format=YYYY-MM-DD' \
                  '&rdb_compression=value&submmitted_form=brief_list&site_no=01491000%2C01645000' \
                  '&qw_sample_wide=separated_wide'
    with open('data/waterdata_qwdata.txt') as text:
        requests_mock.get(request_url, text=text.read())
    qwdata, md = get_record(sites=["01491000", "01645000"], service='qwdata')
    assert type(qwdata) is DataFrame
    assert qwdata.size == 1389300
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_get_gwlevels(requests_mock):
    """Tests get_gwlevels method correctly generates the request url and returns the result in a DataFrame."""
    request_url = 'https://waterservices.usgs.gov/nwis/gwlevels?sites=434400121275801&format=rdb'
    with open('data/waterservices_gwlevels.txt') as text:
        requests_mock.get(request_url,
                          text=text.read())

    gwlevels, md = get_record(sites=["434400121275801"], service='gwlevels')
    assert type(gwlevels) is DataFrame
    assert gwlevels.size == 13
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_get_discharge_peaks(requests_mock):
    """Tests get_discharge_peaks method correctly generates the request url and returns the result in a DataFrame"""
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/peaks?format=rdb&site_no=01594440' \
                  '&begin_date=2000-02-14&end_date=2020-02-15'
    with open('data/waterservices_peaks.txt') as text:
        requests_mock.get(request_url, text=text.read())
    info, md = get_record(sites=["01594440"], service='peaks', start='2000-02-14', end='2020-02-15')
    assert type(info) is DataFrame
    assert info.size == 240
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_get_discharge_measurements(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a
    DataFrame"""
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/measurements?format=rdb&site_no=01594440' \
                  '&begin_date=2000-02-14&end_date=2020-02-15'
    with open('data/waterdata_measurements.txt') as text:
        requests_mock.get(request_url, text=text.read())
    dm, md = get_record(sites=["01594440"], service='measurements', start='2000-02-14', end='2020-02-15')
    assert type(dm) is DataFrame
    assert dm.size == 2130
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_get_pmcodes(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a
    DataFrame"""
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/pmcodes/pmcodes?radio_pm_search=pm_search' \
                  '&pm_group=All%2B--%2Binclude%2Ball%2Bparameter%2Bgroups&pm_search=00618' \
                  '&show=parameter_group_nm&show=casrn&show=srsname&show=parameter_units&format=rdb'
    with open('data/waterdata_pmcodes.txt') as text:
        requests_mock.get(request_url,
                          text=text.read())
    pmcodes, md = get_record(service='pmcodes', parameterCd='00618')
    assert type(pmcodes) is DataFrame
    assert pmcodes.size == 5
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_get_water_use_national(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a
    DataFrame"""
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/water_use?rdb_compression=value&format=rdb&wu_year=ALL' \
                  '&wu_category=ALL&wu_county=ALL'
    with open('data/water_use_national.txt') as text:
        requests_mock.get(request_url,
                          text=text.read())
    water_use, md = get_record(service='water_use')
    assert type(water_use) is DataFrame
    assert water_use.size == 225
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_get_water_use_national(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a
    DataFrame"""
    request_url = 'https://nwis.waterdata.usgs.gov/PA/nwis/water_use?rdb_compression=value&format=rdb&wu_year=ALL' \
                  '&wu_category=ALL&wu_county=003&wu_area=county'
    with open('data/water_use_allegheny.txt') as text:
        requests_mock.get(request_url,
                          text=text.read())
    water_use, md = get_record(service='water_use', state="PA", counties="003")
    assert type(water_use) is DataFrame
    assert water_use.size == 1981
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_get_ratings(requests_mock):
    """Tests get_ratings method correctly generates the request url and returns the result in a DataFrame"""
    request_url = "https://nwis.waterdata.usgs.gov/nwisweb/get_ratings/?site_no=01594440&file_type=base"
    with open('data/waterservices_ratings.txt') as text:
        requests_mock.get(request_url, text=text.read())
    ratings, md = get_record(service='ratings', site='01594440')
    assert type(ratings) is DataFrame
    assert ratings.size == 33
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_what_sites(requests_mock):
    """Tests test_what_sites method correctly generates the request url and returns the result in a DataFrame"""
    request_url = "https://waterservices.usgs.gov/nwis/site?bBox=-83.0%2C36.5%2C-81.0%2C38.5" \
                  "&parameterCd=00010%2C00060&hasDataTypeCd=dv&format=rdb"
    with open('data/nwis_sites.txt') as text:
        requests_mock.get(request_url, text=text.read())
    sites, md = what_sites(bBox=[-83.0,36.5,-81.0,38.5],
                         parameterCd=["00010","00060"],
                         hasDataTypeCd="dv")
    assert type(sites) is DataFrame
    assert sites.size == 2472
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
