import pytest
import requests
import datetime

from pandas import DataFrame

from dataretrieval.nwis import query_waterservices, get_record, query_waterdata, what_sites, get_stats
from dataretrieval.utils import NoSitesError


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


def test_query_validation(requests_mock):
    request_url = "https://waterservices.usgs.gov/nwis/stat?sites=bad_site_id&format=rdb"
    requests_mock.get(request_url, status_code=400)
    with pytest.raises(ValueError) as type_error:
        get_stats(sites="bad_site_id")
    assert request_url in str(type_error)

    request_url = "https://waterservices.usgs.gov/nwis/stat?sites=123456&format=rdb"
    requests_mock.get(request_url,
                      text="No sites/data found using the selection criteria specified")
    with pytest.raises(NoSitesError) as no_sites_error:
        get_stats(sites="123456")
    assert request_url in str(no_sites_error)


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
    site = '01491000%2C01645000'
    request_url = 'https://waterservices.usgs.gov/nwis/dv?format=json&sites={}' \
                  '&startDT=2020-02-14&endDT=2020-02-15'.format(site)
    response_file_path = 'data/waterservices_dv.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_record(sites=["01491000", "01645000"], start='2020-02-14', end='2020-02-15', service='dv')
    assert type(df) is DataFrame
    assert df.size == 8
    assert_metadata(requests_mock, request_url, md, site)


def test_get_iv(requests_mock):
    """Tests get_dv method correctly generates the request url and returns the result in a DataFrame"""
    site = '01491000%2C01645000'
    request_url = 'https://waterservices.usgs.gov/nwis/iv?format=json&sites={}' \
                  '&startDT=2019-02-14&endDT=2020-02-15'.format(site)
    response_file_path = 'data/waterservices_iv.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_record(sites=["01491000", "01645000"], start='2019-02-14', end='2020-02-15', service='iv')
    assert type(df) is DataFrame
    assert df.size == 563380
    assert md.url == request_url
    assert_metadata(requests_mock, request_url, md, site)


def test_get_info(requests_mock):
    """
    Tests get_info method correctly generates the request url and returns the result in a DataFrame.
    Note that only sites and format are passed as query params
    """
    site = '01491000%2C01645000'
    request_url = 'https://waterservices.usgs.gov/nwis/site?sites={}&siteOutput=Expanded&format=rdb'.format(site)
    response_file_path = 'data/waterservices_site.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_record(sites=["01491000", "01645000"], start='2020-02-14', end='2020-02-15', service='site')
    assert type(df) is DataFrame
    assert df.size == 24
    assert md.url == request_url
    assert_metadata(requests_mock, request_url, md, site)


def test_get_qwdata(requests_mock):
    """Tests get_qwdata method correctly generates the request url and returns the result in a DataFrame"""
    site = '01491000%2C01645000'
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/qwdata?agency_cd=USGS&format=rdb' \
                  '&pm_cd_compare=Greater+than&inventory_output=0&rdb_inventory_output=file&TZoutput=0' \
                  '&radio_parm_cds=all_parm_cds&rdb_qw_attributes=expanded&date_format=YYYY-MM-DD' \
                  '&rdb_compression=value&submmitted_form=brief_list&site_no={}' \
                  '&qw_sample_wide=separated_wide'.format(site)
    response_file_path = 'data/waterdata_qwdata.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_record(sites=["01491000", "01645000"], service='qwdata')
    assert type(df) is DataFrame
    assert df.size == 1389300
    assert_metadata(requests_mock, request_url, md, site)


def test_get_gwlevels(requests_mock):
    """Tests get_gwlevels method correctly generates the request url and returns the result in a DataFrame."""
    site = '434400121275801'
    request_url = 'https://waterservices.usgs.gov/nwis/gwlevels?sites={}&format=rdb'.format(site)
    response_file_path = 'data/waterservices_gwlevels.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_record(sites=[site], service='gwlevels')
    assert type(df) is DataFrame
    assert df.size == 13
    assert_metadata(requests_mock, request_url, md, site)


def test_get_discharge_peaks(requests_mock):
    """Tests get_discharge_peaks method correctly generates the request url and returns the result in a DataFrame"""
    site = '01594440'
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/peaks?format=rdb&site_no={}' \
                  '&begin_date=2000-02-14&end_date=2020-02-15'.format(site)
    response_file_path = 'data/waterservices_peaks.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_record(sites=[site], service='peaks', start='2000-02-14', end='2020-02-15')
    assert type(df) is DataFrame
    assert df.size == 240
    assert_metadata(requests_mock, request_url, md, site)


def test_get_discharge_measurements(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a
    DataFrame"""
    site = "01594440"
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/measurements?format=rdb&site_no={}' \
                  '&begin_date=2000-02-14&end_date=2020-02-15'.format(site)
    response_file_path = 'data/waterdata_measurements.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_record(sites=[site], service='measurements', start='2000-02-14', end='2020-02-15')
    assert type(df) is DataFrame
    assert df.size == 2130
    assert_metadata(requests_mock, request_url, md, site)


def test_get_pmcodes(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a
    DataFrame"""
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/pmcodes/pmcodes?radio_pm_search=pm_search' \
                  '&pm_group=All%2B--%2Binclude%2Ball%2Bparameter%2Bgroups&pm_search=00618' \
                  '&show=parameter_group_nm&show=casrn&show=srsname&show=parameter_units&format=rdb'
    response_file_path = 'data/waterdata_pmcodes.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_record(service='pmcodes', parameterCd='00618')
    assert type(df) is DataFrame
    assert df.size == 5
    assert_metadata(requests_mock, request_url, md, None)


def test_get_water_use_national(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a
    DataFrame"""
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/water_use?rdb_compression=value&format=rdb&wu_year=ALL' \
                  '&wu_category=ALL&wu_county=ALL'
    response_file_path = 'data/water_use_national.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_record(service='water_use')
    assert type(df) is DataFrame
    assert df.size == 225
    assert_metadata(requests_mock, request_url, md, None)


def test_get_water_use_allegheny(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a
    DataFrame"""
    request_url = 'https://nwis.waterdata.usgs.gov/PA/nwis/water_use?rdb_compression=value&format=rdb&wu_year=ALL' \
                  '&wu_category=ALL&wu_county=003&wu_area=county'
    response_file_path = 'data/water_use_allegheny.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_record(service='water_use', state="PA", counties="003")
    assert type(df) is DataFrame
    assert df.size == 1981
    assert_metadata(requests_mock, request_url, md, None)


def test_get_ratings_validation():
    """Tests get_ratings method correctly generates the request url and returns the result in a DataFrame"""
    site = "01594440"
    with pytest.raises(ValueError) as value_error:
        get_record(service='ratings', site=site, file_type="BAD")
    assert 'Unrecognized file_type: BAD, must be "base", "corr" or "exsa"' in str(value_error)


def test_get_ratings(requests_mock):
    """Tests get_ratings method correctly generates the request url and returns the result in a DataFrame"""
    site = "01594440"
    request_url = "https://nwis.waterdata.usgs.gov/nwisweb/get_ratings/?site_no={}&file_type=base".format(site)
    response_file_path = 'data/waterservices_ratings.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_record(service='ratings', site=site)
    assert type(df) is DataFrame
    assert df.size == 33
    assert_metadata(requests_mock, request_url, md, site)


def test_what_sites(requests_mock):
    """Tests what_sites method correctly generates the request url and returns the result in a DataFrame"""
    request_url = "https://waterservices.usgs.gov/nwis/site?bBox=-83.0%2C36.5%2C-81.0%2C38.5" \
                  "&parameterCd=00010%2C00060&hasDataTypeCd=dv&format=rdb"
    response_file_path = 'data/nwis_sites.txt'
    mock_request(requests_mock, request_url, response_file_path)

    df, md = what_sites(bBox=[-83.0,36.5,-81.0,38.5],
                         parameterCd=["00010","00060"],
                         hasDataTypeCd="dv")
    assert type(df) is DataFrame
    assert df.size == 2472
    assert_metadata(requests_mock, request_url, md, None)


def test_get_stats(requests_mock):
    """Tests get_stats method correctly generates the request url and returns the result in a DataFrame"""
    request_url = "https://waterservices.usgs.gov/nwis/stat?sites=01491000%2C01645000&format=rdb"
    response_file_path = 'data/waterservices_stats.txt'
    mock_request(requests_mock, request_url, response_file_path)

    df, md = get_stats(sites=["01491000", "01645000"])
    assert type(df) is DataFrame
    assert df.size == 51936
    assert_metadata(requests_mock, request_url, md, None)


def mock_request(requests_mock, request_url, file_path):
    with open(file_path) as text:
        requests_mock.get(request_url, text=text.read(), headers={"mock_header": "value"})


def assert_metadata(requests_mock, request_url, md, site):
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    if site is None:
        assert md.site_info is None
    else:
        site_request_url = "https://waterservices.usgs.gov/nwis/site?sites={}&format=rdb".format(site)
        with open('data/waterservices_site.txt') as text:
            requests_mock.get(site_request_url, text=text.read())
        site_info, _ = md.site_info()
        assert type(site_info) is DataFrame