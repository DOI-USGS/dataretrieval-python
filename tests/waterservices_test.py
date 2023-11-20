import datetime

import pytest
from pandas import DataFrame

from dataretrieval.nwis import (
    get_discharge_measurements,
    get_discharge_peaks,
    get_dv,
    get_gwlevels,
    get_info,
    get_iv,
    get_pmcodes,
    get_qwdata,
    get_ratings,
    get_record,
    get_stats,
    get_water_use,
    query_waterdata,
    query_waterservices,
    what_sites
)
from dataretrieval.utils import NoSitesError


def test_query_waterdata_validation():
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


def test_query_waterservices_validation():
    """Tests the validation parameters of the query_waterservices method"""
    with pytest.raises(TypeError) as type_error:
        query_waterservices(service='dv', format='rdb')
    assert 'Query must specify a major filter: sites, stateCd, bBox, huc, or countyCd' == str(type_error.value)

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


def test_get_dv(requests_mock):
    """Tests get_dv method correctly generates the request url and returns the result in a DataFrame"""
    format = "json"
    site = '01491000%2C01645000'
    request_url = 'https://waterservices.usgs.gov/nwis/dv?format={}' \
                  '&startDT=2020-02-14&endDT=2020-02-15&sites={}'.format(format, site)
    response_file_path = 'data/waterservices_dv.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_dv(sites=["01491000", "01645000"], start='2020-02-14', end='2020-02-15')
    assert type(df) is DataFrame
    assert df.size == 8
    assert_metadata(requests_mock, request_url, md, site, None, format)


@pytest.mark.parametrize("site_input_type_list", [True, False])
def test_get_dv_site_value_types(requests_mock, site_input_type_list):
    """Tests get_dv method for valid input types for the 'sites' parameter"""
    _format = "json"
    site = '01491000'
    request_url = 'https://waterservices.usgs.gov/nwis/dv?format={}' \
                  '&startDT=2020-02-14&endDT=2020-02-15&sites={}'.format(_format, site)
    response_file_path = 'data/waterservices_dv.txt'
    mock_request(requests_mock, request_url, response_file_path)
    if site_input_type_list:
        sites = [site]
    else:
        sites = site
    df, md = get_dv(sites=sites, start='2020-02-14', end='2020-02-15')
    assert type(df) is DataFrame
    assert df.size == 8


def test_get_iv(requests_mock):
    """Tests get_iv method correctly generates the request url and returns the result in a DataFrame"""
    format = "json"
    site = '01491000%2C01645000'
    request_url = 'https://waterservices.usgs.gov/nwis/iv?format={}' \
                  '&startDT=2019-02-14&endDT=2020-02-15&sites={}'.format(format, site)
    response_file_path = 'data/waterservices_iv.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_iv(sites=["01491000", "01645000"], start='2019-02-14', end='2020-02-15')
    assert type(df) is DataFrame
    assert df.size == 563380
    assert md.url == request_url
    assert_metadata(requests_mock, request_url, md, site, None, format)


@pytest.mark.parametrize("site_input_type_list", [True, False])
def test_get_iv_site_value_types(requests_mock, site_input_type_list):
    """Tests get_iv method for valid input type for the 'sites' parameter"""
    _format = "json"
    site = '01491000'
    request_url = 'https://waterservices.usgs.gov/nwis/iv?format={}' \
                  '&startDT=2019-02-14&endDT=2020-02-15&sites={}'.format(_format, site)
    response_file_path = 'data/waterservices_iv.txt'
    mock_request(requests_mock, request_url, response_file_path)
    if site_input_type_list:
        sites = [site]
    else:
        sites = site
    df, md = get_iv(sites=sites, start='2019-02-14', end='2020-02-15')
    assert type(df) is DataFrame
    assert df.size == 563380
    assert md.url == request_url


def test_get_info(requests_mock):
    """
    Tests get_info method correctly generates the request url and returns the result in a DataFrame.
    Note that only sites and format are passed as query params
    """
    format = "rdb"
    site = '01491000%2C01645000'
    parameter_cd = "00618"
    request_url = 'https://waterservices.usgs.gov/nwis/site?sites={}&parameterCd={}&siteOutput=Expanded&format={}'.format(site, parameter_cd, format)
    response_file_path = 'data/waterservices_site.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_info(sites=["01491000", "01645000"], parameterCd="00618")
    assert type(df) is DataFrame
    assert df.size == 24
    assert md.url == request_url
    assert_metadata(requests_mock, request_url, md, site, [parameter_cd], format)


def test_get_qwdata(requests_mock):
    """Tests get_qwdata method correctly generates the request url and returns the result in a DataFrame"""
    format = "rdb"
    site = '01491000%2C01645000'
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/qwdata?site_no={}' \
                  '&qw_sample_wide=qw_sample_wide&agency_cd=USGS&format={}&pm_cd_compare=Greater+than' \
                  '&inventory_output=0&rdb_inventory_output=file&TZoutput=0&rdb_qw_attributes=expanded' \
                  '&date_format=YYYY-MM-DD&rdb_compression=value&submitted_form=brief_list'.format(site, format)
    response_file_path = 'data/waterdata_qwdata.txt'
    mock_request(requests_mock, request_url, response_file_path)
    with pytest.warns(DeprecationWarning):
        df, md = get_qwdata(sites=["01491000", "01645000"])
    assert type(df) is DataFrame
    assert df.size == 1821472
    assert_metadata(requests_mock, request_url, md, site, None, format)


@pytest.mark.parametrize("site_input_type_list", [True, False])
def test_get_qwdata_site_value_types(requests_mock, site_input_type_list):
    """Tests get_qwdata method for valid input types for the 'sites' parameter"""
    _format = "rdb"
    site = '01491000'
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/qwdata?site_no={}' \
                  '&qw_sample_wide=qw_sample_wide&agency_cd=USGS&format={}&pm_cd_compare=Greater+than' \
                  '&inventory_output=0&rdb_inventory_output=file&TZoutput=0&rdb_qw_attributes=expanded' \
                  '&date_format=YYYY-MM-DD&rdb_compression=value&submitted_form=brief_list'.format(site, _format)
    response_file_path = 'data/waterdata_qwdata.txt'
    mock_request(requests_mock, request_url, response_file_path)
    if site_input_type_list:
        sites = [site]
    else:
        sites = site
    with pytest.warns(DeprecationWarning):
        df, md = get_qwdata(sites=sites)
    assert type(df) is DataFrame
    assert df.size == 1821472


def test_get_gwlevels(requests_mock):
    """Tests get_gwlevels method correctly generates the request url and returns the result in a DataFrame."""
    format = "rdb"
    site = '434400121275801'
    request_url = 'https://waterservices.usgs.gov/nwis/gwlevels?startDT=1851-01-01' \
                  '&sites={}&format={}'.format(site, format)
    response_file_path = 'data/waterservices_gwlevels.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_gwlevels(sites=[site])
    assert type(df) is DataFrame
    assert df.size == 16
    assert_metadata(requests_mock, request_url, md, site, None, format)


@pytest.mark.parametrize("site_input_type_list", [True, False])
def test_get_gwlevels_site_value_types(requests_mock, site_input_type_list):
    """Tests get_gwlevels method for valid input types for the 'sites' parameter."""
    _format = "rdb"
    site = '434400121275801'
    request_url = 'https://waterservices.usgs.gov/nwis/gwlevels?startDT=1851-01-01' \
                  '&sites={}&format={}'.format(site, _format)
    response_file_path = 'data/waterservices_gwlevels.txt'
    mock_request(requests_mock, request_url, response_file_path)
    if site_input_type_list:
        sites = [site]
    else:
        sites = site
    df, md = get_gwlevels(sites=sites)
    assert type(df) is DataFrame
    assert df.size == 16


def test_get_discharge_peaks(requests_mock):
    """Tests get_discharge_peaks method correctly generates the request url and returns the result in a DataFrame"""
    format = "rdb"
    site = '01594440'
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/peaks?format={}&site_no={}' \
                  '&begin_date=2000-02-14&end_date=2020-02-15'.format(format, site)
    response_file_path = 'data/waterservices_peaks.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_discharge_peaks(sites=[site], start='2000-02-14', end='2020-02-15')
    assert type(df) is DataFrame
    assert df.size == 240
    assert_metadata(requests_mock, request_url, md, site, None, format)


@pytest.mark.parametrize("site_input_type_list", [True, False])
def test_get_discharge_peaks_sites_value_types(requests_mock, site_input_type_list):
    """Tests get_discharge_peaks for valid input types of the 'sites' parameter"""

    _format = "rdb"
    site = '01594440'
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/peaks?format={}&site_no={}' \
                  '&begin_date=2000-02-14&end_date=2020-02-15'.format(_format, site)
    response_file_path = 'data/waterservices_peaks.txt'
    mock_request(requests_mock, request_url, response_file_path)
    if site_input_type_list:
        sites = [site]
    else:
        sites = site

    df, md = get_discharge_peaks(sites=sites, start='2000-02-14', end='2020-02-15')
    assert type(df) is DataFrame
    assert df.size == 240


def test_get_discharge_measurements(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a
    DataFrame"""
    format = "rdb"
    site = "01594440"
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/measurements?format={}&site_no={}' \
                  '&begin_date=2000-02-14&end_date=2020-02-15'.format(format, site)
    response_file_path = 'data/waterdata_measurements.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_discharge_measurements(sites=[site], start='2000-02-14', end='2020-02-15')
    assert type(df) is DataFrame
    assert df.size == 2130
    assert_metadata(requests_mock, request_url, md, site, None, format)


@pytest.mark.parametrize("site_input_type_list", [True, False])
def test_get_discharge_measurements_sites_value_types(requests_mock, site_input_type_list):
    """Tests get_discharge_measurements method for valid input types for 'sites' parameter"""
    _format = "rdb"
    site = "01594440"
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/measurements?format={}&site_no={}' \
                  '&begin_date=2000-02-14&end_date=2020-02-15'.format(_format, site)
    response_file_path = 'data/waterdata_measurements.txt'
    mock_request(requests_mock, request_url, response_file_path)
    if site_input_type_list:
        sites = [site]
    else:
        sites = site
    df, md = get_discharge_measurements(sites=sites, start='2000-02-14', end='2020-02-15')
    assert type(df) is DataFrame
    assert df.size == 2130


def test_get_pmcodes(requests_mock):
    """Tests get_pmcodes method correctly generates the request url and returns the result in a
    DataFrame"""
    format = "rdb"
    request_url = "https://help.waterdata.usgs.gov/code/parameter_cd_nm_query?fmt=rdb&parm_nm_cd=%2500618%25"
    response_file_path = 'data/waterdata_pmcodes.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_pmcodes(parameterCd='00618')
    assert type(df) is DataFrame
    assert df.size == 13
    assert_metadata(requests_mock, request_url, md, None, None, format)


@pytest.mark.parametrize("parameterCd_input_type_list", [True, False])
def test_get_pmcodes_parameterCd_value_types(requests_mock, parameterCd_input_type_list):
    """Tests get_pmcodes method for valid input types for the 'parameterCd' parameter"""
    _format = "rdb"
    parameterCd = '00618'
    request_url = "https://help.waterdata.usgs.gov/code/parameter_cd_nm_query?fmt={}&parm_nm_cd=%25{}%25"
    request_url = request_url.format(_format, parameterCd)
    response_file_path = 'data/waterdata_pmcodes.txt'
    mock_request(requests_mock, request_url, response_file_path)
    if parameterCd_input_type_list:
        parameterCd = [parameterCd]
    else:
        parameterCd = parameterCd
    df, md = get_pmcodes(parameterCd=parameterCd)
    assert type(df) is DataFrame
    assert df.size == 13


def test_get_water_use_national(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a
    DataFrame"""
    format = "rdb"
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/water_use?rdb_compression=value&format={}&wu_year=ALL' \
                  '&wu_category=ALL&wu_county=ALL'.format(format)
    response_file_path = 'data/water_use_national.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_water_use()
    assert type(df) is DataFrame
    assert df.size == 225
    assert_metadata(requests_mock, request_url, md, None, None, format)


@pytest.mark.parametrize("year_input_type_list", [True, False])
def test_get_water_use_national_year_value_types(requests_mock, year_input_type_list):
    """Tests get_water_use method for valid input types for the 'years' parameter"""
    _format = "rdb"
    year = "ALL"
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/water_use?rdb_compression=value&format={}&wu_year=ALL' \
                  '&wu_category=ALL&wu_county=ALL'.format(_format)
    response_file_path = 'data/water_use_national.txt'
    mock_request(requests_mock, request_url, response_file_path)
    if year_input_type_list:
        years = [year]
    else:
        years = year
    df, md = get_water_use(years=years)
    assert type(df) is DataFrame
    assert df.size == 225


@pytest.mark.parametrize("county_input_type_list", [True, False])
def test_get_water_use_national_county_value_types(requests_mock, county_input_type_list):
    """Tests get_water_use method for valid input types for the 'counties' parameter"""
    _format = "rdb"
    county = "ALL"
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/water_use?rdb_compression=value&format={}&wu_year=ALL' \
                  '&wu_category=ALL&wu_county=ALL'.format(_format)
    response_file_path = 'data/water_use_national.txt'
    mock_request(requests_mock, request_url, response_file_path)
    if county_input_type_list:
        counties = [county]
    else:
        counties = county
    df, md = get_water_use(counties=counties)
    assert type(df) is DataFrame
    assert df.size == 225


@pytest.mark.parametrize("category_input_type_list", [True, False])
def test_get_water_use_national_county_value_types(requests_mock, category_input_type_list):
    """Tests get_water_use method for valid input types for the 'categories' parameter"""
    _format = "rdb"
    category = "ALL"
    request_url = 'https://nwis.waterdata.usgs.gov/nwis/water_use?rdb_compression=value&format={}&wu_year=ALL' \
                  '&wu_category=ALL&wu_county=ALL'.format(_format)
    response_file_path = 'data/water_use_national.txt'
    mock_request(requests_mock, request_url, response_file_path)
    if category_input_type_list:
        categories = [category]
    else:
        categories = category
    df, md = get_water_use(categories=categories)
    assert type(df) is DataFrame
    assert df.size == 225


def test_get_water_use_allegheny(requests_mock):
    """Tests get_discharge_measurements method correctly generates the request url and returns the result in a
    DataFrame"""
    format = "rdb"
    request_url = 'https://nwis.waterdata.usgs.gov/PA/nwis/water_use?rdb_compression=value&format=rdb&wu_year=ALL' \
                  '&wu_category=ALL&wu_county=003&wu_area=county'
    response_file_path = 'data/water_use_allegheny.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_water_use(state="PA", counties="003")
    assert type(df) is DataFrame
    assert df.size == 1981
    assert_metadata(requests_mock, request_url, md, None, None, format)


def test_get_ratings_validation():
    """Tests get_ratings method correctly generates the request url and returns the result in a DataFrame"""
    site = "01594440"
    with pytest.raises(ValueError) as value_error:
        get_ratings(site=site, file_type="BAD")
    assert 'Unrecognized file_type: BAD, must be "base", "corr" or "exsa"' in str(value_error)


def test_get_ratings(requests_mock):
    """Tests get_ratings method correctly generates the request url and returns the result in a DataFrame"""
    format = "rdb"
    site = "01594440"
    request_url = "https://nwis.waterdata.usgs.gov/nwisweb/get_ratings/?site_no={}&file_type=base".format(site)
    response_file_path = 'data/waterservices_ratings.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_ratings(site_no=site)
    assert type(df) is DataFrame
    assert df.size == 33
    assert_metadata(requests_mock, request_url, md, site, None, format)


def test_what_sites(requests_mock):
    """Tests what_sites method correctly generates the request url and returns the result in a DataFrame"""
    format = "rdb"
    parameter_cd = '00010%2C00060'
    parameter_cd_list = ["00010","00060"]
    request_url = "https://waterservices.usgs.gov/nwis/site?bBox=-83.0%2C36.5%2C-81.0%2C38.5" \
                  "&parameterCd={}&hasDataTypeCd=dv&format={}".format(parameter_cd, format)
    response_file_path = 'data/nwis_sites.txt'
    mock_request(requests_mock, request_url, response_file_path)

    df, md = what_sites(bBox=[-83.0,36.5,-81.0,38.5], parameterCd=parameter_cd_list, hasDataTypeCd="dv")
    assert type(df) is DataFrame
    assert df.size == 2472
    assert_metadata(requests_mock, request_url, md, None, parameter_cd_list, format)


def test_get_stats(requests_mock):
    """Tests get_stats method correctly generates the request url and returns the result in a DataFrame"""
    format = "rdb"
    request_url = "https://waterservices.usgs.gov/nwis/stat?sites=01491000%2C01645000&format={}".format(format)
    response_file_path = 'data/waterservices_stats.txt'
    mock_request(requests_mock, request_url, response_file_path)

    df, md = get_stats(sites=["01491000", "01645000"])
    assert type(df) is DataFrame
    assert df.size == 51936
    assert_metadata(requests_mock, request_url, md, None, None, format)


@pytest.mark.parametrize("site_input_type_list", [True, False])
def test_get_stats_site_value_types(requests_mock, site_input_type_list):
    """Tests get_stats method for valid input types for the 'sites' parameter"""
    _format = "rdb"
    site = '01491000'
    request_url = "https://waterservices.usgs.gov/nwis/stat?sites={}&format={}".format(site, _format)
    response_file_path = 'data/waterservices_stats.txt'
    mock_request(requests_mock, request_url, response_file_path)
    if site_input_type_list:
        sites = [site]
    else:
        sites = site
    df, md = get_stats(sites=sites)
    assert type(df) is DataFrame
    assert df.size == 51936


def mock_request(requests_mock, request_url, file_path):
    with open(file_path) as text:
        requests_mock.get(request_url, text=text.read(), headers={"mock_header": "value"})


def assert_metadata(requests_mock, request_url, md, site, parameter_cd, format):
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    if site is not None:
        site_request_url = "https://waterservices.usgs.gov/nwis/site?sites={}&format=rdb".format(site)
        with open('data/waterservices_site.txt') as text:
            requests_mock.get(site_request_url, text=text.read())
        site_info, _ = md.site_info
        assert type(site_info) is DataFrame
    if parameter_cd is None:
        assert md.variable_info is None
    else:
        for param in parameter_cd:
            pcode_request_url = "https://help.waterdata.usgs.gov/code/parameter_cd_nm_query?fmt=rdb&parm_nm_cd=%25{}%25".format(param)
            with open('data/waterdata_pmcodes.txt') as text:
                requests_mock.get(pcode_request_url, text=text.read())
        variable_info, _ = md.variable_info
        assert type(variable_info) is DataFrame

    if format == "rdb":
        assert md.comment is not None
    else:
        assert md.comment is None
