# -*- coding: utf-8 -*-
"""Functions for downloading data from NWIS

Todo:
    * Create a test to check whether functions pull multipe sites
    * Work on multi-index capabilities.
    * Check that all timezones are handled properly for each service.
"""

import pandas as pd
from io import StringIO

from dataretrieval.utils import to_str, format_datetime, update_merge, set_metadata as set_md
from .utils import query

WATERDATA_BASE_URL = 'https://nwis.waterdata.usgs.gov/'
WATERDATA_URL = WATERDATA_BASE_URL + 'nwis/'
WATERSERVICE_URL = 'https://waterservices.usgs.gov/nwis/'

WATERSERVICES_SERVICES = ['dv', 'iv', 'site', 'stat', 'gwlevels']
WATERDATA_SERVICES = ['qwdata', 'measurements', 'peaks', 'pmcodes', 'water_use', 'ratings']


def format_response(df, service=None):
    """Setup index for response from query.
    """
    if service == 'peaks':
        df = preformat_peaks_response(df)

    # check for multiple sites:
    if 'datetime' not in df.columns:
        # XXX: consider making site_no index
        return df

    elif len(df['site_no'].unique()) > 1:
        # setup multi-index
        df.set_index(['site_no', 'datetime'], inplace=True)
        if hasattr(df.index.levels[1], 'tzinfo') and df.index.levels[1].tzinfo is None:
            df = df.tz_localize('UTC', level=1)

    else:
        df.set_index(['datetime'], inplace=True)
        if hasattr(df.index, 'tzinfo') and df.index.tzinfo is None:
            df = df.tz_localize('UTC')

    return df.sort_index()


def preformat_peaks_response(df):
    df['datetime'] = pd.to_datetime(df.pop('peak_dt'), errors='coerce')
    df.dropna(subset=['datetime'])
    return df


def get_qwdata(datetime_index=True, wide_format=True, sites=None, start=None, end=None, **kwargs):
    """
    Get water sample data from qwdata service.

    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
    datetime_index : boolean
        If True, create a datetime index
    wide_format : boolean
        If True, return data in wide format with multiple samples per row and one row per time.
    sites: array of strings
        If the qwdata parameter site_no is supplied, it will overwrite the sites parameter
    start: string
        If the qwdata parameter begin_date is supplied, it will overwrite the start parameter
    end: string
        If the qwdata parameter end_date is supplied, it will overwrite the end parameter

    Returns:
        DataFrame containing times series data from the NWIS json and Metadata as tuple
    """
    if wide_format:
        kwargs['qw_sample_wide'] = 'qw_sample_wide'
    start = kwargs.pop('begin_date', start)
    end = kwargs.pop('end_date', end)
    sites = kwargs.pop('site_no', sites)
    return _qwdata(site_no=sites, begin_date=start, end_date=end, datetime_index=datetime_index,
                   ** kwargs)

def _qwdata(datetime_index=True, **kwargs):
    # check number of sites, may need to create multiindex

    payload = {'agency_cd': 'USGS',
               'format': 'rdb',
               'pm_cd_compare': 'Greater than',
               'inventory_output': '0',
               'rdb_inventory_output': 'file',
               'TZoutput': '0',
               'rdb_qw_attributes': 'expanded',
               'date_format': 'YYYY-MM-DD',
               'rdb_compression': 'value',
               'submmitted_form': 'brief_list'}
    # 'qw_sample_wide': 'separated_wide'}

    # check for parameter codes, and reformat query args
    qwdata_parameter_code_field = 'parameterCd'
    if kwargs.get(qwdata_parameter_code_field):
        parameter_codes = kwargs.pop(qwdata_parameter_code_field)
        parameter_codes = to_str(parameter_codes)
        kwargs['multiple_parameter_cds'] = parameter_codes
        kwargs['param_cd_operator'] = 'OR'

        search_criteria = kwargs.get('list_of_search_criteria')
        if search_criteria:
            kwargs['list_of_search_criteria'] = '{},{}'.format(search_criteria, 'multiple_parameter_cds')
        else:
            kwargs['list_of_search_criteria'] = 'multiple_parameter_cds'
        #search_criteria = kwargs.get('list_of_search_criteria

    #kwargs = {**payload, **kwargs}
    kwargs.update(payload)

    response = query_waterdata('qwdata', **kwargs)

    df = _read_rdb(response.text)

    if datetime_index == True:
        df = format_datetime(df, 'sample_dt', 'sample_tm',
                             'sample_start_time_datum_cd')

    df = format_response(df)
    return df, _set_metadata(response, **kwargs)


def get_discharge_measurements(sites=None, start=None, end=None, **kwargs):
    """
    Get discharge measurements from the waterdata service.

    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
    sites: array of strings
        If the qwdata parameter site_no is supplied, it will overwrite the sites parameter
    start: string
        If the qwdata parameter begin_date is supplied, it will overwrite the start parameter
    end: string
        If the qwdata parameter end_date is supplied, it will overwrite the end parameter

    Returns:
        DataFrame containing times series data from the NWIS json and Metadata as tuple
    """
    start = kwargs.pop('begin_date', start)
    end = kwargs.pop('end_date', end)
    sites = kwargs.pop('site_no', sites)
    return _discharge_measurements(site_no=sites, begin_date=start, end_date=end, **kwargs)


def _discharge_measurements(**kwargs):
    response = query_waterdata('measurements', format='rdb', **kwargs)
    return _read_rdb(response.text), _set_metadata(response, **kwargs)


def get_discharge_peaks(sites=None, start=None, end=None, **kwargs):
    """
    Get discharge peaks from the waterdata service.

    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
    sites: array of strings
        If the waterdata parameter site_no is supplied, it will overwrite the sites parameter
    start: string
        If the waterdata parameter begin_date is supplied, it will overwrite the start parameter
    end: string
        If the waterdata parameter end_date is supplied, it will overwrite the end parameter

    Returns:
        DataFrame containing times series data from the NWIS json and Metadata as tuple
    """
    start = kwargs.pop('begin_date', start)
    end = kwargs.pop('end_date', end)
    sites = kwargs.pop('site_no', sites)
    return _discharge_peaks(site_no=sites, begin_date=start, end_date=end, **kwargs)


def _discharge_peaks(**kwargs):
    response = query_waterdata('peaks', format='rdb', **kwargs)

    df = _read_rdb(response.text)

    return format_response(df, service='peaks'), _set_metadata(response, **kwargs)


def get_gwlevels(start='1851-01-01', end=None, **kwargs):
    """
    Querys the groundwater level service from waterservices

    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
    start: string
        If the waterdata parameter begin_date is supplied, it will overwrite the start
        parameter (defaults to '1851-01-01')
    end: string
        If the waterdata parameter end_date is supplied, it will overwrite the end parameter

    Returns:
        DataFrame containing times series data from the NWIS json and Metadata as tuple
    """
    start = kwargs.pop('startDT', start)
    end = kwargs.pop('endDT', end)
    return _gwlevels(startDT=start, endDT=end, **kwargs)


def _gwlevels(**kwargs):
    response = query_waterservices('gwlevels', **kwargs)

    df = _read_rdb(response.text)
    df = format_datetime(df, 'lev_dt', 'lev_tm', 'lev_tz_cd')

    return format_response(df), _set_metadata(response, **kwargs)


def get_stats(sites, **kwargs):
    """
    Querys waterservices statistics information

    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
    Must specify
        sites (string or list): USGS site number
        statReportType (string): daily (default), monthly, or annual
        statTypeCd (string): all, mean, max, min, median

    Returns:
        Dataframe

    TODO: fix date parsing
    """
    response = query_waterservices('stat', sites=sites, **kwargs)

    return _read_rdb(response.text), _set_metadata(response, **kwargs)


def query_waterdata(service, **kwargs):
    """
    Querys waterdata.
    """
    major_params = ['site_no', 'state_cd']
    bbox_params = ['nw_longitude_va', 'nw_latitude_va',
                   'se_longitude_va', 'se_latitude_va']

    if not any(key in kwargs for key in major_params + bbox_params):
        raise TypeError('Query must specify a major filter: site_no, stateCd, bBox')

    elif any(key in kwargs for key in bbox_params) \
            and not all(key in kwargs for key in bbox_params):
        raise TypeError('One or more lat/long coordinates missing or invalid.')

    if service not in WATERDATA_SERVICES:
        raise TypeError('Service not recognized')

    url = WATERDATA_URL + service

    return query(url, payload=kwargs)


def query_waterservices(service, **kwargs):
    """
    Querys waterservices.usgs.gov

    For more documentation see

    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
        service: string
            'site','stats',etc
        bBox: huc string
            7-digit Hydrologic Unit Code

        startDT: string
            start date (2017-12-31)
        endDT: string
            end date
        modifiedSince: string

    Returns:
        request

    Usage: must specify one major filter: sites, stateCd, bBox,
    """
    if not any(key in kwargs for key in ['sites', 'stateCd', 'bBox', 'huc']):
        raise TypeError('Query must specify a major filter: sites, stateCd, bBox, or huc')

    if service not in WATERSERVICES_SERVICES:
        raise TypeError('Service not recognized')

    if 'format' not in kwargs:
        kwargs['format'] = 'rdb'

    url = WATERSERVICE_URL + service

    return query(url, payload=kwargs)


def get_dv(start=None, end=None, **kwargs):
    """
    Get daily values data from NWIS and return it as a DataFrame.

    Note: If no start or end date are provided, only the most recent record is returned.
    
    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
    start: string
        If the waterdata parameter startDT is supplied, it will overwrite the start parameter
    end: string
        If the waterdata parameter endDT is supplied, it will overwrite the end parameter

    Returns:
        DataFrame containing times series data from the NWIS json and Metadata as tuple
    """
    start = kwargs.pop('startDT', start)
    end = kwargs.pop('endDT', end)
    return _dv(startDT=start, endDT=end, **kwargs)


def _dv(**kwargs):
    response = query_waterservices('dv', format='json', **kwargs)
    df = _read_json(response.json())

    df = format_response(df)
    return df, _set_metadata(response, **kwargs)


def get_info(**kwargs):
    """
    Get site description information from NWIS.

    Note: Must specify one major parameter.

    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
    sites : string or list
        A list of site numters. Sites may be prefixed with an optional agency
        code followed by a colon.

    stateCd : string
        U.S. postal service (2-digit) state code. Only 1 state can be specified
        per request.

    huc : string or list
        A list of hydrologic unit codes (HUC) or aggregated watersheds. Only 1
        major HUC can be specified per request, or up to 10 minor HUCs. A major
        HUC has two digits.

    bBox : list
        A contiguous range of decimal latitude and longitude, starting with the
        west longitude, then the south latitude, then the east longitude, and
        then the north latitude with each value separated by a comma. The
        product of the range of latitude range and longitude cannot exceed 25
        degrees. Whole or decimal degrees must be specified, up to six digits
        of precision. Minutes and seconds are not allowed.

    countyCd : string or list
        A list of county numbers, in a 5 digit numeric format. The first two
        digits of a county's code are the FIPS State Code.
        (url: https://help.waterdata.usgs.gov/code/county_query?fmt=html)

    Minor Parameters
    ----------------
    startDt : string
        Selects sites based on whether data was collected at a point in time
        beginning after startDt (start date). Dates must be in ISO-8601
        Calendar Date format (for example: 1990-01-01).

    endDt : string

    period : string
        Selects sites based on whether or not they were active between now
        and a time in the past. For example, period=P10W will select sites
        active in the last ten weeks.

    modifiedSince : string
        Returns only sites where site attributes or period of record data have
        changed during the request period.

    parameterCd : string or list
        Returns only site data for those sites containing the requested USGS
        parameter codes.

    siteType : string or list
        Restricts sites to those having one or more major and/or minor site
        types, such as stream, spring or well. For a list of all valid site
        types see https://help.waterdata.usgs.gov/site_tp_cd
        For example, siteType='ST' returns streams only.

    Formatting Parameters
    ---------------------
    siteOutput : string ('basic' or 'expanded')
        Indicates the richness of metadata you want for site attributes. Note
        that for visually oriented formats like Google Map format, this
        argument has no meaning. Note: for performance reasons,
        siteOutput=expanded cannot be used if seriesCatalogOutput=true or with
        any values for outputDataTypeCd.

    seriesCatalogOutput : boolean
        A switch that provides detailed period of record information for
        certain output formats. The period of record indicates date ranges for
        a certain kind of information about a site, for example the start and
        end dates for a site's daily mean streamflow.

    For additional parameter options see
    https://waterservices.usgs.gov/rest/Site-Service.html#stateCd
    """

    kwargs['siteOutput'] = 'Expanded'

    response = query_waterservices('site', **kwargs)

    return _read_rdb(response.text), _set_metadata(response, **kwargs)


def get_iv(start=None, end=None, **kwargs):
    """Get instantaneous values data from NWIS and return it as a DataFrame.

    Note: If no start or end date are provided, only the most recent record is returned.
    
    Parameters
    ----------
    start: string
        If the waterdata parameter startDT is supplied, it will overwrite the start parameter
    end: string
        If the waterdata parameter endDT is supplied, it will overwrite the end parameter

    Returns:
        DataFrame containing times series data from the NWIS json and Metadata as tuple
    """
    start = kwargs.pop('startDT', start)
    end = kwargs.pop('endDT', end)
    return _iv(startDT=start, endDT=end, **kwargs)


def _iv(**kwargs):
    response = query_waterservices('iv', format='json', **kwargs)
    return _read_json(response.json()), _set_metadata(response, **kwargs)


def get_pmcodes(parameterCd='All', **kwargs):
    """
    Return a DataFrame containing all NWIS parameter codes.

    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
        parameterCd: string or listlike
    Returns:
        DataFrame containing the USGS parameter codes and Metadata as tuple
    """
    payload = {'radio_pm_search' : 'pm_search',
               'pm_group' : 'All+--+include+all+parameter+groups',
               'pm_search' : parameterCd,
               'casrn_search' : None,
               'srsname_search' : None,
               'show' :  ['parameter_group_nm', 'casrn', 'srsname','parameter_units', 'parameter_nm'],
               'format' : 'rdb'}
    
    payload.update(kwargs)
    url = WATERDATA_URL + 'pmcodes/pmcodes'
    response = query(url, payload)
    return _read_rdb(response.text), _set_metadata(response, **kwargs)


def get_water_use(years="ALL", state=None, counties="ALL", categories="ALL"):
    """
    Water use data retrieval from USGS (NWIS).

    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
        years: Listlike
            List or comma delimited string of years.  Must be years ending in 0 or 5, or "ALL",
            which retrieves all available years
        state: string
            full name, abbreviation or id
        county: string
            County IDs from county lookup or "ALL"
        categories: Listlike
            List or comma delimited string of Two-letter category abbreviations

    Return:
        DataFrame containing requested data and Metadata as tuple
    """
    payload = {'rdb_compression' : 'value',
               'format' : 'rdb',
               'wu_year' : years,
               'wu_category' : categories,
               'wu_county' : counties}
    url = WATERDATA_URL + 'water_use'
    if state is not None:
        url = WATERDATA_BASE_URL + state + "/nwis/water_use"
        payload.update({"wu_area" : "county"})
    response = query(url, payload)
    return _read_rdb(response.text), _set_metadata(response)


def get_ratings(site=None, file_type="base", **kwargs):
    """
    Rating table for an active USGS streamgage retrieval
    Reads current rating table for an active USGS streamgage from NWISweb.
    Data is retrieved from https://waterdata.usgs.gov/nwis.

    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
        site: string
            USGS site number.  This is usually an 8 digit number as a string.
            If the nwis parameter site_no is supplied, it will overwrite the site parameter
        base: string
            can be "base", "corr", or "exsa"
        county: string
            County IDs from county lookup or "ALL"
        categories: Listlike
            List or comma delimited string of Two-letter category abbreviations

    Return:
        DataFrame containing requested data and Metadata as tuple
    """
    site = kwargs.pop('site_no', site)
    return _ratings(site=site, file_type=file_type, **kwargs)


def _ratings(site, file_type):
    payload = {}
    url = WATERDATA_BASE_URL + 'nwisweb/get_ratings/'
    if site is not None:
        payload.update({"site_no": site})
    if file_type is not None:
        if file_type not in ["base", "corr", "exsa"]:
            raise ValueError('Unrecognized file_type: {}, must be "base", "corr" or "exsa"'.format(file_type))
        payload.update({"file_type" : file_type})
    response = query(url, payload)
    return _read_rdb(response.text), _set_metadata(response, site_no=site)


def what_sites(**kwargs):
    """
    Search NWIS for sites within a region with specific data.

    Parameters
    ----------
    same as get_info
    """

    response = query_waterservices(service='site', **kwargs)

    df = _read_rdb(response.text)

    return df, _set_metadata(response, **kwargs)


def get_record(sites=None, start=None, end=None, state=None,
               service='iv', *args, **kwargs):
    """
    Get data from NWIS and return it as a DataFrame.

    Note: If no start or end date are provided, only the most recent record is returned.

    Parameters (Additional parameters, if supplied, will be used as query parameters)
    ----------
        sites: listlike
            List or comma delimited string of site.
        start: string
            Starting date of record (YYYY-MM-DD)
        end: string
            Ending date of record.
        service: string
            - 'iv' : instantaneous data
            - 'dv' : daily mean data
            - 'qwdata' : discrete samples
            - 'site' : site description
            - 'measurements' : discharge measurements
    Return:
        DataFrame containing requested data
    """
    if service not in WATERSERVICES_SERVICES + WATERDATA_SERVICES:
        raise TypeError('Unrecognized service: {}'.format(service))

    if service == 'iv':
        df, _ = get_iv(sites=sites, startDT=start, endDT=end, **kwargs)
        return df

    elif service == 'dv':
        df, _ = get_dv(sites=sites, startDT=start, endDT=end, **kwargs)
        return df

    elif service == 'qwdata':
        df, _ = get_qwdata(site_no=sites, begin_date=start, end_date=end,
                           qw_sample_wide='separated_wide', **kwargs)
        return df

    elif service == 'site':
        df, _ = get_info(sites=sites, **kwargs)
        return df

    elif service == 'measurements':
        df, _ = get_discharge_measurements(site_no=sites, begin_date=start,
                                           end_date=end, **kwargs)
        return df

    elif service == 'peaks':
        df, _ = get_discharge_peaks(site_no=sites, begin_date=start,
                                    end_date=end, **kwargs)
        return df

    elif service == 'gwlevels':
        df, _ = get_gwlevels(sites=sites, startDT=start, endDT=end,
                             **kwargs)
        return df

    elif service == 'pmcodes':
        df, _ = get_pmcodes(**kwargs)
        return df

    elif service == 'water_use':
        df, _ = get_water_use(state=state, **kwargs)
        return df

    elif service == 'ratings':
        df, _ = get_ratings(**kwargs)
        return df

    else:
        raise TypeError('{} service not yet implemented'.format(service))


def _read_json(json, multi_index=False):
    """
    Reads a NWIS Water Services formatted JSON into a DataFrame.

    Args:
        json: dict
            A JSON dictionary Response to be parsed into a DataFrame

    Returns:
        DataFrame containing times series data from the NWIS json and Metadata as tuple
    """
    merged_df = pd.DataFrame()

    for timeseries in json['value']['timeSeries']:

        site_no = timeseries['sourceInfo']['siteCode'][0]['value']
        param_cd = timeseries['variable']['variableCode'][0]['value']
        # check whether min, max, mean record XXX
        option = timeseries['variable']['options']['option'][0].get('value')

        # loop through each parameter in timeseries.
        for parameter in timeseries['values']:
            col_name = param_cd
            method = parameter['method'][0]['methodDescription']

            # if len(timeseries['values']) > 1 and method:
            if method:
                # get method, format it, and append to column name
                method = method.strip("[]()").lower()
                col_name = '{}_{}'.format(col_name, method)

            if option:
                col_name = '{}_{}'.format(col_name, option)

            record_json = parameter['value']

            if not record_json:
                # no data in record
                continue
            # should be able to avoid this by dumping
            record_json = str(record_json).replace("'", '"')

            # read json, converting all values to float64 and all qaulifiers
            # Lists can't be hashed, thus we cannot df.merge on a list column
            record_df = pd.read_json(record_json,
                                     orient='records',
                                     dtype={'value': 'float64',
                                            'qualifiers': 'unicode'})

            record_df['qualifiers'] = (record_df['qualifiers']
                                       .str.strip("[]").str.replace("'", ""))
            record_df['site_no'] = site_no

            record_df.rename(columns={'value': col_name,
                                      'dateTime': 'datetime',
                                      'qualifiers': col_name + '_cd'},
                             inplace=True)

            if merged_df.empty:
                merged_df = record_df

            else:
                merged_df = update_merge(merged_df, record_df, na_only=True,
                                         on=['site_no', 'datetime'])

    merged_df = format_response(merged_df)
    return merged_df


def _read_rdb(rdb):
    """
    Convert NWIS rdb table into a dataframe.

    Args:
        rdb: string
            A string representation of an rdb table
    """
    count = 0

    for line in rdb.splitlines():
        # ignore comment lines
        if line.startswith('#'):
            count = count + 1

        else:
            break

    fields = rdb.splitlines()[count].split('\t')
    dtypes = {'site_no': str, 'dec_long_va': float, 'dec_lat_va': float}

    df = pd.read_csv(StringIO(rdb), delimiter='\t', skiprows=count + 2,
                     names=fields, na_values='NaN', dtype=dtypes)

    df = format_response(df)
    return df


def _set_metadata(response, **parameters):
    """Generates a standard set of metadata informated by the response.

    Args:
        response: Response
             Response object from requests module
        parameters: unpacked dictionary
            unpacked dictionary of the parameters supplied in the request
    """
    md = set_md(response)
    site_aliases = ['sites', 'site_no']
    for alias in site_aliases:
        if alias in parameters:
            md.site_info = lambda: what_sites(sites=parameters[alias])
            break

    if 'parameterCd' in parameters:
        md.variable_info = lambda: get_pmcodes(parameterCd=parameters['parameterCd'])

    comments = ""
    for line in response.text.splitlines():
        if line.startswith("#"):
            comments += line.lstrip("#") + "\n"
    if comments != "":
        md.comment = comments

    return md
