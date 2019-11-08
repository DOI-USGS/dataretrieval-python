#-*- coding: utf-8 -*-
"""Functions for downloading data from NWIS

Todo:
    * Create a test to check whether functions pull multipe sites
    * Work on multi-index capabilities.
    * Check that all timezones are handled properly for each service.
"""

import pandas as pd
import requests
from io import StringIO

from dataretrieval.utils import to_str, format_datetime, update_merge

WATERDATA_URL = 'https://nwis.waterdata.usgs.gov/nwis/'
WATERSERVICE_URL = 'https://waterservices.usgs.gov/nwis/'

WATERSERVICES_SERVICES = ['dv', 'iv', 'site', 'stat', 'gwlevels']
WATERDATA_SERVICES = ['qwdata', 'measurements', 'peaks', 'pmcodes']
# add more services


def format_response(df, service=None):
    """Setup index for response from query.
    """
    if df is None:
        return

    if service == 'peaks':
        df = preformat_peaks_response(df)

    # check for multiple sites:
    if 'datetime' not in df.columns:
        # XXX: consider making site_no index
        return df

    elif len(df['site_no'].unique()) > 1:
        # setup multi-index
        df.set_index(['site_no', 'datetime'], inplace=True)
        if df.index.levels[1].tzinfo is None:
            df = df.tz_localize('UTC', level=1)

    else:
        df.set_index(['datetime'], inplace=True)
        if df.index.tzinfo is None:
            df = df.tz_localize('UTC')

    return df.sort_index()


def preformat_peaks_response(df):
    df['datetime'] = pd.to_datetime(df.pop('peak_dt'), errors='coerce')
    df.dropna(subset=['datetime'])
    return df


def try_format_datetime(df, date_field, time_field, tz_field):
    try:
        return format_datetime(df, date_field, time_field, tz_field)

    except TypeError:
        return None


def get_qwdata(datetime_index=True, **kwargs):
    """Get water sample data from qwdata service.

    Parameters
    ----------
    datetime_index : boolean
        If True, create a datetime index
    qw_sample_wide : string
        separated_wide
    """
    # check number of sites, may need to create multiindex

    payload = {'agency_cd': 'USGS',
               'format': 'rdb',
               'pm_cd_compare': 'Greater than',
               'inventory_output': '0',
               'rdb_inventory_output': 'file',
               'TZoutput': '0',
               'radio_parm_cds': 'all_parm_cds',
               'rdb_qw_attributes': 'expanded',
               'date_format': 'YYYY-MM-DD',
               'rdb_compression': 'value',
               'submmitted_form': 'brief_list'}
               #'qw_sample_wide': 'separated_wide'}

    kwargs = {**payload, **kwargs}

    query = query_waterdata('qwdata', **kwargs)

    df = read_rdb(query)

    if datetime_index == True:
        df = try_format_datetime(df, 'sample_dt', 'sample_tm',
                                 'sample_start_time_datum_cd')

    return format_response(df)


def get_discharge_measurements(**kwargs):
    """ **DEPCRECATED**
    Args:
        sites (listlike):
    """
    query = query_waterdata('measurements', format='rdb', **kwargs)
    df = read_rdb(query)

    return format_response(df)


def get_discharge_peaks(**kwargs):
    """ **DEPRECATED** Implement through waterservices

    Args:
        site_no (listlike):
        state_cd (listline):

    """
    query = query_waterdata('peaks', format='rdb', **kwargs)

    df = read_rdb(query)

    return format_response(df, service='peaks')


def get_gwlevels(**kwargs):
    """Querys the groundwater level service from waterservices
    """
    query = query_waterservices('gwlevels', **kwargs)

    df = read_rdb(query)
    df = try_format_datetime(df, 'lev_dt', 'lev_tm', 'lev_tz_cd')

    return format_response(df)


def get_stats(**kwargs):
    """Querys waterservices statistics information

    Must specify
    Args:
        sites (string or list): USGS site number
        statReportType (string): daily (default), monthly, or annual
        statTypeCd (string): all, mean, max, min, median

    Returns:
        Dataframe

    TODO: fix date parsing
    """
    if 'sites' not in kwargs:
        raise TypeError('Query must specify a site or list of sites')

    query = query_waterservices('stat', **kwargs)

    return read_rdb(query)


def query(url, **kwargs):
    """Send a query.

    Wrapper for requests.get that handles errors, converts listed
    query paramaters to comma separated strings, and returns response.

    Args:
        url:
        kwargs: query parameters passed to requests.get

    Returns:
        string : query response
    """

    payload = {}

    for key, value in kwargs.items():
        value = to_str(value)
        payload[key] = value

    try:

        req = requests.get(url, params=payload)

    except ConnectionError:

        print('could not connect to {}'.format(req.url))

    response_format = kwargs.get('format')

    if req.status_code == 400:
        return False

    if response_format == 'json':
        return req.json()

    else:
        return req.text


def query_waterdata(service, **kwargs):
    """Querys waterdata.
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

    return query(url, **kwargs)


def query_waterservices(service, **kwargs):
    """Querys waterservices.usgs.gov

    For more documentation see

    Args:
        service (string): 'site','stats',etc
        bBox:
        huc (string): 7-digit Hydrologic Unit Code

        startDT (string): start date (2017-12-31)
        endDT (string): end date
        modifiedSince (string): for example

    Returns:
        request

    Usage: must specify one major filter: sites, stateCd, bBox,
    """
    if not any(key in kwargs for key in ['sites', 'stateCd', 'bBox']):
        raise TypeError('Query must specify a major filter: sites, stateCd, bBox')

    if service not in WATERSERVICES_SERVICES:
        raise TypeError('Service not recognized')

    if 'format' not in kwargs:
        kwargs['format'] = 'rdb'

    url = WATERSERVICE_URL + service

    return query(url, **kwargs)


def get_dv(**kwargs):

    query = query_waterservices('dv', format='json', **kwargs)
    df = read_json(query)

    return format_response(df)


def get_info(**kwargs):
    """
    Get site description information from NWIS.

    Note: Must specify one major parameter.

    Major Parameters
    ----------------
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
        Returns only site data for thos sites containing the requested USGS
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

    query = query_waterservices('site', **kwargs)

    return read_rdb(query)


def get_iv(**kwargs):

    query = query_waterservices('iv', format='json', **kwargs)

    df = read_json(query)

    return format_response(df)


def get_pmcodes(**kwargs):
    """Return a DataFrame containing all NWIS parameter codes.

    Returns:
        DataFrame containgin the USGS parameter codes
    """
    payload = {'radio_pm_search': 'param_group',
               'pm_group': 'All+--+include+all+parameter+groups',
               'pm_sarch': None,
               'casrn_search': None,
               'srsname_search': None,
               'show': 'parameter_group_nm',
               'show': 'casrn',
               'show': 'srsname',
               'show': 'parameter_units',
               'format': 'rdb',
               }

    kwargs = {**payload, **kwargs}

    # XXX check that the url is correct
    url = WATERSERVICE_URL + 'pmcodes'
    df = read_rdb(query(url, **kwargs))

    return format_response(df)


def get_record(sites, start=None, end=None, state=None,
               service='iv', *args, **kwargs):
    """
    Get data from NWIS and return it as a DataFrame.

    Args:
        sites (listlike): List or comma delimited string of site.
        start (string): Starting date of record (YYYY-MM-DD)
        end (string): Ending date of record.
        service (string):
            - 'iv' : instantaneous data
            - 'dv' : daily mean data
            - 'qwdata' : discrete samples
            - 'site' : site description
            - 'measurements' : discharge measurements
    Return:
        DataFrame containing requested data.
    """
    if service not in WATERSERVICES_SERVICES + WATERDATA_SERVICES:
        raise TypeError('Unrecognized service: {}'.format(service))

    if service == 'iv':
        record_df = get_iv(sites=sites, startDT=start, endDT=end, **kwargs)

    elif service == 'dv':
        record_df = get_dv(sites=sites, startDT=start, endDT=end, **kwargs)

    elif service == 'qwdata':
        record_df = get_qwdata(site_no=sites, begin_date=start, end_date=end,
                               qw_sample_wide='separated_wide', **kwargs)

    elif service == 'site':
        record_df = get_info(sites=sites)

    elif service == 'measurements':
        record_df = get_discharge_measurements(site_no=sites, begin_date=start,
                                               end_date=end, **kwargs)

    elif service == 'peaks':
        record_df = get_discharge_peaks(site_no=sites, begin_date=start,
                                        end_date=end, **kwargs)

    elif service == 'gwlevels':
        record_df = get_gwlevels(sites=sites, startDT=start, endDT=end,
                                 **kwargs)

    else:
        raise TypeError('{} service not yet implemented'.format(service))

    return record_df


def read_json(json, multi_index=False):
    """Reads a NWIS Water Services formated JSON into a dataframe

    Args:
        json (dict)

    Returns:
        DataFrame containing times series data from the NWIS json.
    """
    merged_df = pd.DataFrame()

    if not json:
        return merged_df

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

    return format_response(merged_df)


def read_rdb(rdb):
    """Convert NWIS rdb table into a dataframe.

    Args:
        rdb (string):
    """
    if rdb.startswith('No sites/data'):
        return None

    count = 0

    for line in rdb.splitlines():
        # ignore comment lines
        if line.startswith('#'):
            count = count + 1

        else:
            break

    fields = rdb.splitlines()[count].split('\t')
    dtypes = {'site_no': str}

    df = pd.read_csv(StringIO(rdb), delimiter='\t', skiprows=count+2,
                     names=fields, na_values='NaN', dtype=dtypes)

    return format_response(df)
