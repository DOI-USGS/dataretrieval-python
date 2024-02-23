"""Functions for downloading data from the `National Water Information System (NWIS)`_.

.. _National Water Information System (NWIS): https://waterdata.usgs.gov/nwis


.. todo::

    * Create a test to check whether functions pull multiple sites
    * Work on multi-index capabilities.
    * Check that all timezones are handled properly for each service.

"""

import re
import warnings
from io import StringIO
from typing import List, Optional, Tuple, Union

import pandas as pd
import requests

from dataretrieval.utils import BaseMetadata, format_datetime, to_str

from .utils import query

WATERDATA_BASE_URL = 'https://nwis.waterdata.usgs.gov/'
WATERDATA_URL = WATERDATA_BASE_URL + 'nwis/'
WATERSERVICE_URL = 'https://waterservices.usgs.gov/nwis/'
PARAMCODES_URL = 'https://help.waterdata.usgs.gov/code/parameter_cd_nm_query?'
ALLPARAMCODES_URL = 'https://help.waterdata.usgs.gov/code/parameter_cd_query?'

WATERSERVICES_SERVICES = ['dv', 'iv', 'site', 'stat', 'gwlevels']
WATERDATA_SERVICES = [
    'qwdata',
    'measurements',
    'peaks',
    'pmcodes',
    'water_use',
    'ratings',
]


def format_response(
    df: pd.DataFrame, service: Optional[str] = None, **kwargs
) -> pd.DataFrame:
    """Setup index for response from query.

    This function formats the response from the NWIS web services, in
    particular it sets the index of the data frame. This function tries to
    convert the NWIS response into pandas datetime values localized to UTC,
    and if possible, uses these timestamps to define the data frame index.

    Parameters
    ----------
    df: ``pandas.DataFrame``
        The data frame to format
    service: string, optional, default is None
        The NWIS service that was queried, important because the 'peaks'
        service returns a different format than the other services.
    **kwargs: optional
        Additional keyword arguments, e.g., 'multi_index'

    Returns
    -------
    df: ``pandas.DataFrame``
        The formatted data frame

    """
    mi = kwargs.pop('multi_index', True)

    if service == 'peaks':
        df = preformat_peaks_response(df)

    # check for multiple sites:
    if 'datetime' not in df.columns:
        # XXX: consider making site_no index
        return df

    elif len(df['site_no'].unique()) > 1 and mi:
        # setup multi-index
        df.set_index(['site_no', 'datetime'], inplace=True)
        if hasattr(df.index.levels[1], 'tzinfo') and df.index.levels[1].tzinfo is None:
            df = df.tz_localize('UTC', level=1)

    else:
        df.set_index(['datetime'], inplace=True)
        if hasattr(df.index, 'tzinfo') and df.index.tzinfo is None:
            df = df.tz_localize('UTC')

    return df.sort_index()


def preformat_peaks_response(df: pd.DataFrame) -> pd.DataFrame:
    """Datetime formatting for the 'peaks' service response.

    Function to format the datetime column of the 'peaks' service response.

    Parameters
    ----------
    df: ``pandas.DataFrame``
        The data frame to format

    Returns
    -------
    df: ``pandas.DataFrame``
        The formatted data frame

    """
    df['datetime'] = pd.to_datetime(df.pop('peak_dt'), errors='coerce')
    df.dropna(subset=['datetime'], inplace=True)
    return df


def get_qwdata(
    sites: Optional[Union[List[str], str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    multi_index: bool = True,
    wide_format: bool = True,
    datetime_index: bool = True,
    ssl_check: bool = True,
    **kwargs,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Get water sample data from qwdata service.

    .. warning::

        WARNING: Beginning in March 2024 the NWIS qw data endpoint will
        not deliver new data or updates to existing data.
        Eventually the endpoint will be retired. For updated information visit:
        https://waterdata.usgs.gov.nwis/qwdata
        For additional details, see the R package vignette:
        https://doi-usgs.github.io/dataRetrieval/articles/Status.html
        If you have additional questions about the qw data service,
        email CompTools@usgs.gov.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        If the qwdata parameter site_no is supplied, it will overwrite the
        sites parameter
    start: string, optional, default is None
        If the qwdata parameter begin_date is supplied, it will overwrite the
        start parameter (YYYY-MM-DD)
    end: string, optional, default is None
        If the qwdata parameter end_date is supplied, it will overwrite the
        end parameter (YYYY-MM-DD)
    multi_index: bool, optional
        If False, a dataframe with a single-level index (datetime) is returned,
        default is True
    wide_format : bool, optional
        If True, return data in wide format with multiple samples per row and
        one row per time, default is True
    datetime_index : bool, optional
        If True, create a datetime index, default is True
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
    df: ``pandas.DataFrame``
        Times series data from the NWIS JSON
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # get water sample information for site 11447650
        >>> df, md = dataretrieval.nwis.get_qwdata(
        ...     sites='11447650', start='2010-01-01', end='2010-02-01'
        ... )

    """
    warnings.warn(('WARNING: Starting in March 2024, the NWIS qw data endpoint is '
                       'retiring and no longer receives updates. For more information, '
                       'refer to https://waterdata.usgs.gov.nwis/qwdata and '
                       'https://doi-usgs.github.io/dataRetrieval/articles/Status.html '
                       'or email CompTools@usgs.gov.'))

    _check_sites_value_types(sites)

    kwargs['site_no'] = kwargs.pop('site_no', sites)
    kwargs['begin_date'] = kwargs.pop('begin_date', start)
    kwargs['end_date'] = kwargs.pop('end_date', end)
    kwargs['multi_index'] = multi_index
    if wide_format:
        kwargs['qw_sample_wide'] = 'qw_sample_wide'

    payload = {
        'agency_cd': 'USGS',
        'format': 'rdb',
        'pm_cd_compare': 'Greater than',
        'inventory_output': '0',
        'rdb_inventory_output': 'file',
        'TZoutput': '0',
        'rdb_qw_attributes': 'expanded',
        'date_format': 'YYYY-MM-DD',
        'rdb_compression': 'value',
        'submitted_form': 'brief_list',
    }

    # check for parameter codes, and reformat query args
    qwdata_parameter_code_field = 'parameterCd'
    if kwargs.get(qwdata_parameter_code_field):
        parameter_codes = kwargs.pop(qwdata_parameter_code_field)
        parameter_codes = to_str(parameter_codes)
        kwargs['multiple_parameter_cds'] = parameter_codes
        kwargs['param_cd_operator'] = 'OR'

        search_criteria = kwargs.get('list_of_search_criteria')
        if search_criteria:
            kwargs['list_of_search_criteria'] = '{},{}'.format(
                search_criteria, 'multiple_parameter_cds'
            )
        else:
            kwargs['list_of_search_criteria'] = 'multiple_parameter_cds'

    kwargs.update(payload)

    warnings.warn(
        'NWIS qw web services are being retired. '
        + 'See this note from the R package for more: '
        + 'https://doi-usgs.github.io/dataRetrieval/articles/qwdata_changes.html',
        category=DeprecationWarning,
    )
    response = query_waterdata('qwdata', ssl_check=ssl_check, **kwargs)

    df = _read_rdb(response.text)

    if datetime_index is True:
        df = format_datetime(df, 'sample_dt', 'sample_tm', 'sample_start_time_datum_cd')

    return format_response(df, **kwargs), NWIS_Metadata(response, **kwargs)


def get_discharge_measurements(
    sites: Optional[Union[List[str], str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    ssl_check: bool = True,
    **kwargs,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Get discharge measurements from the waterdata service.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        If the qwdata parameter site_no is supplied, it will overwrite the
        sites parameter
    start: string, optional, default is None
        If the qwdata parameter begin_date is supplied, it will overwrite the
        start parameter (YYYY-MM-DD)
    end: string, optional, default is None
        If the qwdata parameter end_date is supplied, it will overwrite the
        end parameter (YYYY-MM-DD)
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
    df: ``pandas.DataFrame``
        Times series data from the NWIS JSON
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get discharge measurements for site 05114000
        >>> df, md = dataretrieval.nwis.get_discharge_measurements(
        ...     sites='05114000', start='2000-01-01', end='2000-01-30'
        ... )

        >>> # Get discharge measurements for sites in Alaska
        >>> df, md = dataretrieval.nwis.get_discharge_measurements(
        ...     start='2012-01-09', end='2012-01-10', stateCd='AK'
        ... )

    """
    _check_sites_value_types(sites)

    kwargs['site_no'] = kwargs.pop('site_no', sites)
    kwargs['begin_date'] = kwargs.pop('begin_date', start)
    kwargs['end_date'] = kwargs.pop('end_date', end)

    response = query_waterdata(
        'measurements', format='rdb', ssl_check=ssl_check, **kwargs
    )
    return _read_rdb(response.text), NWIS_Metadata(response, **kwargs)


def get_discharge_peaks(
    sites: Optional[Union[List[str], str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    multi_index: bool = True,
    ssl_check: bool = True,
    **kwargs,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Get discharge peaks from the waterdata service.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        If the waterdata parameter site_no is supplied, it will overwrite the
        sites parameter
    start: string, optional, default is None
        If the waterdata parameter begin_date is supplied, it will overwrite
        the start parameter (YYYY-MM-DD)
    end: string, optional, default is None
        If the waterdata parameter end_date is supplied, it will overwrite
        the end parameter (YYYY-MM-DD)
    multi_index: bool, optional
        If False, a dataframe with a single-level index (datetime) is returned,
        default is True
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
    df: ``pandas.DataFrame``
        Times series data from the NWIS JSON
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get discharge peaks for site 01491000
        >>> df, md = dataretrieval.nwis.get_discharge_peaks(
        ...     sites='01491000', start='1980-01-01', end='1990-01-01'
        ... )

        >>> # Get discharge peaks for sites in Hawaii
        >>> df, md = dataretrieval.nwis.get_discharge_peaks(
        ...     start='1980-01-01', end='1980-01-02', stateCd='HI'
        ... )

    """
    _check_sites_value_types(sites)

    kwargs['site_no'] = kwargs.pop('site_no', sites)
    kwargs['begin_date'] = kwargs.pop('begin_date', start)
    kwargs['end_date'] = kwargs.pop('end_date', end)
    kwargs['multi_index'] = multi_index

    response = query_waterdata('peaks', format='rdb', ssl_check=ssl_check, **kwargs)

    df = _read_rdb(response.text)

    return format_response(df, service='peaks', **kwargs), NWIS_Metadata(
        response, **kwargs
    )


def get_gwlevels(
    sites: Optional[Union[List[str], str]] = None,
    start: str = '1851-01-01',
    end: Optional[str] = None,
    multi_index: bool = True,
    datetime_index: bool = True,
    ssl_check: bool = True,
    **kwargs,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Queries the groundwater level service from waterservices

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        If the waterdata parameter site_no is supplied, it will overwrite the
        sites parameter
    start: string, optional, default is '1851-01-01'
        If the waterdata parameter begin_date is supplied, it will overwrite
        the start parameter
    end: string, optional, default is None
        If the waterdata parameter end_date is supplied, it will overwrite the
        end parameter (YYYY-MM-DD)
    multi_index: bool, optional
        If False, a dataframe with a single-level index (datetime) is returned,
        default is True
    datetime_index : bool, optional
        If True, create a datetime index, default is True
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
    df: ``pandas.DataFrame``
        Times series data from the NWIS JSON
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get groundwater levels for site 434400121275801
        >>> df, md = dataretrieval.nwis.get_gwlevels(sites='434400121275801')

    """
    _check_sites_value_types(sites)

    kwargs['startDT'] = kwargs.pop('startDT', start)
    kwargs['endDT'] = kwargs.pop('endDT', end)
    kwargs['sites'] = kwargs.pop('sites', sites)
    kwargs['multi_index'] = multi_index

    response = query_waterservices('gwlevels', ssl_check=ssl_check, **kwargs)

    df = _read_rdb(response.text)

    if datetime_index is True:
        df = format_datetime(df, 'lev_dt', 'lev_tm', 'lev_tz_cd')

    return format_response(df, **kwargs), NWIS_Metadata(response, **kwargs)


def get_stats(
    sites: Optional[Union[List[str], str]] = None, ssl_check: bool = True, **kwargs
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Queries water services statistics information.

    For more information about the water services statistics service, visit
    https://waterservices.usgs.gov/docs/statistics/statistics-details/

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        USGS site number (or list of site numbers)
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Keyword Arguments
    ---------------------
    statReportType: string
        daily (default), monthly, or annual
    statTypeCd: string
        all, mean, max, min, median

    Returns
    -------
    df: ``pandas.DataFrame``
        Statistics data from the statistics service
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    .. todo::

        fix date parsing

    Examples
    --------
    .. doctest::

        >>> # Get annual water statistics for a site
        >>> df, md = dataretrieval.nwis.get_stats(
        ...     sites='01646500', statReportType='annual', statYearType='water'
        ... )

        >>> # Get monthly statistics for a site
        >>> df, md = dataretrieval.nwis.get_stats(
        ...     sites='01646500', statReportType='monthly'
        ... )

    """
    _check_sites_value_types(sites)

    response = query_waterservices(
        service='stat', sites=sites, ssl_check=ssl_check, **kwargs
    )

    return _read_rdb(response.text), NWIS_Metadata(response, **kwargs)


def query_waterdata(
    service: str, ssl_check: bool = True, **kwargs
) -> requests.models.Response:
    """
    Queries waterdata.

    Parameters
    ----------
    service: string
        Name of the service to query: 'site', 'stats', etc.
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
    request: ``requests.models.Response``
        The response object from the API request to the web service
    """
    major_params = ['site_no', 'state_cd']
    bbox_params = [
        'nw_longitude_va',
        'nw_latitude_va',
        'se_longitude_va',
        'se_latitude_va',
    ]

    if not any(key in kwargs for key in major_params + bbox_params):
        raise TypeError('Query must specify a major filter: site_no, stateCd, bBox')

    elif any(key in kwargs for key in bbox_params) and not all(
        key in kwargs for key in bbox_params
    ):
        raise TypeError('One or more lat/long coordinates missing or invalid.')

    if service not in WATERDATA_SERVICES:
        raise TypeError('Service not recognized')

    url = WATERDATA_URL + service

    return query(url, payload=kwargs, ssl_check=ssl_check)


def query_waterservices(
    service: str, ssl_check: bool = True, **kwargs
) -> requests.models.Response:
    """
    Queries waterservices.usgs.gov

    For more documentation see https://waterservices.usgs.gov/docs/

    .. note::

        User must specify one major filter: sites, stateCd, or bBox

    Parameters
    ----------
    service: string
        Name of the service to query: 'site', 'stats', etc.
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Keyword Arguments
    ----------------
    bBox: string
        7-digit Hydrologic Unit Code (HUC)
    startDT: string
        Start date (e.g., '2017-12-31')
    endDT: string
        End date (e.g., '2018-01-01')
    modifiedSince: string
        Used to return only sites where attributes or period of record data
        have changed during the request period. String expected to be formatted
        in ISO-8601 duration format (e.g., 'P1D' for one day,
        'P1Y' for one year)

    Returns
    -------
    request: ``requests.models.Response``
        The response object from the API request to the web service

    """
    if not any(
        key in kwargs for key in ['sites', 'stateCd', 'bBox', 'huc', 'countyCd']
    ):
        raise TypeError(
            'Query must specify a major filter: sites, stateCd, bBox, huc, or countyCd'
        )

    if service not in WATERSERVICES_SERVICES:
        raise TypeError('Service not recognized')

    if 'format' not in kwargs:
        kwargs['format'] = 'rdb'

    url = WATERSERVICE_URL + service

    return query(url, payload=kwargs, ssl_check=ssl_check)


def get_dv(
    sites: Optional[Union[List[str], str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    multi_index: bool = True,
    ssl_check: bool = True,
    **kwargs,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Get daily values data from NWIS and return it as a ``pandas.DataFrame``.

    .. note:

        If no start or end date are provided, only the most recent record
        is returned.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        USGS site number (or list of site numbers)
    start: string, optional, default is None
        If the waterdata parameter startDT is supplied, it will overwrite the
        start parameter (YYYY-MM-DD)
    end: string, optional, default is None
        If the waterdata parameter endDT is supplied, it will overwrite the
        end parameter (YYYY-MM-DD)
    multi_index: bool, optional
        If True, return a multi-index dataframe, if False, return a
        single-index dataframe, default is True
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
    df: ``pandas.DataFrame``
        Times series data from the NWIS JSON
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get mean statistic daily values for site 04085427
        >>> df, md = dataretrieval.nwis.get_dv(
        ...     sites='04085427', start='2012-01-01', end='2012-06-30', statCd='00003'
        ... )

        >>> # Get the latest daily values for site 01646500
        >>> df, md = dataretrieval.nwis.get_dv(sites='01646500')

    """
    _check_sites_value_types(sites)

    kwargs['startDT'] = kwargs.pop('startDT', start)
    kwargs['endDT'] = kwargs.pop('endDT', end)
    kwargs['sites'] = kwargs.pop('sites', sites)
    kwargs['multi_index'] = multi_index

    response = query_waterservices('dv', format='json', ssl_check=ssl_check, **kwargs)
    df = _read_json(response.json())

    return format_response(df, **kwargs), NWIS_Metadata(response, **kwargs)


def get_info(ssl_check: bool = True, **kwargs) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Get site description information from NWIS.

    **Note:** *Must specify one major parameter.*

    For additional parameter options see
    https://waterservices.usgs.gov/docs/site-service/site-service-details/

    Parameters
    ----------
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Keyword Arguments
    ----------------
    sites: string or list of strings
        A list of site numbers. Sites may be prefixed with an optional agency
        code followed by a colon.
    stateCd: string
        U.S. postal service (2-digit) state code. Only 1 state can be specified
        per request.
    huc: string or list of strings
        A list of hydrologic unit codes (HUC) or aggregated watersheds. Only 1
        major HUC can be specified per request, or up to 10 minor HUCs. A major
        HUC has two digits.
    bBox: string or list of strings
        A contiguous range of decimal latitude and longitude, starting with the
        west longitude, then the south latitude, then the east longitude, and
        then the north latitude with each value separated by a comma. The
        product of the range of latitude range and longitude cannot exceed 25
        degrees. Whole or decimal degrees must be specified, up to six digits
        of precision. Minutes and seconds are not allowed.
    countyCd: string or list of strings
        A list of county numbers, in a 5 digit numeric format. The first two
        digits of a county's code are the FIPS State Code.
        (url: https://help.waterdata.usgs.gov/code/county_query?fmt=html)
    startDt: string
        Selects sites based on whether data was collected at a point in time
        beginning after startDt (start date). Dates must be in ISO-8601
        Calendar Date format (for example: 1990-01-01).
    endDt: string
        The end date for the period of record. Dates must be in ISO-8601
        Calendar Date format (for example: 1990-01-01).
    period: string
        Selects sites based on whether they were active between now
        and a time in the past. For example, period=P10W will select sites
        active in the last ten weeks.
    modifiedSince: string
        Returns only sites where site attributes or period of record data have
        changed during the request period.
    parameterCd: string or list of strings
        Returns only site data for those sites containing the requested USGS
        parameter codes.
    siteType: string or list of strings
        Restricts sites to those having one or more major and/or minor site
        types, such as stream, spring or well. For a list of all valid site
        types see https://help.waterdata.usgs.gov/site_tp_cd
        For example, siteType='ST' returns streams only.
    siteOutput: string ('basic' or 'expanded')
        Indicates the richness of metadata you want for site attributes. Note
        that for visually oriented formats like Google Map format, this
        argument has no meaning. Note: for performance reasons,
        siteOutput=expanded cannot be used if seriesCatalogOutput=true or with
        any values for outputDataTypeCd.
    seriesCatalogOutput: bool
        A switch that provides detailed period of record information for
        certain output formats. The period of record indicates date ranges for
        a certain kind of information about a site, for example the start and
        end dates for a site's daily mean streamflow.

    Returns
    -------
    df: ``pandas.DataFrame``
        Site data from the NWIS web service
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get site information for a single site
        >>> df, md = dataretrieval.nwis.get_info(sites='05114000')

        >>> # Get site information for multiple sites
        >>> df, md = dataretrieval.nwis.get_info(sites=['05114000', '09423350'])

    """
    seriesCatalogOutput = kwargs.pop('seriesCatalogOutput', None)
    if seriesCatalogOutput in ['True', 'TRUE', 'true', True]:

        warnings.warn(('WARNING: Starting in March 2024, the NWIS qw data endpoint is '
                       'retiring and no longer receives updates. For more information, '
                       'refer to https://waterdata.usgs.gov.nwis/qwdata and '
                       'https://doi-usgs.github.io/dataRetrieval/articles/Status.html '
                       'or email CompTools@usgs.gov.'))
        # convert bool to string if necessary
        kwargs['seriesCatalogOutput'] = 'True'
    else:
        # cannot have both seriesCatalogOutput and the expanded format
        kwargs['siteOutput'] = 'Expanded'

    response = query_waterservices('site', ssl_check=ssl_check, **kwargs)

    return _read_rdb(response.text), NWIS_Metadata(response, **kwargs)


def get_iv(
    sites: Optional[Union[List[str], str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    multi_index: bool = True,
    ssl_check: bool = True,
    **kwargs,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """Get instantaneous values data from NWIS and return it as a DataFrame.

    .. note::

        If no start or end date are provided, only the most recent record
        is returned.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        If the waterdata parameter site_no is supplied, it will overwrite the
        sites parameter
    start: string, optional, default is None
        If the waterdata parameter startDT is supplied, it will overwrite the
        start parameter (YYYY-MM-DD)
    end: string, optional, default is None
        If the waterdata parameter endDT is supplied, it will overwrite the
        end parameter (YYYY-MM-DD)
    multi_index: bool, optional
        If False, a dataframe with a single-level index (datetime) is returned,
        default is True
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
    df: ``pandas.DataFrame``
        Times series data from the NWIS JSON
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get instantaneous discharge data for site 05114000
        >>> df, md = dataretrieval.nwis.get_iv(
        ...     sites='05114000',
        ...     start='2013-11-03',
        ...     end='2013-11-03',
        ...     parameterCd='00060',
        ... )

    """
    _check_sites_value_types(sites)

    kwargs['startDT'] = kwargs.pop('startDT', start)
    kwargs['endDT'] = kwargs.pop('endDT', end)
    kwargs['sites'] = kwargs.pop('sites', sites)
    kwargs['multi_index'] = multi_index

    response = query_waterservices(
        service='iv', format='json', ssl_check=ssl_check, **kwargs
    )

    df = _read_json(response.json())
    return format_response(df, **kwargs), NWIS_Metadata(response, **kwargs)


def get_pmcodes(
    parameterCd: Union[str, List[str]] = 'All',
    partial: bool = True,
    ssl_check: bool = True,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Return a ``pandas.DataFrame`` containing all NWIS parameter codes.

    Parameters
    ----------
    parameterCd: string or list of strings, default is 'All'
        Accepts parameter codes or names
    partial: bool, optional
        Default is True (partial querying). If False, the function will query
        only exact matches, default is True
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True

    Returns
    -------
    df: ``pandas.DataFrame``
        Data retrieved from the NWIS web service.
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get information about the '00060' pcode
        >>> df, md = dataretrieval.nwis.get_pmcodes(parameterCd='00060', partial=False)

        >>> # Get information about all 'Discharge' pcodes
        >>> df, md = dataretrieval.nwis.get_pmcodes(
        ...     parameterCd='Discharge', partial=True
        ... )

    """

    payload = {'fmt': 'rdb'}
    url = PARAMCODES_URL

    if isinstance(parameterCd, str):  # when a single code or name is given
        if parameterCd.lower() == 'all':
            payload.update({'group_cd': '%'})
            url = ALLPARAMCODES_URL
            response = query(url, payload, ssl_check=ssl_check)
            return _read_rdb(response.text), NWIS_Metadata(response)

        else:
            parameterCd = [parameterCd]

    if not isinstance(parameterCd, list):
        raise TypeError(
            'Parameter information (code or name) must be type string or list'
        )

    # Querying with a list of parameters names, codes, or mixed
    return_list = []
    for param in parameterCd:
        if isinstance(param, str):
            if partial:
                param = f'%{param}%'
            payload.update({'parm_nm_cd': param})
            response = query(url, payload, ssl_check=ssl_check)
            if len(response.text.splitlines()) < 10:  # empty query
                raise TypeError(
                    'One of the parameter codes or names entered does not'
                    'return any information, please try a different value'
                )
            return_list.append(_read_rdb(response.text))
        else:
            raise TypeError('Parameter information (code or name) must be type string')
    return pd.concat(return_list), NWIS_Metadata(response)


def get_water_use(
    years: Union[str, List[str]] = 'ALL',
    state: Optional[str] = None,
    counties: Union[str, List[str]] = 'ALL',
    categories: Union[str, List[str]] = 'ALL',
    ssl_check: bool = True,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Water use data retrieval from USGS (NWIS).

    Parameters
    ----------
    years: string or list of strings
        List or comma delimited string of years.  Must be years ending in 0 or
        5, or "ALL", which retrieves all available years, default is "ALL"
    state: string, optional, default is None
        full name, abbreviation or id
    counties: string or list of strings
        County IDs from county lookup or "ALL", default is "ALL"
    categories: string or list of strings
        List or comma delimited string of Two-letter category abbreviations,
        default is "ALL"
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True

    Returns
    -------
    df: ``pandas.DataFrame``
        Data from NWIS
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get total population for RI from the NWIS water use service
        >>> df, md = dataretrieval.nwis.get_water_use(
        ...     years='2000', state='RI', categories='TP'
        ... )

        >>> # Get the national total water use for livestock in Bgal/day
        >>> df, md = dataretrieval.nwis.get_water_use(years='2010', categories='L')

        >>> # Get 2005 domestic water use for Apache County in Arizona
        >>> df, md = dataretrieval.nwis.get_water_use(
        ...     years='2005', state='Arizona', counties='001', categories='DO'
        ... )

    """
    if years:
        if not isinstance(years, list) and not isinstance(years, str):
            raise TypeError('years must be a string or a list of strings')

    if counties:
        if not isinstance(counties, list) and not isinstance(counties, str):
            raise TypeError('counties must be a string or a list of strings')

    if categories:
        if not isinstance(categories, list) and not isinstance(categories, str):
            raise TypeError('categories must be a string or a list of strings')

    payload = {
        'rdb_compression': 'value',
        'format': 'rdb',
        'wu_year': years,
        'wu_category': categories,
        'wu_county': counties,
    }
    url = WATERDATA_URL + 'water_use'
    if state is not None:
        url = WATERDATA_BASE_URL + state + '/nwis/water_use'
        payload.update({'wu_area': 'county'})
    response = query(url, payload, ssl_check=ssl_check)
    return _read_rdb(response.text), NWIS_Metadata(response)


def get_ratings(
    site: Optional[str] = None,
    file_type: str = 'base',
    ssl_check: bool = True,
    **kwargs,
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Rating table for an active USGS streamgage retrieval.

    Reads current rating table for an active USGS streamgage from NWISweb.
    Data is retrieved from https://waterdata.usgs.gov/nwis.

    Parameters
    ----------
    site: string, optional, default is None
        USGS site number.  This is usually an 8 digit number as a string.
        If the nwis parameter site_no is supplied, it will overwrite the site
        parameter
    file_type: string, default is "base"
        can be "base", "corr", or "exsa"
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Return
    ------
    df: ``pandas.DataFrame``
        Formatted requested data
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get the rating table for USGS streamgage 01594440
        >>> df, md = dataretrieval.nwis.get_ratings(site='01594440')

    """
    site = kwargs.pop('site_no', site)

    payload = {}
    url = WATERDATA_BASE_URL + 'nwisweb/get_ratings/'
    if site is not None:
        payload.update({'site_no': site})
    if file_type is not None:
        if file_type not in ['base', 'corr', 'exsa']:
            raise ValueError(
                f'Unrecognized file_type: {file_type}, must be "base", "corr" or "exsa"'
            )
        payload.update({'file_type': file_type})
    response = query(url, payload, ssl_check=ssl_check)
    return _read_rdb(response.text), NWIS_Metadata(response, site_no=site)


def what_sites(ssl_check: bool = True, **kwargs) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Search NWIS for sites within a region with specific data.

    Parameters
    ----------
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.nwis.get_info`

    Return
    ------
    df: ``pandas.DataFrame``
        Formatted requested data
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # get information about a single site
        >>> df, md = dataretrieval.nwis.what_sites(sites='05114000')

        >>> # get information about sites with phosphorus in Ohio
        >>> df, md = dataretrieval.nwis.what_sites(stateCd='OH', parameterCd='00665')

    """

    response = query_waterservices(service='site', ssl_check=ssl_check, **kwargs)

    df = _read_rdb(response.text)

    return df, NWIS_Metadata(response, **kwargs)


def get_record(
    sites: Optional[Union[List[str], str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    multi_index: bool = True,
    wide_format: bool = True,
    datetime_index: bool = True,
    state: Optional[str] = None,
    service: str = 'iv',
    ssl_check: bool = True,
    **kwargs,
) -> pd.DataFrame:
    """
    Get data from NWIS and return it as a ``pandas.DataFrame``.

    .. note::

        If no start or end date are provided, only the most recent record is
        returned.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        List or comma delimited string of site.
    start: string, optional, default is None
        Starting date of record (YYYY-MM-DD)
    end: string, optional, default is None
        Ending date of record. (YYYY-MM-DD)
    multi_index: bool, optional
        If False, a dataframe with a single-level index (datetime) is returned,
        default is True
    wide_format : bool, optional
        If True, return data in wide format with multiple samples per row and
        one row per time, default is True
    datetime_index : bool, optional
        If True, create a datetime index. default is True
    state: string, optional, default is None
        full name, abbreviation or id
    service: string, default is 'iv'
        - 'iv' : instantaneous data
        - 'dv' : daily mean data
        - 'qwdata' : discrete samples
        - 'site' : site description
        - 'measurements' : discharge measurements
        - 'peaks': discharge peaks
        - 'gwlevels': groundwater levels
        - 'pmcodes': get parameter codes
        - 'water_use': get water use data
        - 'ratings': get rating table
        - 'stat': get statistics
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
        ``pandas.DataFrame`` containing requested data

    Examples
    --------
    .. doctest::

        >>> # Get latest instantaneous data from site 01585200
        >>> df = dataretrieval.nwis.get_record(sites='01585200', service='iv')

        >>> # Get latest daily mean data from site 01585200
        >>> df = dataretrieval.nwis.get_record(sites='01585200', service='dv')

        >>> # Get all discrete sample data from site 01585200
        >>> df = dataretrieval.nwis.get_record(sites='01585200', service='qwdata')

        >>> # Get site description for site 01585200
        >>> df = dataretrieval.nwis.get_record(sites='01585200', service='site')

        >>> # Get discharge measurements for site 01585200
        >>> df = dataretrieval.nwis.get_record(sites='01585200', service='measurements')

        >>> # Get discharge peaks for site 01585200
        >>> df = dataretrieval.nwis.get_record(sites='01585200', service='peaks')

        >>> # Get latest groundwater level for site 434400121275801
        >>> df = dataretrieval.nwis.get_record(
        ...     sites='434400121275801', service='gwlevels'
        ... )

        >>> # Get information about the discharge parameter code
        >>> df = dataretrieval.nwis.get_record(service='pmcodes', parameterCd='00060')

        >>> # Get water use data for livestock nationally in 2010
        >>> df = dataretrieval.nwis.get_record(
        ...     service='water_use', years='2010', categories='L'
        ... )

        >>> # Get rating table for USGS streamgage 01585200
        >>> df = dataretrieval.nwis.get_record(sites='01585200', service='ratings')

        >>> # Get annual statistics for USGS station 01646500
        >>> df = dataretrieval.nwis.get_record(
        ...     sites='01646500',
        ...     service='stat',
        ...     statReportType='annual',
        ...     statYearType='water',
        ... )

    """
    _check_sites_value_types(sites)

    if service not in WATERSERVICES_SERVICES + WATERDATA_SERVICES:
        raise TypeError(f'Unrecognized service: {service}')

    if service == 'iv':
        df, _ = get_iv(
            sites=sites,
            startDT=start,
            endDT=end,
            multi_index=multi_index,
            ssl_check=ssl_check,
            **kwargs,
        )
        return df

    elif service == 'dv':
        df, _ = get_dv(
            sites=sites,
            startDT=start,
            endDT=end,
            multi_index=multi_index,
            ssl_check=ssl_check,
            **kwargs,
        )
        return df

    elif service == 'qwdata':
        df, _ = get_qwdata(
            site_no=sites,
            begin_date=start,
            end_date=end,
            multi_index=multi_index,
            wide_format=wide_format,
            ssl_check=ssl_check,
            **kwargs,
        )
        return df

    elif service == 'site':
        df, _ = get_info(sites=sites, ssl_check=ssl_check, **kwargs)
        return df

    elif service == 'measurements':
        df, _ = get_discharge_measurements(
            site_no=sites, begin_date=start, end_date=end, ssl_check=ssl_check, **kwargs
        )
        return df

    elif service == 'peaks':
        df, _ = get_discharge_peaks(
            site_no=sites,
            begin_date=start,
            end_date=end,
            multi_index=multi_index,
            ssl_check=ssl_check,
            **kwargs,
        )
        return df

    elif service == 'gwlevels':
        df, _ = get_gwlevels(
            sites=sites,
            startDT=start,
            endDT=end,
            multi_index=multi_index,
            datetime_index=datetime_index,
            ssl_check=ssl_check,
            **kwargs,
        )
        return df

    elif service == 'pmcodes':
        df, _ = get_pmcodes(ssl_check=ssl_check, **kwargs)
        return df

    elif service == 'water_use':
        df, _ = get_water_use(state=state, ssl_check=ssl_check, **kwargs)
        return df

    elif service == 'ratings':
        df, _ = get_ratings(site=sites, ssl_check=ssl_check, **kwargs)
        return df

    elif service == 'stat':
        df, _ = get_stats(sites=sites, ssl_check=ssl_check, **kwargs)
        return df

    else:
        raise TypeError(f'{service} service not yet implemented')


def _read_json(json):
    """
    Reads a NWIS Water Services formatted JSON into a ``pandas.DataFrame``.

    Parameters
    ----------
    json: dict
        A JSON dictionary response to be parsed into a ``pandas.DataFrame``

    Returns
    -------
    df: ``pandas.DataFrame``
        Times series data from the NWIS JSON
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    """
    merged_df = pd.DataFrame(columns=['site_no', 'datetime'])

    site_list = [
        ts['sourceInfo']['siteCode'][0]['value'] for ts in json['value']['timeSeries']
    ]

    # create a list of indexes for each change in site no
    # for example, [0, 21, 22] would be the first and last indeces
    index_list = [0]
    index_list.extend(
        [i + 1 for i, (a, b) in enumerate(zip(site_list[:-1], site_list[1:])) if a != b]
    )
    index_list.append(len(site_list))

    for i in range(len(index_list) - 1):
        start = index_list[i]  # [0]
        end = index_list[i + 1]  # [21]

        # grab a block containing timeseries 0:21,
        # which are all from the same site
        site_block = json['value']['timeSeries'][start:end]
        if not site_block:
            continue

        site_no = site_block[0]['sourceInfo']['siteCode'][0]['value']
        site_df = pd.DataFrame(columns=['datetime'])

        for timeseries in site_block:
            param_cd = timeseries['variable']['variableCode'][0]['value']
            # check whether min, max, mean record XXX
            option = timeseries['variable']['options']['option'][0].get('value')

            # loop through each parameter in timeseries, then concat to the merged_df
            for parameter in timeseries['values']:
                col_name = param_cd
                method = parameter['method'][0]['methodDescription']

                # if len(timeseries['values']) > 1 and method:
                if method:
                    # get method, format it, and append to column name
                    method = method.strip('[]()').lower()
                    col_name = f'{col_name}_{method}'

                if option:
                    col_name = f'{col_name}_{option}'

                record_json = parameter['value']

                if not record_json:
                    # no data in record
                    continue
                # should be able to avoid this by dumping
                record_json = str(record_json).replace("'", '"')

                # read json, converting all values to float64 and all qualifiers
                # Lists can't be hashed, thus we cannot df.merge on a list column
                record_df = pd.read_json(
                    StringIO(record_json),
                    orient='records',
                    dtype={'value': 'float64', 'qualifiers': 'unicode'},
                    convert_dates=False,
                )

                record_df['qualifiers'] = (
                    record_df['qualifiers'].str.strip('[]').str.replace("'", '')
                )

                record_df.rename(
                    columns={
                        'value': col_name,
                        'dateTime': 'datetime',
                        'qualifiers': col_name + '_cd',
                    },
                    inplace=True,
                )

                site_df = site_df.merge(record_df, how='outer', on='datetime')

        # end of site loop
        site_df['site_no'] = site_no
        merged_df = pd.concat([merged_df, site_df])

    # convert to datetime, normalizing the timezone to UTC when doing so
    if 'datetime' in merged_df.columns:
        merged_df['datetime'] = pd.to_datetime(merged_df['datetime'], utc=True)

    return merged_df


def _read_rdb(rdb):
    """
    Convert NWIS rdb table into a ``pandas.dataframe``.

    Parameters
    ----------
    rdb: string
        A string representation of an rdb table

    Returns
    -------
    df: ``pandas.dataframe``
        A formatted pandas data frame

    """
    count = 0

    for line in rdb.splitlines():
        # ignore comment lines
        if line.startswith('#'):
            count = count + 1

        else:
            break

    fields = re.split('[\t]', rdb.splitlines()[count])
    fields = [field.replace(',', '') for field in fields]
    dtypes = {
        'site_no': str,
        'dec_long_va': float,
        'dec_lat_va': float,
        'parm_cd': str,
        'parameter_cd': str,
    }

    df = pd.read_csv(
        StringIO(rdb),
        delimiter='\t',
        skiprows=count + 2,
        names=fields,
        na_values='NaN',
        dtype=dtypes,
    )

    df = format_response(df)
    return df


def _check_sites_value_types(sites):
    if sites:
        if not isinstance(sites, list) and not isinstance(sites, str):
            raise TypeError('sites must be a string or a list of strings')


class NWIS_Metadata(BaseMetadata):
    """Metadata class for NWIS service, derived from BaseMetadata.

    Attributes
    ----------
    url : str
        Response url
    query_time: datetme.timedelta
        Response elapsed time
    header: requests.structures.CaseInsensitiveDict
        Response headers
    comments: str | None
        Metadata comments, if any
    site_info: tuple[pd.DataFrame, NWIS_Metadata] | None
        Site information if the query included `site_no`, `sites`, `stateCd`,
        `huc`, `countyCd` or `bBox`. `site_no` is preferred over `sites` if
        both are present.
    variable_info: tuple[pd.DataFrame, NWIS_Metadata] | None
        Variable information if the query included `parameterCd`.

    """

    def __init__(self, response, **parameters) -> None:
        """Generates a standard set of metadata informed by the response with specific
        metadata for NWIS data.

        Parameters
        ----------
        response: Response
            Response object from requests module
        parameters: unpacked dictionary
            Unpacked dictionary of the parameters supplied in the request

        Returns
        -------
        md: :obj:`dataretrieval.nwis.NWIS_Metadata`
            A ``dataretrieval`` custom :obj:`dataretrieval.nwis.NWIS_Metadata` object.

        """
        super().__init__(response)

        comments = ''
        for line in response.text.splitlines():
            if line.startswith('#'):
                comments += line.lstrip('#') + '\n'
        if comments:
            self.comment = comments

        self._parameters = parameters

    @property
    def site_info(self) -> Optional[Tuple[pd.DataFrame, BaseMetadata]]:
        """
        Return
        ------
        df: ``pandas.DataFrame``
            Formatted requested data from calling `nwis.what_sites`
        md: :obj:`dataretrieval.nwis.NWIS_Metadata`
            A NWIS_Metadata object
        """
        if 'site_no' in self._parameters:
            return what_sites(sites=self._parameters['site_no'])

        elif 'sites' in self._parameters:
            return what_sites(sites=self._parameters['sites'])

        elif 'stateCd' in self._parameters:
            return what_sites(stateCd=self._parameters['stateCd'])

        elif 'huc' in self._parameters:
            return what_sites(huc=self._parameters['huc'])

        elif 'countyCd' in self._parameters:
            return what_sites(countyCd=self._parameters['countyCd'])

        elif 'bBox' in self._parameters:
            return what_sites(bBox=self._parameters['bBox'])

        else:
            return None  # don't set metadata site_info attribute

    @property
    def variable_info(self) -> Optional[Tuple[pd.DataFrame, BaseMetadata]]:
        # define variable_info metadata based on parameterCd if available
        if 'parameterCd' in self._parameters:
            return get_pmcodes(parameterCd=self._parameters['parameterCd'])
