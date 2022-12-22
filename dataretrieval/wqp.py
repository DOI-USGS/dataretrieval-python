"""
Tool for downloading data from the Water Quality Portal (https://waterqualitydata.us)

See https://waterqualitydata.us/webservices_documentation for API reference

.. todo::

    - implement other services like Organization, Activity, etc.

"""
import pandas as pd
from io import StringIO
from .utils import query, set_metadata as set_md


def get_results(**kwargs):
    """
    Parameters
    ----------
    siteid: string
        Concatenate an agency code, a hyphen ("-"), and a site-identification
        number.
    statecode: string
        Concatenate 'US', a colon (":"), and a FIPS numeric code
        (Example: Illinois is US:17)
    countycode: string
        A FIPS county code
    huc: string
        One or more eight-digit hydrologic units, delimited by semicolons.
    bBox: string
        Bounding box (Example: bBox=-92.8,44.2,-88.9,46.0)
    lat: float
        Latitude for radial search, expressed in decimal degrees, WGS84
    long: float
        Longitude for radial search
    within: float
        Distance for a radial search, expressed in decimal miles
    pCode: string
        One or more five-digit USGS parameter codes, separated by semicolons.
        NWIS only.
    startDateLo: string
        Date of earliest desired data-collection activity,
        expressed as 'MM-DD-YYYY'
    startDateHi: string
        Date of last desired data-collection activity,
        expressed as 'MM-DD-YYYY'
    characteristicName: string
        One or more case-sensitive characteristic names, separated by
        semicolons. (See https://www.waterqualitydata.us/public_srsnames/
        for available characteristic names)
    mimeType: string
        String specifying the output format which is 'csv' by default but can
        be 'geojson'
    zip: string
        Parameter to stream compressed data, if 'yes', or uncompressed data
        if 'no'. Default is 'no'.

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------
    """
    kwargs['zip'] = 'no'
    kwargs['mimeType'] = 'csv'
    response = query(wqp_url('Result'), kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')
    return df, set_metadata(response)


def what_sites(**kwargs):
    """ Search WQP for sites within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------
    .. doctest::

    """
    kwargs['zip'] = 'no'
    kwargs['mimeType'] = 'csv'

    url = wqp_url('Station')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_organizations(**kwargs):
    """ Search WQP for organizations within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------

    """
    kwargs['zip'] = 'no'
    kwargs['mimeType'] = 'csv'

    url = wqp_url('Organization')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_projects(**kwargs):
    """ Search WQP for projects within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------

    """
    kwargs['zip'] = 'no'
    kwargs['mimeType'] = 'csv'

    url = wqp_url('Project')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_activities(**kwargs):
    """ Search WQP for activities within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------

    """
    kwargs['zip'] = 'no'
    kwargs['mimeType'] = 'csv'

    url = wqp_url('Activity')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_detection_limits(**kwargs):
    """ Search WQP for result detection limits within a region with specific
    data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------

    """
    kwargs['zip'] = 'no'
    kwargs['mimeType'] = 'csv'

    url = wqp_url('ResultDetectionQuantitationLimit')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_habitat_metrics(**kwargs):
    """ Search WQP for habitat metrics within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------

    """
    kwargs['zip'] = 'no'
    kwargs['mimeType'] = 'csv'

    url = wqp_url('BiologicalMetric')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_project_weights(**kwargs):
    """ Search WQP for project weights within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------

    """
    kwargs['zip'] = 'no'
    kwargs['mimeType'] = 'csv'

    url = wqp_url('ProjectMonitoringLocationWeighting')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_activity_metrics(**kwargs):
    """ Search WQP for activity metrics within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom ``dataretrieval`` metadata object pertaining to the query.

    Examples
    --------

    """
    kwargs['zip'] = 'no'
    kwargs['mimeType'] = 'csv'

    url = wqp_url('ActivityMetric')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def wqp_url(service):
    """Construct the WQP URL for a given service.
    """
    base_url = 'https://www.waterqualitydata.us/data/'
    return '{}{}/Search?'.format(base_url, service)


def set_metadata(response, **parameters):
    """ Set metadata for WQP data.
    """
    md = set_md(response)  # initialize dataretrieval metadata object
    # populate the metadata using NWIS site information
    if 'sites' in parameters:
        md.site_info = lambda: what_sites(sites=parameters['sites'])
    elif 'site' in parameters:
        md.site_info = lambda: what_sites(sites=parameters['site'])
    elif 'site_no' in parameters:
        md.site_info = lambda: what_sites(sites=parameters['site_no'])
    return md
