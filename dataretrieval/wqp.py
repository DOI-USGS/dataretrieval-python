"""
Tool for downloading data from the Water Quality Portal (https://waterqualitydata.us)

See https://waterqualitydata.us/webservices_documentation for API reference

.. todo::

    - implement other services like Organization, Activity, etc.

"""
import pandas as pd
from io import StringIO
from .utils import query, set_metadata as set_md
import warnings


def get_results(**kwargs):
    """Query the WQP for results.

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
    lat: string
        Latitude for radial search, expressed in decimal degrees, WGS84
    long: string
        Longitude for radial search
    within: string
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
    .. code::

        >>> # Get results within a radial distance of a point
        >>> df, md = dataretrieval.wqp.get_results(
        ...     lat='44.2', long='-88.9', within='0.5')

        >>> # Get results within a bounding box
        >>> df, md = dataretrieval.wqp.get_results(
        ...     bBox='-92.8,44.2,-88.9,46.0')

    """
    kwargs = _alter_kwargs(kwargs)
    response = query(wqp_url('Result'), kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')
    return df, set_metadata(response)


def what_sites(**kwargs):
    """Search WQP for sites within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get sites within a radial distance of a point
        >>> df, md = dataretrieval.wqp.what_sites(
        ...     lat='44.2', long='-88.9', within='2.5')

    """
    kwargs = _alter_kwargs(kwargs)

    url = wqp_url('Station')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_organizations(**kwargs):
    """Search WQP for organizations within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get all organizations in the WQP
        >>> df, md = dataretrieval.wqp.what_organizations()

    """
    kwargs = _alter_kwargs(kwargs)

    url = wqp_url('Organization')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_projects(**kwargs):
    """Search WQP for projects within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get projects within a HUC region
        >>> df, md = dataretrieval.wqp.what_projects(huc='19')

    """
    kwargs = _alter_kwargs(kwargs)

    url = wqp_url('Project')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_activities(**kwargs):
    """Search WQP for activities within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get activities within Washington D.C.
        >>> # during a specific time period
        >>> df, md = dataretrieval.wqp.what_activities(
        ...     statecode='US:11', startDateLo='12-30-2019',
        ...     startDateHi='01-01-2020')

    """
    kwargs = _alter_kwargs(kwargs)

    url = wqp_url('Activity')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_detection_limits(**kwargs):
    """Search WQP for result detection limits within a region with specific
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
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get detection limits for Nitrite measurements in Rhode Island
        >>> # between specific dates
        >>> df, md = dataretrieval.wqp.what_detection_limits(
        ...     statecode='US:44', characteristicName='Nitrite',
        ...     startDateLo='01-01-2021', startDateHi='02-20-2021')

    """
    kwargs = _alter_kwargs(kwargs)

    url = wqp_url('ResultDetectionQuantitationLimit')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_habitat_metrics(**kwargs):
    """Search WQP for habitat metrics within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get habitat metrics for a state (Rhode Island in this case)
        >>> df, md = dataretrieval.wqp.what_habitat_metrics(
        ...     statecode='US:44')

    """
    kwargs = _alter_kwargs(kwargs)

    url = wqp_url('BiologicalMetric')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_project_weights(**kwargs):
    """Search WQP for project weights within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get project weights for a state (North Dakota in this case)
        >>> # within a set time period
        >>> df, md = dataretrieval.wqp.what_project_weights(
        ...     statecode='US:38', startDateLo='01-01-2006',
        ...     startDateHi='01-01-2009')

    """
    kwargs = _alter_kwargs(kwargs)

    url = wqp_url('ProjectMonitoringLocationWeighting')
    response = query(url, payload=kwargs, delimiter=';')

    df = pd.read_csv(StringIO(response.text), delimiter=',')

    return df, set_metadata(response)


def what_activity_metrics(**kwargs):
    """Search WQP for activity metrics within a region with specific data.

    Parameters
    ----------
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.wqp.get_results`

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.Metadata`
        Custom metadata object pertaining to the query.

    Examples
    --------
    .. code::

        >>> # Get activity metrics for a state (North Dakota in this case)
        >>> # within a set time period
        >>> df, md = dataretrieval.wqp.what_activity_metrics(
        ...     statecode='US:38', startDateLo='07-01-2006',
        ...     startDateHi='12-01-2006')

    """
    kwargs = _alter_kwargs(kwargs)

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


def _alter_kwargs(kwargs):
    """Private function to manipulate **kwargs.

    Not all query parameters are currently supported by ``dataretrieval``,
    so this function is used to set some of them and raise warnings to the
    user so they are aware of which are being hard-set.

    """
    if kwargs.get('zip', 'no') == 'yes':
        warnings.warn('Compressed data not yet supported, zip set to no.')
    kwargs['zip'] = 'no'

    if kwargs.get('mimeType', 'csv') == 'geojson':
        warnings.warn('GeoJSON not yet supported, mimeType set to csv.')
    kwargs['mimeType'] = 'csv'

    return kwargs
