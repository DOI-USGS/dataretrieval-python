"""
Tool for downloading data from teh Water Quality Portal (https://waterquality.data.us)

See www.waterqualitydata.us/webservices_documentation for API reference

TODO:
    - implement other services like Organization, Acticity, etc.
"""
import pandas as pd
from io import StringIO
from dataretrieval.nwis import query


def get_results(**kwargs):
    """
    Parameters
    ----------
    siteid : string
        Concatenate an agency code, a hyphen ("-"), and a site-identification number.

    statecode : string
        (Example: Illinois is US:17)

    countycode : string

    huc : string
        One or more eight-digit hydrologic units, delimited by semicolons.

    bBox : string
        (Example: bBox=-92.8,44.2,-88.9,46.0)

    lat : float
        Latitude for radial search, expressed in decimal degrees, WGS84

    long :
        Longitude for radial search

    within :

    pCode : string
        One or more five-digit USGS parameter codes, separated by semicolons. NWIS only.

    startDateLo : string
        Date of earliest desired data-collection activity, expressed as MM-DD-YYYY

    startDateHi : string
        Date of last desired data-collection activity, expressed as MM-DD-YYYY

    characteristicName : string
        One or more case-sensitive characteristic names, separated by semicolons.
        (See https://www.waterqualitydata.us/public_srsnames/ for available characteristic names)

    mimeType : string (csv)

    zip : string (yes or no)
    """
    kwargs['zip'] = 'no'
    kwargs['mimeType'] = 'csv'

    response = query(wqp_url('Result'), **kwargs)

    df = pd.read_csv(StringIO(response), delimiter=',')
    return df


def what_sites(**kwargs):
    """ Search WQP for sites within a region with specific data.

    Parameters
    ----------
    same as get_results
    """
    kwargs['zip'] = 'no'
    kwargs['mimeType'] = 'csv'

    url = wqp_url('Station')
    query_string = query(url, **kwargs)

    df = pd.read_csv(StringIO(query_string), delimiter=',')

    return df


def wqp_url(service):
    base_url = 'https://www.waterqualitydata.us/'
    return '{}{}/Search?'.format(base_url, service)
