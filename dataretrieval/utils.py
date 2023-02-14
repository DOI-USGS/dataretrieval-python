"""
Useful utilities for data munging.
"""
import warnings
import pandas as pd
import requests
import dataretrieval
from dataretrieval.codes import tz


def to_str(listlike, delimiter=','):
    """Translates list-like objects into strings.

    Parameters
    ----------
    listlike: list-like object
        An object that is a list, or list-like
        (e.g., ``pandas.core.series.Series``)
    delimiter: string, optional
        The delimiter that is placed between entries in listlike when it is
        turned into a string. Default value is a comma.

    Returns
    -------
    listlike: string
        The listlike object as string separated by the delimiter

    Examples
    --------
    .. doctest::

        >>> dataretrieval.utils.to_str([1, 'a', 2])
        '1,a,2'

        >>> dataretrieval.utils.to_str([0, 10, 42], delimiter='+')
        '0+10+42'

    """
    if type(listlike) == list:
        return delimiter.join([str(x) for x in listlike])

    elif type(listlike) == pd.core.series.Series:
        return delimiter.join(listlike.tolist())

    elif type(listlike) == pd.core.indexes.base.Index:
        return delimiter.join(listlike.tolist())

    elif type(listlike) == str:
        return listlike


def format_datetime(df, date_field, time_field, tz_field):
    """Creates a datetime field from separate date, time, and
    time zone fields.

    Assumes ISO 8601.

    Parameters
    ----------
    df: ``pandas.DataFrame``
        A data frame containing date, time, and timezone fields.
    date_field: string
        Name of date column in df.
    time_field: string
        Name of time column in df.
    tz_field: string
        Name of time zone column in df.

    Returns
    -------
    df: ``pandas.DataFrame``
        The data frame with a formatted 'datetime' column

    """
    # create a datetime index from the columns in qwdata response
    df[tz_field] = df[tz_field].map(tz)

    df['datetime'] = pd.to_datetime(df[date_field] + ' ' +
                                    df[time_field] + ' ' +
                                    df[tz_field],
                                    format='%Y-%m-%d %H:%M',
                                    utc=True)

    # if there are any incomplete dates, warn the user
    if any(pd.isna(df['datetime'])):
        count = sum(pd.isna(df['datetime']) == True)
        warnings.warn(
            f'Warning: {count} incomplete dates found, ' +
            'consider setting datetime_index to False.', UserWarning)

    return df


#This function may be deprecated once pandas.update support joins besides left.
def update_merge(left, right, na_only=False, on=None, **kwargs):
    """Performs a combination update and merge.

    Parameters
    ----------
    left: ``pandas.DataFrame``
        Original data
    right: ``pandas.DataFrame``
        Updated data
    na_only: bool
        If True, only update na values

    Returns
    -------
    df: ``pandas.DataFrame``
        Updated data frame

    .. todo::

        add na_only parameter support

    """
    #df = left.merge(right, how='outer',
    #                left_index=True, right_index=True)
    df = left.merge(right, how='outer', on=on, **kwargs)


    # check for column overlap and resolve update
    for column in df.columns:
        #if duplicated column, use the value from right
        if column[-2:] == '_x':
            name = column[:-2] # find column name

            if na_only:
                df[name] = df[name+'_x'].fillna(df[name+'_y'])

            else:
                df[name] = df[name+'_x'].update(df[name+'_y'])

            df.drop([name + '_x', name + '_y'], axis=1, inplace=True)

    return df


class Metadata:
    """Custom class for metadata.
    """
    url = None
    query_time = None
    site_info = None
    header = None
    variable_info = None
    comment = None

    # note sure what statistic_info is
    statistic_info = None
    # disclaimer seems to be only part of importWaterML1
    disclaimer = None


def set_metadata(response):
    """Function to initialize and set metadata from an API response.
    """
    md = Metadata()
    md.url = response.url
    md.query_time = response.elapsed
    md.header = response.headers
    return md


def query(url, payload, delimiter=','):
    """Send a query.

    Wrapper for requests.get that handles errors, converts listed
    query parameters to comma separated strings, and returns response.

    Parameters
    ----------
    url: string
        URL to query
    payload: dict
        query parameters passed to ``requests.get``
    delimiter: string
        delimiter to use with lists

    Returns
    -------
    string: query response
        The response from the API query ``requests.get`` function call.
    """

    for key, value in payload.items():
        payload[key] = to_str(value, delimiter)
    #for index in range(len(payload)):
    #    key, value = payload[index]
    #    payload[index] = (key, to_str(value))

    # define the user agent for the query
    user_agent = {
        'user-agent': f"python-dataretrieval/{dataretrieval.__version__}"}

    response = requests.get(url, params=payload, headers=user_agent)

    if response.status_code == 400:
        raise ValueError("Bad Request, check that your parameters are correct. URL: {}".format(response.url))
    elif response.status_code == 404:
        raise ValueError(
            "Page Not Found Error. May be the result of an empty query. " +
            f"URL: {response.url}")
    elif response.status_code == 414:
        _reason = response.reason
        _example = """
                    split_list = np.array_split(site_list, n)  # n is number of chunks to divide query into \n
                    data_list = []  # list to store chunk results in \n
                    # loop through chunks and make requests \n
                    for site_list in split_list: \n
                        data = nwis.get_record(sites=site_list, service='dv', start=start, end=end) \n
                        data_list.append(data)  # append results to list"""
        raise ValueError(
            "Request URL too long. Modify your query to use fewer sites. " +
            f"API response reason: {_reason}. Pseudo-code example of how to " +
            f"split your query: \n {_example}"
            )

    if response.text.startswith('No sites/data'):
        raise NoSitesError(response.url)

    return response


class NoSitesError(Exception):
    """Custom error class used when selection criteria returns no sites/data.
    """
    def __init__(self, url):
        self.url = url

    def __str__(self):
        return "No sites/data found using the selection criteria specified in url: {}".format(self.url)
