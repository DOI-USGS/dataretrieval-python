"""
Useful utilities for data munging.
"""
import pandas as pd
import requests
from pandas.core.indexes.multi import MultiIndex
from pandas.core.indexes.datetimes import DatetimeIndex

from dataretrieval.codes import tz

def to_str(listlike, delimiter=','):
    """Translates list-like objects into strings.

    Return:
        List-like object as string
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
    df : DataFrame
        DataFrame containing date, time, and timezone fields.

    date_field : string
        Name of date column in df.

    time_field : string
        Name of time column in df.

    tz_field : string
        Name of time zone column in df.

    Returns
    -------
    df : DataFrame
    """

    #create a datetime index from the columns in qwdata response
    df[tz_field] = df[tz_field].map(tz)

    df['datetime'] = pd.to_datetime(df.pop(date_field) + ' ' +
                                    df.pop(time_field) + ' ' +
                                    df.pop(tz_field),
                                    format = '%Y-%m-%d %H:%M',
                                    utc=True)

    return df


#This function may be deprecated once pandas.update support joins besides left.
def update_merge(left, right, na_only=False, on=None, **kwargs):
    """Performs a combination update and merge.

    Args:
    left (DataFrame): original data
    right (DataFrame): updated data
    na_only (bool): if True, only update na values

    TODO: na_only
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
    md = Metadata()
    md.url = response.url
    md.query_time = response.elapsed
    md.header = response.headers
    return md


def query(url, payload, delimiter=','):
    """Send a query.

    Wrapper for requests.get that handles errors, converts listed
    query paramaters to comma separated strings, and returns response.

    Args:
        url :
        payload : query parameters passed to requests.get
        delimiter : delimeter to use with lists

    Returns:
        string : query response
    """

    for key, value in payload.items():
        payload[key] = to_str(value, delimiter)
    #for index in range(len(payload)):
    #    key, value = payload[index]
    #    payload[index] = (key, to_str(value))

    response = requests.get(url, params=payload)

    if response.status_code == 400:
        raise ValueError("Bad Request, check that your parameters are correct. URL: {}".format(response.url))

    if response.text.startswith('No sites/data'):
        raise NoSitesError(response.url)

    return response


class NoSitesError(Exception):
    def __init__(self, url):
        self.url = url

    def __str__(self):
        return "No sites/data found using the selection criteria specified in url: {}".format(self.url)
