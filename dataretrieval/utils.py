"""
Useful utilities for data munging.
"""
import pandas as pd
from pandas.core.indexes.multi import MultiIndex
from pandas.core.indexes.datetimes import DatetimeIndex

from dataretrieval.codes import tz

def to_str(listlike):
    """Translates list-like objects into strings.

    Return:
        List-like object as string
    """
    if type(listlike) == list:
        return ','.join(listlike)

    elif type(listlike) == pd.core.series.Series:
        return ','.join(listlike.tolist())

    elif type(listlike) == pd.core.indexes.base.Index:
        return ','.join(listlike.tolist())

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


def mmerge_asof(left, right, tolerance=None, **kwargs):
    """Merges two dataframes with multi-index.

    Only works on two-level multi-index where the second level is a time.

    Parameters
    ----------
    left : DataFrame

    right : DataFrame

    tolerance : integer or Timedelta, optional, default None
        Select asof tolerance within this range; must be compatible with the merge index.

    Returns
    -------
    merged : DataFrame

    Examples
    --------
    TODO
    """
    # if not multiindex pass  to merge_asof
    if not isinstance(left.index, MultiIndex) and not isinstance(right.index, MultiIndex):
        return pd.merge_asof(left, right,
                             tolerance=tolerance,
                             right_index=True,
                             left_index=True)

    elif left.index.names != right.index.names:
        raise TypeError('Both indexes must have matching names')

    #check that lowest level is the datetime index

    #check that their are only two levels

    out_df = pd.DataFrame()
    dt_name = left.index.names[1]
    #TODO modify to handle more levels
    index_name = left.index.names[0]

    left_temp = left.reset_index().dropna(subset=[dt_name]).sort_values([dt_name])
    right_temp = right.reset_index().dropna(subset=[dt_name]).sort_values([dt_name])

    merged_df = pd.merge_asof(left_temp, right_temp,
                              tolerance=tolerance,
                              on=dt_name,
                              by=index_name)

    return merged_df.set_index([index_name, dt_name]).sort_index()

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
