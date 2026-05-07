"""
Useful utilities for data munging.
"""

import warnings
from collections.abc import Iterable

import pandas as pd
import requests

import dataretrieval
from dataretrieval.codes import tz


def to_str(listlike, delimiter=","):
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

        >>> dataretrieval.utils.to_str([1, "a", 2])
        '1,a,2'

        >>> dataretrieval.utils.to_str([0, 10, 42], delimiter="+")
        '0+10+42'

    """
    if isinstance(listlike, str):
        return listlike

    if isinstance(listlike, Iterable):
        return delimiter.join(map(str, listlike))

    return None


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

    df["datetime"] = pd.to_datetime(
        df[date_field] + " " + df[time_field] + " " + df[tz_field],
        format="mixed",
        utc=True,
    )

    # if there are any incomplete dates, warn the user
    if df["datetime"].isna().any():
        count = df["datetime"].isna().sum()
        warnings.warn(
            f"Warning: {count} incomplete dates found, "
            + "consider setting datetime_index to False.",
            UserWarning,
            stacklevel=2,
        )

    return df


# Triplet patterns we recognize in WQP and Samples CSV responses. Each entry
# defines how to derive the time/timezone column names from a date column, and
# the suffix to strip when forming the new <prefix>DateTime column name.
_DATETIME_TRIPLET_PATTERNS = (
    # WQX3 / Samples: Activity_StartDate, Activity_StartTime, Activity_StartTimeZone
    {
        "date_suffix": "Date",
        "time_from_date": lambda d: d[: -len("Date")] + "Time",
        "tz_from_date": lambda d: d[: -len("Date")] + "TimeZone",
    },
    # Legacy WQP: <X>Date, <X>Time/Time, <X>Time/TimeZoneCode
    {
        "date_suffix": "Date",
        "time_from_date": lambda d: d[: -len("Date")] + "Time/Time",
        "tz_from_date": lambda d: d[: -len("Date")] + "Time/TimeZoneCode",
    },
)


def _build_utc_datetime(date_series, time_series, tz_series):
    """Combine date + time + tz-abbreviation columns into a UTC pandas Series.

    Unknown timezone codes (and rows missing any of the three values) yield
    ``NaT``. The input columns are not mutated.
    """
    offsets = tz_series.map(tz)
    combined = (
        date_series.astype("string")
        + " "
        + time_series.astype("string")
        + " "
        + offsets.astype("string")
    )
    # Rows where any input is missing produce a string containing "<NA>"; mark
    # those so pd.to_datetime returns NaT rather than guessing.
    invalid = (
        date_series.isna() | time_series.isna() | tz_series.isna() | offsets.isna()
    )
    combined = combined.mask(invalid)
    return pd.to_datetime(combined, format="mixed", utc=True, errors="coerce")


def attach_datetime_columns(df):
    """Add ``<prefix>DateTime`` UTC columns for any Date/Time/TimeZone triplets.

    Detects two naming patterns that appear in USGS Samples and Water Quality
    Portal CSV responses:

    * **WQX3** — ``<prefix>Date``, ``<prefix>Time``, ``<prefix>TimeZone``
    * **Legacy WQP** — ``<prefix>Date``, ``<prefix>Time/Time``,
      ``<prefix>Time/TimeZoneCode``

    For every triplet present, a new ``<prefix>DateTime`` column is appended
    holding a UTC ``Timestamp`` (offsets resolved via
    :data:`dataretrieval.codes.tz`). The original Date/Time/TimeZone columns
    are left intact, and an existing ``<prefix>DateTime`` column is never
    overwritten.

    Parameters
    ----------
    df : ``pandas.DataFrame``
        DataFrame returned from a Samples or WQP CSV endpoint.

    Returns
    -------
    df : ``pandas.DataFrame``
        A DataFrame with any derivable ``<prefix>DateTime`` columns appended.
        Callers should use the returned value (the helper may concatenate
        rather than mutate in place).
    """
    columns = set(df.columns)
    new_columns = {}
    for col in df.columns:
        if not col.endswith("Date"):
            continue
        for pattern in _DATETIME_TRIPLET_PATTERNS:
            time_col = pattern["time_from_date"](col)
            tz_col = pattern["tz_from_date"](col)
            if time_col not in columns or tz_col not in columns:
                continue
            target = col[: -len("Date")] + "DateTime"
            if target in columns or target in new_columns:
                break
            new_columns[target] = _build_utc_datetime(df[col], df[time_col], df[tz_col])
            break
    if not new_columns:
        return df
    # Concat in one shot — appending columns one-by-one to a wide CSV-derived
    # frame triggers pandas' fragmentation PerformanceWarning.
    return pd.concat([df, pd.DataFrame(new_columns, index=df.index)], axis=1)


class BaseMetadata:
    """Base class for metadata.

    Attributes
    ----------
    url : str
        Response url
    query_time: datetme.timedelta
        Response elapsed time
    header: requests.structures.CaseInsensitiveDict
        Response headers

    """

    def __init__(self, response) -> None:
        """Generates a standard set of metadata informed by the response.

        Parameters
        ----------
        response: Response
            Response object from requests module

        Returns
        -------
        md: :obj:`dataretrieval.utils.BaseMetadata`
            A ``dataretrieval`` custom :obj:`dataretrieval.utils.BaseMetadata` object.

        """

        # These are built from the API response
        self.url = response.url
        self.query_time = response.elapsed
        self.header = response.headers
        self.comment = None

        # # not sure what statistic_info is
        # self.statistic_info = None

        # # disclaimer seems to be only part of importWaterML1
        # self.disclaimer = None

    # These properties are to be set by `nwis` or `wqp`-specific metadata classes.
    @property
    def site_info(self):
        raise NotImplementedError(
            "site_info must be implemented by utils.BaseMetadata children"
        )

    @property
    def variable_info(self):
        raise NotImplementedError(
            "variable_info must be implemented by utils.BaseMetadata children"
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(url={self.url})"


def query(url, payload, delimiter=",", ssl_check=True):
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
    ssl_check: bool
        If True, check SSL certificates, if False, do not check SSL,
        default is True

    Returns
    -------
    string: query response
        The response from the API query ``requests.get`` function call.
    """

    for key, value in payload.items():
        payload[key] = to_str(value, delimiter)
    # for index in range(len(payload)):
    #    key, value = payload[index]
    #    payload[index] = (key, to_str(value))

    # define the user agent for the query
    user_agent = {"user-agent": f"python-dataretrieval/{dataretrieval.__version__}"}

    response = requests.get(url, params=payload, headers=user_agent, verify=ssl_check)

    if response.status_code == 400:
        raise ValueError(
            f"Bad Request, check that your parameters are correct. URL: {response.url}"
        )
    elif response.status_code == 404:
        raise ValueError(
            "Page Not Found Error. May be the result of an empty query. "
            + f"URL: {response.url}"
        )
    elif response.status_code == 414:
        _reason = response.reason
        _example = """
                    # n is the number of chunks to divide the query into \n
                    split_list = np.array_split(site_list, n)
                    data_list = []  # list to store chunk results in \n
                    # loop through chunks and make requests \n
                    for site_list in split_list: \n
                        data = nwis.get_record(sites=site_list, service='dv', \n
                                               start=start, end=end) \n
                        data_list.append(data)  # append results to list"""
        raise ValueError(
            "Request URL too long. Modify your query to use fewer sites. "
            + f"API response reason: {_reason}. Pseudo-code example of how to "
            + f"split your query: \n {_example}"
        )
    elif response.status_code in [500, 502, 503]:
        raise ValueError(
            f"Service Unavailable: {response.status_code} {response.reason}. "
            + f"The service at {response.url} may be down or experiencing issues."
        )

    if response.text.startswith("No sites/data"):
        raise NoSitesError(response.url)

    return response


class NoSitesError(Exception):
    """Custom error class used when selection criteria returns no sites/data."""

    def __init__(self, url):
        self.url = url

    def __str__(self):
        return (
            "No sites/data found using the selection criteria specified in "
            f"url: {self.url}"
        )
