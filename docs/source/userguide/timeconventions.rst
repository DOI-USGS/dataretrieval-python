.. timeconventions:

Datetime Information
--------------------

``dataretrieval`` attempts to normalize time data to UTC time when converting
web service data into dataframes. To do this, in-built pandas functions are
used; either :obj:`pandas.to_datetime()` during the initial datetime object
conversion, or :obj:`pandas.DataFrame.tz_localize()` if the datetime objects
exist but are not UTC-localized. In most cases (single-site and multi-site),
``dataretrieval`` assigns the datetime information as the dataframe *index*,
the exception to this is when incomplete datetime information is available, in
these cases integers are used as the dataframe index (see `PR#58`_ for more
details).

.. _PR#58: https://github.com/DOI-USGS/dataretrieval-python/pull/58


Inspecting Timestamps
*********************

For single sites, the index of the returned dataframe contains pandas
timestamps.

.. code:: python

    >>> import dataretrieval.nwis as nwis
    >>> site = '03339000'
    >>> df = nwis.get_record(sites=site, service='peaks',
    ...                      start='2015-01-01', end='2017-12-31')
    >>> print(df)
                              agency_cd   site_no peak_tm  peak_va peak_cd  gage_ht  gage_ht_cd  year_last_pk  ag_dt  ag_tm  ag_gage_ht  ag_gage_ht_cd
    datetime
    2015-06-08 00:00:00+00:00      USGS  03339000   17:30    25100       C    22.83         NaN           NaN    NaN    NaN         NaN            NaN
    2015-12-29 00:00:00+00:00      USGS  03339000   18:45    37600       C    26.66         NaN           NaN    NaN    NaN         NaN            NaN
    2017-05-05 00:00:00+00:00      USGS  03339000   04:45    17000       C    18.47         NaN           NaN    NaN    NaN         NaN            NaN

Here the index of the dataframe ``df`` is a set of datetime objects. Each has
the format, ``YYYY-MM-DD HH:MM:SS+HH:MM``. Because these timestamps are
localized to be in UTC, the expected offset (``+HH:MM``) is ``+00:00``.
These values can be converted to a local timezone of your choosing using
:obj:`pandas` functionality.

.. code:: python

    >>> df.index = df.index.tz_convert(tz='America/New_York')
    >>> print(df)
                              agency_cd   site_no peak_tm  peak_va peak_cd  gage_ht  gage_ht_cd  year_last_pk  ag_dt  ag_tm  ag_gage_ht  ag_gage_ht_cd
    datetime
    2015-06-07 20:00:00-04:00      USGS  03339000   17:30    25100       C    22.83         NaN           NaN    NaN    NaN         NaN            NaN
    2015-12-28 19:00:00-05:00      USGS  03339000   18:45    37600       C    26.66         NaN           NaN    NaN    NaN         NaN            NaN
    2017-05-04 20:00:00-04:00      USGS  03339000   04:45    17000       C    18.47         NaN           NaN    NaN    NaN         NaN            NaN

Above, the index was converted to localize the timestamps to New York.
In the updated dataframe index, the resulting timestamps now have offsets of
``-04:00`` and ``-05:00`` as New York is either 4 or 5 hours behind UTC
depending on the time of year (due to daylight savings).

When information for multiple sites is requested, ``dataretrieval`` creates a
dataframe with a multi-index, with the first entry containing the site number,
and the second containing the datetime information.

.. doctest::

    >>> import dataretrieval.nwis as nwis
    >>> sites = ['180049066381200', '290000095192602']
    >>> df = nwis.get_record(sites=sites, service='gwlevels',
    ...                      start='2021-10-01', end='2022-01-01')
    >>> df
                                              agency_cd site_tp_cd      lev_dt lev_tm lev_tz_cd  ...  lev_dt_acy_cd  lev_acy_cd  lev_src_cd  lev_meth_cd lev_age_cd
    site_no         datetime                                                                     ...
    180049066381200 2021-10-04 19:54:00+00:00      USGS         GW  2021-10-04  19:54     +0000  ...              m         NaN           S            S          A
                    2021-11-16 14:28:00+00:00      USGS         GW  2021-11-16  14:28     +0000  ...              m         NaN           S            S          A
                    2021-12-09 10:43:00+00:00      USGS         GW  2021-12-09  10:43     +0000  ...              m         NaN           S            S          A
    290000095192602 2021-12-08 19:07:00+00:00      USGS         GW  2021-12-08  19:07     +0000  ...              m         NaN           S            S          P
    <BLANKLINE>
    [4 rows x 15 columns]

Here note that the default datetime index information returned is also UTC
localized, and therefore the offset values are ``+00:00``.