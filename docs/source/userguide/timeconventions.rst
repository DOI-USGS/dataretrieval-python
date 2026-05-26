.. timeconventions:

Datetime Information
--------------------

``dataretrieval`` normalizes time data to UTC when converting Water Data API
responses into data frames. Timestamps are returned in the ``time`` column (the
dataframe itself uses a default integer index). For sub-daily data — such as
continuous (instantaneous) values — ``time`` is a timezone-aware
``datetime64[us, UTC]`` column. Daily values represent a whole calendar day,
so their ``time`` column is timezone-naive (dates only).


Inspecting Timestamps
*********************

For continuous data, the ``time`` column holds UTC-localized pandas timestamps.

.. code:: python

    >>> from dataretrieval import waterdata
    >>> df, md = waterdata.get_continuous(
    ...     monitoring_location_id="USGS-05427718",
    ...     parameter_code="00060",
    ...     time="2024-03-01/2024-03-02",
    ... )
    >>> df["time"].head()
    0   2024-03-01 00:00:00+00:00
    1   2024-03-01 00:15:00+00:00
    2   2024-03-01 00:30:00+00:00
    3   2024-03-01 00:45:00+00:00
    4   2024-03-01 01:00:00+00:00
    Name: time, dtype: datetime64[us, UTC]

Each timestamp has the format ``YYYY-MM-DD HH:MM:SS+HH:MM``. Because the values
are localized to UTC, the offset (``+HH:MM``) is ``+00:00``. You can convert
them to a local timezone of your choosing with the pandas ``.dt`` accessor.

.. code:: python

    >>> df["time"] = df["time"].dt.tz_convert("America/New_York")
    >>> df["time"].head()
    0   2024-02-29 19:00:00-05:00
    1   2024-02-29 19:15:00-05:00
    2   2024-02-29 19:30:00-05:00
    3   2024-02-29 19:45:00-05:00
    4   2024-02-29 20:00:00-05:00
    Name: time, dtype: datetime64[us, America/New_York]

After conversion the timestamps carry New York's offset — ``-05:00`` during
standard time, or ``-04:00`` during daylight saving time, since New York is 4
or 5 hours behind UTC depending on the time of year. Note that the first
midnight-UTC reading rolls back to the previous calendar day (``2024-02-29``)
once shifted into New York time.


Daily values
************

Daily data summarize a whole calendar day, so the ``time`` column is
timezone-naive — no offset is applied.

.. code:: python

    >>> df, md = waterdata.get_daily(
    ...     monitoring_location_id="USGS-05427718",
    ...     parameter_code="00060",
    ...     time="2024-03-01/2024-03-05",
    ... )
    >>> df["time"].head()
    0   2024-03-01
    1   2024-03-02
    2   2024-03-03
    3   2024-03-04
    4   2024-03-05
    Name: time, dtype: datetime64[us]
