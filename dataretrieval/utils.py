"""Data-shaping helpers, and the historical home of the legacy query path.

What is *defined* here is frame munging: building a UTC datetime column out of
the separate date/time/zone columns the older services return. The one-shot HTTP
query path that used to sit alongside it now lives in
:mod:`dataretrieval._querying`, which nothing here depends on -- the names below
are re-exported so their documented ``dataretrieval.utils`` paths keep resolving.

By default, do not add new service-specific behavior here.
"""

from __future__ import annotations

import warnings

import pandas as pd

import dataretrieval._querying as _querying
import dataretrieval.transport.http as _transport_http
from dataretrieval._ambient import Ambient  # noqa: F401 - compatibility re-export
from dataretrieval._response_metadata import (
    BaseMetadata,  # noqa: F401  — compatibility re-export; defined there now
)
from dataretrieval.codes import tz

# Compatibility names retained at their historical utility paths.
HTTPX_DEFAULTS = _transport_http.HTTPX_DEFAULTS
USER_AGENT = _transport_http.USER_AGENT
_default_headers = _transport_http.default_headers
_get = _transport_http.get
_network_error = _transport_http.network_error
# Public functions whose implementation moved to the private query module; this
# is the path they are documented at.
query = _querying.query
to_str = _querying.to_str


def format_datetime(
    df: pd.DataFrame, date_field: str, time_field: str, tz_field: str
) -> pd.DataFrame:
    """Create a datetime field from separate date, time, and time zone fields.

    Assumes ISO 8601.

    Parameters
    ----------
    df: ``pandas.DataFrame``
        A data frame containing date, time, and timezone fields.
    date_field: string
        Name of the date column in ``df``.
    time_field: string
        Name of the time column in ``df``.
    tz_field: string
        Name of the time zone column in ``df``.

    Returns
    -------
    df: ``pandas.DataFrame``
        The data frame with a formatted 'datetime' column.

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


# (time-suffix, tz-suffix) pairs that follow a "<prefix>Date" column.
_TIME_TZ_SUFFIXES = (
    # WQX3 / Samples, e.g.
    #   Activity_StartDate / Activity_StartTime / Activity_StartTimeZone
    ("Time", "TimeZone"),
    # Legacy WQP (slash-separated), e.g.
    #   ActivityStartDate / ActivityStartTime/Time / ActivityStartTime/TimeZoneCode
    ("Time/Time", "Time/TimeZoneCode"),
)


def _build_utc_datetime(
    date_series: pd.Series, time_series: pd.Series, tz_series: pd.Series
) -> pd.Series:
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
    return pd.to_datetime(
        combined, format="%Y-%m-%d %H:%M:%S %z", utc=True, errors="coerce"
    )


def _attach_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Append a UTC ``<prefix>DateTime`` column per Date/Time/TimeZone triplet.

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

    Rows are sorted (and the index reset) by the canonical activity-start
    datetime when present — ``Activity_StartDateTime`` (WQX3) or
    ``ActivityStartDateTime`` (legacy WQP) — falling back to the first
    detected ``*Date`` column. Mirrors R ``dataRetrieval``'s
    end-of-pipeline sort in ``importWQP.R``.

    Parameters
    ----------
    df : ``pandas.DataFrame``
        DataFrame returned from a Samples or WQP CSV endpoint.

    Returns
    -------
    df : ``pandas.DataFrame``
        A new DataFrame with derivable ``<prefix>DateTime`` columns appended
        and rows sorted by the activity-start datetime (if any date column
        was detected).
    """
    columns = set(df.columns)
    new_columns = {}
    first_date_col = None
    for col in df.columns:
        if not col.endswith("Date"):
            continue
        if first_date_col is None:
            first_date_col = col
        prefix = col.removesuffix("Date")
        target = prefix + "DateTime"
        if target in columns or target in new_columns:
            continue
        for time_suffix, tz_suffix in _TIME_TZ_SUFFIXES:
            time_col = prefix + time_suffix
            tz_col = prefix + tz_suffix
            if time_col in columns and tz_col in columns:
                new_columns[target] = _build_utc_datetime(
                    df[col], df[time_col], df[tz_col]
                )
                break
    if new_columns:
        # Concat in one shot — per-column assignment on a wide CSV-derived
        # frame triggers pandas' fragmentation PerformanceWarning.
        df = pd.concat([df, pd.DataFrame(new_columns, index=df.index)], axis=1)
    sort_key: str | None
    if "Activity_StartDateTime" in df.columns:
        sort_key = "Activity_StartDateTime"
    elif "ActivityStartDateTime" in df.columns:
        sort_key = "ActivityStartDateTime"
    else:
        sort_key = first_date_col
    if sort_key is not None:
        df = df.sort_values(by=sort_key, ignore_index=True)
    return df
