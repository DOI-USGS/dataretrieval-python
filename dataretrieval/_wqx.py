"""WQX3 / legacy-WQP CSV column conventions.

The Samples database and the Water Quality Portal both split an instant across
three columns -- a date, a time, and a time-zone abbreviation -- and they spell
the trio two different ways. Recognizing either spelling and folding it into one
UTC column is knowledge about those response formats, so it lives in its own
leaf rather than in :mod:`dataretrieval.utils`, whose docstring reserves that
module for shaping that is not service-specific.

Depends on pandas and the time-zone table only; nothing here issues a request.
"""

from __future__ import annotations

import pandas as pd

from dataretrieval.codes import tz

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


__all__ = ["_attach_datetime_columns", "_build_utc_datetime"]
