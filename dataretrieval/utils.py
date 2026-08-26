"""Data-shaping helpers, and the historical home of the legacy query path.

What is *defined* here is frame munging that names no service: building a UTC
datetime column out of the separate date/time/zone columns a caller points at.
The one-shot HTTP query path that used to sit alongside it now lives in
:mod:`dataretrieval._querying`, and the WQX3 / legacy-WQP column conventions
live in :mod:`dataretrieval._wqx`; nothing here depends on either -- the names
below are re-exported so their documented ``dataretrieval.utils`` paths keep
resolving.

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
