"""Coerce result columns to their declared types, warning on lost values.

One mechanism, used by every result path that promises a column a type: parse
the raw values, and when parsing turned a value that was *present* into
``NaN``/``NaT`` -- a parse failure rather than a missing value -- warn naming
the column and the count, so the two are distinguishable. A numeric column is
always ``float64`` (issue #428): ``pandas.to_numeric`` alone infers ``int64``
when every value happens to be whole, so the same column would change dtype
between calls, and ``float64`` also holds the ``NaN`` that marks a missing
measurement.

Which columns to coerce is each adapter's vocabulary (ADR 0013) and stays in
the adapter or its dialect; this leaf holds only the coercion. It depends on
pandas and nothing first-party (ADR 0003), so any result path can use it
without acquiring the rest of the package.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterable

import pandas as pd


def to_numeric(raw: pd.Series, *, name: str) -> pd.Series:
    """Coerce ``raw`` to ``float64``, warning on values that failed to parse.

    Always ``float64`` even when every value is whole (#428), so a column's
    dtype does not depend on its contents. ``name`` is the column name the
    warning should report; pass the caller's own column name.
    """
    parsed = pd.to_numeric(raw, errors="coerce").astype("float64")
    _warn_unparsed(raw, parsed, name, "numbers")
    return parsed


def to_datetime(raw: pd.Series, *, name: str, **kwargs: object) -> pd.Series:
    """Coerce ``raw`` to datetime, warning on values that failed to parse.

    ``kwargs`` are passed to :func:`pandas.to_datetime` (for a fixed
    ``format=`` or ``utc=True``); ``errors="coerce"`` is fixed here so an
    unparseable value becomes ``NaT`` and is reported rather than raising.
    ``name`` is the column name the warning should report.
    """
    parsed = pd.to_datetime(raw, errors="coerce", **kwargs)
    _warn_unparsed(raw, parsed, name, "datetimes")
    return parsed


def coerce_columns(
    df: pd.DataFrame,
    *,
    numeric: Iterable[str] = (),
    datetime: Iterable[str] = (),
) -> pd.DataFrame:
    """Coerce the named ``numeric`` and ``datetime`` columns of ``df`` in place.

    Only columns actually present are touched, so an adapter may name every
    column it ever coerces without checking which this response returned. The
    columns are processed in sorted order for a stable sequence of warnings.
    Returns ``df`` for chaining.
    """
    present = set(df.columns)
    for col in sorted(present.intersection(datetime)):
        df[col] = to_datetime(df[col], name=col)
    for col in sorted(present.intersection(numeric)):
        df[col] = to_numeric(df[col], name=col)
    return df


def _warn_unparsed(raw: pd.Series, parsed: pd.Series, name: str, kind: str) -> None:
    """Warn when coercion turned values that were present into ``NaN``/``NaT``.

    A value counts as present when it is neither null nor an empty string, so
    missing values reported either way do not trigger the warning. ``kind`` is
    the plural noun for the message (``"numbers"`` / ``"datetimes"``).
    """
    if not parsed.isna().any():
        return
    lost = int((parsed.isna() & raw.notna() & (raw != "")).sum())
    if lost:
        values = "1 value" if lost == 1 else f"{lost} values"
        warnings.warn(
            f"{values} in column {name!r} could not be parsed as {kind} and "
            "were set to missing. To inspect the raw values, repeat the call "
            "with convert_type=False.",
            UserWarning,
            stacklevel=2,
        )
