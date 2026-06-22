"""Date and datetime marshalling for OGC time parameters.

Pure helpers that render the OGC getters' time-shaped arguments — single
instants, two-element ``[start, end]`` ranges, ISO-8601 durations, and open
``..`` bounds — into the wire form the API expects. No I/O, no engine state.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)


# Anchored to ``[Pp]\d`` so a normal word containing ``p`` (e.g. ``"Apr"``)
# doesn't get mis-classified as an ISO 8601 duration; the optional ``T``
# admits time-only forms like ``PT36H``.
_DURATION_RE = re.compile(r"^[Pp]T?\d")


# OGC API parameters that carry a date/datetime value (single string,
# two-element range, or interval/duration string) rather than a multi-value
# string list. Used by ``_construct_api_requests`` to keep them out of the
# POST/CQL2 multi-value path and to route them through ``_format_api_dates``,
# and by the default ``_get_args`` no-normalize set to bypass string-iterable
# normalization.
_DATE_RANGE_PARAMS = frozenset(
    {"datetime", "last_modified", "begin", "begin_utc", "end", "end_utc", "time"}
)


def _parse_datetime(value: str) -> datetime | None:
    """Parse a single datetime string against the supported formats.

    Returns a ``datetime`` (tz-aware iff the input carried a UTC offset),
    or ``None`` if no format matched.
    """
    # ``datetime.strptime`` accepts a numeric offset like ``+00:00`` but not
    # the ``Z`` shorthand, so normalize trailing ``Z`` first.
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(candidate, fmt)
        except ValueError:
            continue
    return None


def _format_one(dt: str | None, *, date: bool) -> str | None:
    """Format a single datetime element for inclusion in the API time arg."""
    if pd.isna(dt) or dt == "" or dt is None:
        return ".."
    parsed = _parse_datetime(dt)
    if parsed is None:
        return None
    if date:
        return parsed.strftime("%Y-%m-%d")
    # Naive inputs are interpreted in the system local zone (for backwards
    # compatibility). Use ``.astimezone()`` rather than a fixed offset so each
    # value is resolved against the DST rules for ITS OWN date — a frozen
    # ``datetime.now()`` offset shifted off-season inputs by an hour.
    aware = parsed if parsed.tzinfo is not None else parsed.astimezone()
    return aware.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_api_dates(
    datetime_input: str | Sequence[str | None] | None, date: bool = False
) -> str | None:
    """
    Formats date or datetime input(s) for use with an API.

    Handles single values or ranges, and converting to ISO 8601 or date-only
    formats as needed.

    Parameters
    ----------
    datetime_input : Union[str, List[Optional[str]], None]
        A single date/datetime string or a list of one or two date/datetime
        strings. Accepts formats like "%Y-%m-%d %H:%M:%S", ISO 8601 (with or
        without ``Z``/numeric offset), or relative periods (e.g., "P7D" /
        "PT36H"). Range endpoints may be ``None``/``NaN``/empty to denote a
        half-bounded range.
    date : bool, optional
        If True, uses only the date portion ("YYYY-MM-DD"). If False (default),
        returns full datetime in UTC ISO 8601 format ("YYYY-MM-DDTHH:MM:SSZ").

    Returns
    -------
    Union[str, None]
        - If input is a single value, returns the formatted date/datetime string
        or None if parsing fails.
        - If input is a list of two values, returns a date/datetime range string
        separated by "/" (e.g., "YYYY-MM-DD/YYYY-MM-DD" or
        "YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ").
        - Returns None if input is empty, all NA, or cannot be parsed.

    Raises
    ------
    ValueError
        If `datetime_input` contains more than two values.

    Notes
    -----
    - A single blank/NA value returns None. In a two-value range, a blank/NA
    endpoint is rendered as ``".."`` to denote an open bound (e.g.
    ``"2024-01-01/.."``); the range is only None when *every* element is
    blank/NA or any non-NA element fails to parse.
    - Supports ISO 8601 durations such as "P7D" and "PT36H" and pre-formatted
    intervals containing ``"/"``; both are passed through unchanged.
    - Converts datetimes to UTC and formats as ISO 8601 with 'Z' suffix when
    `date` is False. Inputs with an explicit offset (``Z`` or ``+HH:MM``) are
    converted from that offset to UTC; naive inputs are interpreted in the
    local time zone for backwards compatibility.
    """
    if datetime_input is None:
        return None

    # Convert single string to list for uniform processing
    if isinstance(datetime_input, str):
        datetime_input = [datetime_input]
    elif isinstance(datetime_input, Mapping):
        # `list(mapping)` returns keys, which silently accepts the wrong shape.
        raise TypeError(
            f"date input must be a string or sequence of strings, "
            f"not {type(datetime_input).__name__}."
        )
    elif not isinstance(datetime_input, (list, tuple)):
        # Materialize any other iterable (pandas.Series, numpy.ndarray,
        # generator, ...) so the len()/subscript operations below work.
        datetime_input = list(datetime_input)

    # Check for null or all NA and return None
    if all(pd.isna(dt) or dt == "" or dt is None for dt in datetime_input):
        return None

    if len(datetime_input) > 2:
        raise ValueError("datetime_input should only include 1-2 values")

    # Pass through duration ("P7D", "PT36H") and pre-formatted interval ("a/b")
    # strings untouched.
    if len(datetime_input) == 1 and isinstance(datetime_input[0], str):
        single = datetime_input[0]
        if _DURATION_RE.match(single) or "/" in single:
            return single

    # element invalidates the range.
    formatted: list[str] = []
    for dt in datetime_input:
        one = _format_one(dt, date=date)
        if one is None:
            return None
        formatted.append(one)
    return "/".join(formatted)
