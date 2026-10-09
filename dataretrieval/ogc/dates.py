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


# OGC API parameters that take a date/datetime value (single string,
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

    Returns a ``datetime`` (tz-aware iff the input included a UTC offset),
    or ``None`` if no format matched.
    """
    # ``datetime.strptime`` accepts a numeric offset like ``+00:00`` but not
    # the ``Z`` shorthand, so normalize trailing ``Z`` first.
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    for fmt in _DATETIME_FORMATS:
        try:
            # DTZ007: naive is the documented outcome for a naive input --
            # ``_DATETIME_FORMATS`` carries both the ``%z`` and the bare forms.
            return datetime.strptime(candidate, fmt)  # noqa: DTZ007
        except ValueError:
            continue
    return None


# The OGC API spelling of an open interval bound, as in ``"2024-01-01/.."``.
_OPEN_BOUND = ".."


def _is_blank(dt: str | None) -> bool:
    """True for a None, NaN, empty-string, or ``..`` element.

    Each is a spelling of an open bound, so ``["2024-01-01", ".."]`` means the
    same range as ``["2024-01-01", None]`` and the string ``"2024-01-01/.."``.
    """
    return dt is None or bool(pd.isna(dt)) or dt in ("", _OPEN_BOUND)


def _format_one(dt: str | None, *, date: bool, name: str) -> str:
    """Format a single datetime element for inclusion in the API time arg.

    Raises ``ValueError`` naming *name* when the element is not blank and
    matches no supported format.
    """
    if dt is None or _is_blank(dt):
        return _OPEN_BOUND
    parsed = _parse_datetime(dt)
    if parsed is None:
        raise ValueError(
            f"{name} could not be read as a date or datetime: {dt!r}. "
            "Pass an ISO 8601 date or datetime such as '2024-01-01' or "
            "'2024-01-01T12:00:00Z', and None for an open end of a range "
            "(['2024-01-01', None])."
        )
    if date:
        return parsed.strftime("%Y-%m-%d")
    # Naive inputs are interpreted in the system local zone (for backwards
    # compatibility). Use ``.astimezone()`` rather than a fixed offset so each
    # value is resolved against the DST rules for its own date — a fixed
    # ``datetime.now()`` offset shifted off-season inputs by an hour.
    aware = parsed if parsed.tzinfo is not None else parsed.astimezone()
    return aware.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")


def _coerce_to_list(
    datetime_input: str | Sequence[str | None],
    name: str = "date input",
) -> list[str | None]:
    """Normalize datetime input to a list, raising on invalid shapes."""
    if isinstance(datetime_input, str):
        return [datetime_input]
    if isinstance(datetime_input, Mapping):
        raise TypeError(
            f"{name} must be a string or sequence of strings, "
            f"not {type(datetime_input).__name__}."
        )
    return list(datetime_input)


def _is_passthrough(single: str) -> bool:
    """True when a single-element input should be returned as-is."""
    return bool(_DURATION_RE.match(single))


def _format_interval(interval: str, *, date: bool, name: str) -> str | None:
    """Format each side of a pre-formatted ``"start/end"`` interval string.

    Each side is formatted like an element of the two-value list form, so
    ``"2024-01-01T10:00:00/.."`` sends the same range as
    ``["2024-01-01T10:00:00", None]``. One side may instead be an ISO 8601
    duration paired with an instant (``"2024-01-01/P7D"``), which is kept
    unchanged. Returns None when both sides are open.

    Raises ``ValueError`` naming *name* when the string is not two sides
    separated by one ``"/"``, or a side cannot be read.
    """
    sides = interval.split("/")
    durations = [bool(_DURATION_RE.match(side)) for side in sides]
    if len(sides) != 2 or (
        any(durations) and (all(durations) or any(_is_blank(s) for s in sides))
    ):
        raise ValueError(
            f"{name} is not a valid interval: {interval!r}. Pass a start and "
            "an end separated by '/', such as '2024-01-01/2024-12-31', with "
            "'..' for an open end ('2024-01-01/..')."
        )
    formatted = [
        side if is_duration else _format_one(side, date=date, name=name)
        for side, is_duration in zip(sides, durations, strict=True)
    ]
    if formatted == [_OPEN_BOUND, _OPEN_BOUND]:
        return None
    return "/".join(formatted)


def _all_blank(items: list[str | None]) -> bool:
    """True when every element is None, NaN, the empty string, or ``..``."""
    return all(_is_blank(dt) for dt in items)


def _format_api_dates(
    datetime_input: str | Sequence[str | None] | None,
    date: bool = False,
    *,
    name: str = "date input",
    single_value_hint: str = "an instant or a duration ('2020-01-01', 'P7D')",
) -> str | None:
    """
    Formats date or datetime input(s) for use with an API.

    Handles single values or ranges, converting to ISO 8601 or date-only
    formats as needed.

    Parameters
    ----------
    datetime_input : Union[str, List[Optional[str]], None]
        A single date/datetime string or a list of one or two date/datetime
        strings. Accepts formats like "%Y-%m-%d %H:%M:%S", ISO 8601 (with or
        without ``Z``/numeric offset), or relative periods (e.g., "P7D" /
        "PT36H"). Range endpoints may be ``None``/``NaN``/empty or ``".."``
        to denote a half-bounded range.
    date : bool, optional
        If True, uses only the date portion ("YYYY-MM-DD"). If False (default),
        returns full datetime in UTC ISO 8601 format ("YYYY-MM-DDTHH:MM:SSZ").
    name : str, optional
        The caller's own spelling of this argument, used as the subject of
        every message raised here. Defaults to a generic "date input"; pass
        the real parameter name (``"time"``, ``"last_modified"``) so a caller
        correcting the error edits an argument their getter accepts.
    single_value_hint : str, optional
        How the "too many values" message describes an acceptable single
        value. Wording only -- a getter that rejects some of the default's
        forms (``get_ratings`` rejects durations) enforces that itself and
        passes a hint naming only what it accepts, so the remedy does not
        direct a caller to a value that is rejected.

    Returns
    -------
    Union[str, None]
        - If input is a single value, returns the formatted date/datetime string.
        - If input is a list of two values, returns a date/datetime range string
          separated by "/" (e.g., "YYYY-MM-DD/YYYY-MM-DD" or
          "YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ").
        - Returns None if input is None, empty, or every element is blank.

    Raises
    ------
    ValueError
        If `datetime_input` contains more than two values, or an element that
        is not blank matches no supported format.

    Notes
    -----
    - A single blank/NA value returns None. In a two-value range, a blank/NA
      or ``".."`` endpoint is rendered as ``".."`` to denote an open bound
      (e.g. ``"2024-01-01/.."``); the range is only None when *every* element
      is blank/NA/``".."``.
    - Supports ISO 8601 durations such as "P7D" and "PT36H", which are passed
      through unchanged.
    - A single string containing ``"/"`` is an interval: each side is formatted
      like an element of the two-value form, and may also be an ISO 8601
      duration paired with an instant (``"2024-01-01/P7D"``).
    - Converts datetimes to UTC and formats as ISO 8601 with 'Z' suffix when
      `date` is False. Inputs with an explicit offset (``Z`` or ``+HH:MM``) are
      converted from that offset to UTC; naive inputs are interpreted in the
      local time zone for backwards compatibility.
    """
    if datetime_input is None:
        return None

    items = _coerce_to_list(datetime_input, name)

    if _all_blank(items):
        return None

    if len(items) > 2:
        raise ValueError(
            f"{name} takes at most 2 values, got {len(items)}: {items!r}. "
            f"Pass one value for {single_value_hint}, "
            "or two for a closed interval ('2020-01-01', '2020-12-31')."
        )

    if len(items) == 1 and isinstance(items[0], str) and "/" in items[0]:
        return _format_interval(items[0], date=date, name=name)

    # Pass through duration ("P7D", "PT36H")
    if len(items) == 1 and isinstance(items[0], str) and _is_passthrough(items[0]):
        return items[0]

    return "/".join(_format_one(dt, date=date, name=name) for dt in items)
