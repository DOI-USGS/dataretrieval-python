"""Date and datetime marshalling for OGC time parameters.

Pure helpers that render the OGC getters' time-shaped arguments — single
instants, two-element ``[start, end]`` ranges, ISO-8601 durations, and open
``..`` bounds — into the wire form the API expects. No I/O, no engine state.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import date as _date
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M%z",
    "%Y-%m-%dT%H:%M",
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


# One element of a date argument: an ISO 8601 string, or a ``date``,
# ``datetime`` or ``pandas.Timestamp`` (both subclasses of ``datetime.date``),
# with ``None`` (or ``NaN`` / ``NaT``) for an open bound.
_DateLike = str | _date | None


def _is_blank(dt: _DateLike) -> bool:
    """True for a None, NaN, empty-string, or ``..`` element.

    Each is a spelling of an open bound, so ``["2024-01-01", ".."]`` means the
    same range as ``["2024-01-01", None]`` and the string ``"2024-01-01/.."``.
    """
    return dt is None or bool(pd.isna(dt)) or dt in ("", _OPEN_BOUND)


def _to_datetime(dt: str | _date, *, name: str) -> datetime:
    """Read one non-blank element as a ``datetime`` (naive iff it has no zone).

    A ``date`` is midnight of that day, like the string ``"2024-01-01"``.
    Raises ``ValueError`` naming *name* for an unreadable string or a value of
    another type.
    """
    if isinstance(dt, pd.Timestamp):
        # ``Timestamp.astimezone`` is ``tz_convert`` and requires a zone, so
        # hand ``_format_one`` a plain ``datetime``.
        converted: datetime = dt.to_pydatetime(warn=False)
        return converted
    if isinstance(dt, datetime):
        return dt
    if isinstance(dt, _date):
        return datetime(dt.year, dt.month, dt.day)  # noqa: DTZ001
    parsed = _parse_datetime(dt) if isinstance(dt, str) else None
    if parsed is None:
        raise ValueError(
            f"{name} could not be read as a date or datetime: {dt!r}. "
            "Pass an ISO 8601 date or datetime such as '2024-01-01' or "
            "'2024-01-01T12:00:00Z' (a datetime.date, datetime.datetime or "
            "pandas.Timestamp also works), and None for an open end of a range "
            "(['2024-01-01', None])."
        )
    return parsed


def _format_one(dt: _DateLike, *, date: bool, name: str) -> str:
    """Format a single datetime element for inclusion in the API time arg.

    Raises ``ValueError`` naming *name* when the element is not blank and
    cannot be read as a date or datetime.
    """
    if dt is None or _is_blank(dt):
        return _OPEN_BOUND
    parsed = _to_datetime(dt, name=name)
    # As in pandas, a naive value is UTC and an aware one is converted to UTC,
    # so the same call sends the same range on every machine.
    utc = parsed if parsed.tzinfo is None else parsed.astimezone(ZoneInfo("UTC"))
    if date:
        return utc.strftime("%Y-%m-%d")
    # Keep fractional seconds: truncating them changes a closed bound.
    fraction = f".{utc.microsecond:06d}".rstrip("0") if utc.microsecond else ""
    return f"{utc.strftime('%Y-%m-%dT%H:%M:%S')}{fraction}Z"


def _coerce_to_list(
    datetime_input: str | _date | Sequence[_DateLike],
    name: str = "date input",
) -> list[_DateLike]:
    """Normalize datetime input to a list, raising on invalid shapes."""
    if isinstance(datetime_input, (str, bytes)) or not isinstance(
        datetime_input, Iterable
    ):
        # A lone value (a ``date`` is not iterable; ``bytes`` would iterate as
        # integers); a non-date is rejected per element.
        return [datetime_input]
    if isinstance(datetime_input, Mapping):
        raise TypeError(
            f"{name} must be a date, a string, or a sequence of them, "
            f"not {type(datetime_input).__name__}."
        )
    return list(datetime_input)


def _split_interval(interval: str, *, name: str) -> tuple[str, str]:
    """Split a ``"start/end"`` string into its two sides.

    Each side is then formatted like a list element, so
    ``"2024-01-01T10:00:00/.."`` sends the same range as
    ``["2024-01-01T10:00:00", None]``.

    Raises ``ValueError`` naming *name* unless the string is two sides
    separated by one ``"/"``, neither of them a duration. The Water Data and
    NGWMN services reject ``"2024-01-01/P7D"`` and ``"P7D/2024-01-08"`` with
    HTTP 400.
    """
    sides = [side.strip() for side in interval.split("/")]
    if len(sides) != 2 or any(_DURATION_RE.match(side) for side in sides):
        raise ValueError(
            f"{name} is not a valid interval: {interval!r}. Pass a start and "
            "an end separated by '/', such as '2024-01-01/2024-12-31', with "
            "'..' for an open end ('2024-01-01/..'). Neither side can be a "
            "duration."
        )
    return sides[0], sides[1]


def _all_blank(items: list[_DateLike]) -> bool:
    """True when every element is None, NaN, the empty string, or ``..``."""
    return all(_is_blank(dt) for dt in items)


def _format_api_dates(
    datetime_input: str | _date | Sequence[_DateLike] | None,
    date: bool = False,
    *,
    name: str = "date input",
    single_value_hint: str = "an instant or a duration ('2020-01-01', 'P7D')",
    open_start: str = _OPEN_BOUND,
) -> str | None:
    """
    Formats date or datetime input(s) for use with an API.

    Handles single values or ranges, converting to ISO 8601 or date-only
    formats as needed.

    Parameters
    ----------
    datetime_input : Union[str, date, List[Optional[Union[str, date]]], None]
        A single date/datetime or a list of one or two of them. Each may be a
        ``datetime.date``, ``datetime.datetime`` or ``pandas.Timestamp`` (a
        naive one is read in the local time zone, as a naive string is), or a
        string. Strings accept formats like "%Y-%m-%d %H:%M:%S", ISO 8601 (with or
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
    open_start : str, optional
        The string written for an open range start (``".."`` by default).
        OGC API Features and STAC also allow an empty string
        (``"/2024-01-01"``), which an API whose firewall rejects ``"../"``
        requires; see :attr:`OgcDialect.open_start`. An open end is always
        ``".."``.

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
        is not blank matches no supported format or is of another type.

    Notes
    -----
    - A single blank/NA value returns None. In a two-value range, a blank/NA
      or ``".."`` endpoint is an open bound. An open end is written ``".."``
      (``"2024-01-01/.."``) and an open start as ``open_start``. The range is
      None only when *every* element is blank/NA/``".."``.
    - Supports ISO 8601 durations such as "P7D" and "PT36H", which are passed
      through unchanged.
    - A single string containing ``"/"`` is an interval: it is split into the
      two-value form, so ``"2024-01-01/.."`` and ``["2024-01-01", None]`` are
      formatted alike. A duration is not accepted as either side.
    - Converts datetimes to UTC and formats as ISO 8601 with 'Z' suffix when
      `date` is False. As in pandas, an input with an offset (``Z`` or
      ``+HH:MM``) is converted to UTC and a naive one is UTC; a date-only
      collection gets the UTC date.
    """
    if datetime_input is None:
        return None

    items = _coerce_to_list(datetime_input, name)
    if len(items) == 1 and isinstance(items[0], str) and "/" in items[0]:
        items = list(_split_interval(items[0], name=name))

    if _all_blank(items):
        return None

    if len(items) > 2:
        raise ValueError(
            f"{name} takes at most 2 values, got {len(items)}: {items!r}. "
            f"Pass one value for {single_value_hint}, "
            "or two for a closed interval ('2020-01-01', '2020-12-31')."
        )

    # Pass through a duration ("P7D", "PT36H")
    if len(items) == 1 and isinstance(items[0], str) and _DURATION_RE.match(items[0]):
        return items[0]

    formatted = [_format_one(dt, date=date, name=name) for dt in items]
    if len(formatted) == 2 and formatted[0] == _OPEN_BOUND:
        formatted[0] = open_start
    return "/".join(formatted)
