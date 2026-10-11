"""Writing a date range as an OGC time parameter.

A getter reads each date argument into a :class:`~dataretrieval._dates.DateRange`
when it is called; this module writes that value in the wire form the API
expects -- an instant, a ``start/end`` interval with ``..`` open bounds, or a
duration. No I/O, no engine state.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from dataretrieval._dates import OPEN_BOUND, DateRange

# OGC API parameters that take a date/datetime value (single string,
# two-element range, or interval/duration string) rather than a multi-value
# string list. ``prepare_request_args`` reads them into a ``DateRange``,
# ``_construct_api_requests`` writes them with ``_format_date_range``, and the
# chunk planner never splits them.
_DATE_RANGE_PARAMS = frozenset(
    {"datetime", "last_modified", "begin", "begin_utc", "end", "end_utc", "time"}
)


def _format_bound(bound: datetime | None, *, date: bool) -> str:
    """Write one bound: ``YYYY-MM-DD``, a UTC ``...Z`` datetime, or ``..``."""
    if bound is None:
        return OPEN_BOUND
    if date:
        return bound.strftime("%Y-%m-%d")
    # A naive bound is local time. Use ``.astimezone()`` rather than a fixed
    # offset so each value is resolved against the DST rules for its own date.
    aware = bound if bound.tzinfo is not None else bound.astimezone()
    return aware.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_date_range(
    value: DateRange, *, date: bool = False, open_start: str = OPEN_BOUND
) -> str:
    """Write a date range as an OGC API time parameter.

    Parameters
    ----------
    value : DateRange
        What the caller meant, as read by
        :func:`~dataretrieval._dates.read_date_range`.
    date : bool, optional
        If True, write only the date (``YYYY-MM-DD``) of each bound. If False
        (default), write each bound as a UTC datetime (``YYYY-MM-DDTHH:MM:SSZ``),
        reading a naive bound in the local time zone.
    open_start : str, optional
        How the target API spells the open start of a range (``".."`` by
        default). OGC API Features and STAC also allow an empty string,
        ``"/2024-01-01"``, which an API whose firewall rejects ``"../"`` needs
        instead; see :attr:`OgcDialect.open_start`. An open end is always
        ``".."``.

    Returns
    -------
    str
        The duration unchanged, a single bound for an instant, or two bounds
        joined by ``"/"``.
    """
    if value.duration is not None:
        return value.duration
    if value.instant:
        return _format_bound(value.start, date=date)
    start = open_start if value.start is None else _format_bound(value.start, date=date)
    return f"{start}/{_format_bound(value.end, date=date)}"
