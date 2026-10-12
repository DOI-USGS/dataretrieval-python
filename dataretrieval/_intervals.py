"""The ISO 8601 interval grammar OGC API Features and STAC share (ADR 0014).

A date argument is read into a :class:`~dataretrieval._dates.DateRange`; this
module writes that value as the ``datetime`` parameter of both standards -- an
instant, a ``start/end`` interval with ``..`` open bounds, or a duration --
adjusted by the API's :class:`DateDialect`. It names no service.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from dataretrieval._dates import OPEN_BOUND, DateRange


@dataclass(frozen=True)
class DateDialect:
    """How one API writes dates differently from the interval standard.

    Each default is the standard, so ``DateDialect()`` describes an API that
    follows it. Where a field is set, the declaration records the observed
    server behavior that requires it.

    Attributes
    ----------
    open_start : str
        The string written for the open start of a range: ``".."``
        (``"../2024-01-01"``, the default) or ``""`` (``"/2024-01-01"``). Both
        standards allow either, but each service accepts only one.
    durations : bool
        Whether a lone ISO 8601 duration (``"P7D"``, the last seven days) is
        accepted. Neither standard defines one, so the default is ``False``
        and a duration raises before any request is sent.
    """

    open_start: str = OPEN_BOUND
    durations: bool = False


def _write_bound(bound: datetime | None, *, date_only: bool) -> str:
    """One UTC bound: ``YYYY-MM-DD``, ``YYYY-MM-DDTHH:MM:SS[.ffffff]Z``, or ``..``."""
    if bound is None:
        return OPEN_BOUND
    if date_only:
        return bound.strftime("%Y-%m-%d")
    # Keep fractional seconds: truncating them changes a closed bound.
    fraction = f".{bound.microsecond:06d}".rstrip("0") if bound.microsecond else ""
    return f"{bound.strftime('%Y-%m-%dT%H:%M:%S')}{fraction}Z"


def write_interval(value: DateRange, dialect: DateDialect, *, date_only: bool) -> str:
    """Write a date range as the API's date parameter expects it.

    Parameters
    ----------
    value : DateRange
        What the caller meant, as read by
        :func:`~dataretrieval._dates.read_date_range`, which refuses a duration
        the dialect does not accept.
    dialect : DateDialect
        The API's date dialect; supplies ``open_start``.
    date_only : bool
        Write each bound as its UTC date, ``YYYY-MM-DD``, rather than as a UTC
        datetime; the caller's protocol decides which values are calendar days.
    """
    if value.duration is not None:
        return value.duration
    if value.instant:
        return _write_bound(value.start, date_only=date_only)
    start = (
        dialect.open_start
        if value.start is None
        else _write_bound(value.start, date_only=date_only)
    )
    return f"{start}/{_write_bound(value.end, date_only=date_only)}"
