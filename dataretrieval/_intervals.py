"""The ISO 8601 interval grammar OGC API Features and STAC share (ADR 0014).

A getter reads a date argument into a :class:`~dataretrieval._dates.DateRange`;
this module writes that value as the ``datetime`` parameter of both standards
-- an instant, a ``start/end`` interval with ``..`` open bounds, or a duration
-- adjusted by the endpoint's :class:`DateDialect`. It names no service.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from dataretrieval._dates import OPEN_BOUND, DateRange


@dataclass(frozen=True)
class DateDialect:
    """How one endpoint writes dates differently from the interval standard.

    Each default is the standard, so ``DateDialect()`` describes an endpoint
    that follows it.

    Attributes
    ----------
    open_start : str
        How the open start of a range is written: ``".."`` (``"../2024-01-01"``,
        the default) or ``""`` (``"/2024-01-01"``). Both standards allow
        either, but deployments refuse one or the other; each declaration
        records what its service was observed to refuse.
    durations : bool
        Whether a lone ISO 8601 duration (``"P7D"``, the last seven days) is
        accepted. Neither standard defines one, so the default is ``False`` and
        a duration raises when the getter is called.
    """

    open_start: str = OPEN_BOUND
    durations: bool = False


def _write_bound(bound: datetime | None, *, date: bool) -> str:
    """One bound: ``YYYY-MM-DD``, a UTC ``...Z`` datetime, or ``..``."""
    if bound is None:
        return OPEN_BOUND
    if date:
        return bound.strftime("%Y-%m-%d")
    # A naive bound is local time. Use ``.astimezone()`` rather than a fixed
    # offset so each value is resolved against the DST rules for its own date.
    aware = bound if bound.tzinfo is not None else bound.astimezone()
    return aware.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_interval(value: DateRange, dialect: DateDialect, *, date_only: bool) -> str:
    """Write a date range as the endpoint's date parameter expects it.

    Parameters
    ----------
    value : DateRange
        What the caller meant, as read by
        :func:`~dataretrieval._dates.read_date_range`, which refuses a duration
        the dialect does not accept.
    dialect : DateDialect
        The endpoint's date dialect; supplies ``open_start``.
    date_only : bool
        Write each bound as ``YYYY-MM-DD`` rather than as a UTC datetime; the
        caller's protocol decides which values are calendar days.
    """
    if value.duration is not None:
        return value.duration
    if value.instant:
        return _write_bound(value.start, date=date_only)
    start = (
        dialect.open_start
        if value.start is None
        else _write_bound(value.start, date=date_only)
    )
    return f"{start}/{_write_bound(value.end, date=date_only)}"
