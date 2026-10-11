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
    params : frozenset[str]
        The parameters that carry a date range. The standard has one,
        ``datetime``; an endpoint that filters other fields by date (Water Data's
        ``time``, ``begin``, ``last_modified``) lists them all.
    date_only_collections : frozenset[str]
        Collections whose dates are written as ``YYYY-MM-DD`` rather than as a
        UTC datetime. This is a choice rather than a server restriction: Water
        Data's ``daily`` accepts either, but its values are calendar days, and
        a naive bound converted to UTC could move to the neighboring day.
    timed_params : frozenset[str]
        Parameters written as a UTC datetime even in a date-only collection,
        because they filter a timestamp (Water Data's ``last_modified``).
    open_start : str
        How the open start of a range is written: ``".."`` (``"../2024-01-01"``,
        the default) or ``""`` (``"/2024-01-01"``). Both standards allow
        either, but deployments differ. Observed 2026-10-11: Water Data OGC
        answers the empty start with HTTP 400; the NGWMN and STAC firewalls
        answer ``"../"`` with HTTP 403.
    durations : bool
        Whether a lone ISO 8601 duration (``"P7D"``, the last seven days) is
        sent. Neither standard defines one, so the default is ``False`` and a
        duration raises before sending. Observed 2026-10-11: Water Data OGC
        accepts it; NGWMN and STAC answer HTTP 400.
    """

    params: frozenset[str] = frozenset({"datetime"})
    date_only_collections: frozenset[str] = frozenset()
    timed_params: frozenset[str] = frozenset()
    open_start: str = OPEN_BOUND
    durations: bool = False


def single_value_hint(dialect: DateDialect) -> str:
    """How a "too many values" message describes one acceptable value."""
    if dialect.durations:
        return "an instant or a duration ('2020-01-01', 'P7D')"
    return "an instant ('2020-01-01')"


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


def write_interval(
    value: DateRange,
    dialect: DateDialect,
    *,
    param: str,
    collection: str | None = None,
    name: str | None = None,
) -> str:
    """Write a date range as the endpoint's *param* expects it.

    Parameters
    ----------
    value : DateRange
        What the caller meant, as read by
        :func:`~dataretrieval._dates.read_date_range`.
    dialect : DateDialect
        The endpoint's date dialect.
    param : str
        The request parameter being written; decides, with *collection*,
        whether its bounds are written date-only.
    collection : str, optional
        The collection the request targets, if any.
    name : str, optional
        The caller's spelling of the argument, the subject of the message when
        a duration is refused. Defaults to *param*.

    Raises
    ------
    ValueError
        For a duration, unless the dialect allows durations.
    """
    if value.duration is not None:
        if not dialect.durations:
            raise ValueError(
                f"{name or param} cannot be an ISO 8601 duration for this "
                f"service: {value.duration!r}. Pass a start and an end instead, "
                "such as ['2024-01-01', '2024-01-08'], with None for an open "
                "end (['2024-01-01', None])."
            )
        return value.duration
    date = (
        collection in dialect.date_only_collections
        and param not in dialect.timed_params
    )
    if value.instant:
        return _write_bound(value.start, date=date)
    start = (
        dialect.open_start
        if value.start is None
        else _write_bound(value.start, date=date)
    )
    return f"{start}/{_write_bound(value.end, date=date)}"
