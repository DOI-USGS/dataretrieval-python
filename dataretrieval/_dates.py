"""What a caller meant by a date argument, read once at the public boundary.

A dependency-free leaf (ADR 0014): a getter reads its date arguments into a
:class:`DateRange` when it is called, and the protocol code that sends the
request writes that value in its own wire format. Reading -- which types and
string formats are accepted, what spells an open bound, and that a naive value
is local time -- is package policy, so it lives here and names no service.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime

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


# The ISO 8601 spelling of an open interval bound, as in ``"2024-01-01/.."``.
OPEN_BOUND = ".."


# One element of a date argument: an ISO 8601 string, or a ``date``,
# ``datetime`` or ``pandas.Timestamp`` (both subclasses of ``datetime.date``),
# with ``None`` (or ``NaN`` / ``NaT``) for an open bound.
DateLike = str | _date | None


@dataclass(frozen=True)
class DateRange:
    """One date argument as the caller meant it.

    Exactly one shape is set: an instant (``instant=True``, ``start`` only), a
    range of two bounds where ``None`` is an open bound (``start``, ``end``),
    or an ISO 8601 duration (``duration``, such as ``"P7D"``). A bound is
    naive exactly when the caller gave no zone, and a naive bound is local
    time.
    """

    start: datetime | None = None
    end: datetime | None = None
    instant: bool = False
    duration: str | None = None


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


def _is_blank(dt: DateLike) -> bool:
    """True for a None, NaN, empty-string, or ``..`` element.

    Each is a spelling of an open bound, so ``["2024-01-01", ".."]`` means the
    same range as ``["2024-01-01", None]`` and the string ``"2024-01-01/.."``.
    """
    return dt is None or bool(pd.isna(dt)) or dt in ("", OPEN_BOUND)


def _to_datetime(dt: str | _date, *, name: str) -> datetime:
    """Read one non-blank element as a ``datetime`` (naive iff it has no zone).

    A ``date`` is midnight of that day, like the string ``"2024-01-01"``.
    Raises ``ValueError`` naming *name* for an unreadable string or a value of
    another type.
    """
    if isinstance(dt, pd.Timestamp):
        # A writer converts with ``astimezone``, which on a ``Timestamp`` is
        # ``tz_convert`` and requires a zone, so keep a plain ``datetime``.
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


def _read_bound(dt: DateLike, *, name: str) -> datetime | None:
    """One element as a bound: ``None`` when it is blank (an open bound)."""
    return None if dt is None or _is_blank(dt) else _to_datetime(dt, name=name)


def _coerce_to_list(
    value: str | _date | Sequence[DateLike],
    name: str,
) -> list[DateLike]:
    """Normalize a date argument to a list of elements, raising on a mapping."""
    if isinstance(value, str) or not isinstance(value, Iterable):
        # A lone value (a ``date`` is not iterable); a non-date is rejected
        # per element.
        return [value]
    if isinstance(value, Mapping):
        raise TypeError(
            f"{name} must be a date, a string, or a sequence of them, "
            f"not {type(value).__name__}."
        )
    return list(value)


def _split_interval(interval: str, *, name: str) -> tuple[str, str]:
    """Split a ``"start/end"`` string into its two sides.

    Each side is then read like a list element, so ``"2024-01-01T10:00:00/.."``
    means the same range as ``["2024-01-01T10:00:00", None]``.

    Raises ``ValueError`` naming *name* unless the string is two sides
    separated by one ``"/"``, neither of them a duration, which the services
    this package reaches answer with HTTP 400.
    """
    sides = interval.split("/")
    if len(sides) != 2 or any(_DURATION_RE.match(side) for side in sides):
        raise ValueError(
            f"{name} is not a valid interval: {interval!r}. Pass a start and "
            "an end separated by '/', such as '2024-01-01/2024-12-31', with "
            "'..' for an open end ('2024-01-01/..'). Neither side can be a "
            "duration."
        )
    return sides[0], sides[1]


def read_date_range(
    value: str | _date | Sequence[DateLike] | None,
    *,
    name: str,
    durations: bool,
) -> DateRange | None:
    """Read a date argument as the caller meant it.

    Parameters
    ----------
    value : str, date, sequence of them, or None
        A single date/datetime, a ``"start/end"`` interval string, an ISO 8601
        duration (``"P7D"``, ``"PT36H"``), or a list of one or two elements.
        Each element may be a ``datetime.date``, ``datetime.datetime`` or
        ``pandas.Timestamp``, or a string in ISO 8601 (with or without ``Z`` or
        a numeric offset) or ``"%Y-%m-%d %H:%M:%S"`` form. ``None``, ``NaN``,
        ``NaT``, ``""`` and ``".."`` are open bounds.
    name : str, optional
        The caller's own spelling of this argument, used as the subject of
        every message raised here, so a caller correcting the error edits an
        argument their getter accepts.
    durations : bool
        Whether the endpoint accepts a lone ISO 8601 duration; see
        ``DateDialect.durations``. When it does not, a duration raises, and no
        message offers one.

    Returns
    -------
    DateRange or None
        ``None`` when *value* is ``None`` or every element is an open bound,
        meaning no filter.

    Raises
    ------
    ValueError
        For more than two elements, an unreadable interval string, a duration
        where *durations* is false, or an element that is not blank and matches
        no supported format or type.
    TypeError
        For a mapping.
    """
    if value is None:
        return None

    items = _coerce_to_list(value, name)
    if len(items) == 1 and isinstance(items[0], str) and "/" in items[0]:
        items = list(_split_interval(items[0], name=name))

    if all(_is_blank(dt) for dt in items):
        return None

    if len(items) > 2:
        raise _too_many_values(items, name=name, durations=durations)
    if len(items) == 1:
        return _read_single(items[0], name=name, durations=durations)
    return DateRange(
        start=_read_bound(items[0], name=name), end=_read_bound(items[1], name=name)
    )


def _too_many_values(
    items: list[DateLike], *, name: str, durations: bool
) -> ValueError:
    """The error for more than two elements; it offers a duration only where
    one is accepted."""
    single = (
        "an instant or a duration ('2020-01-01', 'P7D')"
        if durations
        else "an instant ('2020-01-01')"
    )
    return ValueError(
        f"{name} takes at most 2 values, got {len(items)}: {items!r}. "
        f"Pass one value for {single}, "
        "or two for a closed interval ('2020-01-01', '2020-12-31')."
    )


def _read_single(item: DateLike, *, name: str, durations: bool) -> DateRange:
    """One non-blank element: a duration, or an instant."""
    if isinstance(item, str) and _DURATION_RE.match(item):
        if not durations:
            raise ValueError(
                f"{name} cannot be an ISO 8601 duration for this API: "
                f"{item!r}. Pass a start and an end instead, such as "
                "['2024-01-01', '2024-01-08'], with None for an open end "
                "(['2024-01-01', None])."
            )
        return DateRange(duration=item)
    return DateRange(start=_read_bound(item, name=name), instant=True)
