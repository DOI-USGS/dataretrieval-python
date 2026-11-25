"""Which path answers a ``get_daily`` or ``get_continuous`` query.

The Water Data ``bulk`` setting (``WaterdataConfiguration(bulk=...)``)
chooses. ``"off"`` pages the API. ``"auto"``, the default, estimates the rows
the API would page through, without a request, and sends a large query the
files can answer to the files route of :mod:`dataretrieval.waterdata.bulk`,
which :func:`~dataretrieval.waterdata.bulk.get_daily` and
:func:`~dataretrieval.waterdata.bulk.get_continuous` use for every query. The
rule and its measurements are recorded in ADR 0014.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pandas as pd

from dataretrieval import configuration
from dataretrieval.waterdata.bulk._retrieval import (
    INSTALL_PYARROW,
    Query,
    accepts,
    has_pyarrow,
    parse,
    read,
)
from dataretrieval.waterdata.utils import _OUTPUT_ID_BY_COLLECTION, get_ogc_data

if TYPE_CHECKING:
    from dataretrieval._response_metadata import BaseMetadata

__all__ = ["get_data"]


@dataclass(frozen=True)
class _Estimate:
    """The terms of one collection's row estimate."""

    #: The rows the API pages in the time the files route takes before its
    #: first file: about 1.5 s for ``daily``, 2 to 2.5 s for ``continuous``.
    base_rows: int
    #: Values per series per day.
    values_per_day: int
    #: Series per location when the query names no parameter (measured on
    #: discharge gages: all daily parameters returned 1.6-2x the rows of
    #: discharge alone).
    series_per_location: int
    #: Days assumed for an open-ended ``time``: the typical record length.
    open_ended_days: int


_ESTIMATES = {
    # The base is low, because a location with many series is underestimated
    # severalfold.
    "daily": _Estimate(5_000, 1, 2, 40 * 365),
    # Mostly 15-minute values.
    "continuous": _Estimate(10_000, 96, 3, 20 * 365),
}

# The routing rule (ADR 0014), measured against the live services with fresh
# monitoring locations for every run. Paging the API costs about 0.4 s plus
# 1 s per 5,000 rows (2026-11-24); the files cost a fixed time
# (``_Estimate.base_rows``), plus per file the rows the API would page through
# in the same time: a daily or ``current`` continuous file is small (20 of
# them added 0.4 s), a ``historical`` one is 5-15 MB.
_ROWS_PER_SMALL_FILE = 100
_ROWS_PER_HISTORICAL_FILE = 10_000


def get_data(
    args: dict[str, Any], collection: str, *, max_rows: int | None = None
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Return ``(df, md)`` for a ``daily`` or ``continuous`` query.

    :func:`~dataretrieval.waterdata.utils.get_ogc_data` with the Water Data
    ``bulk`` setting applied: the files plus the API's changes when the
    setting sends the query there, the API alone otherwise. ``args`` are the
    getter's request arguments, as built by ``_get_args``.

    Under ``"auto"``, every answer has the same columns whichever path
    answered it: the record id, which the files do not carry, is left out of
    an API answer too, unless ``properties`` names it. Without pyarrow, a
    query for the files pages the API, with a warning.
    """
    if configuration.bulk(adapter="waterdata") == "off":
        return get_ogc_data(args, collection, max_rows=max_rows)
    query = _files_query(args, collection, max_rows)
    if query is not None and _prefers_files(query):
        if has_pyarrow():
            return read(query)
        _warn_without_pyarrow(query)
    answer = get_ogc_data(args, collection, max_rows=max_rows)
    return _without_id(answer, args, collection)


def _files_query(
    args: dict[str, Any], collection: str, max_rows: int | None
) -> Query | None:
    """``args`` as a query the files can answer, ``None`` if only the API can."""
    if max_rows is not None or args.get("convert_type") is False:
        return None
    if set(args) - accepts(collection) - {"convert_type"}:
        return None
    try:
        return parse(collection, args)
    except ValueError:
        # No monitoring_location_id, or a ``time`` the files cannot answer.
        return None


def _prefers_files(query: Query) -> bool:
    """Whether the ``"auto"`` bulk mode should read the files for ``query``.

    The files win when the estimated rows exceed the collection's
    ``base_rows`` plus a per-location download cost. The estimate needs no
    request: locations x series x values per day x days.
    """
    per_file = (
        _ROWS_PER_HISTORICAL_FILE if query.needs_historical else _ROWS_PER_SMALL_FILE
    )
    base = _ESTIMATES[query.collection].base_rows
    return _estimated_rows(query) >= base + per_file * len(query.sites)


def _estimated_rows(query: Query) -> float:
    estimate, args, sites = _ESTIMATES[query.collection], query.args, len(query.sites)
    series: float = estimate.series_per_location
    if "time_series_id" in args:
        series = len(query.values("time_series_id")) / sites
    elif "parameter_code" in args:
        series = len(query.values("parameter_code"))
        if "statistic_id" in args:
            series *= len(query.values("statistic_id"))
    span = query.span
    days = (
        float(estimate.open_ended_days)
        if span is None
        else max(span.total_seconds() / 86400, 1.0)
    )
    return sites * series * estimate.values_per_day * days


def _without_id(
    answer: tuple[pd.DataFrame, BaseMetadata], args: dict[str, Any], collection: str
) -> tuple[pd.DataFrame, BaseMetadata]:
    """An API answer without the record id, unless ``properties`` names it."""
    df, md = answer
    column = _OUTPUT_ID_BY_COLLECTION[collection]
    # ``properties`` may name it by the API's name, ``id``.
    if {column, "id"} & set(_as_list(args.get("properties") or [])):
        return df, md
    return df.drop(columns=column, errors="ignore"), md


def _as_list(value: str | Iterable[str]) -> list[str]:
    return [value] if isinstance(value, str) else list(value)


def _warn_without_pyarrow(query: Query) -> None:
    """Warn that a query for the files pages the API instead.

    Raises ``ValueError`` before any request for a ``time`` the API refuses,
    which only the files can answer.
    """
    install = f"Install it with {INSTALL_PYARROW}"
    if not query.time_fits_api:
        raise ValueError(
            f"This {query.collection} query needs pyarrow, which is not "
            f"installed: its time is wider than the {query.layout.max_days:,} "
            "days the API accepts, or has an open start, so only the files can "
            f"answer it. {install}, or pass a time of at most "
            f"{query.layout.max_days:,} days with a start date."
        )
    warnings.warn(
        "The bulk setting sent this query to the published files, but reading "
        f"them needs pyarrow, which is not installed, so it paged the API. "
        f"{install}.",
        # This function, get_data, the getter, then the getter's caller.
        stacklevel=4,
    )
