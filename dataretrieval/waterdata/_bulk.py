"""Bulk retrieval: the nightly Parquet files for history, the API for changes.

``get_daily`` and ``get_continuous`` fetch through :func:`get_data`, which
reads the files when the Water Data ``bulk`` setting
(``WaterdataConfiguration(bulk=...)``) is ``"always"``, or ``"auto"`` and the
query is large enough, and pages the API otherwise. The design, the merge
rule, and the alternatives rejected are recorded in ADR 0014. In short:

1. Search the STAC catalog for each location's files, as concurrent chunks.
2. Download every file, one fan-out chunk per file, reading only the rows the
   query's stable filters and ``time`` match.
3. Take each location's snapshot time ``T`` as the largest
   ``last_modified`` in its ``current`` file, or in its only file. A record
   changed after the snapshot has a larger one.
4. Send one OGC query: a CQL ``OR`` clause per cutoff day selecting the
   changes, one per gap between a location's two files, and one for the
   whole query at locations without a file. The OGC chunker splits it at
   ``OR`` and fans the pieces out. A ``time`` the API would refuse is sent
   as comparisons in every clause instead.
5. Combine, keeping the API's version of any record in both, then apply the
   filters on columns a record can change after it is written.

Limits: the result has no ``daily_id`` / ``continuous_id`` column, which the
files do not carry, and a record removed from the API since the files were
written is still returned.
"""

from __future__ import annotations

import contextlib
import importlib.util
import warnings
import weakref
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from functools import cached_property
from typing import Any

import anyio
import httpx
import pandas as pd

from dataretrieval import configuration
from dataretrieval._response_metadata import BaseMetadata
from dataretrieval._validation import _render_names, require_argument
from dataretrieval.combining import _combine_chunk_responses
from dataretrieval.exceptions import HTTPError, TransientError
from dataretrieval.interruptions import FanOutInterrupted
from dataretrieval.ogc.dates import _format_one, _is_blank
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.ogc.filters import _cql_in_clause
from dataretrieval.ogc.shaping import GEOPANDAS, _infer_object_cols, _sort_rows
from dataretrieval.transport.fanout import FanOut, active_client, inert_response
from dataretrieval.transport.http import default_headers
from dataretrieval.transport.retry import RetryPolicy
from dataretrieval.waterdata.catalog import (
    _ID_REMEDY,
    MAX_PAGE_SIZE,
    data_assets,
    format_time,
    location_of,
    search,
)
from dataretrieval.waterdata.utils import (
    _OUTPUT_ID_BY_COLLECTION,
    WATERDATA_DIALECT,
    get_ogc_data,
    request_url,
)

__all__ = ["get_data"]

#: Row filters the bulk path accepts on columns fixed when a record is written.
_STABLE_FILTERS = frozenset(
    {"parameter_code", "statistic_id", "time_series_id", "unit_of_measure"}
)
#: Row filters on columns a record can change later: applied after the merge.
_CHANGEABLE_FILTERS = frozenset({"approval_status", "method_category"})
#: The other arguments the bulk path accepts.
_ACCEPTED = frozenset({"monitoring_location_id", "time", "skip_geometry"})

#: Most monitoring locations in one ``IN`` clause of the change query. The
#: OGC chunker splits a filter only at top-level ``OR``, so each clause must
#: fit one ~8 KB request; 100 of the longest ids (``OH015-383638082103300``)
#: are about 2.5 KB.
_SITES_PER_CLAUSE = 100

#: A change query is split into up to ``_CHANGE_REQUESTS`` concurrent requests,
#: then into requests of ``_SITES_PER_CHANGE_REQUEST`` locations, but no more
#: requests than the concurrency cap: the API's time to answer one grows with
#: the locations it names (ADR 0014).
_CHANGE_REQUESTS = 8
_SITES_PER_CHANGE_REQUEST = 10

#: ``current`` holds about the last 150 days. Skip ``historical`` only when the
#: query starts well inside that window.
_CURRENT_WINDOW = timedelta(days=120)

#: A location's ``current`` file starts at the value after its ``historical``
#: file ends. A wider space between them means the two were written on
#: different nights, or the gage recorded nothing; the API is asked which.
_BOUNDARY_GAP = timedelta(hours=12)


@dataclass(frozen=True)
class _Layout:
    """How one collection is published as files, and how its API rows differ.

    Everything the bulk path does differently for ``daily`` and ``continuous``
    is a field here, so the steps below do not test the collection's name.
    """

    #: The API's record id. The files do not carry it, so the result omits it.
    id_column: str
    #: Row filters the collection's files and API both have a column for.
    row_filters: frozenset[str]
    #: ``time`` is a calendar date in the files, a timestamp otherwise.
    dated: bool
    #: Each location has a second, large file for records older than
    #: ``_CURRENT_WINDOW``.
    has_historical: bool
    #: The widest ``time`` argument the API accepts, in days. It refuses a
    #: wider or open-started one with HTTP 400 (measured 2026-11-23).
    max_days: int | None
    #: Whether the change clauses carry the query's ``time``. Without it, a
    #: daily change query is faster and a continuous one far slower (ADR 0014).
    bounds_changes: bool
    #: The rows the API pages in the time the files path takes before its
    #: first file: about 1.5 s for ``daily``, 2 to 2.5 s for ``continuous``.
    base_rows: int
    #: The row estimate of the ``"auto"`` mode: values per series per day;
    #: series per location when the query names no parameter (measured on
    #: discharge gages: all daily parameters returned 1.6-2x the rows of
    #: discharge alone); and days assumed for an open-ended ``time``, the
    #: typical record length.
    values_per_day: int
    series_per_location: int
    open_ended_days: int

    @property
    def accepted(self) -> frozenset[str]:
        """Every argument the bulk path accepts for the collection."""
        return _ACCEPTED | self.row_filters


_ROW_FILTERS = _STABLE_FILTERS | _CHANGEABLE_FILTERS
_LAYOUTS = {
    "daily": _Layout(
        id_column=_OUTPUT_ID_BY_COLLECTION["daily"],
        # ``daily`` has no ``method_category`` column.
        row_filters=_ROW_FILTERS - {"method_category"},
        dated=True,
        has_historical=False,
        max_days=None,
        bounds_changes=False,
        # Low, because a location with many series is underestimated severalfold.
        base_rows=5_000,
        values_per_day=1,
        series_per_location=2,
        open_ended_days=40 * 365,
    ),
    "continuous": _Layout(
        id_column=_OUTPUT_ID_BY_COLLECTION["continuous"],
        row_filters=_ROW_FILTERS,
        dated=False,
        has_historical=True,
        max_days=1100,
        bounds_changes=True,
        base_rows=10_000,
        # Mostly 15-minute values.
        values_per_day=96,
        series_per_location=3,
        open_ended_days=20 * 365,
    ),
}


@dataclass(frozen=True)
class _Query:
    """One getter call the files can answer, with its arguments parsed once."""

    collection: str
    layout: _Layout
    #: The getter's request arguments, as built by ``_get_args``.
    args: dict[str, Any]
    sites: list[str]
    #: The inclusive ``time`` interval in UTC; ``None`` for an open side.
    start: pd.Timestamp | None
    end: pd.Timestamp | None

    def values(self, name: str) -> list[str]:
        """The values of a list-shaped argument."""
        return _as_list(self.args[name])

    def filters(self, names: frozenset[str]) -> dict[str, list[str]]:
        """The values of each argument in ``names`` the query was given."""
        return {name: self.values(name) for name in sorted(names & self.args.keys())}

    @property
    def span(self) -> pd.Timedelta | None:
        """The length of the ``time`` interval, to now for an open end;
        ``None`` for an open start."""
        if self.start is None:
            return None
        end = self.end if self.end is not None else pd.Timestamp.now(tz="UTC")
        return end - self.start

    @property
    def needs_historical(self) -> bool:
        """Whether the query reaches back past ``current`` (about 150 days)."""
        if not self.layout.has_historical:
            return False
        now = pd.Timestamp.now(tz="UTC")
        return self.start is None or self.start < now - _CURRENT_WINDOW

    @cached_property
    def time_fits_api(self) -> bool:
        """Whether the API accepts the query's ``time`` argument.

        When it does not, only the files can answer the query. The limit has
        a day's margin, since the API measures an open end from its own clock.
        """
        limit = self.layout.max_days
        if limit is None or self.args.get("time") is None:
            return True
        span = self.span
        return span is not None and bool(span <= timedelta(days=limit - 1))

    @property
    def time_clauses(self) -> list[str]:
        """The ``time`` interval as CQL-text comparisons."""
        bounds = ((">=", self.start), ("<=", self.end))
        return [f"time {op} '{_cql_time(t)}'" for op, t in bounds if t is not None]


@dataclass(frozen=True)
class _File:
    """One Parquet file to download: a plan item of the fan-out."""

    monitoring_location_id: str
    url: str
    #: The catalog's name for the file: ``current``, ``historical``, ``data``.
    asset: str


@dataclass(frozen=True)
class _FileFacts:
    """What one whole file says about itself, whatever rows the query read."""

    #: The largest ``last_modified``: when the snapshot was taken.
    cutoff: pd.Timestamp
    #: The smallest and largest ``time``.
    first: pd.Timestamp
    last: pd.Timestamp


@dataclass
class _Snapshot:
    """What the downloads learned beside the rows, by location and file."""

    files: dict[str, dict[str, _FileFacts]] = field(default_factory=dict)
    #: Locations with a catalog entry whose file could not be read.
    failed: set[str] = field(default_factory=set)

    def cutoffs(self) -> dict[str, pd.Timestamp]:
        """Each location's ``T``: its ``current`` file's, else its only file's.

        ``current`` is rewritten every night and ``historical`` only after
        one of its records changes, so ``current`` says when the pair was
        last brought up to date.
        """
        return {
            site: (
                by_asset.get("current")
                or max(by_asset.values(), key=lambda facts: facts.cutoff)
            ).cutoff
            for site, by_asset in self.files.items()
        }

    def gaps(self) -> list[tuple[str, pd.Timestamp, pd.Timestamp]]:
        """``(location, after, before)`` where its two files leave a gap."""
        found: list[tuple[str, pd.Timestamp, pd.Timestamp]] = []
        for site, by_asset in sorted(self.files.items()):
            old, new = by_asset.get("historical"), by_asset.get("current")
            if old and new and new.first - old.last > _BOUNDARY_GAP:
                found.append((site, old.last, new.first))
        return found


#: The remedy for a query only the API can answer: the mode that sends it there.
_USE_AUTO = (
    "or run the call inside "
    "`with dataretrieval.configure(WaterdataConfiguration(bulk='auto')):`, "
    "which sends such a query to the API"
)

#: The remedy for a file request the service refused: the mode that skips them.
_USE_OFF = (
    "The bulk setting sent this query to the published files; to page the "
    "API instead, run the call inside "
    "`with dataretrieval.configure(WaterdataConfiguration(bulk='off')):`."
)


def get_data(
    args: dict[str, Any], collection: str, *, max_rows: int | None = None
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Return ``(df, md)`` for a ``daily`` or ``continuous`` query.

    :func:`~dataretrieval.waterdata.utils.get_ogc_data` with the Water Data
    ``bulk`` setting applied: the files plus the API's changes when the
    setting sends the query there, the API alone otherwise. ``args`` are the
    getter's request arguments, as built by ``_get_args``.

    With the setting on, every answer has the same columns whichever way it
    is answered: the record id, which the files do not carry, is left out of
    an API answer too, unless ``properties`` names it. Without pyarrow, a
    query for the files pages the API, with a warning.
    """
    mode = configuration.bulk(adapter="waterdata")
    if mode == "off":
        return get_ogc_data(args, collection, max_rows=max_rows)
    try:
        query = _parse(collection, args, max_rows)
    except ValueError:
        if mode == "always":
            raise
        # Something only the API accepts.
        answer = get_ogc_data(args, collection, max_rows=max_rows)
        return _without_id(answer, args, collection)
    if mode == "always" or _prefers_files(query):
        if importlib.util.find_spec("pyarrow") is not None:
            return _get_bulk(query)
        _warn_without_pyarrow(query, mode)
    return _without_id(get_ogc_data(args, collection), args, collection)


def _without_id(
    answer: tuple[pd.DataFrame, BaseMetadata], args: dict[str, Any], collection: str
) -> tuple[pd.DataFrame, BaseMetadata]:
    """An API answer without the record id, unless ``properties`` names it."""
    df, md = answer
    column = _LAYOUTS[collection].id_column
    # ``properties`` may name it by the API's name, ``id``.
    if {column, "id"} & set(_as_list(args.get("properties") or [])):
        return df, md
    return df.drop(columns=column, errors="ignore"), md


def _warn_without_pyarrow(query: _Query, mode: str) -> None:
    """Warn that a query for the files pages the API instead.

    Raises ``ValueError`` before any request for a ``time`` the API refuses,
    which only the files can answer.
    """
    install = 'Install it with pip install "dataretrieval[pyarrow]"'
    if not query.time_fits_api:
        raise ValueError(
            f"This {query.collection} query needs pyarrow, which is not "
            f"installed: its time is wider than the {query.layout.max_days:,} "
            "days the API accepts, or has an open start, so only the files can "
            f"answer it. {install}, or pass a time of at most "
            f"{query.layout.max_days:,} days with a start date."
        )
    warnings.warn(
        f"The bulk setting is {mode!r}, but reading the files needs pyarrow, "
        f"which is not installed, so this query paged the API. {install}.",
        # This function, get_data, the getter, then the getter's caller.
        stacklevel=4,
    )


def _get_bulk(query: _Query) -> tuple[pd.DataFrame, BaseMetadata]:
    """Return ``(df, md)`` for ``query`` from the files plus the API."""
    items, searched = _search(query)
    files = _plan(items, skip_historical=not query.needs_historical)
    points = _points(items)
    snapshot = _Snapshot()
    # A weak reference: the downloads hold this hook, and a strong one would
    # keep every file's rows alive until a garbage collection.
    downloads: weakref.ref[FanOut[_File]] | None = None

    def complete(
        file_rows: pd.DataFrame, downloaded: httpx.Response | None
    ) -> tuple[pd.DataFrame, BaseMetadata]:
        """The rest of the query, once every file is read.

        It is the downloads' ``finalize`` hook (ADR 0008), so
        ``exc.call.resume()`` after an interruption returns the whole
        result, not only the files.
        """
        # A location whose only file failed has no cutoff; one with two files
        # and one failure has a cutoff but incomplete rows. Both get the whole
        # query.
        cutoffs = snapshot.cutoffs()
        missing = (set(query.sites) - cutoffs.keys()) | snapshot.failed
        try:
            api, md = _query_api(query, cutoffs, missing, snapshot.gaps())
        except FanOutInterrupted as e:
            # Resuming the downloads (all complete) re-runs the change query.
            if downloads is not None and (call := downloads()) is not None:
                e._bind(call)
            raise
        return _merge(file_rows, api, query, points), _metadata(
            query, md, searched, downloaded
        )

    if not files:
        return complete(pd.DataFrame(), None)
    call = FanOut(
        files,
        _fetcher(snapshot, query),
        RetryPolicy.from_configuration(adapter="waterdata"),
        finalize=complete,
        canonical_url=files[0].url,
        service=f"bulk {query.collection}",
        adapter="waterdata",
    )
    downloads = weakref.ref(call)
    return call.resume()


# --- arguments --------------------------------------------------------------


def _as_list(value: str | Iterable[str]) -> list[str]:
    return [value] if isinstance(value, str) else list(value)


def _parse(collection: str, args: dict[str, Any], max_rows: int | None) -> _Query:
    """``args`` as a :class:`_Query`.

    Raises
    ------
    ValueError
        For an argument the bulk path cannot honor.
    """
    layout = _LAYOUTS[collection]
    if max_rows is not None:
        raise ValueError(
            "max_rows is not supported with bulk='always', because the files are "
            f"read whole. Remove max_rows, {_USE_AUTO}."
        )
    sites = require_argument(
        "monitoring_location_id",
        args.get("monitoring_location_id") or None,
        context="with bulk='always', because the files are published per location",
        remedy=(
            "Pass one or more ids, e.g., monitoring_location_id='USGS-01646500', "
            "or find them with get_monitoring_locations()."
        ),
    )
    if args.get("convert_type") is False:
        raise ValueError(
            "bulk='always' always converts column types. Remove "
            f"convert_type=False, {_USE_AUTO}."
        )
    unsupported = sorted(set(args) - layout.accepted - {"convert_type"})
    if unsupported:
        raise ValueError(
            f"bulk='always' does not support {_render_names(unsupported)}. "
            f"Remove {'it' if len(unsupported) == 1 else 'them'}, {_USE_AUTO}. "
            "Supported with bulk='always': "
            f"{_render_names(sorted(layout.accepted))}."
        )
    start, end = _time_bounds(args.get("time"), collection)
    return _Query(collection, layout, args, _as_list(sites), start, end)


# --- routing ------------------------------------------------------------------

# The routing rule (ADR 0014), measured against the live services with fresh
# monitoring locations for every run. Paging the API costs about 0.4 s plus
# 1 s per 5,000 rows (2026-11-24); the files cost a fixed time
# (``_Layout.base_rows``), plus per file the rows the API would page through
# in the same time: a daily or ``current`` continuous file is small (20 of
# them added 0.4 s), a ``historical`` one is 5-15 MB.
_ROWS_PER_SMALL_FILE = 100
_ROWS_PER_HISTORICAL_FILE = 10_000


def _prefers_files(query: _Query) -> bool:
    """Whether the ``"auto"`` bulk mode should read the files for ``query``.

    The files win when the estimated rows exceed the layout's ``base_rows``
    plus a per-location download cost. The estimate needs no request: locations x
    series x values per day x days.
    """
    per_file = (
        _ROWS_PER_HISTORICAL_FILE if query.needs_historical else _ROWS_PER_SMALL_FILE
    )
    return _estimated_rows(query) >= query.layout.base_rows + per_file * len(
        query.sites
    )


def _estimated_rows(query: _Query) -> float:
    layout, args, sites = query.layout, query.args, len(query.sites)
    series: float = layout.series_per_location
    if "time_series_id" in args:
        series = len(query.values("time_series_id")) / sites
    elif "parameter_code" in args:
        series = len(query.values("parameter_code"))
        if "statistic_id" in args:
            series *= len(query.values("statistic_id"))
    span = query.span
    days = (
        float(layout.open_ended_days)
        if span is None
        else max(span.total_seconds() / 86400, 1.0)
    )
    return sites * series * layout.values_per_day * days


def _time_bounds(
    time: Any, collection: str
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """The inclusive ``time`` interval as UTC timestamps, ``None`` if open."""
    text = format_time(time, service=f"bulk {collection}")
    if text is None:
        return None, None
    start, sep, end = text.partition("/")
    end = end if sep else start

    def parse(side: str) -> pd.Timestamp | None:
        return None if _is_blank(side) else pd.Timestamp(side).tz_convert("UTC")

    return parse(start), parse(end)


# --- discovery ----------------------------------------------------------------


def _search(query: _Query) -> tuple[list[dict[str, Any]], httpx.Response]:
    """The catalog items for the query's locations, one per location."""
    try:
        return search(
            query.collection,
            query.sites,
            limit=MAX_PAGE_SIZE,
            ssl_check=True,
            service=f"bulk {query.collection}",
            remedy="Pass fewer or shorter monitoring_location_id values.",
        )
    except TransientError:
        raise
    except HTTPError as e:
        raise _refused(e) from e


def _refused(error: HTTPError) -> HTTPError:
    """A refused catalog or file request, with the mode that avoids both."""
    return HTTPError(f"{error} {_USE_OFF}", status_code=error.status_code or 0)


def _parts(sites: list[str], step: int = _SITES_PER_CLAUSE) -> list[list[str]]:
    """``sites`` in pieces of at most ``step``."""
    return [sites[i : i + step] for i in range(0, len(sites), step)]


def _plan(items: list[dict[str, Any]], *, skip_historical: bool) -> list[_File]:
    """One plan item per file the query needs."""
    return [
        _File(location_of(item), href, name)
        for item, name, href in data_assets(items)
        if not (skip_historical and name == "historical")
    ]


# --- download -----------------------------------------------------------------


def _fetcher(
    snapshot: _Snapshot, query: _Query
) -> Callable[[_File], Awaitable[tuple[pd.DataFrame, httpx.Response]]]:
    """The fan-out's per-file fetch: the file's matching rows.

    The run combines the rows of every file; the file's ``T`` goes to
    ``snapshot``, since it describes the whole file rather than those rows.
    """
    filters, dated = _arrow_filters(query), query.layout.dated

    async def fetch(item: _File) -> tuple[pd.DataFrame, httpx.Response]:
        client = active_client()
        if client is None:
            raise RuntimeError("fetch must run inside a FanOut run.")
        response = await client.get(item.url, headers=default_headers(item.url))
        # The run keeps every response; a body-less stand-in keeps it from
        # holding every file in memory.
        inert = inert_response(
            response.status_code, item.url, response.headers, response.elapsed
        )
        site = item.monitoring_location_id
        if not _file_exists(response):
            snapshot.failed.add(site)
            return pd.DataFrame(), inert
        content = response.content
        del response  # only ``inert`` is kept; the body goes with ``content``
        rows, facts = await anyio.to_thread.run_sync(
            _read_file, content, filters, dated
        )
        if facts is not None:
            snapshot.files.setdefault(site, {})[item.asset] = facts
        return rows, inert

    return fetch


def _file_exists(response: httpx.Response) -> bool:
    """Whether a download found its file; ``False`` for a 404.

    A catalog entry whose file is gone sends the location to the API. Any
    other failure (a refused key, a firewall) is the caller's to see, and a
    transient one is the fan-out's to retry.
    """
    try:
        _raise_for_non_200(response)
    except TransientError:
        raise
    except HTTPError as e:
        if e.status_code == 404:
            return False
        raise _refused(e) from e
    return True


def _arrow_filters(query: _Query) -> list[tuple[str, str, Any]]:
    """pyarrow row filters for the query's stable-column and time filters.

    Changeable columns are filtered after the merge instead: a file row that
    fails the filter now may have been revised to pass it.
    """
    filters: list[tuple[str, str, Any]] = [
        (column, "in", values)
        for column, values in query.filters(_STABLE_FILTERS).items()
    ]
    for op, bound in ((">=", query.start), ("<=", query.end)):
        if bound is not None:
            value = bound.date() if query.layout.dated else bound.to_pydatetime()
            filters.append(("time", op, value))
    return filters


def _read_file(
    content: bytes, filters: list[tuple[str, str, Any]], dated: bool
) -> tuple[pd.DataFrame, _FileFacts | None]:
    """The matching rows of one file, and the whole file's facts.

    The filters are applied while reading, so row groups outside them are not
    decoded. The facts describe the whole file, not those rows; an empty file
    has none. Runs in a worker thread, so the rows are shaped here.
    """
    # pyarrow is optional; get_data checked it is installed.
    import pyarrow as pa
    import pyarrow.parquet as pq

    # ``BufferReader`` reads the downloaded bytes in place, without a copy.
    table = pq.read_table(pa.BufferReader(content), filters=filters or None)
    source = pq.ParquetFile(pa.BufferReader(content))
    facts = None
    if source.metadata.num_rows:
        first, last = _column_range(source, "time")
        facts = _FileFacts(
            cutoff=pd.Timestamp(_column_range(source, "last_modified")[1]).tz_convert(
                "UTC"
            ),
            first=pd.Timestamp(first),
            last=pd.Timestamp(last),
        )
    return _frame(table, dated), facts


def _frame(table: Any, dated: bool) -> pd.DataFrame:
    """A file's rows in the getter's column types, converted while in Arrow."""
    import pyarrow as pa
    import pyarrow.compute as pc

    # The API returns ``qualifier`` as lists and ``value`` as floats.
    qualifier = table["qualifier"].to_pylist()
    # A value no float spells fails the cast; ``to_numeric`` makes it NaN.
    with contextlib.suppress(pa.ArrowInvalid):
        value = pc.cast(table["value"], pa.float64())
        table = table.set_column(table.schema.get_field_index("value"), "value", value)
    df = table.drop_columns(["qualifier"]).to_pandas(date_as_object=False)
    df["value"] = pd.to_numeric(df["value"], errors="coerce").astype("float64")
    df["qualifier"] = qualifier
    if dated:
        df["time"] = df["time"].astype("datetime64[us]")
    return df


def _column_range(source: Any, column: str) -> tuple[Any, Any]:
    """The smallest and largest value of a column over a whole Parquet file.

    From the row-group statistics when every group has them, otherwise from
    the column alone.
    """
    meta = source.metadata
    index = source.schema_arrow.get_field_index(column)
    stats = [
        meta.row_group(i).column(index).statistics for i in range(meta.num_row_groups)
    ]
    if all(s is not None and s.has_min_max for s in stats):
        return min(s.min for s in stats), max(s.max for s in stats)
    import pyarrow.compute as pc

    span = pc.min_max(source.read(columns=[column])[column]).as_py()
    return span["min"], span["max"]


# --- the API part ---------------------------------------------------------------


def _cql_time(time: pd.Timestamp) -> str:
    return _format_one(time, date=False, name="time")


def _clause(sites: list[str], *conditions: str) -> str:
    """One top-level ``OR`` clause: ``sites`` and every condition."""
    ids = _cql_in_clause("monitoring_location_id", sites, remedy=_ID_REMEDY)
    return f"({' AND '.join([ids, *conditions])})"


def _change_clauses(
    cutoffs: dict[str, pd.Timestamp], missing: set[str], bounds: list[str]
) -> list[str]:
    """One clause per cutoff day: the records changed since the snapshot."""
    by_day: dict[pd.Timestamp, list[str]] = {}
    for site, cutoff in cutoffs.items():
        if site not in missing:
            by_day.setdefault(cutoff.floor("D"), []).append(site)
    step = _change_step(len(cutoffs.keys() - missing))
    return [
        _clause(part, f"last_modified >= '{_cql_time(day)}'", *bounds)
        for day, sites in sorted(by_day.items())
        for part in _parts(sites, step)
    ]


def _change_step(changing: int) -> int:
    """How many locations each change clause names."""
    step = min(_SITES_PER_CHANGE_REQUEST, -(-changing // _CHANGE_REQUESTS))
    # Requests beyond the concurrency cap wait their turn, so they add only quota.
    cap = configuration.concurrency(adapter="waterdata")
    if cap is not None:
        step = max(step, -(-changing // cap))
    return min(step, _SITES_PER_CLAUSE)


def _gap_clauses(
    gaps: Iterable[tuple[str, pd.Timestamp, pd.Timestamp]], bounds: list[str]
) -> list[str]:
    """One clause per gap between a location's two files: every record in it."""
    return [
        _clause(
            [site],
            f"time > '{_cql_time(after)}'",
            f"time < '{_cql_time(before)}'",
            *bounds,
        )
        for site, after, before in gaps
    ]


def _query_api(
    query: _Query,
    cutoffs: dict[str, pd.Timestamp],
    missing: set[str],
    gaps: Iterable[tuple[str, pd.Timestamp, pd.Timestamp]],
) -> tuple[pd.DataFrame, BaseMetadata]:
    """The API rows: changes since each snapshot, and what the files lack.

    One query, whose CQL filter has one top-level ``OR`` clause per cutoff
    day, one per gap between a location's two files, and one for the
    locations without a usable file. The OGC chunker
    splits a filter at top-level ``OR`` when it outgrows a URL and fans the
    pieces out under the concurrency cap, so the clauses run concurrently
    without a fan-out of our own. Cutoffs are floored to the day, which gives
    the window a margin and keeps a few stale files from widening the change
    window for every other location.

    Changeable filters are sent only in the clause for locations without a
    file. A change query must also see a record that has just left the filter,
    so the merge can drop the file's stale copy; the merge applies them.

    How ``time`` is sent depends on the collection
    (:attr:`_Layout.bounds_changes`). A ``daily`` request sends none and its
    change clauses carry no bounds, so the rows are kept to the interval
    here; the missing-location clause carries the bounds. A ``continuous``
    request sends ``time``, or, when the API refuses it, every clause
    carries the bounds instead.
    """
    sends_time = query.layout.bounds_changes and query.time_fits_api
    bounds = [] if sends_time else query.time_clauses
    changeable = [
        _cql_in_clause(column, values, remedy=f"Remove that value from {column}.")
        for column, values in query.filters(_CHANGEABLE_FILTERS).items()
    ]
    clauses = [
        *_change_clauses(
            cutoffs, missing, bounds if query.layout.bounds_changes else []
        ),
        *_gap_clauses(gaps, bounds),
        *(_clause(part, *changeable, *bounds) for part in _parts(sorted(missing))),
    ]
    dropped = {"monitoring_location_id", *_CHANGEABLE_FILTERS}
    if not sends_time:
        dropped.add("time")
    request = {k: v for k, v in query.args.items() if k not in dropped}
    request["filter"] = " OR ".join(clauses)
    request["filter_lang"] = "cql-text"
    # One request per clause: the chunker splits a filter at top-level ``OR``.
    api, md = get_ogc_data(request, query.collection, chunks=len(clauses))
    return _within(api, query), md


def _within(api: pd.DataFrame, query: _Query) -> pd.DataFrame:
    """The rows inside the query's ``time`` interval.

    A change clause without bounds also returns records outside it that
    changed since the snapshot, and the merge would keep them.
    """
    if api.empty or (query.start is None and query.end is None):
        return api
    time = pd.to_datetime(api["time"], utc=True)
    inside = pd.Series(True, index=api.index)
    if query.start is not None:
        inside &= time >= query.start
    if query.end is not None:
        inside &= time <= query.end
    return api[inside]


def _metadata(
    query: _Query, md: BaseMetadata, *responses: httpx.Response | None
) -> BaseMetadata:
    """``md`` for the whole call, as the API reports a query it splits.

    The URL is the one ``bulk="off"`` sends, the time is every request's
    summed, and the headers are those with the lowest remaining quota.
    """
    changes = inert_response(200, md.url, md.header, md.query_time)
    sent = [response for response in (*responses, changes) if response is not None]
    url = request_url(query.args, query.collection)
    return BaseMetadata(_combine_chunk_responses(sent, url))


# --- shaping and merging ----------------------------------------------------------


def _points(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Each location's GeoJSON point, from its catalog item.

    The catalog rounds coordinates to 9 decimal places, under a millimeter
    from the API's.
    """
    return {
        location_of(item): item["geometry"] for item in items if item.get("geometry")
    }


def _merge(
    file_rows: pd.DataFrame,
    api: pd.DataFrame,
    query: _Query,
    points: dict[str, Any],
) -> pd.DataFrame:
    """Concatenate, keep the API's version of shared records, filter, sort."""
    if not (file_rows.empty or api.empty):
        api = _typed_like(api, file_rows)
    combined = api.reset_index(drop=True)
    keep = pd.Series(True, index=combined.index)
    if not file_rows.empty:
        file_rows = _with_geometry(file_rows, api, points)
        # An empty API frame has placeholder types, and concatenating it would
        # widen every file column to ``object``.
        parts = [file_rows] if api.empty else [combined, file_rows]
        combined = pd.concat(parts, ignore_index=True)
        keep = ~combined.duplicated(["time_series_id", "time"], keep="first")
    for column, values in query.filters(_CHANGEABLE_FILTERS).items():
        keep &= combined[column].isin(values)
    combined = combined[keep]
    api = api.drop(columns=query.layout.id_column, errors="ignore")
    combined = combined.reindex(columns=api.columns)
    if not api.empty:
        combined = _match_types(combined, api)
    # A file's strings may arrive as ``object`` when no API row gives
    # ``_match_types`` a template.
    return _sort_rows(_infer_object_cols(combined), WATERDATA_DIALECT)


def _typed_like(api: pd.DataFrame, file_rows: pd.DataFrame) -> pd.DataFrame:
    """The API frame, its all-null ``object`` columns given the files' types.

    A text column null in every changed row is ``object``, and the concat
    would widen the files' strings to match.
    """
    types = {
        column: file_rows[column].dtype
        for column in api.columns.intersection(file_rows.columns)
        if api[column].dtype == object
        and file_rows[column].dtype != object
        and api[column].isna().all()
    }
    return api.astype(types) if types else api


def _match_types(combined: pd.DataFrame, api: pd.DataFrame) -> pd.DataFrame:
    """Give every column the API frame's type and null.

    The concat widens a column that is null in the file rows (an
    uncategorized ``method_category``) to ``object``, and a file's string null
    is ``nan`` where the API's is ``None``.
    """
    for column in api.columns.drop("geometry", errors="ignore"):
        values = combined[column]
        if values.dtype == api[column].dtype and values.dtype != object:
            continue
        if values.dtype == object:
            values = values.where(values.notna(), None)
        combined[column] = values.astype(api[column].dtype)
    return combined


def _with_geometry(
    file_rows: pd.DataFrame, api: pd.DataFrame, points: dict[str, Any]
) -> pd.DataFrame:
    """File rows with the API frame's geometry column, so the concat keeps it.

    Without geopandas the API's geometry is the GeoJSON coordinate list
    (``ogc.shaping._attach_coordinates``), so the file rows get the same.
    """
    if "geometry" not in api.columns:
        return file_rows
    sites = file_rows["monitoring_location_id"]
    if not GEOPANDAS:
        coordinates = {site: point.get("coordinates") for site, point in points.items()}
        return file_rows.assign(geometry=sites.map(coordinates))
    import geopandas as gpd
    from shapely.geometry import shape

    # One shape per location, not per row.
    codes, locations = pd.factorize(sites)
    shapes = gpd.array.from_shapely(
        [shape(points[site]) if site in points else None for site in locations]
    )
    return gpd.GeoDataFrame(
        file_rows,
        geometry=shapes.take(codes, allow_fill=True),
        crs=getattr(api, "crs", None),
    )
