"""Bulk retrieval: the nightly Parquet files for history, the API for changes.

``get_daily`` and ``get_continuous`` fetch through :func:`get_data`, which
reads the files when the Water Data ``bulk`` setting
(``WaterdataConfiguration(bulk=...)``) is ``"always"``, or ``"auto"`` and the
query is large enough, and pages the API otherwise. The design, the merge
rule, and the alternatives rejected are recorded in ADR 0014. In short:

1. Search the STAC catalog for each location's files, as concurrent chunks.
2. Download every file, one fan-out chunk per file, reading only the rows the
   query's stable filters and ``time`` match.
3. Take each location's snapshot time ``T`` as its files' largest
   ``last_modified``. A record changed after the snapshot has a larger one.
4. Send one OGC query: a CQL ``OR`` clause per cutoff day selecting the
   changes, and one for the whole query at locations without a file. The OGC
   chunker splits it at ``OR`` and fans the pieces out.
5. Combine, keeping the API's version of any record in both, then apply the
   filters on columns a record can change after it is written.

Limits: the result has no ``daily_id`` / ``continuous_id`` column, which the
files do not carry, and a record removed from the API since the files were
written is still returned.
"""

from __future__ import annotations

import importlib.util
import warnings
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import anyio
import httpx
import pandas as pd

from dataretrieval import configuration
from dataretrieval._validation import render_options, require_argument
from dataretrieval.exceptions import HTTPError
from dataretrieval.interruptions import FanOutInterrupted
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.ogc.filters import _cql_in_clause
from dataretrieval.ogc.shaping import _sort_rows
from dataretrieval.transport.fanout import FanOut, active_client, inert_response
from dataretrieval.transport.http import default_headers
from dataretrieval.transport.retry import RetryPolicy
from dataretrieval.waterdata.catalog import MAX_PAGE_SIZE, format_time, search
from dataretrieval.waterdata.utils import WATERDATA_DIALECT, get_ogc_data

if TYPE_CHECKING:
    from dataretrieval._response_metadata import BaseMetadata

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

#: ``current`` holds about the last 150 days. Skip ``historical`` only when the
#: query starts well inside that window.
_CURRENT_WINDOW = timedelta(days=120)


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
    #: The API returns a geometry column for the collection.
    has_geometry: bool
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
        id_column="daily_id",
        # ``daily`` has no ``method_category`` column.
        row_filters=_ROW_FILTERS - {"method_category"},
        dated=True,
        has_historical=False,
        has_geometry=True,
        values_per_day=1,
        series_per_location=2,
        open_ended_days=40 * 365,
    ),
    "continuous": _Layout(
        id_column="continuous_id",
        row_filters=_ROW_FILTERS,
        dated=False,
        has_historical=True,
        has_geometry=False,
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

    @property
    def needs_historical(self) -> bool:
        """Whether the query reaches back past ``current`` (about 150 days)."""
        if not self.layout.has_historical:
            return False
        now = pd.Timestamp.now(tz="UTC")
        return self.start is None or self.start < now - _CURRENT_WINDOW


@dataclass(frozen=True)
class _File:
    """One Parquet file to download: a plan item of the fan-out."""

    monitoring_location_id: str
    url: str


@dataclass
class _Snapshot:
    """What the downloads learned beside the rows: each location's ``T``."""

    cutoff: dict[str, pd.Timestamp] = field(default_factory=dict)
    #: Locations with a catalog entry whose file could not be read.
    failed: set[str] = field(default_factory=set)


#: The remedy for a query only the API can answer: the mode that sends it there.
_USE_AUTO = (
    "or run the call inside "
    "`with dataretrieval.configure(WaterdataConfiguration(bulk='auto')):`, "
    "which sends such a query to the API"
)


def get_data(
    args: dict[str, Any], collection: str, *, max_rows: int | None = None
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Return ``(df, md)`` for a ``daily`` or ``continuous`` query.

    :func:`~dataretrieval.waterdata.utils.get_ogc_data` with the Water Data
    ``bulk`` setting applied: the files plus the API's changes when the
    setting sends the query there, the API alone otherwise. ``args`` are the
    getter's request arguments, as built by ``_get_args``.

    With the setting on, a query the files can answer returns the same
    columns whichever way it is answered: the record id, which the files do
    not carry, is left out of an API answer too. Without pyarrow, such a
    query pages the API, with a warning.
    """
    mode = configuration.bulk(adapter="waterdata")
    if mode == "off":
        return get_ogc_data(args, collection, max_rows=max_rows)
    try:
        query = _parse(collection, args, max_rows)
    except ValueError:
        if mode == "always":
            raise
        # Something only the API accepts: its answer, as the API gives it.
        return get_ogc_data(args, collection, max_rows=max_rows)
    if mode == "always" or _prefers_files(query):
        if importlib.util.find_spec("pyarrow") is not None:
            return _get_bulk(query)
        warnings.warn(
            f"The bulk setting is {mode!r}, but reading the files needs "
            "pyarrow, which is not installed, so this query paged the API. "
            'Install it with pip install "dataretrieval[bulk]".',
            # This function, the getter, then the getter's caller.
            stacklevel=3,
        )
    df, md = get_ogc_data(args, collection)
    return df.drop(columns=query.layout.id_column, errors="ignore"), md


def _get_bulk(query: _Query) -> tuple[pd.DataFrame, BaseMetadata]:
    """Return ``(df, md)`` for ``query`` from the files plus the API."""
    items = _search(query)
    files = _plan(items, skip_historical=not query.needs_historical)
    snapshot = _Snapshot()
    downloads: FanOut[_File] | None = None

    def complete(file_rows: pd.DataFrame, _: Any) -> tuple[pd.DataFrame, BaseMetadata]:
        """The rest of the query, once every file is read.

        It is the downloads' ``finalize`` hook (ADR 0008), so
        ``exc.call.resume()`` after an interruption returns the whole
        result, not only the files.
        """
        # A location whose only file failed has no cutoff; one with two files
        # and one failure has a cutoff but incomplete rows. Both get the whole
        # query.
        missing = (set(query.sites) - snapshot.cutoff.keys()) | snapshot.failed
        try:
            api, md = _query_api(query, snapshot.cutoff, missing)
        except FanOutInterrupted as e:
            if downloads is not None:
                # Resuming the downloads (all complete) re-runs this step.
                e.call = downloads
            raise
        return _merge(_shape_file_rows(file_rows, query), api, query, items), md

    if not files:
        return complete(pd.DataFrame(), None)
    downloads = FanOut(
        files,
        _fetcher(snapshot, _arrow_filters(query)),
        RetryPolicy.from_configuration(adapter="waterdata"),
        finalize=complete,
        canonical_url=files[0].url,
        service=f"bulk {query.collection}",
        adapter="waterdata",
    )
    return downloads.resume()


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
            f"bulk='always' does not support {render_options(unsupported)}. "
            f"Remove {'it' if len(unsupported) == 1 else 'them'}, {_USE_AUTO}. "
            "Supported with bulk='always': "
            f"{render_options(sorted(layout.accepted))}."
        )
    start, end = _time_bounds(args.get("time"), collection)
    return _Query(collection, layout, args, _as_list(sites), start, end)


# --- routing ------------------------------------------------------------------

# The routing rule, measured 2026-11-22 against the live services with fresh
# monitoring locations for every run (ADR 0014). Paging the API costs about
# 1 s per 8,000-12,000 rows; the files cost about 2 s, plus per file the
# rows the API would page through in the same time: a daily or ``current``
# continuous file is small, a ``historical`` one is 5-15 MB.
_BULK_BASE_ROWS = 10_000
_ROWS_PER_SMALL_FILE = 250
_ROWS_PER_HISTORICAL_FILE = 10_000


def _prefers_files(query: _Query) -> bool:
    """Whether the ``"auto"`` bulk mode should read the files for ``query``.

    The files win when the estimated rows exceed ``_BULK_BASE_ROWS`` plus a
    per-location download cost. The estimate needs no request: locations x
    series x values per day x days.
    """
    per_file = (
        _ROWS_PER_HISTORICAL_FILE if query.needs_historical else _ROWS_PER_SMALL_FILE
    )
    return _estimated_rows(query) >= _BULK_BASE_ROWS + per_file * len(query.sites)


def _estimated_rows(query: _Query) -> float:
    layout, args, sites = query.layout, query.args, len(query.sites)
    series: float = layout.series_per_location
    if "time_series_id" in args:
        series = len(query.values("time_series_id")) / sites
    elif "parameter_code" in args:
        series = len(query.values("parameter_code"))
        if "statistic_id" in args:
            series *= len(query.values("statistic_id"))
    if query.start is None:
        days = float(layout.open_ended_days)
    else:
        stop = query.end if query.end is not None else pd.Timestamp.now(tz="UTC")
        days = max((stop - query.start).total_seconds() / 86400, 1.0)
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
        return None if side in ("", "..") else pd.Timestamp(side).tz_convert("UTC")

    return parse(start), parse(end)


# --- discovery ----------------------------------------------------------------


def _search(query: _Query) -> list[dict[str, Any]]:
    """The catalog items for the query's locations, one per location."""
    items, _ = search(
        query.collection,
        query.sites,
        limit=MAX_PAGE_SIZE,
        ssl_check=True,
        service=f"bulk {query.collection}",
        remedy="Pass fewer or shorter monitoring_location_id values.",
    )
    return items


def _parts(sites: list[str]) -> list[list[str]]:
    """``sites`` in pieces of at most ``_SITES_PER_CLAUSE``."""
    step = _SITES_PER_CLAUSE
    return [sites[i : i + step] for i in range(0, len(sites), step)]


def _in_clause(column: str, values: list[str]) -> str:
    return _cql_in_clause(
        column, values, remedy="Pass an 'AGENCY-ID' id, e.g., 'USGS-01646500'."
    )


def _plan(items: list[dict[str, Any]], *, skip_historical: bool) -> list[_File]:
    """One plan item per file the query needs."""
    return [
        _File(item["id"], asset["href"])
        for item in items
        for name, asset in item.get("assets", {}).items()
        if "data" in asset.get("roles", [])
        and asset.get("href")
        and not (skip_historical and name == "historical")
    ]


# --- download -----------------------------------------------------------------


def _fetcher(
    snapshot: _Snapshot, filters: list[tuple[str, str, Any]]
) -> Callable[[_File], Awaitable[tuple[pd.DataFrame, httpx.Response]]]:
    """The fan-out's per-file fetch: the file's matching rows.

    The run combines the rows of every file; the file's ``T`` goes to
    ``snapshot``, since it describes the whole file rather than those rows.
    """

    async def fetch(item: _File) -> tuple[pd.DataFrame, httpx.Response]:
        client = active_client()
        if client is None:
            raise RuntimeError("fetch must run inside a FanOut run.")
        response = await client.get(item.url, headers=default_headers(item.url))
        # The run keeps every response; a body-less stand-in keeps it from
        # holding every file in memory.
        inert = inert_response(response.status_code, item.url, response.headers)
        site = item.monitoring_location_id
        try:
            _raise_for_non_200(response)
        except HTTPError as e:
            # A catalog entry whose file is gone: query the API instead. Any
            # other failure (a refused key, a firewall) is the caller's to see.
            if e.status_code != 404:
                raise
            snapshot.failed.add(site)
            return pd.DataFrame(), inert
        content = response.content
        del response  # only ``inert`` is kept; the body goes with ``content``
        rows, cutoff = await anyio.to_thread.run_sync(_read_file, content, filters)
        if cutoff is not None:
            # ``historical`` is rewritten the night after one of its records
            # changes, so the later of the two files' cutoffs applies to both.
            snapshot.cutoff[site] = max(snapshot.cutoff.get(site, cutoff), cutoff)
        return rows, inert

    return fetch


def _arrow_filters(query: _Query) -> list[tuple[str, str, Any]]:
    """pyarrow row filters for the query's stable-column and time filters.

    Changeable columns are filtered after the merge instead: a file row that
    fails the filter now may have been revised to pass it.
    """
    filters: list[tuple[str, str, Any]] = [
        (column, "in", query.values(column))
        for column in sorted(_STABLE_FILTERS & query.args.keys())
    ]
    for op, bound in ((">=", query.start), ("<=", query.end)):
        if bound is not None:
            value = bound.date() if query.layout.dated else bound.to_pydatetime()
            filters.append(("time", op, value))
    return filters


def _read_file(
    content: bytes, filters: list[tuple[str, str, Any]]
) -> tuple[pd.DataFrame, pd.Timestamp | None]:
    """The matching rows of one file, and the file's largest ``last_modified``.

    The filters are applied while reading, so row groups outside them are not
    decoded. The cutoff stands for when the snapshot was taken, so it covers
    the whole file: from the row-group statistics when every group has them,
    otherwise from the ``last_modified`` column alone.
    """
    # pyarrow is optional; get_data checked it is installed.
    import pyarrow as pa
    import pyarrow.parquet as pq

    # ``BufferReader`` reads the downloaded bytes in place, without a copy.
    table = pq.read_table(pa.BufferReader(content), filters=filters or None)
    latest = _latest_modified(pq.ParquetFile(pa.BufferReader(content)))
    cutoff = None if latest is None else pd.Timestamp(latest).tz_convert("UTC")
    return table.to_pandas(), cutoff


def _latest_modified(source: Any) -> Any:
    """The largest ``last_modified`` in a Parquet file, or ``None`` if empty."""
    meta = source.metadata
    if meta.num_rows == 0:
        return None
    column = source.schema_arrow.get_field_index("last_modified")
    stats = [
        meta.row_group(i).column(column).statistics for i in range(meta.num_row_groups)
    ]
    if all(s is not None and s.has_min_max for s in stats):
        return max(s.max for s in stats)
    import pyarrow.compute as pc

    return pc.max(source.read(columns=["last_modified"])["last_modified"]).as_py()


# --- the API part ---------------------------------------------------------------


def _query_api(
    query: _Query, cutoffs: dict[str, pd.Timestamp], missing: set[str]
) -> tuple[pd.DataFrame, BaseMetadata]:
    """The API rows: changes since each snapshot, and locations without files.

    One query, whose CQL filter has one top-level ``OR`` clause per cutoff
    day plus one for the locations without a usable file. The OGC chunker
    splits a filter at top-level ``OR`` when it outgrows a URL and fans the
    pieces out under the concurrency cap, so the clauses run concurrently
    without a fan-out of our own. Cutoffs are floored to the day, which gives
    the window a margin and keeps a few stale files from widening the change
    window for every other location.

    Changeable filters are sent only in the clause for locations without a
    file. A change query must also see a record that has just left the filter,
    so the merge can drop the file's stale copy; the merge applies them.
    """
    by_day: dict[pd.Timestamp, list[str]] = {}
    for site, cutoff in cutoffs.items():
        if site not in missing:
            by_day.setdefault(cutoff.floor("D"), []).append(site)
    # Each clause names at most ``_SITES_PER_CLAUSE`` locations: the chunker
    # splits the filter only at top-level ``OR``, so one clause must fit a URL.
    clauses = [
        f"({_in_clause('monitoring_location_id', part)} AND "
        f"last_modified >= '{day.strftime('%Y-%m-%dT%H:%M:%SZ')}')"
        for day, sites in sorted(by_day.items())
        for part in _parts(sites)
    ]
    changeable = [
        _in_clause(column, query.values(column))
        for column in sorted(_CHANGEABLE_FILTERS & query.args.keys())
    ]
    clauses += [
        f"({' AND '.join([_in_clause('monitoring_location_id', part), *changeable])})"
        for part in _parts(sorted(missing))
    ]

    request = {
        k: v
        for k, v in query.args.items()
        if k not in _CHANGEABLE_FILTERS and k != "monitoring_location_id"
    }
    request["filter"] = " OR ".join(clauses)
    request["filter_lang"] = "cql-text"
    return get_ogc_data(request, query.collection)


# --- shaping and merging ----------------------------------------------------------


def _shape_file_rows(df: pd.DataFrame, query: _Query) -> pd.DataFrame:
    """File rows in the getter's column types."""
    if df.empty:
        return pd.DataFrame()
    if query.layout.dated:
        df["time"] = pd.to_datetime(df["time"]).astype("datetime64[us]")
    df["value"] = pd.to_numeric(df["value"], errors="coerce").astype("float64")
    # pyarrow returns a list column as arrays; the API returns lists.
    df["qualifier"] = [None if q is None else list(q) for q in df["qualifier"]]
    # The API frame's strings are pandas' string type; a file's may arrive as
    # ``object``. Matters when no API rows give ``_match_types`` a template.
    return df.infer_objects()


def _points(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Each location's GeoJSON point, from its catalog item."""
    return {item["id"]: item["geometry"] for item in items if item.get("geometry")}


def _merge(
    file_rows: pd.DataFrame,
    api: pd.DataFrame,
    query: _Query,
    items: list[dict[str, Any]],
) -> pd.DataFrame:
    """Concatenate, keep the API's version of shared records, filter, sort."""
    combined = api.reset_index(drop=True)
    if not file_rows.empty:
        if query.layout.has_geometry:
            file_rows = _with_geometry(file_rows, api, _points(items))
        # An empty API frame has placeholder types, and concatenating it would
        # widen every file column to ``object``.
        parts = [file_rows] if api.empty else [combined, file_rows]
        combined = pd.concat(parts, ignore_index=True)
        combined = combined.drop_duplicates(["time_series_id", "time"], keep="first")
    for column in sorted(_CHANGEABLE_FILTERS & query.args.keys()):
        combined = combined[combined[column].isin(query.values(column))]
    api = api.drop(columns=query.layout.id_column, errors="ignore")
    combined = combined.reindex(columns=api.columns)
    if not api.empty:
        combined = _match_types(combined, api)
    return _sort_rows(combined, WATERDATA_DIALECT)


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
    try:
        import geopandas as gpd
        from shapely.geometry import shape
    except ImportError:
        coordinates = {site: point.get("coordinates") for site, point in points.items()}
        return file_rows.assign(geometry=sites.map(coordinates))
    # One shape per location, not per row.
    shapes = {site: shape(point) for site, point in points.items()}
    return gpd.GeoDataFrame(
        file_rows, geometry=sites.map(shapes), crs=getattr(api, "crs", None)
    )
