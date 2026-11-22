"""Discovery of the Water Data Parquet files indexed by the STAC catalog.

USGS publishes the ``daily`` and ``continuous`` collections a second way: as
Parquet files, one set per monitoring location, served from
``https://api.waterdata.usgs.gov/stac-files/`` and indexed by the STAC
catalog (https://waterdata.usgs.gov/blog/api-faster-access/). Downloading a
location's file is far faster than paging through the OGC API for the same
records, which makes the files the better source for bulk pulls.

The file host cannot be listed, so the catalog is the only way to learn which
files exist. :func:`get_parquet_files` searches it and returns one row per
file. It downloads nothing: the URLs go to whatever reads Parquet over HTTP,
such as DuckDB, Dask, pandas, or pyarrow.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Literal, get_args

import httpx
import pandas as pd

from dataretrieval._response_metadata import BaseMetadata
from dataretrieval._validation import require_one_of
from dataretrieval.ogc.filters import _cql_in_clause
from dataretrieval.ogc.requests import _check_monitoring_location_id
from dataretrieval.waterdata.catalog import (
    MAX_PAGE_SIZE,
    chunk_in_filters,
    format_time,
    require_url_fits,
    search_items,
    search_items_chunked,
)

__all__ = ["get_parquet_files"]

PARQUET_COLLECTION = Literal["daily", "continuous"]
_PARQUET_COLLECTIONS = get_args(PARQUET_COLLECTION)

#: The columns of the returned frame, in order. An empty result has them too.
_COLUMNS = [
    "monitoring_location_id",
    "asset",
    "url",
    "start_datetime",
    "end_datetime",
]


def get_parquet_files(
    collection: PARQUET_COLLECTION,
    *,
    monitoring_location_id: str | Iterable[str] | None = None,
    time: Any = None,
    bbox: list[float] | None = None,
    filter: str | None = None,
    limit: int = MAX_PAGE_SIZE,
    ssl_check: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """List the Parquet files that hold a Water Data collection.

    Searches the Water Data STAC catalog and returns one row per Parquet file,
    with its URL and the time span the monitoring location covers. Nothing
    is downloaded. Pass the ``url`` column to a Parquet reader: one that reads
    over HTTP with range requests, such as DuckDB, fetches only the row
    groups a query needs (see the examples).

    The files mirror the ``daily`` and ``continuous`` OGC collections, which
    :func:`get_daily` and :func:`get_continuous` read. USGS recommends them
    for bulk pulls: every value for a state, or for a multi-year period. For
    the most recent observations, use the OGC getters, because the files are
    regenerated on a schedule rather than as data arrive.

    ``daily`` has one file per monitoring location, with asset ``"data"``.
    ``continuous`` splits each location in two: ``"current"`` holds about the
    last 150 days and ``"historical"`` everything older. A location may have
    only one of them.

    The files are served from the same host as the API, and each download
    counts against the API key's hourly rate limit, like an API request.
    Send the key with the reader's requests (see the examples), or the
    anonymous limit applies.

    Parameters
    ----------
    collection : {"daily", "continuous"}
        Which collection's files to list.
    monitoring_location_id : string or iterable of strings, optional
        One or more identifiers in ``AGENCY-ID`` form (e.g.
        ``"USGS-01646500"``). If omitted, the other filters select the
        monitoring locations.
    time : string, date, datetime, or pandas.Timestamp, or a list of them, optional
        Keep only monitoring locations whose records overlap this interval: a
        single instant, an interval (``"start/end"``, optionally half-bounded
        with ``..``), or a list of two values with ``None`` for an open end.
        A naive value is UTC. ISO 8601 durations (``"P1M"``) are not
        supported. This selects *files*; it does not trim the records inside
        them, so filter the rows when you read them.
    bbox : list of numbers, optional
        Only monitoring locations inside the bounding box
        ``[xmin, ymin, xmax, ymax]``, in longitude and latitude (CRS 4326).
    filter : string, optional
        A CQL2-text expression over the catalog's queryable properties, which
        are the monitoring-location fields of
        :func:`get_monitoring_locations` (``state_code``, ``county_code``,
        ``hydrologic_unit_code``, ``site_type_code``, and others), for
        example ``"state_code = '39' AND site_type_code = 'ST'"``. Combined
        with the other arguments by ``AND``.
    limit : int, default 10000
        Page size of the catalog search, capped at 10000. Every page is
        fetched, so this does not cap the number of files returned.
    ssl_check : bool, default True
        Verify the server's SSL certificate.

    Returns
    -------
    df : pandas.DataFrame
        One row per file, with columns ``monitoring_location_id``, ``asset``
        (``"data"``, ``"current"``, or ``"historical"``), ``url``, and
        ``start_datetime`` and ``end_datetime`` (UTC), the span of the
        location's records in the collection.
    md : BaseMetadata
        Metadata of the catalog search.

    Raises
    ------
    ValueError
        For a ``collection`` other than ``"daily"`` or ``"continuous"``, an
        ISO 8601 duration or non-date in ``time``, or a malformed
        ``monitoring_location_id``.
    DataRetrievalError
        The typed subclass for an HTTP error response from the catalog, or
        :class:`~dataretrieval.exceptions.NetworkError` if a request can't
        reach the service in a way retrying cannot fix.
    FanOutInterrupted
        A transient failure (429 / 5xx / timeout) was not resolved by the
        built-in retries. ``exc.call.resume()`` finishes the search.

    Examples
    --------
    .. code::

        >>> # Daily-value files for Ohio stream gages with records since 2004
        >>> files, md = dataretrieval.waterdata.get_parquet_files(
        ...     "daily",
        ...     filter="state_code = '39' AND site_type_code = 'ST'",
        ...     time=["2004-01-01", None],
        ... )

        >>> # Read them as one table with DuckDB, which reads only the row
        >>> # groups the WHERE clause needs. Send the API key with each read.
        >>> import os, duckdb
        >>> con = duckdb.connect()
        >>> key = os.environ["API_USGS_PAT"]
        >>> con.execute(
        ...     "CREATE SECRET (TYPE http, "
        ...     f"EXTRA_HTTP_HEADERS MAP {{'X-Api-Key': '{key}'}})"
        ... )
        >>> df = con.execute(
        ...     "SELECT * FROM read_parquet(?, union_by_name = true) "
        ...     "WHERE parameter_code = '00060' AND time >= DATE '2004-01-01'",
        ...     [files["url"].tolist()],
        ... ).df()

        >>> # Or one Dask partition per file. pandas sends ``storage_options``
        >>> # as request headers for an HTTP(S) URL.
        >>> import dask.dataframe as dd
        >>> import pandas as pd
        >>> ddf = dd.from_map(
        ...     pd.read_parquet,
        ...     files["url"].tolist(),
        ...     storage_options={"X-Api-Key": key},
        ... )

    """
    require_one_of(collection, _PARQUET_COLLECTIONS, name="collection")
    monitoring_location_id = _check_monitoring_location_id(monitoring_location_id)
    time_str = format_time(time, service=f"{collection} Parquet file")
    items, response = _search(
        collection,
        monitoring_location_id,
        filter,
        time_str=time_str,
        bbox=bbox,
        limit=limit,
        ssl_check=ssl_check,
    )
    return _files_frame(items), BaseMetadata(response)


def _search(
    collection: str,
    monitoring_location_id: str | list[str] | None,
    filter: str | None,
    *,
    time_str: str | None,
    bbox: list[float] | None,
    limit: int,
    ssl_check: bool,
) -> tuple[list[dict[str, Any]], httpx.Response]:
    """Search the catalog, one chunk per run of ids that fits in a URL.

    The catalog's edge refuses a URL over ``EDGE_URL_LIMIT`` characters, so a
    long ``monitoring_location_id`` list is split across concurrent searches.
    """
    remedy = (
        "Shorten `filter`, `time` or `bbox`, or split the call into several "
        "calls, each with a shorter `filter` or fewer `monitoring_location_id` "
        "values."
    )
    lead = f"({filter}) AND " if filter is not None and filter.strip() else ""
    if monitoring_location_id is None:
        filter_str = _build_filter(None, filter)
        require_url_fits(
            filter_str,
            collection,
            limit,
            time_str=time_str,
            bbox=bbox,
            remedy=remedy,
        )
        return search_items(
            collection,
            filter_str=filter_str,
            time_str=time_str,
            bbox=bbox,
            limit=limit,
            ssl_check=ssl_check,
            service=collection,
        )
    ids = (
        [monitoring_location_id]
        if isinstance(monitoring_location_id, str)
        else monitoring_location_id
    )
    filters = chunk_in_filters(
        ids,
        collection,
        limit,
        lead=lead,
        time_str=time_str,
        bbox=bbox,
        remedy=remedy,
    )
    return search_items_chunked(
        collection,
        filters,
        limit=limit,
        ssl_check=ssl_check,
        service=collection,
        time_str=time_str,
        bbox=bbox,
    )


def _build_filter(
    monitoring_location_id: str | list[str] | None, filter: str | None
) -> str | None:
    """Compose the CQL2-text filter, or ``None`` when nothing constrains it."""
    parts: list[str] = []
    if filter is not None and filter.strip():
        # Parenthesized so an ``OR`` in the caller's expression stays inside it.
        parts.append(f"({filter})")
    if monitoring_location_id is not None:
        ids = (
            [monitoring_location_id]
            if isinstance(monitoring_location_id, str)
            else monitoring_location_id
        )
        parts.append(
            _cql_in_clause(
                "monitoring_location_id",
                ids,
                remedy="Pass an 'AGENCY-ID' id, e.g., 'USGS-01646500'.",
            )
        )
    return " AND ".join(parts) if parts else None


def _files_frame(items: list[dict[str, Any]]) -> pd.DataFrame:
    """One row per data asset of each catalog item, sorted by location."""
    rows = [
        {
            "monitoring_location_id": item.get("properties", {}).get(
                "monitoring_location_id", item.get("id")
            ),
            "asset": key,
            "url": asset["href"],
            "start_datetime": item.get("properties", {}).get("start_datetime"),
            "end_datetime": item.get("properties", {}).get("end_datetime"),
        }
        for item in items
        for key, asset in item.get("assets", {}).items()
        if "data" in asset.get("roles", []) and asset.get("href")
    ]
    df = pd.DataFrame(rows, columns=_COLUMNS)
    for column in ("start_datetime", "end_datetime"):
        df[column] = pd.to_datetime(df[column], utc=True, format="ISO8601")
    return df.sort_values(["monitoring_location_id", "asset"], ignore_index=True)
