"""USGS rating-curve retrieval via the Water Data STAC catalog.

Wraps ``https://api.waterdata.usgs.gov/stac/v0/search`` and the per-feature
RDB downloads that follow. The STAC endpoint hosts standard NWIS rating
files (``exsa``, ``base``, ``corr``) for active streamgages — see the
service overview at https://api.waterdata.usgs.gov/docs/stac/ and the
WDFN announcement at https://waterdata.usgs.gov/blog/wdfn-rating-curves/.
"""

from __future__ import annotations

import os
import warnings
from collections.abc import Iterable
from typing import Any, Literal, get_args

import anyio
import httpx
import pandas as pd

from dataretrieval._validation import render_options
from dataretrieval.exceptions import DataRetrievalError, SkippedRatingWarning
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.ogc.filters import _cql_in_clause
from dataretrieval.ogc.requests import _check_monitoring_location_id
from dataretrieval.rdb import extract_rdb_comment, read_rdb
from dataretrieval.transport.fanout import FanOut, active_client
from dataretrieval.transport.http import (
    default_headers as _default_headers,
)
from dataretrieval.transport.retry import RetryPolicy
from dataretrieval.waterdata.catalog import (
    chunk_in_filters,
    format_time,
    require_url_fits,
    search_items,
    search_items_chunked,
)
from dataretrieval.waterdata.endpoints import ratings_catalog_url

__all__ = ["get_ratings"]


RATING_FILE_TYPE = Literal["exsa", "base", "corr"]
_VALID_FILE_TYPES = get_args(RATING_FILE_TYPE)


_REMEDY = "Shorten `time` or `bbox`, or split the call into several calls."


def get_ratings(
    monitoring_location_id: str | Iterable[str] | None = None,
    file_type: RATING_FILE_TYPE | list[RATING_FILE_TYPE] = "exsa",
    file_path: str | None = None,
    time: str | list[str] | None = None,
    bbox: list[float] | None = None,
    limit: int = 10000,
    download_and_parse: bool = True,
    ssl_check: bool = True,
) -> dict[str, pd.DataFrame] | list[dict[str, Any]]:
    """Get USGS stage-discharge rating curves from the Water Data STAC catalog.

    Returns the current rating tables for one or more active USGS streamgages.
    The catalog hosts three file types:

    - ``"exsa"`` — expanded shift-adjusted rating (default). Adds a ``SHIFT``
      column to ``"base"`` indicating the current shift for each ``INDEP``.
    - ``"base"`` — three columns: ``INDEP`` (typically gage height, ft);
      ``DEP`` (typically discharge, ft^3/s); ``STOR`` ("``*``" marks fixed
      points of the rating).
    - ``"corr"`` — three columns: ``INDEP``; ``CORR`` (correction for that
      value); ``CORRINDEP`` (corrected INDEP).

    See https://api.waterdata.usgs.gov/docs/stac/ for the upstream service
    docs and https://waterdata.usgs.gov/blog/wdfn-rating-curves/ for the
    background announcement.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        One or more identifiers in ``AGENCY-ID`` form (e.g.
        ``"USGS-01104475"``). If omitted, the spatial / temporal filters
        determine the result set.
    file_type : ``"exsa"``, ``"base"``, ``"corr"``, or a list, default ``"exsa"``
        Which rating file(s) to request.
    file_path : string, optional
        Directory the downloaded RDB files are written to. If ``None``
        (the default), the parsed ``DataFrame`` is returned without
        persisting the bytes to disk; ``df.attrs["url"]`` still records
        where each rating came from.
    time : string, date, datetime, or pandas.Timestamp, or a list of them, optional
        STAC ``datetime`` filter — a single date / datetime, an interval
        (``"start/end"``, optionally half-bounded with ``..``), or a list of
        two values with ``None`` for an open end (``["2026-04-29", None]``).
        An interval string is read like the list form: each side is
        converted to UTC, and a naive one is UTC, as in pandas. A value
        that is not a date raises ``ValueError``. ISO 8601 *durations*
        (``"P1M"``, ``"PT36H"``, …) are **not** supported by the
        rating-curve service; passing one raises ``ValueError``.
    bbox : list of numbers, optional
        Only features whose geometry intersects the bounding box are
        selected. Format: ``[xmin, ymin, xmax, ymax]`` in CRS 4326
        (longitude / latitude, west-south-east-north).
    limit : int, default 10000
        Page size for the STAC ``/search`` request (capped at 10000).
    download_and_parse : bool, default ``True``
        If ``True``, download every matching RDB file and parse it into a
        ``DataFrame``. If ``False``, return the raw list of STAC feature
        dicts so the caller can inspect what's available before downloading.
    ssl_check : bool, default ``True``
        Verify the server's SSL certificate.

    Returns
    -------
    dict[str, pandas.DataFrame] or list[dict]
        When ``download_and_parse=True`` (the default), a dict keyed by
        feature ID (e.g. ``"USGS-01104475.exsa.rdb"``) mapping to a parsed
        ``DataFrame``. Each frame records provenance in
        ``df.attrs["comment"]`` (the RDB ``#``-prefixed header lines, like
        rating id, parameter, last-shifted timestamp) and
        ``df.attrs["url"]`` (the asset URL it was fetched from). When
        ``download_and_parse=False``, the raw list of STAC feature dicts
        as returned by the search endpoint.

    Raises
    ------
    ValueError
        For an unrecognized ``file_type`` value or an ISO 8601 duration in
        ``time``.
    DataRetrievalError
        The typed subclass for an HTTP error response during the search (see
        :func:`transport.pagination.paginate`);
        or :class:`~dataretrieval.exceptions.NetworkError` if a request
        can't reach the service in a way retrying cannot fix.
    FanOutInterrupted
        A transient failure (429 / 5xx / timeout) was not resolved by the built-in
        retries during the search or a download. ``exc.call.resume()`` finishes the
        interrupted stage (see :doc:`/userguide/errors`); the assembled per-feature dict
        is returned by a new ``get_ratings`` call.

    Warns
    -----
    SkippedRatingWarning
        One feature of the batch failed *deterministically* -- a stale catalog entry
        (404 on its data asset), a feature with no data asset, a malformed RDB file.
        That feature is skipped and its id is absent from the returned dict; the rest of
        the batch is unaffected. A monitoring location with no published rating never
        produces a warning -- it matches no feature in the search, so there is nothing
        to skip. See
        :class:`~dataretrieval.exceptions.SkippedItemWarning` for the policy
        (transients never skip) and the ``filterwarnings`` call that makes a skip fatal.

    Examples
    --------
    .. code::

        >>> # Default exsa ratings for two sites
        >>> ratings = dataretrieval.waterdata.get_ratings(
        ...     monitoring_location_id=["USGS-01104475", "USGS-01104460"],
        ...     file_type="exsa",
        ... )
        >>> ratings["USGS-01104475.exsa.rdb"].head()

        >>> # Both exsa and corr files for the same two sites
        >>> ratings = dataretrieval.waterdata.get_ratings(
        ...     monitoring_location_id=["USGS-01104475", "USGS-01104460"],
        ...     file_type=["exsa", "corr"],
        ... )

        >>> # Bounding-box query, listing what's available without downloading
        >>> features = dataretrieval.waterdata.get_ratings(
        ...     bbox=[-95.0, 40.0, -92.0, 42.0],
        ...     download_and_parse=False,
        ... )

        >>> # Restrict to features in a date range (durations not supported)
        >>> features = dataretrieval.waterdata.get_ratings(
        ...     bbox=[-95.0, 40.0, -92.0, 42.0],
        ...     time=["2026-04-29", ".."],
        ...     download_and_parse=False,
        ... )

    """
    monitoring_location_id = _check_monitoring_location_id(monitoring_location_id)
    file_types = _as_list(file_type)
    _validate_file_types(file_types)
    time_str = format_time(time, service="rating-curve")

    # Match R: set file_type server-side only when one type is requested.
    server_file_type = file_types[0] if len(file_types) == 1 else None
    features, _ = _search(
        monitoring_location_id,
        server_file_type,
        time_str=time_str,
        bbox=bbox,
        limit=limit,
        ssl_check=ssl_check,
    )

    if not download_and_parse:
        return features

    matching = _filter_features_by_type(features, file_types)

    if file_path is not None:
        os.makedirs(file_path, exist_ok=True)

    return _download_all(matching, file_path, ssl_check)


def _as_list(x: str | Iterable[str]) -> list[str]:
    """Normalize a string or iterable-of-strings to a list."""
    return [x] if isinstance(x, str) else list(x)


def _validate_file_types(file_types: list[str]) -> None:
    """Raise ValueError for any unrecognized file type."""
    invalid = [ft for ft in file_types if ft not in _VALID_FILE_TYPES]
    if invalid:
        raise ValueError(
            f"Invalid file_type: {render_options(invalid)}. "
            f"Valid options are: {render_options(_VALID_FILE_TYPES)}."
        )


def _filter_features_by_type(
    features: list[dict[str, Any]], file_types: list[str]
) -> list[dict[str, Any]]:
    """Return only features whose file_type matches the requested types."""
    requested = set(file_types)
    return [
        f for f in features if f.get("properties", {}).get("file_type") in requested
    ]


def _search(
    monitoring_location_id: str | list[str] | None,
    file_type: str | None,
    *,
    time_str: str | None,
    bbox: list[float] | None,
    limit: int,
    ssl_check: bool,
) -> tuple[list[dict[str, Any]], httpx.Response]:
    """Search the ``ratings`` collection, chunked so each URL fits the edge.

    The catalog's edge refuses a URL over ``EDGE_URL_LIMIT`` characters, so a
    long ``monitoring_location_id`` list is split across concurrent searches.
    """
    if monitoring_location_id is None:
        filter_str = _build_filter(None, file_type)
        require_url_fits(
            filter_str,
            "ratings",
            limit,
            time_str=time_str,
            bbox=bbox,
            remedy=_REMEDY,
        )
        return search_items(
            "ratings",
            filter_str=filter_str,
            time_str=time_str,
            bbox=bbox,
            limit=limit,
            ssl_check=ssl_check,
            service="ratings",
        )
    filters = chunk_in_filters(
        _as_list(monitoring_location_id),
        "ratings",
        limit,
        # Already checked against _VALID_FILE_TYPES, none of which has a quote.
        tail="" if file_type is None else f" AND file_type = '{file_type}'",
        time_str=time_str,
        bbox=bbox,
        remedy=_REMEDY,
    )
    return search_items_chunked(
        "ratings",
        filters,
        limit=limit,
        ssl_check=ssl_check,
        service="ratings",
        time_str=time_str,
        bbox=bbox,
    )


def _build_filter(
    monitoring_location_id: str | list[str] | None,
    file_type: str | None,
) -> str | None:
    """Compose the CQL filter sent to STAC ``/search``.

    Returns ``None`` when neither argument constrains the search.
    """
    parts: list[str] = []
    if monitoring_location_id is not None:
        parts.append(
            _cql_in_clause(
                "monitoring_location_id",
                _as_list(monitoring_location_id),
                remedy="Pass an 'AGENCY-ID' id, e.g., 'USGS-01646500'.",
            )
        )
    if file_type is not None:
        # Already checked against _VALID_FILE_TYPES, none of which has a quote.
        parts.append(f"file_type = '{file_type}'")
    return " AND ".join(parts) if parts else None


def _asset_href(feature: dict[str, Any]) -> str | None:
    """The feature's data-asset href, or ``None`` when the catalog omits it."""
    href: str | None = feature.get("assets", {}).get("data", {}).get("href")
    return href


def _inert_response(
    status: int, url: str, headers: httpx.Headers | None = None
) -> httpx.Response:
    """A body-less stand-in the executor can aggregate.

    The executor keeps every completed item's response until the run ends,
    but its aggregation reads only status, headers, and URL -- never the
    body. Passing it a stand-in keeps a large batch from holding every
    downloaded file in memory for the whole run. ``elapsed`` is left
    unset; the aggregate's ``_safe_elapsed`` treats that as zero.
    """
    return httpx.Response(status, headers=headers, request=httpx.Request("GET", url))


def _write_rating(path: str, body: str) -> None:
    """Write one rating to disk as UTF-8, preserving line endings."""
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(body)


async def _fetch_rating(
    feature: dict[str, Any], file_path: str | None
) -> tuple[pd.DataFrame, httpx.Response]:
    """Fetch one feature's data asset, parse RDB, optionally persist to disk.

    Headers are evaluated against each asset href -- assets can be on a different host
    than the catalog, and must not receive its credentials. Runs inside a fan-out run:
    the executor sets the shared client before any fetch starts.
    """
    fid = feature["id"]
    href = _asset_href(feature)
    if not href:
        raise ValueError(
            f"Rating feature {fid!r} has no download URL, so there is nothing "
            "to fetch. Retrying will not help; exclude this monitoring "
            "location, or report it if a rating curve file is expected."
        )
    headers = _default_headers(href)
    session = active_client()
    if session is None:
        raise RuntimeError("_fetch_rating must run inside a FanOut run.")
    response = await session.get(href, headers=headers)
    _raise_for_non_200(response)

    if file_path is not None:
        await anyio.to_thread.run_sync(
            _write_rating, os.path.join(file_path, fid), response.text
        )

    df = read_rdb(response.text)
    df.attrs["comment"] = extract_rdb_comment(response.text)
    df.attrs["url"] = href
    return df, response


def _download_all(
    features: list[dict[str, Any]],
    file_path: str | None,
    ssl_check: bool,
) -> dict[str, pd.DataFrame]:
    """Download every feature's rating over the shared fan-out executor.

    The plan is the feature list itself -- ``FanOut`` requires a plan only to be
    sized and iterable -- so the downloads get bounded concurrency,
    per-attempt retry, the progress line, and the resumable interruption
    taxonomy.

    Failure policy (rationale on
    :class:`~dataretrieval.exceptions.SkippedItemWarning`): a *transient*
    failure (429 / 5xx / timeout / connection drop) re-raises so the executor
    retries and then raises resumable; a *deterministic* per-feature failure
    warns with :class:`~dataretrieval.exceptions.SkippedRatingWarning` and
    skips the feature. ``OSError`` writing ``file_path`` propagates -- a
    local disk problem is not a per-feature condition. Raw ``httpx`` errors
    pass through unchanged so the executor can classify and retry them.

    The public result is a dict keyed by feature id, so the fetch closure
    accumulates it; the executor's combined frame is not the return shape and
    is discarded. Both outcomes return to the executor a body-less
    :func:`_inert_response` -- a skip so the item counts as complete (a later
    ``resume()`` continues past it rather than re-attempting), a success so
    the run does not hold every downloaded file in memory while keeping the
    response's status and quota headers for aggregation.
    """
    out: dict[str, pd.DataFrame] = {}
    if not features:
        return out

    async def fetch(feature: dict[str, Any]) -> tuple[pd.DataFrame, httpx.Response]:
        fid = feature.get("id", "<missing id>")
        try:
            df, response = await _fetch_rating(feature, file_path)
        except (DataRetrievalError, LookupError, ValueError) as e:
            if isinstance(e, DataRetrievalError) and e.retryable:
                raise  # transient: the executor retries, then raises resumable
            warnings.warn(
                f"Skipping rating {fid!r}: {e}",
                SkippedRatingWarning,
                stacklevel=2,
            )
            # 204: completed, no content.
            return pd.DataFrame(), _inert_response(
                204, _asset_href(feature) or f"{ratings_catalog_url()}/search"
            )
        out[fid] = df
        return df, _inert_response(
            response.status_code, str(response.url), response.headers
        )

    FanOut(
        features,
        fetch,
        RetryPolicy.from_configuration(adapter="waterdata"),
        client_options={"verify": ssl_check},
        # No single URL expresses "all of these assets" -- the aggregate
        # reports the first, matching what a single-feature call would show.
        canonical_url=_asset_href(features[0]),
        service="ratings",
        adapter="waterdata",
    ).resume()
    return out
