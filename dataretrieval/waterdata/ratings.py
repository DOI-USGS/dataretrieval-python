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
from datetime import timedelta
from typing import Any, Literal, get_args

import httpx
import pandas as pd

from dataretrieval.exceptions import DataRetrievalError, SkippedRatingWarning
from dataretrieval.ogc.dates import _DURATION_RE, _format_api_dates
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.ogc.filters import _quote_cql_str
from dataretrieval.ogc.requests import _check_monitoring_location_id
from dataretrieval.rdb import extract_rdb_comment, read_rdb
from dataretrieval.transport.fanout import FanOut, active_client
from dataretrieval.transport.http import (
    default_headers as _default_headers,
)
from dataretrieval.transport.http import (
    open_async_client as _open_async_client,
)
from dataretrieval.transport.links import resolve_next_url
from dataretrieval.transport.pagination import paginate
from dataretrieval.transport.retry import RetryPolicy
from dataretrieval.waterdata.endpoints import STAC_URL

__all__ = ["get_ratings"]


RATING_FILE_TYPE = Literal["exsa", "base", "corr"]
_VALID_FILE_TYPES = get_args(RATING_FILE_TYPE)


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
    time : string or list of strings, optional
        STAC ``datetime`` filter (passed through verbatim under that name)
        — a single date / datetime, or an interval (``"start/end"``,
        optionally half-bounded with ``..``). ISO 8601 *durations*
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
        dicts so the caller can inspect what's available before pulling
        bytes.
    ssl_check : bool, default ``True``
        Verify the server's SSL certificate.

    Returns
    -------
    dict[str, pandas.DataFrame] or list[dict]
        When ``download_and_parse=True`` (the default), a dict keyed by
        feature ID (e.g. ``"USGS-01104475.exsa.rdb"``) mapping to a parsed
        ``DataFrame``. Each frame carries provenance in
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
        A transient failure (429 / 5xx / timeout) survived the built-in
        retries during the search or a download. ``exc.call.resume()``
        finishes the interrupted stage (see :doc:`/userguide/errors`); the
        assembled per-feature dict is returned by a fresh ``get_ratings``
        call.

    Warns
    -----
    SkippedRatingWarning
        One feature of the batch failed *deterministically* -- a stale
        catalog entry (404 on its data asset), a feature with no data asset,
        a malformed RDB file. That feature is skipped and its id is absent
        from the returned dict; the rest of the batch is unaffected. A site
        with no published rating never warns -- it matches no feature in the
        search, so there is nothing to skip. Transient failures are never
        skipped (they retry, then raise resumable). To make a skip fatal::

            warnings.filterwarnings("error", category=SkippedRatingWarning)

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
    invalid = [ft for ft in file_types if ft not in _VALID_FILE_TYPES]
    if invalid:
        raise ValueError(
            f"Invalid file_type {invalid!r}; "
            f"valid options are {list(_VALID_FILE_TYPES)}."
        )

    if time is not None and any(_DURATION_RE.match(str(v)) for v in _as_list(time)):
        raise ValueError(
            "ISO 8601 durations (e.g. 'P7D') are not supported in `time` "
            "for the rating-curve service. Provide a date or interval instead."
        )
    time_str = _format_api_dates(time) if time is not None else None

    # Mirror R: pin file_type server-side only when one type is requested.
    server_file_type = file_types[0] if len(file_types) == 1 else None
    filter_str = _build_filter(monitoring_location_id, server_file_type)

    features = _search(filter_str, time_str, bbox, limit, ssl_check)

    if not download_and_parse:
        return features

    requested = set(file_types)
    matching = [
        f for f in features if f.get("properties", {}).get("file_type") in requested
    ]

    if file_path is not None:
        os.makedirs(file_path, exist_ok=True)

    return _download_all(matching, file_path, ssl_check)


def _as_list(x: str | Iterable[str]) -> list[str]:
    """Normalize a string or iterable-of-strings to a list."""
    return [x] if isinstance(x, str) else list(x)


def _build_filter(
    monitoring_location_id: str | list[str] | None,
    file_type: str | None,
) -> str | None:
    """Compose the CQL filter sent to STAC ``/search``.

    Returns ``None`` when neither argument constrains the search.
    """
    parts: list[str] = []
    if monitoring_location_id is not None:
        ids = _as_list(monitoring_location_id)
        joined = "', '".join(_quote_cql_str(i) for i in ids)
        parts.append(f"monitoring_location_id IN ('{joined}')")
    if file_type is not None:
        parts.append(f"file_type = '{_quote_cql_str(file_type)}'")
    return " AND ".join(parts) if parts else None


def _search(
    filter_str: str | None,
    time_str: str | None,
    bbox: list[float] | None,
    limit: int,
    ssl_check: bool,
) -> list[dict[str, Any]]:
    """Run STAC ``/search`` and return ALL matching features.

    ``limit`` is the page size (clamped to the service maximum of 10,000); the
    STAC ``next`` link is followed until exhausted so a result set larger than
    one page isn't silently truncated.

    The page walk is :func:`dataretrieval.transport.pagination.paginate` with
    STAC strategies, driven as a one-item
    :class:`~dataretrieval.transport.fanout.FanOut` -- the same executor every
    other getter uses -- so the search gets per-attempt retry, the stall
    budget, a progress line, and the resumable interruption taxonomy instead
    of a bespoke sync loop that had none of them. Pages carry features rather
    than rows, so each page frame wraps the raw feature dicts in a single
    ``feature`` column.
    """
    query_params: dict[str, Any] = {"limit": min(limit, 10000)}
    if filter_str is not None:
        query_params["filter"] = filter_str
    if time_str is not None:
        query_params["datetime"] = time_str
    if bbox is not None:
        query_params["bbox"] = ",".join(map(str, bbox))

    url = f"{STAC_URL}/search"
    req = httpx.Request("GET", url, params=query_params, headers=_default_headers(url))

    def parse_response(resp: httpx.Response) -> tuple[pd.DataFrame, str | None]:
        body = resp.json()
        page = pd.DataFrame({"feature": body.get("features", [])})
        # The STAC ``next`` link is a fully-formed GET href carrying the
        # limit/filter/bbox and a continuation token, so it becomes the
        # cursor verbatim -- except for the shared safety policy: the href is
        # response data, so it is checked before it becomes a request. A link
        # to another host would carry this request's API key off the
        # authorized host, and one carrying ``user:pass@`` would mint an
        # ``Authorization: Basic`` header the caller never configured.
        href = next(
            (lnk["href"] for lnk in body.get("links", []) if lnk.get("rel") == "next"),
            None,
        )
        cursor = (
            None if href is None else resolve_next_url(href, resp, service="ratings")
        )
        return page, cursor

    async def follow_up(cursor: str, sess: httpx.AsyncClient) -> httpx.Response:
        return await sess.get(cursor, headers=_default_headers(cursor))

    async def fetch(request: httpx.Request) -> tuple[pd.DataFrame, httpx.Response]:
        return await paginate(
            request,
            parse_response=parse_response,
            follow_up=follow_up,
            # Borrow the executor's shared client for every page.
            client=active_client(),
            raise_for_status=_raise_for_non_200,
        )

    df, _ = FanOut(
        [req],
        fetch,
        RetryPolicy.from_env(),
        client_options={"verify": ssl_check},
        canonical_url=str(req.url),
        service="ratings",
    ).resume()
    return [] if df.empty else list(df["feature"])


async def _fetch_rating(
    feature: dict[str, Any], file_path: str | None
) -> tuple[pd.DataFrame, httpx.Response]:
    """Fetch one feature's data asset, parse RDB, optionally persist to disk.

    Headers are evaluated against each asset href -- assets can live on a
    different host than the catalog, and must not inherit its auth. Borrows
    the executor's shared client; outside a drive (a direct unit call) it
    opens a short-lived one.
    """
    fid = feature["id"]
    href = feature.get("assets", {}).get("data", {}).get("href")
    if not href:
        raise ValueError(f"STAC feature {fid!r} carries no data asset href.")
    headers = _default_headers(href)
    session = active_client()
    if session is not None:
        response = await session.get(href, headers=headers)
    else:
        async with _open_async_client() as own:
            response = await own.get(href, headers=headers)
    _raise_for_non_200(response)

    if file_path is not None:
        with open(os.path.join(file_path, fid), "w") as f:
            f.write(response.text)

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

    The plan is the feature list itself -- ``FanOut`` asks a plan only to be
    sized and iterable -- so the downloads get bounded concurrency,
    per-attempt retry, the progress line, and the resumable interruption
    taxonomy in place of the previous serial loop, which had none of them.

    Failure policy -- split by whether retrying could help. A *transient*
    failure (429 / 5xx / timeout / connection drop) is retried and, if
    retries run out, raised as a resumable interruption: rate limiting is
    systematic, so skipping it would silently drop every remaining feature,
    which was the old serial loop's worst case. A *deterministic* per-feature
    failure (a stale catalog entry 404ing, a feature with no data asset, a
    malformed RDB) is that feature's problem, not the batch's: it is skipped
    under a :class:`~dataretrieval.exceptions.SkippedRatingWarning` naming
    the feature, visible by default and escalatable to an error for strict
    all-or-nothing behavior. ``OSError`` writing ``file_path`` propagates --
    a local disk problem is not a per-feature condition. Raw ``httpx``
    errors pass through untouched so the executor can classify and retry
    them.

    The public result is a dict keyed by feature id, so the fetch closure
    accumulates it; the executor's combined frame is not the return shape and
    is discarded. A skipped feature returns an inert placeholder pair so the
    executor marks it complete -- a later ``resume()`` continues past it
    rather than re-attempting the skip.
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
            href = feature.get("assets", {}).get("data", {}).get("href")
            # The executor aggregates each completed item's response
            # (headers / elapsed / url), so the skip must hand back a real,
            # inert response rather than None. 204: completed, no content.
            placeholder = httpx.Response(
                204, request=httpx.Request("GET", href or f"{STAC_URL}/search")
            )
            placeholder.elapsed = timedelta(0)
            return pd.DataFrame(), placeholder
        out[fid] = df
        return df, response

    FanOut(
        features,
        fetch,
        RetryPolicy.from_env(),
        client_options={"verify": ssl_check},
        # No single URL expresses "all of these assets" -- the aggregate
        # reports the first, matching what a single-feature call would show.
        canonical_url=features[0].get("assets", {}).get("data", {}).get("href"),
        service="ratings",
    ).resume()
    return out
