"""USGS rating-curve retrieval via the Water Data STAC catalog.

Wraps ``https://api.waterdata.usgs.gov/stac/v0/search`` and the per-feature
RDB downloads that follow. The STAC endpoint hosts standard NWIS rating
files (``exsa``, ``base``, ``corr``) for active streamgages — see the
service overview at https://api.waterdata.usgs.gov/docs/stac/ and the
WDFN announcement at https://waterdata.usgs.gov/blog/wdfn-rating-curves/.

The R analogue is ``read_waterdata_ratings`` in
https://github.com/DOI-USGS/dataRetrieval/.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from typing import Any, Literal, get_args

import pandas as pd
import requests

from dataretrieval.rdb import extract_rdb_comment, read_rdb

from .utils import _DURATION_RE, BASE_URL, _default_headers, _format_api_dates

logger = logging.getLogger(__name__)

STAC_URL = f"{BASE_URL}/stac/v0"

RATING_FILE_TYPE = Literal["exsa", "base", "corr"]
_VALID_FILE_TYPES = get_args(RATING_FILE_TYPE)


def get_ratings(
    monitoring_location_id: str | list[str] | None = None,
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
    background announcement. The R analogue is ``read_waterdata_ratings``
    in https://github.com/DOI-USGS/dataRetrieval/.

    Parameters
    ----------
    monitoring_location_id : string or list of strings, optional
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

    out: dict[str, pd.DataFrame] = {}
    for feature in matching:
        fid = feature["id"]
        try:
            out[fid] = _download_and_parse(feature, file_path, ssl_check)
        except (requests.RequestException, ValueError, OSError) as e:
            logger.warning("Failed to download / parse %s: %s", fid, e)

    return out


def _as_list(x: str | Iterable[str]) -> list[str]:
    """Normalize a string or iterable-of-strings to a list."""
    return [x] if isinstance(x, str) else list(x)


def _quote_cql_str(value: str) -> str:
    """Escape a single-quoted CQL literal by doubling embedded quotes.

    Defends against malformed filters / injection on arbitrary user input,
    even though valid USGS monitoring-location IDs cannot contain a quote.
    """
    return value.replace("'", "''")


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
    """Run a single STAC ``/search`` request and return its features."""
    params: dict[str, Any] = {"limit": limit}
    if filter_str is not None:
        params["filter"] = filter_str
    if time_str is not None:
        params["datetime"] = time_str
    if bbox is not None:
        params["bbox"] = ",".join(map(str, bbox))

    response = requests.get(
        f"{STAC_URL}/search",
        params=params,
        headers=_default_headers(),
        verify=ssl_check,
    )
    response.raise_for_status()
    return response.json().get("features", [])


def _download_and_parse(
    feature: dict[str, Any],
    file_path: str | None,
    ssl_check: bool,
) -> pd.DataFrame:
    """Fetch the feature's data asset, parse RDB, optionally persist to disk."""
    url = feature["assets"]["data"]["href"]
    response = requests.get(url, headers=_default_headers(), verify=ssl_check)
    response.raise_for_status()

    if file_path is not None:
        with open(os.path.join(file_path, feature["id"]), "w") as f:
            f.write(response.text)

    df = read_rdb(response.text)
    df.attrs["comment"] = extract_rdb_comment(response.text)
    df.attrs["url"] = url
    return df
