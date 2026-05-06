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
from typing import Any, Literal, get_args

import pandas as pd
import requests

# Rating files use the same USGS RDB shape as NWIS responses (comment
# block prefixed with ``#``, header row, format-spec row, then tab-separated
# data), so we reuse the parser already in ``nwis``. ``_read_rdb`` is private;
# if it ever moves or its contract changes we want a loud failure here, hence
# the explicit import rather than a copy.
from dataretrieval.nwis import _read_rdb

from .utils import BASE_URL, _default_headers, _format_api_dates

logger = logging.getLogger(__name__)

STAC_URL = f"{BASE_URL}/stac/v0"

RATING_FILE_TYPE = Literal["exsa", "base", "corr"]
_VALID_FILE_TYPES = get_args(RATING_FILE_TYPE)


def _quote_cql_str(value: str) -> str:
    """Escape a string for inclusion in a single-quoted CQL literal.

    CQL escapes a single quote by doubling it. Most monitoring-location IDs
    can never contain a quote, but the function accepts arbitrary strings,
    so we defend against malformed filters / injection regardless.
    """
    return value.replace("'", "''")


def _build_filter(
    monitoring_location_id: str | list[str] | None,
    file_type: str | None,
) -> str | None:
    """Compose the CQL filter sent to STAC ``/search``.

    Mirrors R's logic: only pin ``file_type`` when a single value was given,
    so a multi-type request returns every matching site and the file-type
    filtering happens client-side from the per-feature properties.
    """
    parts: list[str] = []
    if monitoring_location_id is not None:
        ids = (
            [monitoring_location_id]
            if isinstance(monitoring_location_id, str)
            else list(monitoring_location_id)
        )
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
        params["bbox"] = ",".join(str(b) for b in bbox)

    response = requests.get(
        f"{STAC_URL}/search",
        params=params,
        headers=_default_headers(),
        verify=ssl_check,
    )
    response.raise_for_status()
    return response.json().get("features", [])


def _extract_rdb_comment(rdb: str) -> list[str]:
    """Return the RDB ``#``-prefixed comment block as a list of header lines.

    The comment block carries useful per-rating metadata — rating id,
    parameter description, expansion type, last-shifted timestamp, etc.
    R's ``read_waterdata_ratings`` exposes this via ``comment(df)``; we
    attach it to ``df.attrs["comment"]`` so callers can inspect or log
    provenance without re-reading the on-disk RDB.
    """
    return [line for line in rdb.splitlines() if line.startswith("#")]


def _download_and_parse(
    feature: dict[str, Any],
    file_path: str | None,
    ssl_check: bool,
) -> pd.DataFrame:
    """Fetch the feature's data asset, parse RDB, optionally persist to disk."""
    url = feature["assets"]["data"]["href"]
    fid = feature["id"]

    response = requests.get(url, headers=_default_headers(), verify=ssl_check)
    response.raise_for_status()

    if file_path is not None:
        with open(os.path.join(file_path, fid), "w") as f:
            f.write(response.text)

    df = _read_rdb(response.text)
    df.attrs["comment"] = _extract_rdb_comment(response.text)
    df.attrs["url"] = url
    return df


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
    file_types = [file_type] if isinstance(file_type, str) else list(file_type)
    invalid = [ft for ft in file_types if ft not in _VALID_FILE_TYPES]
    if invalid:
        raise ValueError(
            f"Invalid file_type {invalid!r}. Valid options: {list(_VALID_FILE_TYPES)}."
        )

    if time is not None:
        # The rating-curve STAC service rejects ISO 8601 durations; surface a
        # clear error rather than letting the server return a confusing 4xx.
        time_values = time if isinstance(time, list) else [time]
        if any(v is not None and "P" in str(v).upper() for v in time_values):
            raise ValueError(
                "ISO 8601 durations (e.g. 'P7D') are not supported in "
                "`time` for the rating-curve service. Provide a date or "
                "interval instead."
            )
        time_str = _format_api_dates(time, date=False)
    else:
        time_str = None

    # Mirror R: only pin file_type in the server-side filter when one type
    # is requested. With multiple types, fetch all and filter locally.
    server_file_type = file_types[0] if len(file_types) == 1 else None
    filter_str = _build_filter(monitoring_location_id, server_file_type)

    features = _search(filter_str, time_str, bbox, limit, ssl_check)

    if not download_and_parse:
        return features

    if file_path is not None:
        os.makedirs(file_path, exist_ok=True)

    out: dict[str, pd.DataFrame] = {}
    requested = set(file_types)
    for feature in features:
        # Multi-type requests skip the server-side file_type filter, so
        # filter here on the per-feature property (more reliable than
        # substring-matching the URL).
        feat_type = feature.get("properties", {}).get("file_type")
        if feat_type not in requested:
            continue
        fid = feature["id"]
        try:
            out[fid] = _download_and_parse(feature, file_path, ssl_check)
        except (requests.RequestException, ValueError, OSError) as e:
            logger.warning("Failed to download / parse %s: %s", fid, e)

    return out
