"""USGS Water Data Statistics API client.

Wraps ``https://api.waterdata.usgs.gov/statistics/v0`` — the daily-statistics
service (period-of-record and date-range normals/intervals). This is a
*separate*, non-OGC API: it has no chunkable multi-value axes, so it drives
:func:`engine._paginate` directly through a blocking portal rather than going
through ``multi_value_chunked``. The typed getters ``get_stats_por`` and
``get_stats_date_range`` in :mod:`dataretrieval.waterdata.api` call
:func:`get_data` here.
"""

from __future__ import annotations

from typing import Any

import httpx
import pandas as pd

from dataretrieval.ogc.engine import (
    BASE_URL,
    GEOPANDAS,
    _attach_coordinates,
    _default_headers,
    _empty_feature_frame,
    _paginate,
    _run_sync,
)
from dataretrieval.utils import BaseMetadata

# ``_handle_nesting``'s geopandas branch calls ``gpd.GeoDataFrame.from_features``
# directly, so this module needs its own bound ``gpd`` name. Import it under the
# same guard the engine uses; when geopandas is absent ``gpd`` is left unbound
# (``GEOPANDAS`` is ``False``, so the stats path never touches it). The
# empty-page short-circuit instead delegates to
# ``dataretrieval.ogc._responses._empty_feature_frame``, which resolves
# ``_responses.gpd`` — so an empty-page test patches ``_responses.gpd`` while
# the populated geopandas branch uses ``stats.gpd``.
try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - exercised only without geopandas
    pass

STATISTICS_API_VERSION = "v0"
STATISTICS_API_URL = f"{BASE_URL}/statistics/{STATISTICS_API_VERSION}"


def _handle_nesting(
    body: dict[str, Any],
    geopd: bool = False,
) -> pd.DataFrame:
    """
    Takes nested json from stats service and flattens into a dataframe with
    one row per monitoring location, parameter, and statistic.

    Parameters
    ----------
    body : Dict[str, Any]
        The JSON response body from the statistics service containing nested data.
    geopd : bool, optional
        Whether ``geopandas`` is available — when ``True`` the returned
        frame is a ``GeoDataFrame``; when ``False`` (default) a plain
        ``pd.DataFrame`` is returned with geometry flattened.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the flattened statistical data.

    Notes
    -----
    The non-geopandas branch uses the same schema-aware extraction as
    :func:`engine._get_resp_data`: it builds the per-feature outer frame
    directly from each feature's ``properties`` (minus the nested
    ``data`` field, which is unrolled separately below via the
    ``record_path`` json_normalize), then adds ``geometry`` only when
    present. Unlike :func:`engine._get_resp_data`, no top-level ``id``
    column is added — stats features don't carry one, so this matches the
    geopandas branch. Skipping the GeoJSON envelope keeps newly-added
    fields like ``geometry.type`` from leaking into the result.
    """
    if body is None:
        return _empty_feature_frame(geopd)

    # An empty (or missing) features list — a real mid-pagination
    # shape — would otherwise crash the downstream merge with
    # ``KeyError: 'monitoring_location_id'`` because neither df nor
    # dat would carry the merge key. ``_empty_feature_frame`` bails out
    # with a geo-typed empty frame so a later ``pd.concat`` with non-empty
    # geo pages doesn't downgrade to a plain DataFrame and strip geometry/CRS.
    features = body.get("features") or []
    if not features:
        return _empty_feature_frame(geopd)

    # The geopd-missing warning is emitted once at import (see engine module);
    # doing it here would log per page.
    if not geopd:
        outer_props = [
            {k: v for k, v in (f.get("properties") or {}).items() if k != "data"}
            for f in features
        ]
        df = pd.json_normalize(outer_props, sep=".")
        df.columns = df.columns.str.split(".").str[-1]
        # Stats features don't carry a top-level ``id`` field — the
        # geopandas branch (``GeoDataFrame.from_features``) doesn't
        # surface one either, so the non-geopd branch stays
        # consistent by NOT adding an id column.
        _attach_coordinates(df, features)
    else:
        # Default a missing ``geometry`` key to ``None`` per feature so
        # ``from_features`` (which indexes ``feature["geometry"]`` directly)
        # can't ``KeyError`` on a stats feature that omits geometry — mirrors
        # the guard in :func:`engine._get_resp_data`.
        df = gpd.GeoDataFrame.from_features(
            [f if "geometry" in f else {**f, "geometry": None} for f in features]
        ).drop(columns=["data"], errors="ignore")

    # Unnest json features, properties, data, and values while retaining necessary
    # metadata to merge with main dataframe.
    dat = pd.json_normalize(
        body,
        record_path=["features", "properties", "data", "values"],
        meta=[
            ["features", "properties", "monitoring_location_id"],
            ["features", "properties", "data", "parameter_code"],
            ["features", "properties", "data", "unit_of_measure"],
            ["features", "properties", "data", "parent_time_series_id"],
        ],
        meta_prefix="",
        errors="ignore",
    )
    dat.columns = dat.columns.str.split(".").str[-1]

    return df.merge(dat, on="monitoring_location_id", how="left")


def _expand_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes percentile value and thresholds columns containing lists
    of values and turns each list element into its own row in the
    original dataframe. Exploded ``'nan'`` values are dropped. If
    no percentile data exist, it adds a percentile column and
    populates it with the percentile assigned to min, max, and
    median.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe returned from using one of the statistics services.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the flattened percentile data.
    """
    if len(df) > 0:
        if "percentile" in df["computation"].unique():
            # Explode percentile lists into rows called "value" and "percentile"
            percentiles = df.loc[df["computation"] == "percentile"]
            percentiles_explode = percentiles[
                ["computation_id", "values", "percentiles"]
            ].explode(["values", "percentiles"], ignore_index=True)
            percentiles_explode = percentiles_explode.loc[
                percentiles_explode["values"] != "nan"
            ]
            percentiles_explode["value"] = pd.to_numeric(percentiles_explode["values"])
            percentiles_explode["percentile"] = pd.to_numeric(
                percentiles_explode["percentiles"]
            )
            percentiles_explode = percentiles_explode.drop(
                columns=["values", "percentiles"]
            )

            # Merge exploded values back to other metadata/geometry
            percentiles = percentiles.drop(
                columns=["values", "percentiles", "value"], errors="ignore"
            ).merge(percentiles_explode, on="computation_id", how="left")

            # Concatenate back to original
            dfs = pd.concat(
                [df.loc[df["computation"] != "percentile"], percentiles]
            ).drop(columns=["values", "percentiles"])
        else:
            dfs = df
            dfs["percentile"] = pd.NA

        # Give min, max, median a percentile value
        dfs.loc[dfs["computation"] == "maximum", "percentile"] = 100
        dfs.loc[dfs["computation"] == "minimum", "percentile"] = 0
        dfs.loc[dfs["computation"] == "median", "percentile"] = 50

        # Make sure numeric
        dfs["percentile"] = pd.to_numeric(dfs["percentile"])

        # Move percentile column
        cols = dfs.columns.tolist()
        cols.remove("percentile")
        col_index = cols.index("value") + 1
        cols.insert(col_index, "percentile")

        return dfs[cols]

    else:
        return df


def get_data(
    args: dict[str, Any],
    service: str,
    expand_percentiles: bool,
    client: httpx.AsyncClient | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Retrieves statistical data from a specified endpoint and returns it
    as a pandas DataFrame with metadata.

    This function prepares request arguments, constructs API requests,
    handles pagination, processes results, and formats output according
    to the specified parameters.

    The stats path doesn't go through ``multi_value_chunked`` (its query
    shape has no chunkable list axes), so it drives :func:`engine._paginate`
    directly through an ``anyio`` blocking portal. The portal runs the
    pagination loop in a short-lived worker thread, so this works whether
    or not the caller is already inside an event loop.

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of request arguments for the statistics service.
    service : str
        The statistics service type (for example,
        "observationNormals" or "observationIntervals").
    expand_percentiles : bool
        Determines whether the percentiles column is expanded so that
        each percentile gets its own row in the returned dataframe. If
        True and the user requests a computation_type other than
        percentiles, a percentile column is still returned.
    client : httpx.AsyncClient, optional
        Caller-borrowed async client. ``None`` (default) opens a
        temporary one inside the portal. Primarily a test seam.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the retrieved and processed statistical data.
    BaseMetadata
        A metadata object containing request information including URL and query time.

    Raises
    ------
    DataRetrievalError
        The typed subclass for an HTTP error response (see :func:`engine._paginate`);
        or :class:`~dataretrieval.exceptions.NetworkError` if the initial request
        can't reach the service (timeout / DNS), the ``httpx`` exception chained
        on ``__cause__``.
    """

    url = f"{STATISTICS_API_URL}/{service}"
    req = httpx.Request(
        method="GET",
        url=url,
        headers=_default_headers(),
        params=args,
    )
    method = req.method
    headers = req.headers

    def parse_response(resp: httpx.Response) -> tuple[pd.DataFrame, str | None]:
        body = resp.json()
        # Coerce falsy cursors ("", 0) to None so _paginate terminates.
        # USGS uses "next": null at end-of-stream, but defensive coerce
        # protects against any "" sentinel a future schema might use.
        return _handle_nesting(body, geopd=GEOPANDAS), body.get("next") or None

    async def follow_up(cursor: str, sess: httpx.AsyncClient) -> httpx.Response:
        # Build a fresh params dict per page so the caller's ``args``
        # is never mutated.
        return await sess.request(
            method, url=url, params={**args, "next_token": cursor}, headers=headers
        )

    async def _run() -> tuple[pd.DataFrame, httpx.Response]:
        return await _paginate(
            req,
            parse_response=parse_response,
            follow_up=follow_up,
            client=client,
        )

    df, response = _run_sync(_run, service=service)

    if expand_percentiles:
        df = _expand_percentiles(df)
    return df, BaseMetadata(response)
