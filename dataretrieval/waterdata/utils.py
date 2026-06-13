"""Water Data API layer over the generic OGC engine.

The API-agnostic OGC machinery (request construction, pagination, response
shaping, the chunked ``get_ogc_data`` entry point) lives in
:mod:`dataretrieval.ogc.engine`. This module is the Water-Data-specific layer
on top of it: it supplies the service-to-id map, the CQL2/date-only dialect,
the statistics path, profile validation, and a thin ``get_ogc_data`` wrapper
that injects the Water Data defaults. Every engine symbol the Water Data
getters (``api.py``, ``ratings.py``, ``nearest.py``) and the test suite import
from here is re-exported below, so those call sites are unchanged by the split.
"""

from __future__ import annotations

import logging
from typing import Any, get_args

import httpx
import pandas as pd

from dataretrieval.ogc import engine
from dataretrieval.ogc.engine import (
    _DATE_RANGE_PARAMS,
    _DURATION_RE,
    BASE_URL,
    GEOPANDAS,
    OGC_API_URL,
    OgcDialect,
    _arrange_cols,
    _as_str_list,
    _check_id_format,
    _check_monitoring_location_id,
    _check_ogc_requests,
    _construct_api_requests,
    _construct_cql_request,
    _deal_with_empty,
    _default_headers,
    _error_body,
    _format_api_dates,
    _get_resp_data,
    _next_req_url,
    _normalize_str_iterable,
    _paginate,
    _paginated_failure_message,
    _parse_retry_after,
    _raise_for_non_200,
    _row_cap,
    _run_sync,
    _switch_properties_id,
    _to_snake_case,
    _walk_pages,
)
from dataretrieval.ogc.engine import (
    _get_args as _engine_get_args,
)
from dataretrieval.utils import BaseMetadata
from dataretrieval.waterdata.types import (
    PROFILE_LOOKUP,
    PROFILES,
    SERVICES,
)

# ``_handle_stats_nesting`` (below) builds GeoDataFrames when geopandas is
# present, so this module needs its own bound ``gpd`` name. Import it under the
# same guard the engine uses; when geopandas is absent ``gpd`` is left unbound
# (``GEOPANDAS`` is ``False``, so the stats path never touches it), matching the
# engine's behavior. Tests patch ``utils.gpd`` with ``create=True`` to cover
# both environments.
try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - exercised only without geopandas
    pass

# Set up logger for this module
logger = logging.getLogger(__name__)

SAMPLES_URL = f"{BASE_URL}/samples-data"
STATISTICS_API_VERSION = "v0"
STATISTICS_API_URL = f"{BASE_URL}/statistics/{STATISTICS_API_VERSION}"

# Maps each OGC waterdata service to its user-facing ``id`` column (the name the
# typed getters rename the wire ``id`` to, e.g. ``daily`` -> ``daily_id``).
# ``get_cql`` validates its ``service`` argument against these keys and
# uses the value as the ``output_id`` for result shaping. Keep in sync with the
# ``types.WATERDATA_SERVICES`` Literal (same keys).
_OUTPUT_ID_BY_SERVICE: dict[str, str] = {
    "channel-measurements": "channel_measurements_id",
    "combined-metadata": "combined_meta_id",
    "continuous": "continuous_id",
    "daily": "daily_id",
    "field-measurements": "field_measurement_id",
    "field-measurements-metadata": "field_series_id",
    "latest-continuous": "latest_continuous_id",
    "latest-daily": "latest_daily_id",
    "monitoring-locations": "monitoring_location_id",
    "peaks": "peak_id",
    "time-series-metadata": "time_series_id",
}

# Every service's output id EXCEPT the two that are genuinely user-facing
# (``monitoring_location_id`` and ``time_series_id``). The rest are synthetic
# per-record ids that ``_arrange_cols`` moves to the end of a result frame.
# Derived from ``_OUTPUT_ID_BY_SERVICE`` so adding a service can't silently
# leave a stray id column at the front again.
_EXTRA_ID_COLS = frozenset(
    set(_OUTPUT_ID_BY_SERVICE.values()) - {"monitoring_location_id", "time_series_id"}
)

# The Water Data API dialect: ``monitoring-locations`` doesn't accept
# comma-separated multi-value GET params (so it must POST CQL2 JSON),
# ``daily`` renders its time arguments date-only (``YYYY-MM-DD``), and the
# ``time_cols``/``numerical_cols``/``sort_cols`` are the Water-Data column
# vocabulary the generic engine used to hardcode.
WATERDATA_DIALECT = OgcDialect(
    cql2_services=frozenset({"monitoring-locations"}),
    date_only_services=frozenset({"daily"}),
    time_cols=frozenset(
        {
            "begin",
            "begin_utc",
            "construction_date",
            "end",
            "end_utc",
            "last_modified",
            "time",
        }
    ),
    numerical_cols=frozenset(
        {
            "altitude",
            "altitude_accuracy",
            "contributing_drainage_area",
            "drainage_area",
            "hole_constructed_depth",
            "value",
            "well_constructed_depth",
        }
    ),
    sort_cols=("time", "monitoring_location_id"),
)

# Iterable-shaped params that ``_get_args`` must NOT push through
# ``_normalize_str_iterable`` (scalar non-string knobs are caught by runtime
# type, so only iterables with special handling need to be named here):
#   - date-range params may contain ``pd.NaT``/None or interval strings
#   - ``bbox``/``boundingBox`` are ``list[float]``, sometimes ``numpy.ndarray``
#   - ``get_peaks``'s int-valued filters (``water_year`` etc.) are ``list[int]``
#   - ``get_combined_metadata``'s ``thresholds`` is ``list[float]``
_NO_NORMALIZE_PARAMS = _DATE_RANGE_PARAMS | {
    "bbox",
    "boundingBox",
    "water_year",
    "year",
    "month",
    "day",
    "peak_since",
    "thresholds",
}


def _get_args(
    local_vars: dict[str, Any], exclude: set[str] | None = None
) -> dict[str, Any]:
    """Water-Data wrapper over :func:`engine._get_args`.

    Supplies the Water Data API's extended ``no_normalize`` set (numeric
    params such as ``water_year``, ``thresholds``, ``boundingBox``) so they
    keep their element types. See :func:`engine._get_args` for the full
    normalization contract.
    """
    return _engine_get_args(local_vars, exclude, no_normalize=_NO_NORMALIZE_PARAMS)


def get_ogc_data(
    args: dict[str, Any],
    service: str,
    output_id: str | None = None,
    max_rows: int | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Water-Data wrapper over :func:`engine.get_ogc_data`.

    Defaults ``output_id`` from the Water Data service map when not given,
    and supplies the Water Data extra-id columns and dialect, so the typed
    getters in ``api.py`` call this unchanged. (Sibling OGC APIs such as
    NGWMN call ``engine.get_ogc_data`` directly with their own base URL and
    dialect rather than going through this Water Data wrapper.)

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of request arguments for the OGC service.
    service : str
        The OGC API collection name (e.g., ``"daily"``).
    output_id : str, optional
        The user-facing id column the wire ``id`` is renamed to. Defaults
        to ``_OUTPUT_ID_BY_SERVICE[service]``; pass it explicitly only for
        collections outside that map (e.g. reference-table collections).
    max_rows : int, optional
        Stop paginating once this many rows have been collected and
        truncate the result to exactly ``max_rows``. ``None`` (default)
        fetches the full result.

    Returns
    -------
    pd.DataFrame or gpd.GeoDataFrame
        A DataFrame containing the retrieved and processed OGC data.
    BaseMetadata
        A metadata object containing request information including URL and query time.
    """
    if output_id is None:
        output_id = _OUTPUT_ID_BY_SERVICE[service]
    return engine.get_ogc_data(
        args,
        service,
        output_id,
        max_rows=max_rows,
        base_url=OGC_API_URL,
        extra_id_cols=_EXTRA_ID_COLS,
        dialect=WATERDATA_DIALECT,
    )


def _finalize_ogc(
    frame: pd.DataFrame,
    response: httpx.Response,
    *,
    properties: list[str] | None,
    output_id: str,
    convert_type: bool,
    service: str,
    max_rows: int | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Water-Data wrapper over :func:`engine._finalize_ogc`.

    Injects the Water Data ``extra_id_cols`` and ``dialect`` so a direct
    call (e.g. from ``get_cql``) orders synthetic id columns and coerces/
    sorts result columns identically to the typed getters. See
    :func:`engine._finalize_ogc` for the full result-shaping contract.
    """
    return engine._finalize_ogc(
        frame,
        response,
        properties=properties,
        output_id=output_id,
        convert_type=convert_type,
        service=service,
        max_rows=max_rows,
        extra_id_cols=_EXTRA_ID_COLS,
        dialect=WATERDATA_DIALECT,
    )


def _handle_stats_nesting(
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
        return gpd.GeoDataFrame() if geopd else pd.DataFrame()

    # An empty (or missing) features list — a real mid-pagination
    # shape — would otherwise crash the downstream merge with
    # ``KeyError: 'monitoring_location_id'`` because neither df nor
    # dat would carry the merge key. Bail out with an empty frame —
    # ``GeoDataFrame`` when geopd is available so the eventual
    # ``pd.concat`` with non-empty geo pages doesn't downgrade to a
    # plain DataFrame and strip geometry/CRS.
    features = body.get("features") or []
    if not features:
        return gpd.GeoDataFrame() if geopd else pd.DataFrame()

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
        geoms = [(f.get("geometry") or {}).get("coordinates") for f in features]
        if any(g is not None for g in geoms):
            df["geometry"] = geoms
    else:
        df = gpd.GeoDataFrame.from_features(features).drop(
            columns=["data"], errors="ignore"
        )

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


def get_stats_data(
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
        return _handle_stats_nesting(body, geopd=GEOPANDAS), body.get("next") or None

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


def _check_profiles(
    service: SERVICES,
    profile: PROFILES,
) -> None:
    """Check whether a service profile is valid.

    Parameters
    ----------
    service : string
        One of the service names from the "services" list.
    profile : string
        One of the profile names from "results_profiles",
        "locations_profiles", "activities_profiles",
        "projects_profiles" or "organizations_profiles".
    """
    valid_services = get_args(SERVICES)
    if service not in valid_services:
        raise ValueError(
            f"Invalid service: '{service}'. Valid options are: {valid_services}."
        )

    valid_profiles = PROFILE_LOOKUP[service]
    if profile not in valid_profiles:
        raise ValueError(
            f"Invalid profile: '{profile}' for service '{service}'. "
            f"Valid options are: {valid_profiles}."
        )


__all__ = [
    "BASE_URL",
    "GEOPANDAS",
    "OGC_API_URL",
    "OgcDialect",
    "SAMPLES_URL",
    "STATISTICS_API_URL",
    "STATISTICS_API_VERSION",
    "WATERDATA_DIALECT",
    "_DATE_RANGE_PARAMS",
    "_DURATION_RE",
    "_EXTRA_ID_COLS",
    "_NO_NORMALIZE_PARAMS",
    "_OUTPUT_ID_BY_SERVICE",
    "_arrange_cols",
    "_as_str_list",
    "_check_id_format",
    "_check_monitoring_location_id",
    "_check_ogc_requests",
    "_check_profiles",
    "_construct_api_requests",
    "_construct_cql_request",
    "_deal_with_empty",
    "_default_headers",
    "_error_body",
    "_expand_percentiles",
    "_finalize_ogc",
    "_format_api_dates",
    "_get_args",
    "_get_resp_data",
    "_handle_stats_nesting",
    "_next_req_url",
    "_normalize_str_iterable",
    "_paginate",
    "_paginated_failure_message",
    "_parse_retry_after",
    "_raise_for_non_200",
    "_row_cap",
    "_run_sync",
    "_switch_properties_id",
    "_to_snake_case",
    "_walk_pages",
    "get_ogc_data",
    "get_stats_data",
    "gpd",
]
