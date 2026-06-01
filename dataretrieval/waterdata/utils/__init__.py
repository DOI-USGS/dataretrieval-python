"""Backward-compatible façade over the Water Data internals.

Historically this module was a ~2k-LOC catch-all spanning request building,
response parsing, result finalization, pagination/async execution, stats
post-processing, validation, and constants. It is now a package: the public
surface lives in this ``__init__`` and the implementation is split across six
cohesive submodules:

- :mod:`dataretrieval.waterdata.utils.constants` — URLs, id-column map, regexes,
  parameter-classification frozensets.
- :mod:`dataretrieval.waterdata.utils.http` — headers, error bodies, the typed
  non-200 raiser, the queryables/schema probe.
- :mod:`dataretrieval.waterdata.utils.validate` — argument normalization and
  service/profile validation.
- :mod:`dataretrieval.waterdata.utils.requests` — request construction and date
  formatting.
- :mod:`dataretrieval.waterdata.utils.responses` — geometry-agnostic response
  parsing, finalization, and stats reshaping.
- :mod:`dataretrieval.waterdata.utils.engine` — the async pagination driver, client
  resolution, the row cap, and the sync-from-async bridge.

This module re-exports every name the rest of the package
(``api.py``/``ratings.py``/``nearest.py``) and the test suite reference, so the
~30 import sites and the ``mock.patch("dataretrieval.waterdata.utils.<name>")``
targets keep working unchanged.

A small set of functions remain *physically defined here* rather than in a
private module, because the test suite resolves their globals at
``dataretrieval.waterdata.utils``:

- ``_fetch_once`` / ``get_ogc_data`` — ``_fetch_once``'s body resolves
  ``_construct_api_requests`` and ``_walk_pages`` as module globals, and the
  filters tests ``mock.patch`` those at ``...utils.*``.
- ``get_stats_data`` — its ``parse_response`` closure resolves
  ``_handle_stats_nesting`` as a module global, which a test monkeypatches at
  ``...utils._handle_stats_nesting``.
- ``_get_resp_data`` / ``_handle_stats_nesting`` / ``_ogc_parse_response`` /
  ``_walk_pages`` — the first two read ``gpd`` as a module global (tests
  monkeypatch ``...utils.gpd``); the latter two form the OGC parse/pagination
  chain that calls them, so they are kept here alongside the functions they
  patch. (These could move to a submodule — the ``engine``/``responses``
  submodules do not import this package, so there is no cycle — but doing so
  would require re-targeting the test patches to the defining submodule; left
  as a follow-up.)

For the same reason, the import-time ``geopandas`` probe (the ``GEOPANDAS``
flag and the one-time warning) lives here, so ``gpd`` is a global of this
module and the ``gpd`` monkeypatch resolves against the functions above.
"""

from __future__ import annotations

import functools
import logging
import numbers
from typing import Any

import httpx
import pandas as pd

from dataretrieval.utils import BaseMetadata
from dataretrieval.waterdata import _progress, chunking
from dataretrieval.waterdata.utils.constants import (
    _CQL2_REQUIRED_SERVICES,
    _DATE_RANGE_PARAMS,
    _DATETIME_FORMATS,
    _DURATION_RE,
    _EXTRA_ID_COLS,
    _MONITORING_LOCATION_ID_RE,
    _NO_NORMALIZE_PARAMS,
    _OUTPUT_ID_BY_SERVICE,
    BASE_URL,
    OGC_API_URL,
    OGC_API_VERSION,
    SAMPLES_URL,
    STATISTICS_API_URL,
    STATISTICS_API_VERSION,
)
from dataretrieval.waterdata.utils.engine import (
    _aggregate_paginated_response,
    _client_for,
    _paginate,
    _row_cap,
    _row_cap_var,
    _run_sync,
)
from dataretrieval.waterdata.utils.http import (
    _check_ogc_requests,
    _default_headers,
    _error_body,
    _paginated_failure_message,
    _parse_retry_after,
    _raise_for_non_200,
)
from dataretrieval.waterdata.utils.requests import (
    _construct_api_requests,
    _construct_cql_request,
    _cql2_param,
    _format_api_dates,
    _format_one,
    _ogc_query_params,
    _parse_datetime,
    _switch_arg_id,
    _switch_properties_id,
)
from dataretrieval.waterdata.utils.responses import (
    _arrange_cols,
    _deal_with_empty,
    _expand_percentiles,
    _finalize_ogc,
    _next_req_url,
    _sort_rows,
    _type_cols,
)
from dataretrieval.waterdata.utils.validate import (
    _as_str_list,
    _check_id_format,
    _check_monitoring_location_id,
    _check_profiles,
    _get_args,
    _normalize_str_iterable,
)

try:
    import geopandas as gpd

    GEOPANDAS = True
except ImportError:
    GEOPANDAS = False

# Set up logger for this module
logger = logging.getLogger(__name__)

# Whether geopandas is present is a static, environment-level fact, so warn once
# here at import time rather than per query/chunk. That avoids the warning
# repeating on every call and avoids it interleaving with the progress line's
# carriage-return rewrites.
if not GEOPANDAS:
    logger.warning(
        "Geopandas not installed. Geometries will be flattened into pandas DataFrames."
    )


def _get_resp_data(
    resp: httpx.Response,
    geopd: bool,
    *,
    body: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Extracts and normalizes data from an HTTP response containing GeoJSON features.

    Parameters
    ----------
    resp : httpx.Response
        The HTTP response object expected to contain a JSON body
        with a "features" key.
    geopd : bool
        Indicates whether geopandas is installed and should be used to
        handle geometries.
    body : dict, optional
        Pre-parsed JSON body for ``resp``. When provided, skips the
        ``resp.json()`` call — useful when the caller has already
        decoded the body for its own use (avoids a second parse pass).

    Returns
    -------
    gpd.GeoDataFrame or pd.DataFrame
        A ``GeoDataFrame`` when ``geopd`` is True; otherwise a plain
        ``DataFrame`` carrying the feature properties plus an ``id``
        column and a ``geometry`` column (coordinates list) where the
        response includes them. Returns an empty ``DataFrame`` when no
        features are returned.

    Notes
    -----
    The non-geopandas branch builds the frame directly from each
    feature's ``properties`` dict, plus the top-level ``id`` and
    ``geometry.coordinates`` columns — but adds the ``id`` and
    ``geometry`` columns only when at least one feature actually
    carries them. This skips the GeoJSON envelope entirely, so
    newly-added Feature-level fields (e.g. ``geometry.type`` after
    USGS migrated to full GeoJSON geometry objects) can't leak into
    the result frame; no reactive drop-list needs maintenance every
    time the upstream schema grows.
    """
    if body is None:
        body = resp.json()
    if not body.get("numberReturned"):
        # Preserve the GeoDataFrame type on empty short-circuit so a
        # downstream ``pd.concat([empty_page, geo_page])`` doesn't
        # downgrade the geopd-installed user's result to a plain
        # DataFrame (stripping geometry/CRS).
        return gpd.GeoDataFrame() if geopd else pd.DataFrame()

    # Defensive: a 200 with ``numberReturned > 0`` but missing
    # ``features`` is a real schema-drift shape (mirrors the guard in
    # ``_handle_stats_nesting``). Treat as empty rather than crash with
    # ``KeyError`` — the wrapped failure would otherwise look like a
    # transient transport error to ``_paginate``'s exception handler.
    features = body.get("features") or []
    if not features:
        return gpd.GeoDataFrame() if geopd else pd.DataFrame()

    if not geopd:
        df = pd.json_normalize([f.get("properties") or {} for f in features], sep="_")
        # Always materialize the ``id`` column (may be all-None) so
        # ``_arrange_cols``'s ``df.rename(columns={"id": output_id})``
        # produces the documented service-specific output_id column
        # (daily_id, channel_measurements_id, …) even if the upstream
        # response carried no feature-level id.
        df["id"] = [f.get("id") for f in features]
        geoms = [(f.get("geometry") or {}).get("coordinates") for f in features]
        if any(g is not None for g in geoms):
            df["geometry"] = geoms
        return df

    # Organize json into geodataframe and make sure id column comes along.
    df = gpd.GeoDataFrame.from_features(features)
    # Mirror the non-geopandas branch's defensive ``f.get("id")`` so a feature
    # missing a top-level ``id`` yields None rather than a KeyError.
    df["id"] = [f.get("id") for f in features]
    df = df[["id"] + [col for col in df.columns if col != "id"]]

    # If no geometry present, then return pandas dataframe. A geodataframe
    # is not needed.
    if df["geometry"].isnull().all():
        df = pd.DataFrame(df.drop(columns="geometry"))

    return df


def _ogc_parse_response(
    resp: httpx.Response, *, geopd: bool
) -> tuple[pd.DataFrame, str | None]:
    """Parse one OGC API page: extract the DataFrame and the next-page URL.

    The parse strategy :func:`_walk_pages` hands to
    :func:`_paginate`. Coerces falsy cursors (empty href, etc.) to
    ``None`` so the paginate loop's ``while cursor is not None``
    terminates instead of spinning on a meaningless value.
    """
    body = resp.json()
    return (
        _get_resp_data(resp, geopd=geopd, body=body),
        _next_req_url(resp, body=body) or None,
    )


async def _walk_pages(
    geopd: bool,
    req: httpx.Request,
    client: httpx.AsyncClient | None = None,
) -> tuple[pd.DataFrame, httpx.Response]:
    """
    Iterate paginated OGC API responses asynchronously and aggregate
    them into one DataFrame.

    Thin wrapper that hands off to :func:`_paginate` with
    OGC-specific strategies: pages are parsed via :func:`_get_resp_data`
    (through :func:`_ogc_parse_response`) and the next-page cursor is the
    URL from the response's ``links`` array (per :func:`_next_req_url`).

    Parameters
    ----------
    geopd : bool
        Whether geopandas is installed (drives geometry handling).
    req : httpx.Request
        The initial HTTP request to send.
    client : httpx.AsyncClient, optional
        Caller-borrowed client; ``None`` defers client management to
        :func:`_paginate`.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the aggregated results from all pages.
    httpx.Response
        Aggregated response — initial-request URL (for query identity),
        final page's headers (so downstream sees current rate-limit
        state), and cumulative ``elapsed`` summed across pages.

    Raises
    ------
    RuntimeError
        See :func:`_paginate`.
    httpx.HTTPError
        See :func:`_paginate`.
    """
    method = req.method  # ``httpx.Request.method`` is already upper-cased.
    headers = req.headers
    content = req.content if method == "POST" else None

    async def follow_up(cursor: str, sess: httpx.AsyncClient) -> httpx.Response:
        return await sess.request(method, cursor, headers=headers, content=content)

    return await _paginate(
        req,
        parse_response=functools.partial(_ogc_parse_response, geopd=geopd),
        follow_up=follow_up,
        client=client,
    )


def get_ogc_data(
    args: dict[str, Any],
    service: str,
    output_id: str | None = None,
    max_rows: int | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Retrieves OGC (Open Geospatial Consortium) data from a specified
    endpoint and returns it as a pandas DataFrame with metadata.

    This function prepares request arguments, constructs API requests,
    handles pagination, processes the results, and formats output
    according to the specified parameters.

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of request arguments for the OGC service.
    service : str
        The OGC API collection name (e.g., ``"daily"``,
        ``"monitoring-locations"``, ``"continuous"``).
    output_id : str, optional
        The user-facing id column the wire ``id`` is renamed to. Defaults
        to ``_OUTPUT_ID_BY_SERVICE[service]``; pass it explicitly only for
        collections outside that map (e.g. reference-table collections).
    max_rows : int, optional
        Stop paginating once this many rows have been collected and
        truncate the result to exactly ``max_rows``. ``None`` (default)
        fetches the full result. Intended for cheap previews of large,
        un-chunked tables (e.g. :func:`get_reference_table`).

    Returns
    -------
    pd.DataFrame or gpd.GeoDataFrame
        A DataFrame containing the retrieved and processed OGC data.
    BaseMetadata
        A metadata object containing request information including URL and query time.

    Notes
    -----
    - The function does not mutate the input `args` dictionary.
    - Handles optional arguments such as `convert_type`.
    - Applies column cleanup and reordering based on service and properties.
    """
    # Enforce a genuine positive integer: a float (even ``10.0``) or ``bool``
    # would pass a bare ``< 1`` check and then crash deep in
    # ``pd.DataFrame.head`` with an opaque ``TypeError`` after HTTP I/O has
    # already fired. ``numbers.Integral`` (not ``int``) so numpy integers —
    # e.g. ``max_rows`` derived from a numpy/pandas computation — are accepted;
    # ``bool`` is an ``Integral`` subtype, so exclude it explicitly.
    if max_rows is not None and (
        not isinstance(max_rows, numbers.Integral)
        or isinstance(max_rows, bool)
        or max_rows < 1
    ):
        raise ValueError(f"max_rows must be a positive integer (got {max_rows!r}).")

    # Each service renames its wire ``id`` to a service-specific column; that
    # name is derived from ``service`` via the canonical map so the getters
    # don't each repeat it. Callers for collections outside the map (e.g.
    # get_reference_table's metadata collections) pass output_id explicitly.
    if output_id is None:
        output_id = _OUTPUT_ID_BY_SERVICE[service]

    args = args.copy()
    args["service"] = service
    args = _switch_arg_id(args, id_name=output_id, service=service)
    # Capture `properties` before the id-switch so post-processing sees
    # the user-facing names, not the wire-format ones.
    properties = args.get("properties")
    args["properties"] = _switch_properties_id(
        properties, id_name=output_id, service=service
    )
    convert_type = args.pop("convert_type", False)
    args = {k: v for k, v in args.items() if v is not None}

    # Post-processing is injected into the chunker rather than applied here,
    # so it runs on *every* exit: the normal return AND a later
    # ``exc.call.resume()`` after a ChunkInterrupted (which never re-enters
    # this function). ``_finalize_ogc`` is the single source of result shape;
    # it also applies ``max_rows`` to the *combined* frame so the cap is the
    # exact total even when the plan chunks or the call is resumed, while
    # ``_row_cap`` below only early-stops each sub-request's pagination.
    finalize = functools.partial(
        _finalize_ogc,
        properties=properties,
        output_id=output_id,
        convert_type=convert_type,
        service=service,
        max_rows=max_rows,
    )
    with _progress.progress_context(service=service), _row_cap(max_rows):
        return _fetch_once(args, finalize=finalize)


@chunking.multi_value_chunked(build_request=_construct_api_requests)
async def _fetch_once(
    args: dict[str, Any],
) -> tuple[pd.DataFrame, httpx.Response]:
    """Send one prepared-args OGC request asynchronously; return the
    frame + response.

    ``@chunking.multi_value_chunked`` models every multi-value list
    parameter and the cql-text filter as a chunkable axis, greedy-halves
    the biggest chunk across all axes until each sub-request URL fits,
    and iterates the cartesian product. With no chunkable inputs the
    decorator passes args through unchanged. The decorator gathers every
    sub-request over one shared :class:`httpx.AsyncClient` (concurrency
    bounded by the connection pool, sized from ``API_USGS_CONCURRENT``)
    and returns a *synchronous* wrapper, so ``get_ogc_data`` keeps calling
    ``_fetch_once(args, finalize=...)`` synchronously. The return shape is
    ``(frame, response)``.
    """
    req = _construct_api_requests(**args)
    return await _walk_pages(geopd=GEOPANDAS, req=req)


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
    :func:`_get_resp_data`: it builds the per-feature outer frame
    directly from each feature's ``properties`` (minus the nested
    ``data`` field, which is unrolled separately below via the
    ``record_path`` json_normalize), then adds ``id`` and ``geometry``
    only when present. Skipping the GeoJSON envelope keeps newly-added
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

    # The geopd-missing warning is emitted once at import (see top of module);
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
    shape has no chunkable list axes), so it drives :func:`_paginate`
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
        True and user requests a computation_type other than
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


__all__ = [
    # constants / module-level values
    "BASE_URL",
    "GEOPANDAS",
    "OGC_API_URL",
    "OGC_API_VERSION",
    "SAMPLES_URL",
    "STATISTICS_API_URL",
    "STATISTICS_API_VERSION",
    "_CQL2_REQUIRED_SERVICES",
    "_DATETIME_FORMATS",
    "_DATE_RANGE_PARAMS",
    "_DURATION_RE",
    "_EXTRA_ID_COLS",
    "_MONITORING_LOCATION_ID_RE",
    "_NO_NORMALIZE_PARAMS",
    "_OUTPUT_ID_BY_SERVICE",
    # http
    "_check_ogc_requests",
    "_default_headers",
    "_error_body",
    "_paginated_failure_message",
    "_parse_retry_after",
    "_raise_for_non_200",
    # validation
    "_as_str_list",
    "_check_id_format",
    "_check_monitoring_location_id",
    "_check_profiles",
    "_get_args",
    "_normalize_str_iterable",
    # request construction
    "_construct_api_requests",
    "_construct_cql_request",
    "_cql2_param",
    "_format_api_dates",
    "_format_one",
    "_ogc_query_params",
    "_parse_datetime",
    "_switch_arg_id",
    "_switch_properties_id",
    # response parsing / finalization / stats shaping
    "_arrange_cols",
    "_deal_with_empty",
    "_expand_percentiles",
    "_finalize_ogc",
    "_get_resp_data",
    "_handle_stats_nesting",
    "_next_req_url",
    "_ogc_parse_response",
    "_sort_rows",
    "_type_cols",
    # pagination / async engine
    "_aggregate_paginated_response",
    "_client_for",
    "_paginate",
    "_row_cap",
    "_row_cap_var",
    "_run_sync",
    "_walk_pages",
    # public engines
    "get_ogc_data",
    "get_stats_data",
    "_fetch_once",
]
