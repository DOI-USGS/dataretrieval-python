"""Response shaping for the OGC API engine: wire response → DataFrame.

This module owns the whole "wire response → DataFrame" concern. It holds the
geopandas-free result-shaping helpers (empty-frame handling, column
arrangement/renaming, type coercion, row sorting, snake_case conversion, and
the combined ``_finalize_ogc`` shaping hook) AND the geopandas-touching feature
extraction chain (``_empty_feature_frame``, ``_attach_coordinates``,
``_get_resp_data``, ``_ogc_parse_response``) plus the paginated-response
aggregator (``_aggregate_paginated_response``).

The geopandas seam lives here: ``_empty_feature_frame`` / ``_get_resp_data``
look up the bare module global ``gpd``, so tests patch it on this module
(``mock.patch.object(_responses_module, "gpd", ...)``).
"""

from __future__ import annotations

import copy
from datetime import timedelta
from typing import Any

import httpx
import pandas as pd

from dataretrieval.ogc._constants import (
    _CAMEL_BOUNDARY_RE,
    _DEFAULT_DIALECT,
    OgcDialect,
    gpd,
)
from dataretrieval.ogc._requests import _check_ogc_requests, _next_req_url
from dataretrieval.utils import BaseMetadata


def _deal_with_empty(
    return_list: pd.DataFrame, properties: list[str] | None, service: str
) -> pd.DataFrame:
    """
    Handles empty DataFrame results by returning a DataFrame with appropriate columns.

    If `return_list` is empty, determines the column names to use:
        - If `properties` is not provided or contains only NaN values,
          retrieves schema properties from the specified service.
        - Otherwise, uses the provided `properties` list as column names.

    Parameters
    ----------
    return_list : pd.DataFrame
        The DataFrame to check for emptiness.
    properties : Optional[List[str]]
        List of property names to use as columns, or None.
    service : str
        The service endpoint to query for schema properties if needed.

    Returns
    -------
    pd.DataFrame
        The original DataFrame if not empty, otherwise an empty
        DataFrame with the appropriate columns.
    """
    if return_list.empty:
        if not properties or all(pd.isna(properties)):
            schema = _check_ogc_requests(endpoint=service, req_type="schema")
            properties = list(schema.get("properties", {}).keys())
        return pd.DataFrame(columns=properties)
    return return_list


def _arrange_cols(
    df: pd.DataFrame,
    properties: list[str] | None,
    output_id: str,
    extra_id_cols: frozenset[str] | set[str] = frozenset(),
) -> pd.DataFrame:
    """
    Rearranges and renames columns in a DataFrame based on provided
    properties and the service output id.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame whose columns are to be rearranged or renamed.
    properties : Optional[List[str]]
        A list of column names to possibly rename. If None or contains
        only NaN, the function renames 'id' to output_id.
    output_id : str
        The name to which the 'id' column should be renamed if applicable.
    extra_id_cols : set or frozenset, optional
        Synthetic, meaningless-to-user id columns to move to the end of the
        result frame when the wire ``id`` is returned (i.e. ``properties`` was
        not specified). Defaults to an empty set (no reordering).

    Returns
    -------
    pd.DataFrame or gpd.GeoDataFrame
        The DataFrame with columns rearranged and/or renamed according
        to the specified properties and output_id.
    """

    # Rename id column to output_id
    df = df.rename(columns={"id": output_id})

    if properties and not all(pd.isna(properties)):
        # Don't alias the caller's list — we mutate below.
        local_properties = list(properties)
        if "geometry" in df.columns and "geometry" not in local_properties:
            local_properties.append("geometry")
        # 'id' is a valid service column, but expose it under the
        # service-specific output_id name instead.
        if "id" in local_properties:
            local_properties[local_properties.index("id")] = output_id
        df = df.loc[:, [col for col in local_properties if col in df.columns]]

    # Move meaningless-to-user, extra id columns to the end
    # of the dataframe, if they exist
    extra_id_col = set(df.columns).intersection(extra_id_cols)

    # If the arbitrary id column is returned (either due to properties
    # being none or NaN), then move it to the end of the dataframe, but
    # if part of properties, keep in requested order
    if extra_id_col and (properties is None or all(pd.isna(properties))):
        id_col_order = [col for col in df.columns if col not in extra_id_col] + list(
            extra_id_col
        )
        df = df.loc[:, id_col_order]

    return df


def _type_cols(df: pd.DataFrame, dialect: OgcDialect) -> pd.DataFrame:
    """
    Casts columns into appropriate types per the API ``dialect``.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame.
    dialect : OgcDialect
        Supplies ``time_cols`` / ``numerical_cols`` — which columns to
        coerce to datetime/numeric. The engine itself holds no
        API-specific column knowledge.

    Returns
    -------
    pd.DataFrame
        The DataFrame with columns cast to appropriate types.

    """
    cols = set(df.columns)
    for col in cols.intersection(dialect.time_cols):
        df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in cols.intersection(dialect.numerical_cols):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _sort_rows(df: pd.DataFrame, dialect: OgcDialect) -> pd.DataFrame:
    """
    Sorts rows by the API ``dialect``'s ``sort_cols`` (in priority order).

    Sorting is applied only when the primary (first) sort column is
    present; any later sort columns also present become secondary keys.
    This mirrors the historical Water Data behavior (sort by ``time``,
    then ``monitoring_location_id``) while letting other APIs key off
    their own columns (e.g. NGWMN's ``sample_time``).

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame.
    dialect : OgcDialect
        Supplies ``sort_cols``.

    Returns
    -------
    pd.DataFrame
        The DataFrame with rows ordered per the dialect.

    """
    if not dialect.sort_cols or dialect.sort_cols[0] not in df.columns:
        return df
    present = [c for c in dialect.sort_cols if c in df.columns]
    return df.sort_values(by=present, ignore_index=True)


def _to_snake_case(name: str) -> str:
    """Convert a camelCase/PascalCase column name to snake_case.

    Inserts an underscore only at a lowercase-or-digit followed by an
    uppercase boundary, then lowercases the whole string. Names that are
    already snake_case or all-lowercase are returned unchanged; runs of
    capitals (e.g. ``someXMLField``) are handled best-effort.

    Examples
    --------
    >>> _to_snake_case("waterLevelObs")
    'water_level_obs'
    >>> _to_snake_case("monitoring_location_id")
    'monitoring_location_id'
    >>> _to_snake_case("navd88")
    'navd88'
    """
    return _CAMEL_BOUNDARY_RE.sub(r"\1_\2", name).lower()


def _finalize_ogc(
    frame: pd.DataFrame,
    response: httpx.Response,
    *,
    properties: list[str] | None,
    output_id: str,
    convert_type: bool,
    service: str,
    max_rows: int | None = None,
    extra_id_cols: frozenset[str] | set[str] = frozenset(),
    dialect: OgcDialect = _DEFAULT_DIALECT,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Shape a combined OGC result into the user-facing ``(df, md)``.

    The single home for the OGC getters' result shaping: empties
    normalized, column names normalized to snake_case, types coerced (when
    ``convert_type``), the wire ``id`` renamed and columns ordered, rows
    sorted, optionally truncated to ``max_rows``, and the response wrapped
    as :class:`~dataretrieval.utils.BaseMetadata`.

    Injected into the chunker as its ``finalize`` hook (see
    :data:`~dataretrieval.ogc.chunking._Finalize`) so the
    un-interrupted return *and* a resumed ``ChunkInterrupted.call.resume()``
    produce the same post-processed ``(DataFrame, BaseMetadata)`` shape, not
    the chunker's raw frame and bare ``httpx.Response``.

    ``max_rows`` is applied here (after dedup/sort, on the *combined* frame)
    rather than only per-sub-request, so a chunked call's total is bounded
    to exactly ``max_rows`` and a resumed call honors the cap too — the
    per-``_paginate`` ``_row_cap`` is only an early-stop download bound.
    """
    frame = _deal_with_empty(frame, properties, service)
    # Normalize to PEP-8 snake_case column names *first*, so the dialect's
    # ``time_cols``/``numerical_cols``/``sort_cols`` (all snake_case) match
    # regardless of whether the API returns snake_case (Water Data, where
    # this is a no-op) or camelCase (a sibling OGC API). Doing it before
    # type coercion is what makes ``convert_type`` reach a camelCase field.
    renames = {
        col: snake
        for col in frame.columns
        if isinstance(col, str) and (snake := _to_snake_case(col)) != col
    }
    if renames:
        frame = frame.rename(columns=renames)
    if convert_type:
        frame = _type_cols(frame, dialect)
    frame = _arrange_cols(frame, properties, output_id, extra_id_cols)
    frame = _sort_rows(frame, dialect)
    if max_rows is not None:
        frame = frame.head(max_rows)
    return frame, BaseMetadata(response)


def _empty_feature_frame(geopd: bool) -> pd.DataFrame:
    """Empty result frame for a page that carries no features.

    Returns a ``GeoDataFrame`` when geopandas is available so a downstream
    ``pd.concat([empty_page, geo_page])`` doesn't downgrade a geopandas
    user's result to a plain ``DataFrame`` (stripping geometry/CRS). The
    single home for this empty-page contract, shared by the feature-frame
    builders that flatten GeoJSON pages.
    """
    return gpd.GeoDataFrame() if geopd else pd.DataFrame()


def _attach_coordinates(df: pd.DataFrame, features: list[dict[str, Any]]) -> None:
    """Attach a ``geometry`` column of raw coordinate lists (in place) when
    any feature carries geometry. Shared by the non-geopandas GeoJSON
    feature-frame builders.
    """
    geoms = [(f.get("geometry") or {}).get("coordinates") for f in features]
    if any(g is not None for g in geoms):
        df["geometry"] = geoms


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
        column (always present, possibly all-None) and a ``geometry``
        column (coordinates list) when at least one feature includes
        geometry. Returns an empty ``DataFrame`` when no features are
        returned.

    Notes
    -----
    The non-geopandas branch builds the frame directly from each
    feature's ``properties`` dict, plus the top-level ``id`` and
    ``geometry.coordinates`` columns — the ``id`` column is always
    added (so the downstream rename to the service-specific output id
    works even on an all-None id), while the ``geometry`` column is
    added only when at least one feature carries geometry. This skips
    the GeoJSON envelope entirely, so
    newly-added Feature-level fields (e.g. ``geometry.type`` after
    USGS migrated to full GeoJSON geometry objects) can't leak into
    the result frame; no reactive drop-list needs maintenance every
    time the upstream schema grows.
    """
    if body is None:
        body = resp.json()
    # Key the empty-result short-circuit off ``features`` rather than
    # ``numberReturned``: the main Water Data API reports ``numberReturned``,
    # but the NGWMN OGC API omits it, so trusting it would discard pages that
    # actually carry features. An absent/empty ``features`` is also the real
    # schema-drift shape (a 200 with no features) — treat it as empty rather
    # than crash with a ``KeyError`` downstream, which ``_paginate`` would
    # mistake for a transient transport error. ``_empty_feature_frame``
    # preserves the GeoDataFrame type on the short-circuit (see its docstring).
    features = body.get("features") or []
    if not features:
        return _empty_feature_frame(geopd)

    if not geopd:
        df = pd.json_normalize([f.get("properties") or {} for f in features], sep="_")
        # Always materialize the ``id`` column (may be all-None) so
        # ``_arrange_cols``'s ``df.rename(columns={"id": output_id})``
        # produces the documented service-specific output_id column
        # (daily_id, channel_measurements_id, …) even if the upstream
        # response carried no feature-level id.
        df["id"] = [f.get("id") for f in features]
        _attach_coordinates(df, features)
        return df

    # Organize json into geodataframe and make sure id column comes along.
    # NGWMN observation collections (water levels, lithology, …) return
    # features with no ``geometry`` key at all, which
    # ``GeoDataFrame.from_features`` can't handle (it indexes
    # ``feature["geometry"]`` directly). Default the key to ``None`` for only
    # those features so the call is safe; the all-null check below then yields
    # a plain DataFrame. Features that already carry geometry (the common
    # sites case) are passed through without a per-feature dict copy.
    df = gpd.GeoDataFrame.from_features(
        [f if "geometry" in f else {**f, "geometry": None} for f in features]
    )
    # Mirror the non-geopandas branch's defensive ``f.get("id")`` so a feature
    # missing a top-level ``id`` yields None rather than a KeyError.
    df["id"] = [f.get("id") for f in features]
    df = df[["id"] + [col for col in df.columns if col != "id"]]

    # If no geometry present, then return pandas dataframe. A geodataframe
    # is not needed.
    if df["geometry"].isnull().all():
        df = pd.DataFrame(df.drop(columns="geometry"))

    return df


def _aggregate_paginated_response(
    initial: httpx.Response,
    last: httpx.Response,
    total_elapsed: timedelta,
) -> httpx.Response:
    """
    Build a single response covering a paginated call.

    Returns a shallow copy of ``initial`` with ``.headers`` set to the
    LAST page's (so downstream sees current ``x-ratelimit-remaining``)
    and ``.elapsed`` set to total wall-clock. The canonical
    ``initial.url`` is preserved (it's the user's original query).
    Both ``initial`` and ``last`` are left unmutated, mirroring the
    convention of
    :func:`dataretrieval.ogc.chunking._combine_chunk_responses`.

    Parameters
    ----------
    initial : httpx.Response
        First-page response (the canonical one for ``md.url``).
    last : httpx.Response
        Last-page response — supplies the headers to copy over.
    total_elapsed : datetime.timedelta
        Cumulative wall-clock across every page, including ``initial``.

    Returns
    -------
    httpx.Response
        A shallow copy of ``initial`` with ``.headers`` set to a fresh
        ``httpx.Headers`` and ``.elapsed`` set to the cumulative
        wall-clock. ``initial.headers`` / ``initial.elapsed`` are
        never mutated, so callers holding a pre-pagination reference
        still see the original first-page values.
    """
    final = copy.copy(initial)
    final.headers = httpx.Headers(last.headers)
    final.elapsed = total_elapsed
    return final


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
