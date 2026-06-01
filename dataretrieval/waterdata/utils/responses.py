"""Response parsing, result finalization, and stats shaping.

The geometry-agnostic half of the response pipeline: extracting the next-page
URL, backfilling empty results with schema columns, reordering/renaming
columns, coercing dtypes, sorting rows, the combined ``_finalize_ogc`` shaper,
and the stats ``_expand_percentiles`` reshape. Depends on
:mod:`dataretrieval.waterdata.utils.constants`,
:mod:`dataretrieval.waterdata.utils.http`, and
:class:`dataretrieval.utils.BaseMetadata`.

The two functions that read ``geopandas`` directly (``_get_resp_data`` and
``_handle_stats_nesting``) live in the :mod:`dataretrieval.waterdata.utils`
façade instead, because the test suite monkeypatches ``gpd`` at
``dataretrieval.waterdata.utils`` and the patched name must resolve in the
defining module's globals.
"""

from __future__ import annotations

from typing import Any

import httpx
import pandas as pd

from dataretrieval.utils import BaseMetadata
from dataretrieval.waterdata.utils.constants import _EXTRA_ID_COLS
from dataretrieval.waterdata.utils.http import _check_ogc_requests


def _next_req_url(
    resp: httpx.Response, *, body: dict[str, Any] | None = None
) -> str | None:
    """
    Extracts the URL for the next page of results from an HTTP response from a
    water data endpoint.

    Parameters
    ----------
    resp : httpx.Response
        The HTTP response object containing JSON data and headers.
    body : dict, optional
        Pre-parsed JSON body for ``resp``. When provided, skips the
        ``resp.json()`` call — useful when the caller has already
        decoded the body for its own use (avoids a second parse pass).

    Returns
    -------
    Optional[str]
        The URL for the next page of results if available, otherwise None.

    Notes
    -----
    - Returns None when the response carries no features.
    - Expects the response JSON to contain a "links" list with objects having
    "rel" and "href" keys.
    - Checks for the "next" relation in the "links" to determine the next URL.
    """
    if body is None:
        body = resp.json()
    if not body.get("numberReturned"):
        return None
    for link in body.get("links", []):
        if link.get("rel") != "next":
            continue
        href = link.get("href")
        if not href:
            return None
        # Refuse to follow a next-page link to a different host —
        # the request's headers/auth were minted for the original
        # host and shouldn't leak to whatever a poisoned response
        # body might supply. Guarded against mock-shaped ``resp.url``
        # attributes (tests sometimes set strings or ``MagicMock``)
        # by falling open when host extraction isn't reliable.
        try:
            next_host = httpx.URL(href).host
            resp_url = (
                resp.url
                if isinstance(resp.url, httpx.URL)
                else httpx.URL(str(resp.url))
            )
            cur_host = resp_url.host
        except (httpx.InvalidURL, TypeError):
            next_host = cur_host = None
        if next_host and cur_host and next_host != cur_host:
            raise RuntimeError(
                f"Refusing to follow cross-host next-page URL: "
                f"{next_host} != {cur_host}"
            )
        return href
    return None


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
    df: pd.DataFrame, properties: list[str] | None, output_id: str
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
    extra_id_col = set(df.columns).intersection(_EXTRA_ID_COLS)

    # If the arbitrary id column is returned (either due to properties
    # being none or NaN), then move it to the end of the dataframe, but
    # if part of properties, keep in requested order
    if extra_id_col and (properties is None or all(pd.isna(properties))):
        id_col_order = [col for col in df.columns if col not in extra_id_col] + list(
            extra_id_col
        )
        df = df.loc[:, id_col_order]

    return df


def _type_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Casts columns into appropriate types.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing water data.

    Returns
    -------
    pd.DataFrame
        The DataFrame with columns cast to appropriate types.

    """
    cols = set(df.columns)
    numerical_cols = [
        "altitude",
        "altitude_accuracy",
        "contributing_drainage_area",
        "drainage_area",
        "hole_constructed_depth",
        "value",
        "well_constructed_depth",
    ]
    time_cols = [
        "begin",
        "begin_utc",
        "construction_date",
        "end",
        "end_utc",
        "last_modified",
        "time",
    ]

    for col in cols.intersection(time_cols):
        df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in cols.intersection(numerical_cols):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _sort_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts rows by 'time' and 'monitoring_location_id' columns if they
    exist.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing water data.

    Returns
    -------
    pd.DataFrame
        The DataFrame with rows ordered by time and site.

    """
    if "time" in df.columns and "monitoring_location_id" in df.columns:
        df = df.sort_values(by=["time", "monitoring_location_id"], ignore_index=True)
    elif "time" in df.columns:
        df = df.sort_values(by="time", ignore_index=True)

    return df


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
    """Shape a combined OGC result into the user-facing ``(df, md)``.

    The single home for the OGC getters' result shaping: empties
    normalized, types coerced (when ``convert_type``), the wire ``id``
    renamed and columns ordered, rows sorted, optionally truncated to
    ``max_rows``, and the response wrapped as
    :class:`~dataretrieval.utils.BaseMetadata`.

    Injected into the chunker as its ``finalize`` hook (see
    :data:`~dataretrieval.waterdata.chunking._Finalize`) so the
    un-interrupted return *and* a resumed ``ChunkInterrupted.call.resume()``
    produce the same shape — closing the gap where resume used to hand back
    the chunker's raw frame and bare ``httpx.Response``.

    ``max_rows`` is applied here (after dedup/sort, on the *combined* frame)
    rather than only per-sub-request, so a chunked call's total is bounded
    to exactly ``max_rows`` and a resumed call honors the cap too — the
    per-``_paginate`` ``_row_cap`` is only an early-stop download bound.
    """
    frame = _deal_with_empty(frame, properties, service)
    if convert_type:
        frame = _type_cols(frame)
    frame = _arrange_cols(frame, properties, output_id)
    frame = _sort_rows(frame)
    if max_rows is not None:
        frame = frame.head(max_rows)
    return frame, BaseMetadata(response)


def _expand_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes percentile value and thresholds columns containing lists
    of values and turns each list element into its own row in the
    original dataframe. 'nan's are removed from the dataframe. If
    no percentile data exist, it adds a percentile column and
    populates column with percentile assigned to min, max, and
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
