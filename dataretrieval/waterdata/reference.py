"""Getters for the API's own vocabularies.

Reference tables and per-collection queryables -- the parameter codes, statistic
codes, and filterable properties the other getters accept. These describe the
service rather than the water, so they are the one family whose results are
mostly stable between calls.
"""

from __future__ import annotations

from typing import Any, get_args

import pandas as pd

from dataretrieval.ogc.schema import _check_ogc_requests
from dataretrieval.response_metadata import BaseMetadata
from dataretrieval.waterdata.types import (
    METADATA_COLLECTIONS,
)
from dataretrieval.waterdata.utils import (
    get_ogc_data,
)


def get_reference_table(
    collection: str,
    limit: int | None = None,
    query: dict[str, Any] | None = None,
    max_rows: int | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get metadata reference tables for the USGS Water Data API.

    Reference tables provide the range of allowable values for parameter
    arguments in the waterdata module.

    Parameters
    ----------
    collection : string
        One of the following options: "agency-codes", "altitude-datums",
        "aquifer-codes", "aquifer-types", "coordinate-accuracy-codes",
        "coordinate-datum-codes", "coordinate-method-codes", "counties",
        "hydrologic-unit-codes", "medium-codes", "national-aquifer-codes",
        "parameter-codes", "reliability-codes", "site-types", "states",
        "statistic-codes", "topographic-codes", "time-zone-codes"
    limit : int, optional
        The number of features returned in each page. The maximum allowable
        limit is 50000; the default (None) requests that maximum. Set a lower
        number if your internet connection is spotty.
    query: dictionary, optional
        A dictionary of extra query parameters to pass to the collection API
        call.
    max_rows : int, optional
        Cap the total number of rows returned, stopping pagination early
        instead of downloading the whole table. Useful for cheaply
        previewing large tables (e.g. ``hydrologic-unit-codes`` has ~125k
        rows). Unlike ``limit`` (the per-page size), this bounds the total
        result. The default (None) downloads every page.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query. The primary metadata
        of each reference table will show up in the first column, where
        the name of the column is the singular form of the collection name,
        separated by underscores (e.g. the "medium-codes" reference table
        has a column called "medium_code", which contains all possible
        medium code values).
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object including the URL request and query time.

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get table of USGS parameter codes
        >>> ref, md = dataretrieval.waterdata.get_reference_table(
        ...     collection="parameter-codes"
        ... )

        >>> # Get table of selected USGS parameter codes
        >>> ref, md = dataretrieval.waterdata.get_reference_table(
        ...     collection="parameter-codes",
        ...     query={"id": "00001,00002"},
        ... )
    """
    valid_code_services = get_args(METADATA_COLLECTIONS)
    if collection not in valid_code_services:
        raise ValueError(
            f"Invalid code service: '{collection}'. "
            f"Valid options are: {valid_code_services}."
        )

    # Give the ID column the collection name, singularized and underscored.
    if collection == "counties":
        output_id = "county"
    elif collection.endswith("s"):
        output_id = collection[:-1].replace("-", "_")
    else:
        output_id = collection.replace("-", "_")

    query_args = dict(query) if query else {}
    if limit is not None:
        query_args["limit"] = limit
    return get_ogc_data(
        args=query_args, output_id=output_id, service=collection, max_rows=max_rows
    )


def get_queryables(collection: str) -> tuple[pd.DataFrame, BaseMetadata]:
    """List the queryable properties of a Water Data API collection.

    Every OGC collection (``daily``, ``continuous``, ``monitoring-locations``,
    ...) advertises the set of properties that can be filtered on -- exposed as
    the typed keyword arguments of the matching ``get_*`` function, and usable
    directly in a CQL2 ``filter``. This function returns that set, so you can
    discover the available filters programmatically and monitor them for
    upstream additions.

    Parameters
    ----------
    collection : string
        The collection id, e.g. ``"daily"``, ``"continuous"``,
        ``"monitoring-locations"``, or ``"time-series-metadata"``. See
        :data:`dataretrieval.waterdata.types.WATERDATA_SERVICES` for the data
        collections; reference collections (e.g. ``"parameter-codes"``) work
        too.

    Returns
    -------
    df : ``pandas.DataFrame``
        One row per queryable, sorted by name, with columns ``queryable`` (the
        property name), ``type``, ``title``, and ``description``.
    md : :class:`dataretrieval.utils.BaseMetadata`
        Metadata describing the request (URL, query time, response headers).

    Raises
    ------
    DataRetrievalError
        On an HTTP error response (e.g. an unknown ``collection`` yields a 404),
        the typed subclass for the status.

    Examples
    --------
    .. doctest::
        :skipif: True  # network

        >>> from dataretrieval import waterdata
        >>> df, md = waterdata.get_queryables("daily")
        >>> df.set_index("queryable").loc["state_name", "type"]
        'string'
    """
    # The OGC queryables document is a JSON Schema whose ``properties`` map each
    # filterable property name to a ``{title, type, description}`` definition.
    body, response = _check_ogc_requests(endpoint=collection, req_type="queryables")
    properties: dict[str, Any] = body.get("properties", {})
    df = pd.DataFrame(
        [
            {
                "queryable": name,
                "type": prop.get("type"),
                "title": prop.get("title"),
                "description": (prop.get("description") or "").strip(),
            }
            for name, prop in sorted(properties.items())
        ],
        columns=["queryable", "type", "title", "description"],
    )
    return df, BaseMetadata(response)


__all__ = ["get_reference_table", "get_queryables"]
