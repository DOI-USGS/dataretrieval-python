"""Getters for the API's own vocabularies.

Reference tables and per-collection queryables -- the parameter codes, statistic
codes, and filterable properties the other getters accept. These describe the
service rather than the measurements, so they are the one family whose results are
mostly stable between calls.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, get_args

import pandas as pd

from dataretrieval._validation import require_one_of
from dataretrieval.ogc.schema import queryables_frame
from dataretrieval.waterdata.endpoints import ogc_api_url
from dataretrieval.waterdata.types import (
    METADATA_COLLECTIONS,
)
from dataretrieval.waterdata.utils import get_ogc_data

if TYPE_CHECKING:
    from dataretrieval._response_metadata import BaseMetadata


# The ID column of each reference table. Most are the collection name singularized;
# the method and citation tables name it after the column that refers to it, so
# they join on it.
_OUTPUT_ID_BY_REFERENCE_COLLECTION: dict[str, str] = {
    "agency-codes": "agency_code",
    "altitude-datums": "altitude_datum",
    "aquifer-codes": "aquifer_code",
    "aquifer-types": "aquifer_type",
    "citations": "citation_name",
    "combined-method-citations": "combined_method_citation_id",
    "coordinate-accuracy-codes": "coordinate_accuracy_code",
    "coordinate-datum-codes": "coordinate_datum_code",
    "coordinate-method-codes": "coordinate_method_code",
    "counties": "county",
    "countries": "country",
    "hydrologic-unit-codes": "hydrologic_unit_code",
    "medium-codes": "medium_code",
    "method-categories": "method_category",
    "method-citations": "citation_method_id",
    "methods": "method_id",
    "national-aquifer-codes": "national_aquifer_code",
    "parameter-codes": "parameter_code",
    "reliability-codes": "reliability_code",
    "site-types": "site_type",
    "states": "state",
    "statistic-codes": "statistic_code",
    "topographic-codes": "topographic_code",
    "time-zone-codes": "time_zone_code",
}


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
        "aquifer-codes", "aquifer-types", "citations",
        "combined-method-citations", "coordinate-accuracy-codes",
        "coordinate-datum-codes", "coordinate-method-codes", "counties",
        "countries", "hydrologic-unit-codes", "medium-codes",
        "method-categories", "method-citations", "methods",
        "national-aquifer-codes", "parameter-codes", "reliability-codes",
        "site-types", "states", "statistic-codes", "topographic-codes",
        "time-zone-codes".
    limit : int, optional
        The number of features returned in each page. The maximum allowable
        limit is 50000; the default (None) requests that maximum. Set a lower
        number if your internet connection is unreliable.
    query: dictionary, optional
        A dictionary of extra query parameters to pass to the collection API
        call.
    max_rows : int, optional
        Cap the total number of rows returned, stopping pagination early
        instead of downloading the whole table. Useful for previewing
        large tables in a few requests (e.g. ``hydrologic-unit-codes`` has
        ~125k rows). Unlike ``limit`` (the per-page size), this bounds the total
        result. The default (None) downloads every page.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query. The primary metadata
        of each reference table appears in the first column. Its name is
        the singular form of the collection name, separated by underscores
        (e.g. the "medium-codes" reference table has a column called
        "medium_code", which contains all possible medium code values).
        The method and citation tables name it after the column that
        refers to it elsewhere, so they join on it: "methods" has
        ``method_id``, "method-categories" has ``method_category``,
        "method-citations" has ``citation_method_id``, "citations" has
        ``citation_name``, and "combined-method-citations" has
        ``combined_method_citation_id`` (``<method_id>.<citation_method_id>``).
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
    require_one_of(collection, get_args(METADATA_COLLECTIONS), name="collection")

    query_args = dict(query) if query else {}
    if limit is not None:
        query_args["limit"] = limit
    return get_ogc_data(
        args=query_args,
        output_id=_OUTPUT_ID_BY_REFERENCE_COLLECTION[collection],
        collection=collection,
        max_rows=max_rows,
        spatial=False,
    )


def get_queryables(collection: str) -> tuple[pd.DataFrame, BaseMetadata]:
    """List the queryable properties of a Water Data API collection.

    Every OGC collection (``daily``, ``continuous``, ``monitoring-locations``,
    ...) lists the set of properties that can be filtered on -- exposed as
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
    # Reading the queryables document is OGC protocol work; this getter only
    # names the API to query -- which is the redirected one when a ``configure``
    # block set a base URL, so the queryables describe the API the getters are
    # querying.
    return queryables_frame(collection, base_url=ogc_api_url())


__all__ = ["get_reference_table", "get_queryables"]
