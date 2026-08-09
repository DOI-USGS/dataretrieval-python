"""One getter for queries the typed getters cannot express.

The other families expose a fixed argument per filter, which covers the common
cases and keeps them discoverable. This is the escape hatch: an arbitrary CQL2
filter against any collection, for the query nobody anticipated. Prefer a typed
getter when one fits -- it validates more and reads better.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

import pandas as pd

from dataretrieval.ogc import fetch_ogc_request
from dataretrieval.ogc.requests import (
    _as_str_list,
    _construct_cql_request,
    _switch_properties_id,
)
from dataretrieval.response_metadata import BaseMetadata
from dataretrieval.waterdata.types import (
    WATERDATA_SERVICES,
)
from dataretrieval.waterdata.utils import (
    _OUTPUT_ID_BY_SERVICE,
    _finalize_ogc,
)


def get_cql(
    service: WATERDATA_SERVICES,
    cql: str | dict[str, Any],
    *,
    properties: str | Iterable[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    skip_geometry: bool | None = None,
    convert_type: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Query a Water Data OGC API collection with an arbitrary CQL2 filter.

    Sends ``cql`` as a CQL2 filter against ``service`` and returns the matching
    features, shaped like the typed getters (``get_daily``, ``get_continuous``,
    …): the wire ``id`` renamed to the service's id column, columns ordered and
    sorted, and dtypes coerced. Use it when you need a predicate the typed
    getters can't express — a top-level ``or``, ``like`` with ``%`` wildcards,
    comparison operators, nested boolean trees, or a geometry predicate beyond a
    bounding box; prefer a typed getter when one covers the query.

    The request is a single POST with the ``cql`` body sent verbatim, so there
    are no multi-value arguments to chunk: narrow a query whose URL or body
    would exceed the server's size cap rather than relying on automatic
    chunking.

    The CQL2 grammar is documented at
    https://api.waterdata.usgs.gov/docs/ogcapi/complex-queries/.

    Parameters
    ----------
    service : str
        OGC collection name. Must be one of
        :data:`dataretrieval.waterdata.types.WATERDATA_SERVICES`
        (e.g. ``"daily"``, ``"monitoring-locations"``).
    cql : str or dict
        CQL2 query. A ``dict`` is JSON-serialized for transport; a ``str`` is
        sent through unchanged. The query goes into the HTTP POST body with
        ``Content-Type: application/query-cql-json``.
    properties : str or iterable of str, optional
        Server-side property whitelist (passed as ``properties=`` on the URL).
        Reduces payload size. ``"id"`` resolves to the service's ``output_id``
        (e.g. ``daily_id``) the same way it does in the typed wrappers.
    bbox : list of float, optional
        Bounding box ``[xmin, ymin, xmax, ymax]`` in CRS 4326. Combines with the
        CQL filter as an additional spatial predicate.
    limit : int, optional
        Page size, clamped server-side to 50,000.
    skip_geometry : bool, optional
        If True, the server omits geometry from each feature
        (``skipGeometry=true``).
    convert_type : bool, default True
        Coerce date/datetime/numeric columns to typed dtypes after the
        DataFrame is built.

    Returns
    -------
    df : pandas.DataFrame or geopandas.GeoDataFrame
        Result of the query. GeoDataFrame when ``geopandas`` is installed and
        geometry is present.
    md : :class:`dataretrieval.utils.BaseMetadata`
        Request metadata (URL, query time, response headers).

    Examples
    --------
    .. code::

        >>> # Daily values for two parameter codes at two sites
        >>> # (compound AND-of-INs).
        >>> from dataretrieval import waterdata
        >>> cql = {
        ...     "op": "and",
        ...     "args": [
        ...         {
        ...             "op": "in",
        ...             "args": [
        ...                 {"property": "parameter_code"},
        ...                 ["00060", "00065"],
        ...             ],
        ...         },
        ...         {
        ...             "op": "in",
        ...             "args": [
        ...                 {"property": "monitoring_location_id"},
        ...                 ["USGS-07367300", "USGS-03277200"],
        ...             ],
        ...         },
        ...     ],
        ... }
        >>> df, md = waterdata.get_cql(service="daily", cql=cql)

        >>> # Monitoring locations whose HUC starts with "02070010"
        >>> # (LIKE with the CQL2 ``%`` wildcard).
        >>> df, md = waterdata.get_cql(
        ...     service="monitoring-locations",
        ...     cql='{"op": "like", "args": ['
        ...     '{"property": "hydrologic_unit_code"},'
        ...     ' "02070010%"]}',
        ... )
    """
    if service not in _OUTPUT_ID_BY_SERVICE:
        raise ValueError(
            f"Unknown service {service!r}. Valid services: "
            f"{sorted(_OUTPUT_ID_BY_SERVICE)}."
        )
    output_id = _OUTPUT_ID_BY_SERVICE[service]

    # ``dict`` is the pythonic input — serialize on the way out. ``str`` is sent
    # verbatim so callers who already have a CQL2 doc (e.g. imported from a
    # config file) don't need to re-parse it.
    body = json.dumps(cql, separators=(",", ":")) if isinstance(cql, dict) else cql

    properties_list = _as_str_list(properties, "properties")

    # Drop id aliases (``daily_id``/``id``) and ``geometry`` from the wire
    # ``properties`` (the feature ``id`` is always returned and renamed
    # downstream), matching the typed getters.
    wire_properties = _switch_properties_id(properties_list, output_id, service)

    req = _construct_cql_request(
        service,
        body,
        properties=wire_properties,
        bbox=bbox,
        limit=limit,
        skip_geometry=skip_geometry,
    )

    df, response = fetch_ogc_request(req, service=service)

    return _finalize_ogc(
        df,
        response,
        properties=properties_list,
        output_id=output_id,
        convert_type=convert_type,
        service=service,
    )


__all__ = ["get_cql"]
