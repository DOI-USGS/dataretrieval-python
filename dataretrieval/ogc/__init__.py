"""Generic OGC API engine shared by the Water Data and NGWMN getters.

The public facade exposes only the minimal collection-adapter seam:

- :class:`OgcDialect` — per-API request/response quirks.
- :class:`ShapingOptions` — how a result frame is shaped, carried apart from
  the query.
- :func:`prepare_request_args` — normalize caller kwargs for the engine.
- :func:`get_ogc_data` — full orchestrated OGC fetch (chunking + pagination),
  including verbatim-CQL2 queries via its ``cql_body`` parameter.

Collection adapters (NGWMN, Water Data's generic wrapper) import from this
facade rather than importing engine internals — every name here is usable
through the facade alone. Generic execution policy is in
:mod:`dataretrieval.transport`, which the engine calls directly.
"""

from dataretrieval.ogc.engine import get_ogc_data
from dataretrieval.ogc.policy import DEFAULT_SHAPING, OgcDialect, ShapingOptions
from dataretrieval.ogc.requests import prepare_request_args

__all__ = [
    "DEFAULT_SHAPING",
    "OgcDialect",
    "ShapingOptions",
    "get_ogc_data",
    "prepare_request_args",
]
