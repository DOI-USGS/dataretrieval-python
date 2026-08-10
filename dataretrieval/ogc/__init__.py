"""Generic OGC API engine shared by the Water Data and NGWMN getters.

The public facade exposes only the minimal collection-adapter seam:

- :class:`OgcDialect` — per-API request/response quirks.
- :func:`prepare_request_args` — normalize caller kwargs for the engine.
- :func:`get_ogc_data` — full orchestrated OGC fetch (chunking + pagination),
  including verbatim-CQL2 queries via its ``cql_body`` parameter.

Collection adapters (NGWMN, Water Data's generic wrapper) import from this
facade rather than reaching into engine internals — every name here is usable
through the facade alone. Generic execution policy lives in
:mod:`dataretrieval.transport`; the engine retains compatibility wrappers at
previous private paths.
"""

from dataretrieval.ogc.engine import get_ogc_data
from dataretrieval.ogc.policy import OgcDialect
from dataretrieval.ogc.requests import prepare_request_args

__all__ = [
    "OgcDialect",
    "get_ogc_data",
    "prepare_request_args",
]
