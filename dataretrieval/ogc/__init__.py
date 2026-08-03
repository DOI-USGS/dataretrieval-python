"""Generic OGC API engine shared by the Water Data and NGWMN getters.

The public facade exposes only the minimal service-adapter seam:

- :class:`OgcDialect` — per-API request/response quirks.
- :func:`prepare_request_args` — normalize caller kwargs for the engine.
- :func:`get_ogc_data` — full orchestrated OGC fetch (chunking + pagination).
- :func:`fetch_ogc_request` — execute a pre-built request with pagination.

Service adapters (NGWMN, Water Data's generic wrapper) import from this
facade rather than reaching into engine internals. The engine module remains
available for lower-level orchestration needs (e.g. ``_paginate``,
``_run_sync``) that sibling modules like ``wateruse`` use under the accepted
temporary variance.
"""

from dataretrieval.ogc.engine import fetch_ogc_request, get_ogc_data
from dataretrieval.ogc.policy import OgcDialect
from dataretrieval.ogc.requests import prepare_request_args

__all__ = [
    "OgcDialect",
    "fetch_ogc_request",
    "get_ogc_data",
    "prepare_request_args",
]
