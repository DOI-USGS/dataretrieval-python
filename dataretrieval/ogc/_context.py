"""Per-call ambient runtime state for the OGC API engine.

The contextvars (and their :func:`contextlib.contextmanager` accessors) that let
the shared request builder / pagination loop read per-call settings — the row
cap, the active OGC base URL, and the active dialect — without threading them
through the generic chunker. A leaf above :mod:`dataretrieval.ogc._constants`
(its only ``ogc`` import) and below every module that reads the active state, so
there are no import cycles.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from dataretrieval.ogc._constants import _DEFAULT_DIALECT, OGC_API_URL, OgcDialect

# Optional cap on the total rows a single paginated call accumulates before it
# stops following ``next`` links. ``None`` (the default the data getters use)
# means "no cap — fetch the whole series". Set via :func:`_row_cap` so the deep
# ``_paginate`` loop can honor it without threading the value through the
# generic chunker; this mirrors the ``_progress`` ambient-reporter pattern.
_row_cap_var: ContextVar[int | None] = ContextVar("ogc_row_cap", default=None)


@contextmanager
def _row_cap(max_rows: int | None) -> Iterator[None]:
    """Cap the rows any :func:`_paginate` under this context will
    accumulate (``None`` = uncapped). Used by :func:`get_reference_table`
    to preview large tables without downloading every page."""
    token = _row_cap_var.set(max_rows)
    try:
        yield
    finally:
        _row_cap_var.reset(token)


# OGC base URL for the active request. ``get_ogc_data`` sets it per call so the
# shared request builder (:func:`_construct_api_requests`) can target either the
# main Water Data API or the NGWMN sub-API without threading the value through
# the generic chunker; this mirrors the ``_row_cap`` ambient pattern. The
# default is the main API, so every existing getter is unaffected.
_ogc_base_url_var: ContextVar[str] = ContextVar("ogc_base_url", default=OGC_API_URL)


@contextmanager
def _ogc_base_url(base_url: str) -> Iterator[None]:
    """Point :func:`_construct_api_requests` (and the chunk planner that calls
    it) at ``base_url`` for the duration of the block. Used by
    :func:`get_ogc_data` to serve NGWMN collections from their own OGC base."""
    token = _ogc_base_url_var.set(base_url)
    try:
        yield
    finally:
        _ogc_base_url_var.reset(token)


# Per-call OGC dialect (which services need POST/CQL2, which use date-only time
# args). ``get_ogc_data`` sets it so the shared request builder
# (:func:`_construct_api_requests`) can adapt to the active API without
# threading the value through the generic chunker; this mirrors the
# ``_ogc_base_url`` ambient pattern. The default is a plain OGC API.
_dialect_var: ContextVar[OgcDialect] = ContextVar(
    "ogc_dialect", default=_DEFAULT_DIALECT
)


@contextmanager
def _dialect(dialect: OgcDialect) -> Iterator[None]:
    """Make ``dialect`` the active :class:`OgcDialect` that
    :func:`_construct_api_requests` reads for CQL2-vs-GET routing and
    date-only formatting, for the duration of the block."""
    token = _dialect_var.set(dialect)
    try:
        yield
    finally:
        _dialect_var.reset(token)
