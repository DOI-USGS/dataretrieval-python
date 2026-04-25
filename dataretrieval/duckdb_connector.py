"""Backwards-compatible alias for :mod:`dataretrieval.duckdb_connectors.waterdata`.

The single-source connector originally lived here. It has since been
generalised into a per-source package so additional sources (WQP,
NGWMN, …) can be added without bloating one module. New code should
import directly from :mod:`dataretrieval.duckdb_connectors`::

    from dataretrieval.duckdb_connectors import waterdata

    con = waterdata.connect()

This module preserves the older entry point::

    from dataretrieval import duckdb_connector

    con = duckdb_connector.connect()

which is equivalent to the waterdata connector.
"""

from __future__ import annotations

from .duckdb_connectors._base import DUCKDB
from .duckdb_connectors.waterdata import WaterdataConnection, connect

__all__ = [
    "DUCKDB",
    "WaterdataConnection",
    "connect",
]
