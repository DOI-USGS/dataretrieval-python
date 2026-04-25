"""DuckDB connectors for ``dataretrieval`` data sources.

Each submodule wraps one ``dataretrieval`` source (waterdata, wqp, …)
behind a duckdb connection so its endpoints can be queried as named
SQL views. The connectors are an *optional* extension; install with::

    pip install dataretrieval[duckdb]

Quickstart
----------
>>> from dataretrieval.duckdb_connectors import waterdata, wqp
>>> with waterdata.connect() as con:
...     con.monitoring_locations(state_name="Illinois", register_as="sites")
...     con.sql("SELECT count(*) FROM sites").fetchone()
"""

from __future__ import annotations

from . import waterdata, wqp
from ._base import DUCKDB

__all__ = ["DUCKDB", "waterdata", "wqp"]
