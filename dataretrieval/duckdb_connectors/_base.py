"""Shared building blocks for the per-source DuckDB connectors.

The connectors are an *optional* extension. Each public connector
(``waterdata``, ``wqp``, ...) wraps a ``duckdb.DuckDBPyConnection`` and
exposes the corresponding ``dataretrieval`` getters as helper methods
that return ``duckdb.DuckDBPyRelation`` objects.

This module hosts what's common to all of them:

* the optional-import guard,
* a ``_flatten_geometry`` helper for GeoDataFrame inputs,
* a ``_BaseConnection`` class with the connection-lifecycle plumbing
  and the generic ``register_table`` / ``_endpoint`` helpers.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pandas as pd

try:
    import duckdb

    DUCKDB = True
except ImportError:
    DUCKDB = False

try:
    import geopandas as gpd

    GEOPANDAS = True
except ImportError:
    GEOPANDAS = False

logger = logging.getLogger(__name__)

_INSTALL_HINT = (
    "duckdb is required for the dataretrieval DuckDB connectors. "
    "Install it with `pip install dataretrieval[duckdb]`."
)


def _require_duckdb() -> None:
    """Raise a clear ``ImportError`` if duckdb isn't installed."""
    if not DUCKDB:
        raise ImportError(_INSTALL_HINT)


def _load_spatial(con: duckdb.DuckDBPyConnection) -> None:
    """Install (if needed) and load DuckDB's ``spatial`` extension.

    The extension is a runtime C++ binary that DuckDB itself downloads
    on first ``INSTALL spatial``; it is not a pip-installable package.
    Once loaded, registered ``geometry`` columns (stored as WKT by the
    connectors) can be parsed with ``ST_GeomFromText(geometry)``.

    Raises
    ------
    RuntimeError
        If the extension can't be installed or loaded — typically
        because the host has no network access on first install.
    """
    try:
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
    except Exception as exc:  # pragma: no cover - depends on DuckDB build
        raise RuntimeError(
            "Failed to install/load DuckDB's spatial extension. "
            "DuckDB downloads the extension on first install; check "
            "network access, or install the spatial-aware DuckDB build."
        ) from exc


def _flatten_geometry(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce a GeoDataFrame to a plain DataFrame DuckDB can register.

    DuckDB registers pandas DataFrames natively but does not understand
    a ``geopandas`` ``GeoSeries`` without the spatial extension. To keep
    the prototype dependency-light we convert any geometry column to
    WKT and surface ``longitude`` / ``latitude`` columns when point
    geometries are available. Non-geo input is returned unchanged.
    """
    if not GEOPANDAS or not isinstance(df, gpd.GeoDataFrame):
        return df

    geom_name = df.geometry.name
    out = pd.DataFrame(df).copy()

    geom = df.geometry
    try:
        out["longitude"] = geom.x
        out["latitude"] = geom.y
    except ValueError:
        # geopandas raises ValueError on .x/.y for non-Point geometries.
        pass

    out[geom_name] = geom.to_wkt()
    return out


class _BaseConnection:
    """Connection-lifecycle plumbing shared by every connector class.

    Subclasses are expected to add per-source endpoint helpers that
    delegate to :meth:`_endpoint`.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self._con = con

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        """The underlying ``duckdb.DuckDBPyConnection``."""
        return self._con

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """Run a SQL query against the connection.

        Equivalent to ``self.con.sql(query)``.
        """
        return self._con.sql(query)

    def close(self) -> None:
        """Close the underlying duckdb connection."""
        self._con.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def register_table(
        self,
        name: str,
        fn: Callable[..., tuple[pd.DataFrame, Any]],
        **kwargs: Any,
    ) -> duckdb.DuckDBPyRelation:
        """Call any ``(DataFrame, metadata)`` getter and register it.

        Parameters
        ----------
        name : str
            Name to register the resulting view under.
        fn : callable
            Any function returning ``(DataFrame, metadata)`` — for
            example a ``dataretrieval.waterdata`` or ``dataretrieval.wqp``
            getter.
        **kwargs
            Forwarded to ``fn``.

        Returns
        -------
        duckdb.DuckDBPyRelation
            A relation pointing at the newly registered view.
        """
        return self._endpoint(fn, name, kwargs)

    def _endpoint(
        self,
        fn: Callable[..., tuple[pd.DataFrame, Any]],
        register_as: str | None,
        kwargs: dict[str, Any],
    ) -> duckdb.DuckDBPyRelation:
        df, _ = fn(**kwargs)
        df = _flatten_geometry(df)
        if register_as is not None:
            self._con.register(register_as, df)
            return self._con.table(register_as)
        return self._con.from_df(df)
