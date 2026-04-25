"""DuckDB connector for the :mod:`dataretrieval.waterdata` endpoints.

Wraps a ``duckdb.DuckDBPyConnection`` and exposes the OGC waterdata
getters as helper methods. Each helper returns a
``duckdb.DuckDBPyRelation``; pass ``register_as=<name>`` to also
register the result as a named view that subsequent SQL can reference.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dataretrieval import waterdata

from ._base import DUCKDB, _BaseConnection, _load_spatial, _require_duckdb

if TYPE_CHECKING:
    import duckdb


class WaterdataConnection(_BaseConnection):
    """A duckdb connection bundled with waterdata helper methods.

    Each helper calls the corresponding ``dataretrieval.waterdata``
    function, flattens any geometry, and returns a
    ``duckdb.DuckDBPyRelation``. Pass ``register_as=<name>`` to also
    register the result as a named view on the connection so it can be
    referenced from SQL by that name.

    The wrapper exposes :meth:`sql` and the underlying :attr:`con`
    for any operation the helpers don't cover.
    """

    def monitoring_locations(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.waterdata.get_monitoring_locations`."""
        return self._endpoint(waterdata.get_monitoring_locations, register_as, kwargs)

    def daily(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.waterdata.get_daily`."""
        return self._endpoint(waterdata.get_daily, register_as, kwargs)

    def continuous(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.waterdata.get_continuous`."""
        return self._endpoint(waterdata.get_continuous, register_as, kwargs)

    def time_series_metadata(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.waterdata.get_time_series_metadata`."""
        return self._endpoint(waterdata.get_time_series_metadata, register_as, kwargs)

    def latest_continuous(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.waterdata.get_latest_continuous`."""
        return self._endpoint(waterdata.get_latest_continuous, register_as, kwargs)

    def latest_daily(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.waterdata.get_latest_daily`."""
        return self._endpoint(waterdata.get_latest_daily, register_as, kwargs)

    def field_measurements(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.waterdata.get_field_measurements`."""
        return self._endpoint(waterdata.get_field_measurements, register_as, kwargs)

    def samples(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.waterdata.get_samples`.

        The samples endpoint queries the USGS Aquarius Samples database
        and is distinct from :class:`dataretrieval.duckdb_connectors.wqp`,
        which queries the multi-agency Water Quality Portal.
        """
        return self._endpoint(waterdata.get_samples, register_as, kwargs)


def connect(
    database: str = ":memory:",
    *,
    spatial: bool = False,
    **kwargs: Any,
) -> WaterdataConnection:
    """Open a DuckDB connection with waterdata helpers attached.

    Parameters
    ----------
    database : str, default ``":memory:"``
        Path forwarded to :func:`duckdb.connect`. Use ``":memory:"``
        for an ephemeral connection.
    spatial : bool, default ``False``
        If ``True``, ``INSTALL spatial; LOAD spatial;`` is run on the
        underlying connection so that registered geometry columns
        (stored as WKT) can be parsed with ``ST_GeomFromText``. The
        extension is downloaded by DuckDB on first install and is not
        a pip dependency. Pair with the ``spatial`` extra
        (``pip install dataretrieval[spatial]``) to also pull in
        ``geopandas`` for richer client-side geometry handling.
    **kwargs
        Additional keyword arguments forwarded to :func:`duckdb.connect`.

    Returns
    -------
    WaterdataConnection
        A connection wrapper exposing :meth:`daily`, :meth:`continuous`,
        :meth:`monitoring_locations`, etc.

    Raises
    ------
    ImportError
        If the optional ``duckdb`` dependency is not installed.
    RuntimeError
        If ``spatial=True`` and the spatial extension cannot be loaded.
    """
    _require_duckdb()
    import duckdb  # local import: only required after the guard above

    raw = duckdb.connect(database, **kwargs)
    if spatial:
        _load_spatial(raw)
    return WaterdataConnection(raw)


__all__ = ["DUCKDB", "WaterdataConnection", "connect"]
