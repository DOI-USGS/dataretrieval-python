"""DuckDB connector for the :mod:`dataretrieval.wqp` endpoints.

The Water Quality Portal (WQP) is a multi-agency repository covering
USGS, EPA, state agencies, etc. Its API exposes two parallel schemas:

* the legacy WQX data profiles (default; ``legacy=True``), and
* the modern WQX 3.0 profiles (``legacy=False``).

The connection holds the chosen schema as a default that's threaded
into every helper call; individual calls can override it with
``legacy=False`` (or vice versa) when needed. ``ssl_check`` is also a
connection-level default for the same reason.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dataretrieval import wqp

from ._base import DUCKDB, _BaseConnection, _load_spatial, _require_duckdb

if TYPE_CHECKING:
    import duckdb


class WQPConnection(_BaseConnection):
    """A duckdb connection bundled with WQP helper methods.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        The underlying duckdb connection.
    legacy : bool, default ``True``
        Default schema used by every helper. ``True`` queries the
        legacy WQX profiles (e.g. ``resultPhysChem``, ``narrowResult``,
        ``biological``); ``False`` queries WQX 3.0 (``fullPhysChem``,
        ``basicPhysChem``, ``narrow``). Override per-call with
        ``legacy=...`` in any helper.
    ssl_check : bool, default ``True``
        Default value for the ``ssl_check`` argument forwarded to the
        WQP getters. Override per-call when needed.
    """

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        legacy: bool = True,
        ssl_check: bool = True,
    ) -> None:
        super().__init__(con)
        self._legacy = legacy
        self._ssl_check = ssl_check

    @property
    def legacy(self) -> bool:
        """Default schema used by every helper (``True`` = legacy WQX)."""
        return self._legacy

    # --- Endpoint helpers -------------------------------------------------
    #
    # Each helper forwards arbitrary CamelCase WQP query parameters as
    # **kwargs. See the corresponding ``dataretrieval.wqp`` function for
    # the full list of supported parameters.

    def get_results(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.wqp.get_results`."""
        return self._wqp_call(wqp.get_results, register_as, kwargs)

    def what_sites(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.wqp.what_sites`."""
        return self._wqp_call(wqp.what_sites, register_as, kwargs)

    def what_organizations(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.wqp.what_organizations`."""
        return self._wqp_call(wqp.what_organizations, register_as, kwargs)

    def what_projects(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.wqp.what_projects`."""
        return self._wqp_call(wqp.what_projects, register_as, kwargs)

    def what_activities(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.wqp.what_activities`."""
        return self._wqp_call(wqp.what_activities, register_as, kwargs)

    def what_detection_limits(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.wqp.what_detection_limits`."""
        return self._wqp_call(wqp.what_detection_limits, register_as, kwargs)

    def what_habitat_metrics(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.wqp.what_habitat_metrics`."""
        return self._wqp_call(wqp.what_habitat_metrics, register_as, kwargs)

    def what_activity_metrics(
        self, *, register_as: str | None = None, **kwargs: Any
    ) -> duckdb.DuckDBPyRelation:
        """Wrap :func:`dataretrieval.wqp.what_activity_metrics`."""
        return self._wqp_call(wqp.what_activity_metrics, register_as, kwargs)

    # --- Internal ---------------------------------------------------------

    def _wqp_call(
        self,
        fn: Any,
        register_as: str | None,
        kwargs: dict[str, Any],
    ) -> duckdb.DuckDBPyRelation:
        """Inject the connection-level ``legacy`` / ``ssl_check`` defaults."""
        kwargs.setdefault("legacy", self._legacy)
        kwargs.setdefault("ssl_check", self._ssl_check)
        return self._endpoint(fn, register_as, kwargs)


def connect(
    database: str = ":memory:",
    *,
    legacy: bool = True,
    ssl_check: bool = True,
    spatial: bool = False,
    **kwargs: Any,
) -> WQPConnection:
    """Open a DuckDB connection with WQP helpers attached.

    Parameters
    ----------
    database : str, default ``":memory:"``
        Path forwarded to :func:`duckdb.connect`.
    legacy : bool, default ``True``
        Default schema for every helper. See :class:`WQPConnection`.
    ssl_check : bool, default ``True``
        Default ``ssl_check`` flag forwarded to the WQP getters.
    spatial : bool, default ``False``
        If ``True``, install + load DuckDB's ``spatial`` extension on
        the underlying connection. See
        :func:`dataretrieval.duckdb_connectors.waterdata.connect`.
    **kwargs
        Additional keyword arguments forwarded to :func:`duckdb.connect`.

    Returns
    -------
    WQPConnection
        A connection wrapper exposing :meth:`get_results`,
        :meth:`what_sites`, etc.

    Raises
    ------
    ImportError
        If the optional ``duckdb`` dependency is not installed.
    RuntimeError
        If ``spatial=True`` and the spatial extension cannot be loaded.
    """
    _require_duckdb()
    import duckdb  # local: only required after the guard above

    raw = duckdb.connect(database, **kwargs)
    if spatial:
        _load_spatial(raw)
    return WQPConnection(raw, legacy=legacy, ssl_check=ssl_check)


__all__ = ["DUCKDB", "WQPConnection", "connect"]
