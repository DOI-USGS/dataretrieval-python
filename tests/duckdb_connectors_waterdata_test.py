"""Tests for the optional waterdata DuckDB connector.

The waterdata getters are mocked at the function boundary so tests run
without a network or an API key. We rely on a real ``duckdb`` install
to validate the SQL-side behaviour. Tests are skipped when duckdb is
not available so the test suite still runs for users without the
optional extra installed.
"""

from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")

from dataretrieval.duckdb_connectors import waterdata as wd_connector  # noqa: E402


@pytest.fixture
def sites_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["USGS-01", "USGS-02", "USGS-03"],
            "site_type": ["Stream", "Well", "Stream"],
            "state_name": ["Illinois", "Illinois", "Iowa"],
        }
    )


@pytest.fixture
def daily_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "monitoring_location_id": ["USGS-01"] * 3 + ["USGS-03"] * 3,
            "time": pd.to_datetime(
                [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                ],
                utc=True,
            ),
            "value": [10.0, 11.0, 12.0, 20.0, 21.0, 22.0],
            "parameter_code": ["00060"] * 6,
        }
    )


def test_connect_returns_wrapper():
    con = wd_connector.connect()
    try:
        assert isinstance(con, wd_connector.WaterdataConnection)
        assert isinstance(con.con, duckdb.DuckDBPyConnection)
        assert con.sql("SELECT 1 AS x").fetchone() == (1,)
    finally:
        con.close()


def test_connect_raises_without_duckdb(monkeypatch):
    """If duckdb isn't installed, ``connect`` must give a clear error."""
    from dataretrieval.duckdb_connectors import _base

    monkeypatch.setattr(_base, "DUCKDB", False)
    with pytest.raises(ImportError, match="duckdb"):
        wd_connector.connect()


def test_monitoring_locations_forwards_kwargs(sites_df):
    with mock.patch("dataretrieval.waterdata.get_monitoring_locations") as m:
        m.return_value = (sites_df, mock.Mock())
        with wd_connector.connect() as con:
            rel = con.monitoring_locations(state_name="Illinois", site_type="Stream")
            assert isinstance(rel, duckdb.DuckDBPyRelation)
            assert rel.fetchall() == list(sites_df.itertuples(index=False, name=None))
        m.assert_called_once_with(state_name="Illinois", site_type="Stream")


def test_register_as_creates_named_view(sites_df):
    with mock.patch("dataretrieval.waterdata.get_monitoring_locations") as m:
        m.return_value = (sites_df, mock.Mock())
        with wd_connector.connect() as con:
            con.monitoring_locations(register_as="sites")
            count = con.sql("SELECT count(*) FROM sites").fetchone()[0]
            assert count == len(sites_df)
            stream_ids = {
                row[0]
                for row in con.sql(
                    "SELECT id FROM sites WHERE site_type = 'Stream'"
                ).fetchall()
            }
            assert stream_ids == {"USGS-01", "USGS-03"}


def test_register_table_works_with_arbitrary_getter(sites_df):
    """``register_table`` should accept any (df, meta)-returning callable."""

    def fake_getter(**kwargs):
        assert kwargs == {"foo": "bar"}
        return sites_df, mock.Mock()

    with wd_connector.connect() as con:
        rel = con.register_table("custom", fake_getter, foo="bar")
        assert isinstance(rel, duckdb.DuckDBPyRelation)
        assert con.sql("SELECT count(*) FROM custom").fetchone() == (len(sites_df),)


def test_sql_join_across_two_endpoints(sites_df, daily_df):
    """Demonstrates the actual value-add: SQL across registered views."""
    with (
        mock.patch("dataretrieval.waterdata.get_monitoring_locations") as m_sites,
        mock.patch("dataretrieval.waterdata.get_daily") as m_daily,
    ):
        m_sites.return_value = (sites_df, mock.Mock())
        m_daily.return_value = (daily_df, mock.Mock())
        with wd_connector.connect() as con:
            con.monitoring_locations(register_as="sites")
            con.daily(register_as="daily")
            rows = con.sql(
                """
                SELECT s.id, s.state_name, avg(d.value) AS mean_value
                FROM sites s
                JOIN daily d ON s.id = d.monitoring_location_id
                WHERE s.site_type = 'Stream'
                GROUP BY s.id, s.state_name
                ORDER BY s.id
                """
            ).fetchall()
    assert rows == [("USGS-01", "Illinois", 11.0), ("USGS-03", "Iowa", 21.0)]


def test_spatial_flag_loads_extension():
    """``spatial=True`` should make ``ST_*`` functions available.

    Skipped if the host can't reach DuckDB's extension registry.
    """
    try:
        con = wd_connector.connect(spatial=True)
    except RuntimeError as exc:  # network-less test runner
        pytest.skip(f"Spatial extension unavailable: {exc}")
    try:
        wkt = con.sql("SELECT ST_AsText(ST_Point(-90.1, 38.6))").fetchone()[0]
        assert wkt == "POINT (-90.1 38.6)"
    finally:
        con.close()


def test_geometry_is_flattened_to_wkt():
    """GeoDataFrame input should be converted to a registerable frame.

    Skipped if geopandas isn't installed; the connector handles that
    case by passing through, and the smoke tests above already cover
    the non-geo path.
    """
    gpd = pytest.importorskip("geopandas")
    from shapely.geometry import Point

    gdf = gpd.GeoDataFrame(
        {
            "id": ["USGS-01", "USGS-02"],
            "geometry": [Point(-90.1, 38.6), Point(-89.4, 39.7)],
        },
        crs="EPSG:4326",
    )
    with mock.patch("dataretrieval.waterdata.get_monitoring_locations") as m:
        m.return_value = (gdf, mock.Mock())
        with wd_connector.connect() as con:
            con.monitoring_locations(register_as="sites")
            cols = [c[0] for c in con.sql("DESCRIBE sites").fetchall()]
            assert {"longitude", "latitude", "geometry"} <= set(cols)
            row = con.sql(
                "SELECT longitude, latitude, geometry FROM sites WHERE id = 'USGS-01'"
            ).fetchone()
            assert row[0] == pytest.approx(-90.1)
            assert row[1] == pytest.approx(38.6)
            assert row[2].startswith("POINT")
