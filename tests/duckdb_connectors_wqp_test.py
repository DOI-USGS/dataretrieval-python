"""Tests for the optional WQP DuckDB connector.

The wqp getters are mocked at the function boundary so tests run
without a network. Like ``duckdb_connectors_waterdata_test``, the
whole module is skipped when duckdb isn't installed.
"""

from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")

from dataretrieval.duckdb_connectors import wqp as wqp_connector  # noqa: E402


@pytest.fixture
def results_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "OrganizationIdentifier": ["USGS-IL", "USGS-IL"],
            "MonitoringLocationIdentifier": ["USGS-05586100", "USGS-05586100"],
            "ActivityStartDate": ["2023-05-01", "2023-06-01"],
            "CharacteristicName": ["pH", "Temperature, water"],
            "ResultMeasureValue": [7.4, 19.2],
            "ResultMeasure/MeasureUnitCode": ["std units", "deg C"],
        }
    )


@pytest.fixture
def sites_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "OrganizationIdentifier": ["USGS-IL", "USGS-IL"],
            "MonitoringLocationIdentifier": ["USGS-05586100", "USGS-05543500"],
            "MonitoringLocationName": [
                "ILLINOIS RIVER AT VALLEY CITY, IL",
                "ILLINOIS RIVER AT MARSEILLES, IL",
            ],
            "StateCode": ["US:17", "US:17"],
        }
    )


def test_connect_returns_wrapper():
    con = wqp_connector.connect()
    try:
        assert isinstance(con, wqp_connector.WQPConnection)
        assert con.legacy is True
        assert isinstance(con.con, duckdb.DuckDBPyConnection)
    finally:
        con.close()


def test_connect_legacy_flag_is_threaded():
    con = wqp_connector.connect(legacy=False)
    try:
        assert con.legacy is False
    finally:
        con.close()


def test_get_results_forwards_kwargs_and_legacy(results_df):
    """The connection-level ``legacy`` flag should reach the underlying call."""
    with mock.patch("dataretrieval.wqp.get_results") as m:
        m.return_value = (results_df, mock.Mock())
        with wqp_connector.connect(legacy=False) as con:
            rel = con.get_results(siteid="USGS-05586100", characteristicName="pH")
            assert isinstance(rel, duckdb.DuckDBPyRelation)
        kwargs = m.call_args.kwargs
        assert kwargs["siteid"] == "USGS-05586100"
        assert kwargs["characteristicName"] == "pH"
        assert kwargs["legacy"] is False
        assert kwargs["ssl_check"] is True


def test_per_call_overrides_connection_default(results_df):
    """Passing ``legacy=`` to a helper must override the connection default."""
    with mock.patch("dataretrieval.wqp.get_results") as m:
        m.return_value = (results_df, mock.Mock())
        with wqp_connector.connect(legacy=True) as con:
            con.get_results(siteid="USGS-05586100", legacy=False)
        assert m.call_args.kwargs["legacy"] is False


def test_what_sites_register_as(sites_df):
    with mock.patch("dataretrieval.wqp.what_sites") as m:
        m.return_value = (sites_df, mock.Mock())
        with wqp_connector.connect() as con:
            con.what_sites(statecode="US:17", register_as="sites")
            count = con.sql("SELECT count(*) FROM sites").fetchone()[0]
            assert count == len(sites_df)


def test_join_results_to_sites(results_df, sites_df):
    """A WQP join across two services in one query."""
    with (
        mock.patch("dataretrieval.wqp.what_sites") as m_sites,
        mock.patch("dataretrieval.wqp.get_results") as m_results,
    ):
        m_sites.return_value = (sites_df, mock.Mock())
        m_results.return_value = (results_df, mock.Mock())
        with wqp_connector.connect() as con:
            con.what_sites(statecode="US:17", register_as="sites")
            con.get_results(statecode="US:17", register_as="results")
            rows = con.sql(
                """
                SELECT s.MonitoringLocationName AS name,
                       r.CharacteristicName AS characteristic,
                       r.ResultMeasureValue AS value
                FROM sites s
                JOIN results r USING (MonitoringLocationIdentifier)
                ORDER BY r.ActivityStartDate
                """
            ).fetchall()
    assert rows == [
        ("ILLINOIS RIVER AT VALLEY CITY, IL", "pH", 7.4),
        ("ILLINOIS RIVER AT VALLEY CITY, IL", "Temperature, water", 19.2),
    ]


@pytest.mark.parametrize(
    "helper",
    [
        "what_organizations",
        "what_projects",
        "what_activities",
        "what_detection_limits",
        "what_habitat_metrics",
        "what_activity_metrics",
    ],
)
def test_what_endpoint_invokes_correct_underlying(results_df, helper):
    """Each helper should invoke its corresponding wqp function."""
    with mock.patch(f"dataretrieval.wqp.{helper}") as m:
        m.return_value = (results_df, mock.Mock())
        with wqp_connector.connect() as con:
            getattr(con, helper)(statecode="US:17")
        assert m.call_count == 1
