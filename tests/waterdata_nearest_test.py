"""Tests for ``waterdata.get_nearest_continuous``.

All network interaction is mocked at the ``get_continuous`` boundary, so
these run without an API key and without touching the USGS servers.
"""

from unittest import mock

import pandas as pd
import pytest

from dataretrieval.waterdata.api import get_nearest_continuous


def _fake_df(rows):
    """Build a minimal continuous-response-shaped DataFrame."""
    return pd.DataFrame(
        {
            "time": pd.to_datetime([r["time"] for r in rows], utc=True),
            "value": [r["value"] for r in rows],
            "monitoring_location_id": [r.get("site", "USGS-02238500") for r in rows],
        }
    )


@pytest.fixture
def patch_get_continuous():
    """Replace ``waterdata.api.get_continuous`` with a controllable stub."""
    with mock.patch("dataretrieval.waterdata.api.get_continuous") as m:
        yield m


def test_returns_nearest_per_target(patch_get_continuous):
    targets = pd.to_datetime(["2023-06-15T10:30:31Z", "2023-06-15T10:45:16Z"], utc=True)
    patch_get_continuous.return_value = (
        _fake_df(
            [
                {"time": "2023-06-15T10:30:00Z", "value": 22.4},
                {"time": "2023-06-15T10:45:00Z", "value": 22.5},
            ]
        ),
        mock.Mock(),
    )
    result, _ = get_nearest_continuous(
        targets,
        monitoring_location_id="USGS-02238500",
        parameter_code="00060",
    )
    assert len(result) == 2
    assert list(result["value"]) == [22.4, 22.5]
    assert list(result["target_time"]) == list(targets)


def test_builds_one_or_clause_per_target(patch_get_continuous):
    targets = pd.to_datetime(["2023-06-15T10:30:00Z", "2023-06-16T12:00:00Z"], utc=True)
    patch_get_continuous.return_value = (_fake_df([]), mock.Mock())
    get_nearest_continuous(
        targets,
        monitoring_location_id="USGS-02238500",
        parameter_code="00060",
        window="7min30s",
    )
    _, kwargs = patch_get_continuous.call_args
    filter_expr = kwargs["filter"]
    assert kwargs["filter_lang"] == "cql-text"
    # Two windows — one top-level OR separator
    assert filter_expr.count(") OR (") == 1
    # Each target produces >= and <= bounds
    assert filter_expr.count("time >= '") == 2
    assert filter_expr.count("time <= '") == 2
    # Lower bound of the first window is 7:30 before the target
    assert "'2023-06-15T10:22:30Z'" in filter_expr
    assert "'2023-06-15T10:37:30Z'" in filter_expr


def test_tie_first_keeps_earlier(patch_get_continuous):
    # Target at the midpoint between two grid points
    targets = pd.to_datetime(["2023-06-15T10:22:30Z"], utc=True)
    patch_get_continuous.return_value = (
        _fake_df(
            [
                {"time": "2023-06-15T10:15:00Z", "value": 22.0},
                {"time": "2023-06-15T10:30:00Z", "value": 22.4},
            ]
        ),
        mock.Mock(),
    )
    result, _ = get_nearest_continuous(
        targets,
        monitoring_location_id="USGS-02238500",
        on_tie="first",
        window="7min30s",
    )
    assert len(result) == 1
    assert result.iloc[0]["value"] == 22.0
    assert result.iloc[0]["time"] == pd.Timestamp("2023-06-15T10:15:00Z")


def test_tie_last_keeps_later(patch_get_continuous):
    targets = pd.to_datetime(["2023-06-15T10:22:30Z"], utc=True)
    patch_get_continuous.return_value = (
        _fake_df(
            [
                {"time": "2023-06-15T10:15:00Z", "value": 22.0},
                {"time": "2023-06-15T10:30:00Z", "value": 22.4},
            ]
        ),
        mock.Mock(),
    )
    result, _ = get_nearest_continuous(
        targets,
        monitoring_location_id="USGS-02238500",
        on_tie="last",
        window="7min30s",
    )
    assert result.iloc[0]["value"] == 22.4
    assert result.iloc[0]["time"] == pd.Timestamp("2023-06-15T10:30:00Z")


def test_tie_mean_averages_numeric_and_uses_target_time(patch_get_continuous):
    targets = pd.to_datetime(["2023-06-15T10:22:30Z"], utc=True)
    patch_get_continuous.return_value = (
        _fake_df(
            [
                {"time": "2023-06-15T10:15:00Z", "value": 22.0},
                {"time": "2023-06-15T10:30:00Z", "value": 22.4},
            ]
        ),
        mock.Mock(),
    )
    result, _ = get_nearest_continuous(
        targets,
        monitoring_location_id="USGS-02238500",
        on_tie="mean",
        window="7min30s",
    )
    assert result.iloc[0]["value"] == pytest.approx(22.2)
    # Time is set to the target since no real observation sits at the midpoint
    assert result.iloc[0]["time"] == targets[0]


def test_target_without_observations_is_dropped(patch_get_continuous):
    targets = pd.to_datetime(["2023-06-15T10:30:31Z", "2023-07-15T10:30:31Z"], utc=True)
    # Only the June target has nearby data; July returns nothing.
    patch_get_continuous.return_value = (
        _fake_df([{"time": "2023-06-15T10:30:00Z", "value": 22.4}]),
        mock.Mock(),
    )
    result, _ = get_nearest_continuous(targets, monitoring_location_id="USGS-02238500")
    assert len(result) == 1
    assert result.iloc[0]["target_time"] == targets[0]


def test_multi_site_returns_row_per_target_per_site(patch_get_continuous):
    targets = pd.to_datetime(["2023-06-15T10:30:31Z"], utc=True)
    patch_get_continuous.return_value = (
        _fake_df(
            [
                {"time": "2023-06-15T10:30:00Z", "value": 22.4, "site": "USGS-1"},
                {"time": "2023-06-15T10:30:00Z", "value": 99.9, "site": "USGS-2"},
            ]
        ),
        mock.Mock(),
    )
    result, _ = get_nearest_continuous(
        targets,
        monitoring_location_id=["USGS-1", "USGS-2"],
        parameter_code="00060",
    )
    assert len(result) == 2
    assert set(result["monitoring_location_id"]) == {"USGS-1", "USGS-2"}


def test_empty_targets_returns_empty_frame_without_building_filter(
    patch_get_continuous,
):
    patch_get_continuous.return_value = (_fake_df([]), mock.Mock())
    result, _ = get_nearest_continuous([], monitoring_location_id="USGS-02238500")
    assert result.empty
    # The one call that happens uses a trivial time= window, not a filter.
    _, kwargs = patch_get_continuous.call_args
    assert "filter" not in kwargs
    assert "time" in kwargs


def test_rejects_time_kwarg(patch_get_continuous):
    with pytest.raises(TypeError, match="time"):
        get_nearest_continuous(
            [pd.Timestamp("2023-06-15", tz="UTC")],
            monitoring_location_id="USGS-02238500",
            time="2023-06-01/2023-07-01",
        )


def test_rejects_filter_kwarg(patch_get_continuous):
    with pytest.raises(TypeError, match="filter"):
        get_nearest_continuous(
            [pd.Timestamp("2023-06-15", tz="UTC")],
            monitoring_location_id="USGS-02238500",
            filter="x = 1",
        )


def test_rejects_invalid_on_tie(patch_get_continuous):
    with pytest.raises(ValueError, match="on_tie"):
        get_nearest_continuous(
            [pd.Timestamp("2023-06-15", tz="UTC")],
            monitoring_location_id="USGS-02238500",
            on_tie="random",
        )


def test_accepts_naive_datetimes_as_utc(patch_get_continuous):
    """Naive inputs must be treated as UTC (matching pandas default)."""
    naive = [pd.Timestamp("2023-06-15T10:30:00")]
    patch_get_continuous.return_value = (
        _fake_df([{"time": "2023-06-15T10:30:00Z", "value": 22.4}]),
        mock.Mock(),
    )
    result, _ = get_nearest_continuous(naive, monitoring_location_id="USGS-02238500")
    assert len(result) == 1


def test_accepts_list_of_strings(patch_get_continuous):
    patch_get_continuous.return_value = (
        _fake_df([{"time": "2023-06-15T10:30:00Z", "value": 22.4}]),
        mock.Mock(),
    )
    result, _ = get_nearest_continuous(
        ["2023-06-15T10:30:31Z"], monitoring_location_id="USGS-02238500"
    )
    assert len(result) == 1


def test_forwards_kwargs_to_get_continuous(patch_get_continuous):
    patch_get_continuous.return_value = (_fake_df([]), mock.Mock())
    get_nearest_continuous(
        [pd.Timestamp("2023-06-15", tz="UTC")],
        monitoring_location_id="USGS-02238500",
        parameter_code="00060",
        statistic_id="00011",
        approval_status="Approved",
    )
    _, kwargs = patch_get_continuous.call_args
    assert kwargs["statistic_id"] == "00011"
    assert kwargs["approval_status"] == "Approved"
