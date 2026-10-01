"""Tests for ``waterdata.get_nearest_continuous``.

All network interaction is mocked at the ``get_continuous`` boundary, so
these run without an API key and without contacting the USGS servers.
"""

from unittest import mock

import numpy as np
import pandas as pd
import pytest

from dataretrieval.exceptions import DataRetrievalError
from dataretrieval.interruptions import QuotaExhausted, ServiceInterrupted
from dataretrieval.waterdata.nearest import get_nearest_continuous

_SITE = "USGS-02238500"


def _fake_df(rows):
    """Build a minimal DataFrame with the continuous-response columns."""
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
    with mock.patch("dataretrieval.waterdata.nearest.get_continuous") as m:
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
        window="PT7M30S",
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
        window="PT7M30S",
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
        window="PT7M30S",
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
        window="PT7M30S",
    )
    assert result.iloc[0]["value"] == pytest.approx(22.2)
    # Time is set to the target since no real observation falls at the midpoint
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


# --- targets validation ------------------------------------------------------
# Every rejection happens before ``get_continuous`` is called, so each test also
# asserts no request was made. Where a message suggests a fix, a companion test
# applies that fix literally and checks the call then succeeds.


def test_empty_targets_raises(patch_get_continuous):
    """An empty ``targets`` is a call with no useful work to do and almost always a
    caller bug — raise rather than issue a request that returns nothing."""
    with pytest.raises(ValueError, match="targets"):
        get_nearest_continuous([], monitoring_location_id=_SITE)
    patch_get_continuous.assert_not_called()


_LONE_MISSING_TARGETS = {
    "nat": pd.NaT,
    "none": None,
    "empty-string": "",
    "float-nan": float("nan"),
    "numpy-nat": pd.NaT.to_datetime64(),
    "one-element-list": [None],
}


@pytest.mark.parametrize(
    ("targets", "message"),
    [(t, "its only entry is missing") for t in _LONE_MISSING_TARGETS.values()]
    + [([None, pd.NaT, float("nan")], "all 3 entries are missing")],
    ids=[*_LONE_MISSING_TARGETS, "all-of-three"],
)
def test_all_missing_targets_ask_for_a_timestamp(
    patch_get_continuous, targets, message
):
    """Removing every entry would leave nothing, so ask for a timestamp rather
    than suggesting the caller remove them."""
    with pytest.raises(ValueError, match=message) as exc_info:
        get_nearest_continuous(targets, monitoring_location_id=_SITE)
    assert "Remove" not in str(exc_info.value)
    patch_get_continuous.assert_not_called()


def test_missing_target_message_wording(patch_get_continuous):
    """The full message for the common case: one gap in an otherwise valid list.
    Missing timestamps must not become ``'nan'`` CQL bounds."""
    with pytest.raises(ValueError) as exc_info:
        get_nearest_continuous(
            ["2024-01-01T12:00:00Z", None], monitoring_location_id=_SITE
        )
    assert str(exc_info.value) == (
        "targets contains 1 missing timestamp (NaT, None, NaN, or ''), at "
        "position 1, counting from 0. Remove those entries or replace them with "
        "valid timestamps, e.g. targets.dropna() for a pandas Series or "
        "DatetimeIndex."
    )
    patch_get_continuous.assert_not_called()


@pytest.mark.parametrize(
    ("targets", "located"),
    [
        (
            pd.Series([pd.Timestamp("2024-01-01T12:00:00Z"), pd.NaT]),
            "1 missing timestamp (NaT, None, NaN, or ''), at position 1",
        ),
        (
            pd.DatetimeIndex(["2024-01-01T12:00:00Z", pd.NaT]),
            "1 missing timestamp (NaT, None, NaN, or ''), at position 1",
        ),
        (
            pd.Series(["2024-01-01", float("nan"), "2024-01-03", ""]),
            "2 missing timestamps (NaT, None, NaN, or ''); the first is at position 1",
        ),
    ],
    ids=["series", "index", "series-nan-and-empty-string"],
)
def test_missing_target_entries_are_located(patch_get_continuous, targets, located):
    """A long target list is only correctable if the message says where to look,
    whatever container the targets arrive in."""
    with pytest.raises(ValueError) as exc_info:
        get_nearest_continuous(targets, monitoring_location_id=_SITE)
    assert located in str(exc_info.value)
    patch_get_continuous.assert_not_called()


def test_missing_target_remedy_works_when_followed(patch_get_continuous):
    """The suggested ``dropna()`` must produce a call that succeeds."""
    patch_get_continuous.return_value = (_fake_df([]), mock.Mock())
    targets = pd.Series(["2024-01-01T12:00:00Z", None])
    with pytest.raises(ValueError, match=r"targets\.dropna\(\)"):
        get_nearest_continuous(targets, monitoring_location_id=_SITE)
    get_nearest_continuous(targets.dropna(), monitoring_location_id=_SITE)
    patch_get_continuous.assert_called_once()


@pytest.mark.parametrize(
    ("targets", "shown"),
    [
        (1.5e9, "1500000000.0"),
        ([1.5e9, 1.6e9], "1500000000.0"),
        (np.array([1, 2]), "1"),
        (pd.Series([np.nan, 1.5e9]), "1500000000.0"),
    ],
    ids=["scalar", "list", "int-array", "series-leading-nan"],
)
def test_numeric_targets_are_refused(patch_get_continuous, targets, shown):
    """``pandas.to_datetime(1.5e9)`` is 1970-01-01T00:00:01.5Z, never what was
    meant by an epoch value. The message shows the first non-missing number."""
    with pytest.raises(TypeError, match="targets must be timestamps") as exc_info:
        get_nearest_continuous(targets, monitoring_location_id=_SITE)
    assert f"(got {shown})" in str(exc_info.value)
    patch_get_continuous.assert_not_called()


@pytest.mark.parametrize(
    ("unit", "number"),
    [("s", 1.5e9), ("ms", 1.5e12), ("us", 1.5e15), ("ns", 1.5e18)],
    ids=["s", "ms", "us", "ns"],
)
def test_numeric_targets_remedy_works_for_every_listed_unit(
    patch_get_continuous, unit, number
):
    """Each unit the message lists must convert to the intended timestamp."""
    patch_get_continuous.return_value = (_fake_df([]), mock.Mock())
    get_nearest_continuous(
        pd.to_datetime([number], unit=unit), monitoring_location_id=_SITE
    )
    assert "2017-07-14T02:32:30Z" in patch_get_continuous.call_args.kwargs["filter"]


@pytest.mark.parametrize(
    "targets",
    [["2024-01-01", "not a date"], ["2024-01-01", "2024-01-01T12:00Z"]],
    ids=["garbage", "mixed-iso-forms"],
)
def test_unparseable_targets_name_the_argument(patch_get_continuous, targets):
    """pandas' own message names no argument and suggests a ``format=`` this
    getter does not accept; the rewrapped one names ``targets`` instead."""
    with pytest.raises(ValueError, match="targets could not be parsed") as exc_info:
        get_nearest_continuous(targets, monitoring_location_id=_SITE)
    assert "You might want to try" not in str(exc_info.value)
    patch_get_continuous.assert_not_called()


# --- window validation -------------------------------------------------------


def _call_with_window(window):
    """Call the getter with one valid target, varying only ``window``."""
    return get_nearest_continuous(
        ["2024-01-01T12:00:00Z"], monitoring_location_id=_SITE, window=window
    )


@pytest.mark.parametrize(
    ("window", "message"),
    [
        (None, r"window is missing \(got None\)"),
        ("NaT", r"window is missing \(got 'NaT'\)"),
        ("seven minutes", r"window could not be parsed as a duration"),
        ("-PT5M", r"window must not be negative.*window='P0DT0H5M0S'"),
        (
            pd.Timedelta(minutes=-7, seconds=-30),
            r"window must not be negative.*window='P0DT0H7M30S'",
        ),
    ],
    ids=["none", "nat", "garbage", "negative-string", "negative-timedelta"],
)
def test_unusable_window_raises_before_query(patch_get_continuous, window, message):
    """A missing window crashed while building the filter; a negative one
    inverted every bound and returned an empty frame as if no data existed."""
    with pytest.raises(ValueError, match=message):
        _call_with_window(window)
    patch_get_continuous.assert_not_called()


@pytest.mark.parametrize("window", [450, 7.5], ids=["int", "float"])
def test_numeric_window_is_refused(patch_get_continuous, window):
    """``pandas.Timedelta(450)`` is 450 nanoseconds, never what was meant."""
    with pytest.raises(TypeError, match="window must be a duration.*nanoseconds"):
        _call_with_window(window)
    patch_get_continuous.assert_not_called()


@pytest.mark.parametrize(
    ("window", "lower", "upper"),
    [
        # The fix the negative-window message suggests: same span, sign dropped.
        ("P0DT0H5M0S", "2024-01-01T11:55:00Z", "2024-01-01T12:05:00Z"),
        # Zero is a legitimate degenerate window: an exact-match query.
        ("PT0S", "2024-01-01T12:00:00Z", "2024-01-01T12:00:00Z"),
    ],
    ids=["negative-window-remedy", "zero"],
)
def test_accepted_window_bounds(patch_get_continuous, window, lower, upper):
    patch_get_continuous.return_value = (_fake_df([]), mock.Mock())
    _call_with_window(window)
    assert patch_get_continuous.call_args.kwargs["filter"] == (
        f"(time >= '{lower}' AND time <= '{upper}')"
    )


# --- other arguments and behavior --------------------------------------------


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


@pytest.mark.parametrize(
    "window",
    [
        "00:07:30",  # HH:MM:SS
        "7min30s",  # pandas shorthand
        "450s",  # seconds shorthand
        "PT7M30S",  # ISO 8601 duration
        pd.Timedelta(minutes=7, seconds=30),  # Timedelta object
    ],
)
def test_window_accepts_any_pandas_timedelta_form(patch_get_continuous, window):
    """Every string or ``Timedelta`` form ``pandas.Timedelta`` parses must
    produce the same CQL filter. Documents the public contract; bare numbers
    and negative or missing windows are rejected (see window validation)."""
    targets = pd.to_datetime(["2023-06-15T10:30:00Z"], utc=True)
    patch_get_continuous.return_value = (_fake_df([]), mock.Mock())

    get_nearest_continuous(targets, monitoring_location_id="USGS-1", window=window)
    filter_expr = patch_get_continuous.call_args.kwargs["filter"]
    # Bounds are 7:30 away from the target regardless of input spelling
    assert "'2023-06-15T10:22:30Z'" in filter_expr
    assert "'2023-06-15T10:37:30Z'" in filter_expr


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


def test_accepts_single_string_target(patch_get_continuous):
    """A bare scalar target must round-trip through pd.to_datetime.

    Regression: previously `pd.DatetimeIndex(pd.to_datetime("...", utc=True))`
    raised TypeError because pd.to_datetime returns a scalar Timestamp for a
    single-string input.
    """
    patch_get_continuous.return_value = (
        _fake_df([{"time": "2023-06-15T10:30:00Z", "value": 22.4}]),
        mock.Mock(),
    )
    result, _ = get_nearest_continuous(
        "2023-06-15T10:30:31Z", monitoring_location_id="USGS-02238500"
    )
    assert len(result) == 1
    assert result["target_time"].iloc[0] == pd.Timestamp("2023-06-15T10:30:31Z")


def test_accepts_single_timestamp_target(patch_get_continuous):
    """A single ``pd.Timestamp`` target also round-trips."""
    patch_get_continuous.return_value = (
        _fake_df([{"time": "2023-06-15T10:30:00Z", "value": 22.4}]),
        mock.Mock(),
    )
    target = pd.Timestamp("2023-06-15T10:30:31Z")
    result, _ = get_nearest_continuous(target, monitoring_location_id="USGS-02238500")
    assert len(result) == 1


def test_accepts_pandas_series_targets(patch_get_continuous):
    """A ``pd.Series`` of timestamps preserves all elements (not just the first)."""
    patch_get_continuous.return_value = (
        _fake_df(
            [
                {"time": "2023-06-15T10:30:00Z", "value": 22.4},
                {"time": "2023-06-16T10:30:00Z", "value": 22.5},
            ]
        ),
        mock.Mock(),
    )
    targets = pd.Series(["2023-06-15T10:30:31Z", "2023-06-16T10:30:31Z"])
    result, _ = get_nearest_continuous(targets, monitoring_location_id="USGS-02238500")
    assert len(result) == 2


def test_missing_time_column_reports_incomplete_response(patch_get_continuous):
    """A nonempty response missing a requested match column is a service error."""
    df_no_time = pd.DataFrame(
        {
            "value": [22.4],
            "monitoring_location_id": ["USGS-02238500"],
        }
    )
    patch_get_continuous.return_value = (df_no_time, mock.Mock())

    with pytest.raises(DataRetrievalError, match="omitted.*'time'"):
        get_nearest_continuous(
            ["2023-06-15T10:30:31Z"],
            monitoring_location_id="USGS-02238500",
            properties=["value", "monitoring_location_id"],
        )


def test_interruption_preserves_nearest_shape_for_partial_and_resumed_rows(
    patch_get_continuous,
):
    """Partial and resumed results keep the outer getter's return shape."""
    targets = pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"], utc=True)
    raw = _fake_df(
        [
            {"time": "2024-01-01T00:03:00Z", "value": 8.1},
            {"time": "2024-01-02T00:03:00Z", "value": 8.2},
        ]
    )
    response = mock.Mock()
    metadata = mock.Mock()

    class FakeCall:
        partial_frame = raw
        partial_response = response

        def resume(self):
            return raw, metadata

    patch_get_continuous.side_effect = QuotaExhausted(
        completed_chunks=1, total_chunks=2, call=FakeCall()
    )

    with pytest.raises(QuotaExhausted) as excinfo:
        get_nearest_continuous(
            targets,
            monitoring_location_id="USGS-02238500",
            window="PT1H",
        )

    interrupted = excinfo.value
    assert list(interrupted.partial_frame["target_time"]) == list(targets)
    assert list(interrupted.call.partial_frame["target_time"]) == list(targets)
    assert interrupted.partial_response is response

    resumed, resumed_metadata = interrupted.call.resume()

    assert list(resumed["target_time"]) == list(targets)
    assert list(resumed["value"]) == [8.1, 8.2]
    assert resumed_metadata is metadata


def test_nearest_shape_survives_repeated_resume_interruptions(patch_get_continuous):
    """A resume that is interrupted again returns another outer-shaped call."""
    target = pd.to_datetime(["2024-01-01T00:00:00Z"], utc=True)
    raw = _fake_df([{"time": "2024-01-01T00:03:00Z", "value": 8.1}])
    metadata = mock.Mock()

    class FakeCall:
        partial_frame = raw
        partial_response = mock.Mock()
        attempts = 0

        def resume(self):
            self.attempts += 1
            if self.attempts == 1:
                raise ServiceInterrupted(completed_chunks=1, total_chunks=2, call=self)
            return raw, metadata

    call = FakeCall()
    patch_get_continuous.side_effect = QuotaExhausted(
        completed_chunks=1, total_chunks=2, call=call
    )

    with pytest.raises(QuotaExhausted) as first:
        get_nearest_continuous(
            target,
            monitoring_location_id="USGS-02238500",
            window="PT1H",
        )
    with pytest.raises(ServiceInterrupted) as second:
        first.value.call.resume()

    assert list(second.value.partial_frame["target_time"]) == list(target)
    assert list(second.value.call.partial_frame["target_time"]) == list(target)

    resumed, resumed_metadata = second.value.call.resume()

    assert list(resumed["target_time"]) == list(target)
    assert resumed_metadata is metadata


def test_caller_properties_keep_the_columns_the_match_needs(patch_get_continuous):
    """A caller's ``properties`` list gains 'time' and the grouping column.

    Without the injection a list like ``['time', 'value']`` reached the
    service unchanged, the response came back with no
    ``monitoring_location_id``, and every site but one was dropped without an error --
    an incomplete result the caller could not detect.
    """
    patch_get_continuous.return_value = (
        pd.DataFrame(
            [
                {
                    "time": "2023-06-15T10:30:00Z",
                    "value": 1.0,
                    "monitoring_location_id": "USGS-A",
                },
                {
                    "time": "2023-06-15T10:30:00Z",
                    "value": 2.0,
                    "monitoring_location_id": "USGS-B",
                },
            ]
        ),
        mock.Mock(),
    )
    result, _ = get_nearest_continuous(
        ["2023-06-15T10:30:31Z"],
        monitoring_location_id=["USGS-A", "USGS-B"],
        properties=["time", "value"],
    )
    sent = patch_get_continuous.call_args.kwargs["properties"]
    assert "monitoring_location_id" in sent
    assert sent[:2] == ["time", "value"]
    # One row per site, not one row for the pair.
    assert len(result) == 2


def test_properties_are_left_alone_when_already_complete(patch_get_continuous):
    patch_get_continuous.return_value = (
        pd.DataFrame(
            [{"time": "2023-06-15T10:30:00Z", "monitoring_location_id": "USGS-A"}]
        ),
        mock.Mock(),
    )
    asked = ["time", "monitoring_location_id"]
    get_nearest_continuous(
        ["2023-06-15T10:30:31Z"], monitoring_location_id="USGS-A", properties=asked
    )
    assert patch_get_continuous.call_args.kwargs["properties"] == asked


@pytest.mark.parametrize(
    ("frame", "missing"),
    [
        (pd.DataFrame({"monitoring_location_id": ["USGS-A"]}), "time"),
        (pd.DataFrame({"time": ["2023-06-15T10:30:00Z"]}), "monitoring_location_id"),
    ],
)
def test_nonempty_response_requires_matching_columns(
    patch_get_continuous, frame, missing
):
    """Required request properties are also validated on the response boundary."""
    patch_get_continuous.return_value = (frame, mock.Mock())

    with pytest.raises(DataRetrievalError) as excinfo:
        get_nearest_continuous(
            ["2023-06-15T10:30:31Z"], monitoring_location_id="USGS-A"
        )

    message = str(excinfo.value)
    assert repr(missing) in message
    assert "Retry the request" in message
    assert "report the response" in message


def test_empty_response_does_not_require_matching_columns(patch_get_continuous):
    """No observations is a valid result even when the empty frame has no schema."""
    patch_get_continuous.return_value = (pd.DataFrame(), mock.Mock())

    result, _ = get_nearest_continuous(
        ["2023-06-15T10:30:31Z"], monitoring_location_id="USGS-A"
    )

    assert result.empty
    assert "target_time" in result.columns


def test_no_observation_inside_the_window_returns_the_empty_shape(
    patch_get_continuous,
):
    """A target with nothing near it is a valid result, not a failure --
    but the frame must keep the result columns so a caller can concatenate it
    with a populated one instead of special-casing empty frames."""
    patch_get_continuous.return_value = (
        pd.DataFrame(
            [
                {
                    "time": "2023-06-15T10:30:00Z",
                    "value": 1.0,
                    "monitoring_location_id": "A",
                }
            ]
        ),
        mock.Mock(),
    )
    result, _ = get_nearest_continuous(
        ["2020-01-01T00:00:00Z"],  # years from the only observation
        monitoring_location_id="A",
        window="1h",
    )
    assert result.empty
    assert "target_time" in result.columns


class TestNearestPartialResults:
    """When a fan-out is interrupted the caller still gets the chunks that
    finished, shaped like a normal result -- otherwise recovering from an
    interruption means handling a second frame layout."""

    def test_a_partial_frame_with_no_completed_chunks_keeps_the_result_shape(self):
        from dataretrieval.waterdata.nearest import _NearestSelector

        selector = _NearestSelector(
            pd.to_datetime(["2023-06-15T10:30:00Z"]), pd.Timedelta("1h"), "first"
        )
        out = selector.select_partial(pd.DataFrame())

        assert out.empty
        assert "target_time" in out.columns

    def test_a_partial_frame_with_rows_is_selected_normally(self):
        from dataretrieval.waterdata.nearest import _NearestSelector

        selector = _NearestSelector(
            pd.to_datetime(["2023-06-15T10:30:00Z"]), pd.Timedelta("1h"), "first"
        )
        frame = pd.DataFrame(
            [
                {
                    "time": "2023-06-15T10:30:05Z",
                    "value": 1.0,
                    "monitoring_location_id": "A",
                }
            ]
        )
        out = selector.select_partial(frame)

        assert len(out) == 1

    def test_the_wrapper_passes_the_inner_calls_live_response_through(self):
        """``partial_response`` is how a caller inspects what arrived before
        the interruption; the nearest wrapper must not shadow it."""
        from dataretrieval.waterdata.nearest import _NearestCall, _NearestSelector

        inner = mock.Mock()
        inner.partial_response = "sentinel-response"
        call = _NearestCall(
            inner,
            _NearestSelector(
                pd.to_datetime(["2023-06-15T10:30:00Z"]), pd.Timedelta("1h"), "first"
            ),
        )

        assert call.partial_response == "sentinel-response"
