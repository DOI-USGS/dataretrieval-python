"""Contract of the shared column-coercion leaf.

Component layer: exercises ``dataretrieval._coercion`` directly, with no service
and no HTTP. The result paths that use it — the OGC getters
(``waterdata_test.py``, ``ngwmn_test.py``), the Samples/WQP datetime shaping
(``utils_test.py``), and the statistics getter (``waterdata_test.py``) — cover
how each adopts it.
"""

import warnings

import pandas as pd
import pytest

from dataretrieval._coercion import coerce_columns, to_datetime, to_numeric


@pytest.mark.parametrize(
    "values",
    [["1", "2"], [1, 2], [], [None, None]],
    ids=["whole-strings", "integers", "empty", "all-null"],
)
def test_to_numeric_is_always_float64(values):
    """Whole, integer, empty, and all-null inputs all come back ``float64``
    (#428): ``pandas.to_numeric`` alone would keep the integers ``int64``, so a
    column's dtype would depend on its contents."""
    raw = pd.Series(values, dtype=object)
    assert to_numeric(raw, name="value").dtype == "float64"


def test_to_numeric_warns_on_present_unparseable_values():
    """A value that is present but cannot be parsed becomes ``NaN`` and is
    counted; a null or empty string is missing and is not."""
    raw = pd.Series(["1.5", None, "", "abc", "n/a"])
    with pytest.warns(UserWarning, match=r"^2 values in column 'value'.*numbers"):
        out = to_numeric(raw, name="value")
    assert out.isna().sum() == 4


def test_to_datetime_warns_on_present_unparseable_values():
    raw = pd.Series(["2024-01-01T00:00:00Z", None, "", "not a date", "n/a"])
    with pytest.warns(UserWarning, match=r"^2 values in column 'time'.*datetimes"):
        out = to_datetime(raw, name="time")
    assert out.isna().sum() == 4


def test_to_datetime_accepts_a_fixed_format():
    """Extra keywords reach ``pandas.to_datetime``; ``errors='coerce'`` is fixed
    so a value the format cannot read is ``NaT`` rather than a raise."""
    raw = pd.Series(["2024-01-09 10:00:00 +0000", "nope"])
    with pytest.warns(UserWarning, match=r"^1 value in column 'ts'"):
        out = to_datetime(raw, name="ts", format="%Y-%m-%d %H:%M:%S %z", utc=True)
    assert out[0] == pd.Timestamp("2024-01-09 10:00:00", tz="UTC")
    assert pd.isna(out[1])


def test_warning_is_singular_for_one_value():
    raw = pd.Series(["1.5", "abc"])
    with pytest.warns(UserWarning, match=r"^1 value in column 'value'"):
        to_numeric(raw, name="value")


def test_no_warning_when_everything_parses_or_is_missing():
    raw = pd.Series(["1.5", None, ""])
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        to_numeric(raw, name="value")


def test_coerce_columns_touches_only_present_named_columns():
    """An adapter may name every column it ever coerces; only those actually in
    the frame are touched, and others keep their dtype."""
    df = pd.DataFrame(
        {
            "value": ["1", "2"],
            "time": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"],
            "label": ["a", "b"],
        }
    )
    original_label_dtype = df["label"].dtype
    out = coerce_columns(
        df,
        numeric={"value", "absent_numeric"},
        datetime={"time", "absent_time"},
    )
    assert out["value"].dtype == "float64"
    assert pd.api.types.is_datetime64_any_dtype(out["time"])
    assert out["label"].dtype == original_label_dtype
    assert out is df


def test_coerce_columns_returns_the_same_frame_for_chaining():
    df = pd.DataFrame({"value": ["1"]})
    assert coerce_columns(df, numeric={"value"}) is df
