"""Reading a date argument into a ``DateRange`` (ADR 0014).

How each shape is written for the wire is tested with the OGC request builder
in ``waterdata_utils_test.py``.
"""

import datetime

import pandas as pd
import pytest

from dataretrieval._dates import DateRange, read_date_range
from dataretrieval.ogc import prepare_request_args
from dataretrieval.ogc.policy import OgcDialect
from dataretrieval.ogc.requests import _read_raw_dates

UTC = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2024-01-01", DateRange(start=UTC, instant=True)),
        (datetime.date(2024, 1, 1), DateRange(start=UTC, instant=True)),
        ("P7D", DateRange(duration="P7D")),
        (["2024-01-01T00:00:00Z", None], DateRange(start=UTC)),
        ("../2024-01-01", DateRange(end=UTC)),
        ([pd.NaT, pd.Timestamp("2024-01-01")], DateRange(end=UTC)),
        ("2023-12-31T18:00:00-06:00", DateRange(start=UTC, instant=True)),
    ],
    ids=["string", "date", "duration", "open_end", "open_start", "nat", "offset"],
)
def test_read_date_range_keeps_the_shape_the_caller_passed(value, expected):
    """An instant, a duration, and a range with open bounds stay distinct, and
    every bound is UTC: a naive one is read as UTC, an aware one converted."""
    assert read_date_range(value, name="time", durations=True) == expected


@pytest.mark.parametrize("value", [None, pd.NaT, [None, ".."], "../.."])
def test_read_date_range_returns_none_for_no_filter(value):
    assert read_date_range(value, name="time", durations=True) is None


def test_prepare_request_args_reads_date_arguments_on_entry():
    """A getter's date arguments are read when it is called, so an unreadable
    one raises before any request is planned, and an all-open one is dropped
    like ``None``."""
    dialect = OgcDialect(date_params=frozenset({"time", "last_modified"}))
    args = prepare_request_args(
        {"time": ["2024-01-01", None], "last_modified": [None, ".."]},
        dialect=dialect,
    )
    assert args == {"time": DateRange(start=UTC)}
    with pytest.raises(ValueError, match="^time could not be read"):
        prepare_request_args({"time": "yesterday"}, dialect=dialect)


def test_prepare_request_args_reads_no_date_without_a_dialect():
    """An API that is not OGC (Samples) gets its arguments as given."""
    assert prepare_request_args({"time": "2024-01-01"}) == {"time": "2024-01-01"}


def test_a_date_range_the_dialect_does_not_list_raises():
    """Arguments read with one dialect and sent with another would put a
    ``DateRange`` repr in the URL."""
    args = {"time": DateRange(start=UTC)}
    assert _read_raw_dates(args, OgcDialect(date_params=frozenset({"time"}))) == args
    with pytest.raises(TypeError, match="^time was read as a date range"):
        _read_raw_dates(args, OgcDialect())
