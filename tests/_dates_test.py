"""Reading a date argument into a ``DateRange`` (ADR 0014).

How each shape is written for the wire is tested with the OGC request builder
in ``waterdata_utils_test.py``.
"""

import datetime

import pandas as pd
import pytest

from dataretrieval._dates import DateRange, read_date_range
from dataretrieval._intervals import DateDialect
from dataretrieval.ogc import prepare_request_args

NAIVE = datetime.datetime(2024, 1, 1)  # noqa: DTZ001 -- a naive bound is the case
UTC = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2024-01-01", DateRange(start=NAIVE, instant=True)),
        (datetime.date(2024, 1, 1), DateRange(start=NAIVE, instant=True)),
        ("P7D", DateRange(duration="P7D")),
        (["2024-01-01T00:00:00Z", None], DateRange(start=UTC)),
        ("../2024-01-01", DateRange(end=NAIVE)),
        ([pd.NaT, pd.Timestamp("2024-01-01")], DateRange(end=NAIVE)),
    ],
)
def test_read_date_range_keeps_the_shape_the_caller_passed(value, expected):
    """An instant, a duration, and a range with open bounds stay distinct: a
    one-element instant is not a range with an open end."""
    assert read_date_range(value) == expected


@pytest.mark.parametrize("value", [None, pd.NaT, [None, ".."], "../.."])
def test_read_date_range_returns_none_for_no_filter(value):
    assert read_date_range(value) is None


def test_prepare_request_args_reads_date_arguments_on_entry():
    """A getter's date arguments are read when it is called, so an unreadable
    one raises before any request is planned, and an all-open one is dropped
    like ``None``."""
    dates = DateDialect(params=frozenset({"time", "last_modified"}))
    args = prepare_request_args(
        {"time": ["2024-01-01", None], "last_modified": [None, ".."]}, dates=dates
    )
    assert args == {"time": DateRange(start=NAIVE)}
    with pytest.raises(ValueError, match="^time could not be read"):
        prepare_request_args({"time": "yesterday"}, dates=dates)
