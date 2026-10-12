"""Reading a date argument into a ``DateRange`` (ADR 0014).

How each shape is written for the wire is tested with the OGC request builder
in ``waterdata_utils_test.py``.
"""

import datetime

import pandas as pd
import pytest

from dataretrieval._dates import DateRange, read_date_range

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
