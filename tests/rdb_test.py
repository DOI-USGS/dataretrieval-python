import pandas as pd
import pytest

from dataretrieval.rdb import extract_rdb_comment, read_rdb

# A minimally complete RDB: comment block, header row, format-spec row,
# data rows. Both NWIS responses and ratings RDBs share this shape.
_BASIC_RDB = """\
# header line one
# header line two
agency_cd\tsite_no\tINDEP\tDEP
5s\t15s\t10n\t10n
USGS\t01104475\t0.10\t0.0
USGS\t01104475\t0.20\t0.5
USGS\t01104475\t0.30\t1.2
"""


def test_read_rdb_parses_basic_shape():
    df = read_rdb(_BASIC_RDB)
    assert list(df.columns) == ["agency_cd", "site_no", "INDEP", "DEP"]
    assert len(df) == 3
    assert df["INDEP"].tolist() == [0.10, 0.20, 0.30]


def test_read_rdb_skips_format_spec_row():
    """The "5s 15s 10n 10n" row is metadata, not data."""
    df = read_rdb(_BASIC_RDB)
    # If the format-spec row had been treated as data, df would have 4 rows
    # and "5s" / "15s" would appear in the parsed values.
    assert "5s" not in df["agency_cd"].tolist()


def test_read_rdb_dtype_hints_applied():
    """Caller-supplied dtype hints are forwarded to pandas; unknown names ignored."""
    df = read_rdb(_BASIC_RDB, dtypes={"site_no": str, "DEP": float, "unknown": int})
    # Without the str hint, pandas would parse "01104475" as int and drop the
    # leading zero. Check the values, not the dtype name (which varies across
    # pandas versions: object, StringDtype, etc.).
    assert df["site_no"].iloc[0] == "01104475"
    assert df["DEP"].dtype == float


def test_read_rdb_empty_when_only_comments():
    """All-comments input is a legitimate "no data" response, not an error."""
    df = read_rdb("# only a comment\n# and another\n")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_read_rdb_raises_on_html_response():
    """If the service returns an HTML error page, surface it loudly."""
    with pytest.raises(ValueError, match="HTML"):
        read_rdb("<html><body>Service Unavailable</body></html>")
    with pytest.raises(ValueError, match="HTML"):
        read_rdb("<!DOCTYPE html>\n<html>...")


def test_extract_rdb_comment_returns_only_hash_lines():
    comments = extract_rdb_comment(_BASIC_RDB)
    assert comments == ["# header line one", "# header line two"]


def test_extract_rdb_comment_empty_when_no_comments():
    assert extract_rdb_comment("a\tb\n1\t2\n") == []
