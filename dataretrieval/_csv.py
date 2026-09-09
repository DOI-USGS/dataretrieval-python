"""Identify the code columns in a response, and read CSV with them as strings."""

from collections.abc import Iterable
from io import StringIO

import pandas as pd
from pandas import DataFrame


def _is_code_column(name: str) -> bool:
    """Report whether a column name denotes a code or identifier.

    USGS services spell a code column three ways -- a ``code`` suffix
    (``Location_HUCEightDigitCode``), the RDB abbreviation ``_cd``
    (``huc_cd``), or a code vocabulary in the name (``identifier``, ``huc``,
    ``fips``). All three qualify. Their leading zeros carry meaning, so the
    column must be read as ``str``.

    A name ending in ``count`` is a tally rather than a code, even when it
    reads like one: ``AlternateLocation_IdentifierCount`` counts a location's
    alternate identifiers.
    """
    lname = name.lower()
    if lname.endswith("count"):
        return False
    return (
        lname.endswith("code")
        or lname.endswith("_cd")
        or any(token in lname for token in ("identifier", "huc", "fips"))
    )


def code_columns(names: Iterable[object]) -> dict[str, type]:
    """Map each code or identifier name in ``names`` to ``str``.

    Returns the ``dtype`` map for a caller that already has the column names
    and parses the body itself, such as :func:`dataretrieval.rdb.read_rdb`.
    """
    return {str(name): str for name in names if _is_code_column(str(name))}


def read_code_csv(text: str) -> DataFrame:
    """Read CSV text with code and identifier columns as strings.

    Read the header first to select those columns before numeric inference
    discards their leading zeros (``"00060"`` -> ``60``). Other columns keep
    pandas' inferred types, and missing-value handling is unchanged.

    ``low_memory=False`` types each column from the whole body rather than per
    chunk. It adds parse time on a large response and keeps a column's dtype
    independent of where the chunk boundaries fall.
    """
    columns = pd.read_csv(StringIO(text), delimiter=",", nrows=0).columns
    return pd.read_csv(
        StringIO(text),
        delimiter=",",
        low_memory=False,
        dtype=code_columns(columns),
    )
