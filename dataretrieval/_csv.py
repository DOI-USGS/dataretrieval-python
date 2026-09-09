"""Shared CSV parsing that preserves significant zeros in codes and identifiers."""

from collections.abc import Collection
from io import StringIO

import pandas as pd
from pandas import DataFrame


def _is_code_column(name: str) -> bool:
    """Report whether a column name denotes a code or identifier.

    Such columns (HUCs, parameter codes, FIPS codes) have leading zeros that
    are significant and must be preserved as ``str``. A name qualifies if it
    ends with "code" or contains "identifier", "huc", or "fips".
    """
    lname = name.lower()
    return lname.endswith("code") or any(
        token in lname for token in ("identifier", "huc", "fips")
    )


def read_code_csv(text: str, *, infer_columns: Collection[str] = ()) -> DataFrame:
    """Read CSV text with code/identifier columns as strings.

    Read the header first to select string columns before numeric inference can
    discard leading zeros (``"00060"`` -> ``60``). Other columns retain pandas'
    inferred types, and the default missing-value handling is unchanged.
    ``infer_columns`` also retain inference when their names match a code or
    identifier, allowing a caller to distinguish counts from identifiers.
    """
    columns = pd.read_csv(StringIO(text), delimiter=",", nrows=0).columns
    str_cols = {
        col: str for col in columns if col not in infer_columns and _is_code_column(col)
    }
    return pd.read_csv(StringIO(text), delimiter=",", low_memory=False, dtype=str_cols)
