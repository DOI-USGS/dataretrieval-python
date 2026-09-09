"""Contract of the shared code-preserving CSV reader.

Component layer: exercises ``dataretrieval._csv`` directly, with no service and no
HTTP. The adapters' own tests (``wqp_test.py``, ``waterdata_test.py``,
``nwdc_test.py``, ``rdb_test.py``) cover how each getter uses it.
"""

import pandas as pd
import pytest

from dataretrieval._csv import _is_code_column, code_columns, read_code_csv


@pytest.mark.parametrize(
    "name",
    [
        "USGSpcode",
        "Location_HUCEightDigitCode",
        "Activity_TypeCode",
        "stateFips",
        "countyFips",
        "MonitoringLocationIdentifier",
        "huc12_id",
        "huc_cd",
        "parameter_cd",
    ],
)
def test_is_code_column_flags_codes_and_identifiers(name):
    """A code suffix, the RDB ``_cd`` abbreviation, or a code vocabulary qualifies."""
    assert _is_code_column(name)


@pytest.mark.parametrize(
    "name",
    [
        "ResultMeasureValue",
        "resultCount",
        "Activity_StartDate",
        "value",
        "year_month",
        "dec_lat_va",
        # A tally, though its name reads as an identifier.
        "AlternateLocation_IdentifierCount",
    ],
)
def test_is_code_column_passes_over_values(name):
    """Measurement, count, and date names are left to pandas' inference."""
    assert not _is_code_column(name)


def test_read_code_csv_preserves_leading_zeros():
    """Codes keep their significant zeros; value columns stay numeric.

    A bare ``read_csv`` infers code columns as int/float and drops the zeros
    without warning (``"00060"`` -> ``60``, HUC8 ``"07090002"`` -> ``7090002``).
    """
    csv = (
        "Location_HUCEightDigitCode,USGSpcode,ResultMeasureValue\n07090002,00060,1.5\n"
    )

    df = read_code_csv(csv)

    assert df["Location_HUCEightDigitCode"].iloc[0] == "07090002"
    assert df["USGSpcode"].iloc[0] == "00060"
    assert df["ResultMeasureValue"].iloc[0] == 1.5


def test_read_code_csv_infers_counts():
    """A count stays numeric even though its name ends in ``Identifier`` + ``Count``."""
    csv = "AlternateLocation_IdentifierCount,USGSpcode\n0,00060\n"

    df = read_code_csv(csv)

    assert pd.api.types.is_numeric_dtype(df["AlternateLocation_IdentifierCount"])
    assert df["AlternateLocation_IdentifierCount"].iloc[0] == 0
    assert df["USGSpcode"].iloc[0] == "00060"


def test_code_columns_selects_by_name():
    """The dtype map holds the code columns only, for a caller that parses itself."""
    names = ["huc_cd", "site_no", "resultCount", "USGSpcode"]

    assert code_columns(names) == {"huc_cd": str, "USGSpcode": str}
