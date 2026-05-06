"""Parser for the USGS RDB tab-separated text format.

RDB (Relational DataBase) is the text format used by NWIS web services
and by the Water Data STAC catalog's rating-curve assets. Every RDB
file has the same shape:

- One or more ``#``-prefixed comment lines carrying provenance metadata
  (data source, retrieval timestamp, station name, parameter codes, etc.).
- A tab-separated header row naming each column.
- A second tab-separated row giving column format specs (e.g. ``5s 15s``);
  it is informational only and skipped during parsing.
- Tab-separated data rows.

This module exposes the parsing primitives that both ``dataretrieval.nwis``
and ``dataretrieval.waterdata.ratings`` use. Callers layer their own
post-processing (NWIS-specific datetime indexing, ratings-specific
``df.attrs`` provenance, etc.) on top of the raw frame.
"""

from __future__ import annotations

from io import StringIO

import pandas as pd


def read_rdb(text: str, dtypes: dict[str, type] | None = None) -> pd.DataFrame:
    """Parse an RDB text response into a ``pandas.DataFrame``.

    Parameters
    ----------
    text : str
        The RDB text response from a USGS web service.
    dtypes : dict[str, type] or None, optional
        Optional column-name to dtype hints, forwarded to
        ``pandas.read_csv``. Unknown column names are silently ignored, so
        callers may safely pass a dict of all columns they might be
        interested in.

    Returns
    -------
    pandas.DataFrame
        The parsed data. An RDB consisting only of comment lines (e.g. a
        "no sites found" response) returns an empty DataFrame rather than
        raising.

    Raises
    ------
    ValueError
        If the response body looks like HTML, which usually means the
        service has been moved, is degraded, or returned an error page.
    """
    if "<html>" in text.lower() or "<!doctype html>" in text.lower():
        raise ValueError(
            "Received HTML response instead of RDB. This often indicates "
            "that the service has been moved or is currently unavailable."
        )

    lines = text.splitlines()
    header_idx = next(
        (i for i, line in enumerate(lines) if not line.startswith("#")),
        len(lines),
    )
    if header_idx == len(lines):
        # All lines are comments — a legitimate empty result.
        return pd.DataFrame()

    fields = [f.replace(",", "").strip() for f in lines[header_idx].split("\t")]
    fields = [f for f in fields if f]

    return pd.read_csv(
        StringIO(text),
        delimiter="\t",
        skiprows=header_idx + 2,  # +1 for header, +1 for the format-spec row
        names=fields,
        na_values="NaN",
        dtype=dtypes,
    )


def extract_rdb_comment(text: str) -> list[str]:
    """Return the RDB ``#``-prefixed comment block, raw and in original order.

    Each entry includes its leading ``#`` and any whitespace, matching what
    R's ``dataRetrieval`` returns from ``comment(df)``. The comment block
    carries provenance metadata that is otherwise lost during parsing —
    data source, retrieval timestamp, parameter codes, rating id and
    last-shifted timestamp for ratings, etc.
    """
    return [line for line in text.splitlines() if line.startswith("#")]
