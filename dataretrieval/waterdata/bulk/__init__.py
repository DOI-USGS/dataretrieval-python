"""Bulk retrieval of the Water Data ``daily`` and ``continuous`` collections.

USGS publishes both collections a second way: as Parquet files, one set per
monitoring location, written every night and indexed by the Water Data STAC
catalog (https://waterdata.usgs.gov/blog/api-faster-access/).
:func:`get_daily` and :func:`get_continuous` read a query's history from
those files and only the records changed since from the API, then return
one frame with the columns and types of
:func:`dataretrieval.waterdata.get_daily` and
:func:`dataretrieval.waterdata.get_continuous`, less the record id. Their
arguments are the ones the files can answer. :func:`get_urls` lists the
files for a Parquet reader of your own.

Reading the files needs pyarrow: ``pip install "dataretrieval[pyarrow]"``.
The design is recorded in ADR 0014.
"""

from __future__ import annotations

from dataretrieval.waterdata.bulk._getters import get_continuous, get_daily
from dataretrieval.waterdata.bulk._urls import get_urls

__all__ = ["get_continuous", "get_daily", "get_urls"]
