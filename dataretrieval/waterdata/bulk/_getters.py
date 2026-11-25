"""``bulk.get_daily`` and ``bulk.get_continuous``: every query from the files."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

import pandas as pd

from dataretrieval.waterdata.bulk._retrieval import parse, read
from dataretrieval.waterdata.utils import _get_args

if TYPE_CHECKING:
    from dataretrieval._response_metadata import BaseMetadata

__all__ = ["get_continuous", "get_daily"]


def get_daily(
    monitoring_location_id: str | Iterable[str],
    *,
    parameter_code: str | Iterable[str] | None = None,
    statistic_id: str | Iterable[str] | None = None,
    time_series_id: str | Iterable[str] | None = None,
    approval_status: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    time: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get daily values from the published files, and the changes since.

    Reads each monitoring location's history from the Parquet file USGS
    writes every night, and asks the API only for the records changed since
    that file was written, and for the whole query at a location without a
    file. Returns the columns and types of
    :func:`dataretrieval.waterdata.get_daily`, less ``daily_id``, which the
    files do not hold; ``(time_series_id, time)`` identifies a record. A
    large pull finishes sooner this way than by paging the API (ADR 0014).
    For a few locations over a short ``time``,
    :func:`dataretrieval.waterdata.get_daily` is faster.

    A record deleted from the API since the files were written is still
    returned until the files are next written, usually the next night. A point
    comes from the STAC catalog and agrees with the API's to 9 decimal places.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings
        One or more identifiers in ``AGENCY-ID`` form (e.g.
        ``"USGS-02238500"``). The files are published per location, so at
        least one is required.
    parameter_code : string or iterable of strings, optional
        5-digit codes of the constituents measured (e.g. ``"00060"``,
        discharge). See
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or iterable of strings, optional
        Codes of the statistic each value represents, such as ``"00001"``
        (maximum), ``"00002"`` (minimum), and ``"00003"`` (mean).
    time_series_id : string or iterable of strings, optional
        Identifiers of time series, the ``id`` field of
        :func:`dataretrieval.waterdata.get_time_series_metadata`.
    approval_status : string or iterable of strings, optional
        ``"Approved"`` or ``"Provisional"``. Applied after the changes are
        merged, because a record can be approved after its file was written.
    unit_of_measure : string or iterable of strings, optional
        Units of measurement, as the API spells them (e.g. ``"ft^3/s"``).
    time : string or list of two, optional
        The dates to return: a single date, an interval (``"start/end"``,
        optionally half-bounded with ``..``), or a list of two values with
        ``None`` for an open end, such as ``["2000-01-01", None]``. A naive
        value is UTC. ISO 8601 durations (``"P7D"``) are not supported.
        Omitted, every record is returned.
    skip_geometry : boolean, optional
        If True, return a ``pandas.DataFrame`` without the point of each
        record.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        The records, sorted as :func:`dataretrieval.waterdata.get_daily`
        sorts them.
    md : :obj:`dataretrieval.utils.BaseMetadata`
        ``md.url`` is the URL :func:`dataretrieval.waterdata.get_daily` sends
        for the same arguments, ``md.query_time`` the sum of every request's
        time, and ``md.header`` the headers reporting the lowest remaining
        quota.

    Raises
    ------
    ImportError
        Without pyarrow, which reads the files:
        ``pip install "dataretrieval[pyarrow]"``.
    ValueError
        For no ``monitoring_location_id``, or an ISO 8601 duration in
        ``time``.
    DataRetrievalError
        The typed subclass for an HTTP error response from the catalog, a
        file, or the API.
    FanOutInterrupted
        A transient failure (429 / 5xx / timeout) was not resolved by the
        built-in retries. ``exc.call.resume()`` returns the whole result.

    Examples
    --------
    .. code::

        >>> # Daily discharge since 1990 for every stream gage in Ohio
        >>> sites, _ = dataretrieval.waterdata.get_monitoring_locations(
        ...     state="Ohio", site_type="Stream"
        ... )
        >>> df, md = dataretrieval.waterdata.bulk.get_daily(
        ...     sites["monitoring_location_id"].tolist(),
        ...     parameter_code="00060",
        ...     time="1990-01-01/..",
        ... )
    """
    return read(parse("daily", _get_args(locals())))


def get_continuous(
    monitoring_location_id: str | Iterable[str],
    *,
    parameter_code: str | Iterable[str] | None = None,
    statistic_id: str | Iterable[str] | None = None,
    time_series_id: str | Iterable[str] | None = None,
    approval_status: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    method_category: str | Iterable[str] | None = None,
    time: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get continuous values from the published files, and the changes since.

    Reads each monitoring location's history from the two Parquet files USGS
    publishes for it: ``historical``, which ends about 150 days before it was
    written, and ``current``, written every night. Asks the API only for the
    records changed since, and for the whole query at a location without a
    file. Returns the columns and types of
    :func:`dataretrieval.waterdata.get_continuous`, less ``continuous_id``,
    which the files do not hold; ``(time_series_id, time)`` identifies a
    record. ``historical`` is not downloaded when ``time`` starts within the
    last 120 days, and ``time`` may span more than the 1,100 days the API
    accepts, or have an open start.

    A large pull finishes sooner this way than by paging the API (ADR 0014).
    For one location over a few days,
    :func:`dataretrieval.waterdata.get_continuous` is faster.

    A record deleted from the API since the files were written is still
    returned until the files are next written, usually the next night. A point
    comes from the STAC catalog and agrees with the API's to 9 decimal places.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings
        One or more identifiers in ``AGENCY-ID`` form (e.g.
        ``"USGS-02238500"``). The files are published per location, so at
        least one is required.
    parameter_code : string or iterable of strings, optional
        5-digit codes of the constituents measured (e.g. ``"00060"``,
        discharge). See
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    statistic_id : string or iterable of strings, optional
        Codes of the statistic each value represents. Continuous data are
        nearly always ``"00011"``.
    time_series_id : string or iterable of strings, optional
        Identifiers of time series, the ``id`` field of
        :func:`dataretrieval.waterdata.get_time_series_metadata`.
    approval_status : string or iterable of strings, optional
        ``"Approved"`` or ``"Provisional"``. Applied after the changes are
        merged, because a record can be approved after its file was written.
    unit_of_measure : string or iterable of strings, optional
        Units of measurement, as the API spells them (e.g. ``"ft^3/s"``).
    method_category : string or iterable of strings, optional
        The RLMS method category code for the method in effect over each
        value's interval: ``"STNRD"``, ``"LMTUS"``, ``"EXPER"``, or
        ``"UNKWN"``. Applied after the changes are merged, like
        ``approval_status``.
    time : string or list of two, optional
        The times to return: a single instant, an interval (``"start/end"``,
        optionally half-bounded with ``..``), or a list of two values with
        ``None`` for an open end, such as ``["2025-07-01", None]``. A naive
        value is UTC. ISO 8601 durations (``"P7D"``) are not supported.
        Omitted, every record is returned.
    skip_geometry : boolean, optional
        If True, return a ``pandas.DataFrame`` without the point of each
        record.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        The records, sorted as :func:`dataretrieval.waterdata.get_continuous`
        sorts them.
    md : :obj:`dataretrieval.utils.BaseMetadata`
        ``md.url`` is the URL :func:`dataretrieval.waterdata.get_continuous`
        sends for the same arguments, ``md.query_time`` the sum of every
        request's time, and ``md.header`` the headers reporting the lowest
        remaining quota.

    Raises
    ------
    ImportError
        Without pyarrow, which reads the files:
        ``pip install "dataretrieval[pyarrow]"``.
    ValueError
        For no ``monitoring_location_id``, or an ISO 8601 duration in
        ``time``.
    DataRetrievalError
        The typed subclass for an HTTP error response from the catalog, a
        file, or the API.
    FanOutInterrupted
        A transient failure (429 / 5xx / timeout) was not resolved by the
        built-in retries. ``exc.call.resume()`` returns the whole result.

    Examples
    --------
    .. code::

        >>> # Every discharge value since July 2025 at two gages
        >>> df, md = dataretrieval.waterdata.bulk.get_continuous(
        ...     ["USGS-01646500", "USGS-01638500"],
        ...     parameter_code="00060",
        ...     time="2025-07-01/..",
        ... )
    """
    return read(parse("continuous", _get_args(locals())))
