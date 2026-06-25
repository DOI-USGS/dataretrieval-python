"""
Useful utilities for data munging.
"""

from __future__ import annotations

import os
import warnings
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Generic, TypeVar

import httpx
import pandas as pd

import dataretrieval
from dataretrieval.codes import tz
from dataretrieval.exceptions import (
    NetworkError,
    NoSitesError,
    URLTooLong,
    error_for_status,
)

# Typed as ``dict[str, Any]`` (not the inferred ``dict[str, object]``) so that
# splatting it as ``**HTTPX_DEFAULTS`` into ``httpx.get`` / ``httpx.AsyncClient``
# type-checks: the values are a heterogeneous bag of httpx keyword arguments.
HTTPX_DEFAULTS: dict[str, Any] = {
    "follow_redirects": True,
    "timeout": httpx.Timeout(60.0, connect=10.0),
}

_T = TypeVar("_T")


class Ambient(Generic[_T]):
    """A :class:`~contextvars.ContextVar` paired with a scoping contextmanager.

    Bundles the var and its set/reset-token dance into one object, so an ambient
    value needs a single declaration instead of a ``var`` + setter-function pair.
    Read the current value with :meth:`get`; set it for a ``with`` block by
    *calling* the instance — the previous value is restored on exit (and can't
    leak into a later call the way a hand-written ``try/finally`` can when its
    ``reset`` is dropped)::

        _base_url = Ambient("ogc_base_url", DEFAULT)
        with _base_url(other):  # scoped to the block
            _base_url.get()  # -> other
    """

    def __init__(self, name: str, default: _T) -> None:
        self._var: ContextVar[_T] = ContextVar(name, default=default)

    def get(self) -> _T:
        """The current value — the default outside any active scope."""
        return self._var.get()

    @contextmanager
    def __call__(self, value: _T) -> Iterator[None]:
        """Set the value for the duration of the ``with`` block."""
        token = self._var.set(value)
        try:
            yield
        finally:
            self._var.reset(token)


def _default_headers() -> dict[str, str]:
    """Build the default HTTP headers for a USGS web-API request.

    Always sets a descriptive ``User-Agent`` plus ``Accept`` /
    ``Accept-Encoding`` and ``lang``. If the ``API_USGS_PAT`` environment
    variable is set, its value is added as the ``X-Api-Key`` header — a USGS
    personal access token raises the request rate limit.

    Shared by the OGC engine (:mod:`dataretrieval.ogc`), the Water Data getters
    (:mod:`dataretrieval.waterdata`), and :mod:`dataretrieval.wateruse`, so the
    request identity is consistent across every USGS API the package talks to.

    Returns
    -------
    dict[str, str]
        Headers suitable for an ``httpx`` request against a USGS API.
    """
    headers = {
        "Accept-Encoding": "compress, gzip",
        "Accept": "application/json",
        "User-Agent": f"python-dataretrieval/{dataretrieval.__version__}",
        "lang": "en-US",
    }
    token = os.getenv("API_USGS_PAT")
    if token:
        headers["X-Api-Key"] = token
    return headers


def to_str(listlike: object, delimiter: str = ",") -> str | None:
    """Translates list-like objects into strings.

    Parameters
    ----------
    listlike: list-like object
        An object that is a list, or list-like
        (e.g., ``pandas.core.series.Series``)
    delimiter: string, optional
        The delimiter that is placed between entries in listlike when it is
        turned into a string. Default value is a comma.

    Returns
    -------
    listlike: string
        The listlike object as string separated by the delimiter

    Examples
    --------
    .. doctest::

        >>> dataretrieval.utils.to_str([1, "a", 2])
        '1,a,2'

        >>> dataretrieval.utils.to_str([0, 10, 42], delimiter="+")
        '0+10+42'

    """
    if isinstance(listlike, str):
        return listlike

    if isinstance(listlike, Iterable):
        return delimiter.join(map(str, listlike))

    return None


def format_datetime(
    df: pd.DataFrame, date_field: str, time_field: str, tz_field: str
) -> pd.DataFrame:
    """Creates a datetime field from separate date, time, and
    time zone fields.

    Assumes ISO 8601.

    Parameters
    ----------
    df: ``pandas.DataFrame``
        A data frame containing date, time, and timezone fields.
    date_field: string
        Name of date column in df.
    time_field: string
        Name of time column in df.
    tz_field: string
        Name of time zone column in df.

    Returns
    -------
    df: ``pandas.DataFrame``
        The data frame with a formatted 'datetime' column

    """
    # create a datetime index from the columns in qwdata response
    df[tz_field] = df[tz_field].map(tz)

    df["datetime"] = pd.to_datetime(
        df[date_field] + " " + df[time_field] + " " + df[tz_field],
        format="mixed",
        utc=True,
    )

    # if there are any incomplete dates, warn the user
    if df["datetime"].isna().any():
        count = df["datetime"].isna().sum()
        warnings.warn(
            f"Warning: {count} incomplete dates found, "
            + "consider setting datetime_index to False.",
            UserWarning,
            stacklevel=2,
        )

    return df


# (time-suffix, tz-suffix) pairs that follow a "<prefix>Date" column.
_TIME_TZ_SUFFIXES = (
    # WQX3 / Samples, e.g.
    #   Activity_StartDate / Activity_StartTime / Activity_StartTimeZone
    ("Time", "TimeZone"),
    # Legacy WQP (slash-separated), e.g.
    #   ActivityStartDate / ActivityStartTime/Time / ActivityStartTime/TimeZoneCode
    ("Time/Time", "Time/TimeZoneCode"),
)


def _build_utc_datetime(
    date_series: pd.Series, time_series: pd.Series, tz_series: pd.Series
) -> pd.Series:
    """Combine date + time + tz-abbreviation columns into a UTC pandas Series.

    Unknown timezone codes (and rows missing any of the three values) yield
    ``NaT``. The input columns are not mutated.
    """
    offsets = tz_series.map(tz)
    combined = (
        date_series.astype("string")
        + " "
        + time_series.astype("string")
        + " "
        + offsets.astype("string")
    )
    return pd.to_datetime(
        combined, format="%Y-%m-%d %H:%M:%S %z", utc=True, errors="coerce"
    )


def _attach_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``<prefix>DateTime`` UTC columns for any Date/Time/TimeZone triplets
    and sort the frame by the activity-start datetime.

    Detects two naming patterns that appear in USGS Samples and Water Quality
    Portal CSV responses:

    * **WQX3** — ``<prefix>Date``, ``<prefix>Time``, ``<prefix>TimeZone``
    * **Legacy WQP** — ``<prefix>Date``, ``<prefix>Time/Time``,
      ``<prefix>Time/TimeZoneCode``

    For every triplet present, a new ``<prefix>DateTime`` column is appended
    holding a UTC ``Timestamp`` (offsets resolved via
    :data:`dataretrieval.codes.tz`). The original Date/Time/TimeZone columns
    are left intact, and an existing ``<prefix>DateTime`` column is never
    overwritten.

    Rows are sorted (and the index reset) by the canonical activity-start
    datetime when present — ``Activity_StartDateTime`` (WQX3) or
    ``ActivityStartDateTime`` (legacy WQP) — falling back to the first
    detected ``*Date`` column. Mirrors R ``dataRetrieval``'s
    end-of-pipeline sort in ``importWQP.R``.

    Parameters
    ----------
    df : ``pandas.DataFrame``
        DataFrame returned from a Samples or WQP CSV endpoint.

    Returns
    -------
    df : ``pandas.DataFrame``
        A new DataFrame with derivable ``<prefix>DateTime`` columns appended
        and rows sorted by the activity-start datetime (if any date column
        was detected).
    """
    columns = set(df.columns)
    new_columns = {}
    first_date_col = None
    for col in df.columns:
        if not col.endswith("Date"):
            continue
        if first_date_col is None:
            first_date_col = col
        prefix = col.removesuffix("Date")
        target = prefix + "DateTime"
        if target in columns or target in new_columns:
            continue
        for time_suffix, tz_suffix in _TIME_TZ_SUFFIXES:
            time_col = prefix + time_suffix
            tz_col = prefix + tz_suffix
            if time_col in columns and tz_col in columns:
                new_columns[target] = _build_utc_datetime(
                    df[col], df[time_col], df[tz_col]
                )
                break
    if new_columns:
        # Concat in one shot — per-column assignment on a wide CSV-derived
        # frame triggers pandas' fragmentation PerformanceWarning.
        df = pd.concat([df, pd.DataFrame(new_columns, index=df.index)], axis=1)
    sort_key: str | None
    if "Activity_StartDateTime" in df.columns:
        sort_key = "Activity_StartDateTime"
    elif "ActivityStartDateTime" in df.columns:
        sort_key = "ActivityStartDateTime"
    else:
        sort_key = first_date_col
    if sort_key is not None:
        df = df.sort_values(by=sort_key, ignore_index=True)
    return df


class BaseMetadata:
    """Base class for metadata.

    Attributes
    ----------
    url : str
        Response url
    query_time: datetime.timedelta
        Response elapsed time
    header: httpx.Headers
        Response headers

    """

    def __init__(self, response: httpx.Response) -> None:
        """Generates a standard set of metadata informed by the response.

        Parameters
        ----------
        response: ``httpx.Response``
            Response object from the ``httpx`` module.

        """

        # Coerce httpx.URL -> str: BaseMetadata.url has always been str.
        self.url = str(response.url)
        self.query_time = response.elapsed
        self.header = response.headers
        self.comment: str | None = None

        # # not sure what statistic_info is
        # self.statistic_info = None

        # # disclaimer seems to be only part of importWaterML1
        # self.disclaimer = None

    # ``site_info`` is set by ``nwis`` / ``wqp``-specific metadata classes; the
    # modern ``waterdata`` metadata leaves it unimplemented (use
    # ``waterdata.get_monitoring_locations`` to retrieve site descriptions).
    @property
    def site_info(self) -> Any:
        raise NotImplementedError(
            "site_info must be implemented by utils.BaseMetadata children"
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(url={self.url})"


_URL_TOO_LONG_EXAMPLE = """
                    # n is the number of chunks to divide the query into \n
                    split_list = np.array_split(site_list, n)
                    data_list = []  # list to store chunk results in \n
                    # loop through chunks and make requests \n
                    for site_list in split_list: \n
                        data = nwis.get_record(sites=site_list, service='dv', \n
                                               start=start, end=end) \n
                        data_list.append(data)  # append results to list"""


def _url_too_long_error(detail: str) -> URLTooLong:
    return URLTooLong(
        "Request URL too long. Modify your query to use fewer sites. "
        f"{detail}. Pseudo-code example of how to split your query: "
        f"\n {_URL_TOO_LONG_EXAMPLE}"
    )


def _network_error(url: str | httpx.URL, exc: httpx.TransportError) -> NetworkError:
    """Build the :class:`~dataretrieval.exceptions.NetworkError` for a failed
    round-trip ``exc`` (no HTTP response: timeout, DNS, refused connection)."""
    # Some httpx transport errors stringify empty (e.g. ``ConnectTimeout()``);
    # fall back to the class name so the message is always informative.
    detail = str(exc) or type(exc).__name__
    return NetworkError(f"Could not reach the service at {url}: {detail}")


def _get(url: str | httpx.URL, **kwargs: Any) -> httpx.Response:
    """``httpx.get`` for the single-shot paths, surfacing a transport failure as
    a typed :class:`~dataretrieval.exceptions.NetworkError` (the chunker wraps its
    own as resumable interruptions, so it stays off this wrapper)."""
    try:
        return httpx.get(url, **kwargs)
    except httpx.TransportError as exc:
        raise _network_error(url, exc) from exc


def _raise_for_status(
    response: httpx.Response,
    *,
    detail_from: Callable[[httpx.Response], str | None] | None = None,
) -> None:
    """Raise the typed :class:`DataRetrievalError` for an HTTP error response;
    return ``None`` on success.

    Shared by the legacy :func:`query` path (and ``streamstats`` /
    ``wateruse``). Delegates the status-to-type mapping to
    :func:`dataretrieval.exceptions.error_for_status`, except a too-long-URL
    status (413 / 414): that gets the same actionable "split your query"
    remediation as the client-side over-long-URL case below, rather than a bare
    ``HTTP 414`` (both still raise :class:`~dataretrieval.exceptions.URLTooLong`).

    ``detail_from``, when given, is called *only on an error response* to pull an
    API-specific detail string (e.g. a JSON error envelope's message) out of the
    body; a truthy result is appended to the raised message. This lets callers
    surface their API's error wording without re-implementing the status-to-type
    mapping and message format.
    """
    status = response.status_code
    if status < 400:
        return
    if status in (413, 414):
        raise _url_too_long_error(f"API response reason: {response.reason_phrase}")
    message = f"HTTP {status} {response.reason_phrase}".rstrip()
    detail = detail_from(response) if detail_from is not None else None
    if detail:
        message += f": {detail}"
    message += f" (URL: {response.url})"
    raise error_for_status(status, message)


def query(
    url: str,
    payload: dict[str, Any],
    delimiter: str = ",",
    ssl_check: bool = True,
) -> httpx.Response:
    """Send a query.

    Wrapper for httpx.get that handles errors, converts listed
    query parameters to comma separated strings, and returns response.

    Parameters
    ----------
    url: string
        URL to query
    payload: dict
        query parameters passed to ``httpx.get``. Not mutated.
    delimiter: string
        delimiter to use with lists
    ssl_check: bool
        If True, check SSL certificates, if False, do not check SSL,
        default is True

    Returns
    -------
    response: ``httpx.Response``
        The response from the API query ``httpx.get`` function call.

    Raises
    ------
    DataRetrievalError
        On an HTTP error response, the typed subclass for the status (see
        :func:`dataretrieval.exceptions.error_for_status` for the mapping); or
        :class:`~dataretrieval.exceptions.NoSitesError` when a 200 response
        reports no data matched; or :class:`~dataretrieval.exceptions.NetworkError`
        on a connection-level failure (timeout, DNS), with the underlying
        ``httpx`` exception on ``__cause__``.
    """

    # Build a fresh params dict; never mutate the caller's payload.
    params = {key: to_str(value, delimiter) for key, value in payload.items()}
    # httpx serializes None params as ``foo=``; USGS rejects with 400.
    # Drop them. (``to_str`` returns None for non-iterable scalars like bools.)
    params = {k: v for k, v in params.items() if v is not None}

    user_agent = {"user-agent": f"python-dataretrieval/{dataretrieval.__version__}"}

    try:
        response = _get(
            url,
            params=params,
            headers=user_agent,
            verify=ssl_check,
            **HTTPX_DEFAULTS,
        )
    except httpx.InvalidURL as exc:
        raise _url_too_long_error(f"httpx rejected the URL client-side: {exc}") from exc

    _raise_for_status(response)

    # USGS waterservices signals an empty result with a 200 whose body starts
    # "No sites/data ..." (its legacy wording); surface it as NoSitesError.
    if response.text.startswith("No sites/data"):
        raise NoSitesError(response.url)

    return response
