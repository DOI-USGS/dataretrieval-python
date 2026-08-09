"""Useful utilities for data munging."""

from __future__ import annotations

import numbers
import warnings
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Generic, TypeVar

import httpx
import pandas as pd

import dataretrieval.credentials as _credentials
import dataretrieval.transport.http as _transport_http
from dataretrieval.codes import tz
from dataretrieval.exceptions import (
    NoSitesError,
    URLTooLong,
    error_for_status,
)
from dataretrieval.response_metadata import (
    BaseMetadata,  # noqa: F401  — compatibility re-export; defined there now
)
from dataretrieval.transport.retry import (
    _GATEWAY_STATUSES,
    RetryPolicy,
    parse_retry_after,
    retry_sync,
)

# Compatibility names retained at their historical utility paths.
_AUTHORIZED_API_KEY_HOST = _credentials._AUTHORIZED_API_KEY_HOST
HTTPX_ASYNC_DEFAULTS = _transport_http.HTTPX_ASYNC_DEFAULTS
HTTPX_DEFAULTS = _transport_http.HTTPX_DEFAULTS
USER_AGENT = _transport_http.USER_AGENT
_default_headers = _transport_http.default_headers
_get = _transport_http.get
_network_error = _transport_http.network_error
_strip_api_key_from_untrusted_host = _transport_http.strip_api_key_from_untrusted_host
_strip_api_key_from_untrusted_host_async = (
    _transport_http.strip_api_key_from_untrusted_host_async
)

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


def _require_positive_int(
    value: int, name: str, *, examples: str | None = None
) -> None:
    """Validate that ``value`` is a positive integer, else raise ``ValueError``.

    Accepts any :class:`numbers.Integral` (so a numpy/pandas integer passes,
    not only ``int``) but rejects ``bool`` — an ``Integral`` subtype that is
    nonsensical as a count. A non-integer (float, str, ``None``) or a value
    ``< 1`` raises before any I/O, rather than crashing later (e.g. deep in
    ``pd.DataFrame.head``). Shared by the user-facing count knobs ``max_rows``
    and ``parallel_chunks(n)`` so their boundary validation can't drift.
    (``ChunkPlan.max_chunks`` is an internal, already-``int`` precondition with
    its own domain-specific message, so it keeps a lighter ``< 1`` guard rather
    than routing through here.)

    Parameters
    ----------
    value : int
        The value to check. Typed ``int`` for callers, but validated at
        runtime because the real value may be anything the user passed.
    name : str
        Parameter name, used as the subject of the error message.
    examples : str, optional
        Illustrative values appended to the message (e.g. ``"2, 8, 32"``).
    """
    if not isinstance(value, numbers.Integral) or isinstance(value, bool) or value < 1:
        eg = f", e.g. {examples}" if examples else ""
        raise ValueError(f"{name} must be a positive integer{eg} (got {value!r}).")


def to_str(listlike: object, delimiter: str = ",") -> str | None:
    """Translate a list-like object into a delimited string.

    Parameters
    ----------
    listlike: list-like object
        A list, or a list-like object
        (e.g. ``pandas.core.series.Series``).
    delimiter: string, optional
        String placed between entries of ``listlike`` when it is turned into a
        string. Default value is a comma.

    Returns
    -------
    listlike: string
        The listlike object as a string separated by the delimiter.

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
    """Create a datetime field from separate date, time, and time zone fields.

    Assumes ISO 8601.

    Parameters
    ----------
    df: ``pandas.DataFrame``
        A data frame containing date, time, and timezone fields.
    date_field: string
        Name of the date column in ``df``.
    time_field: string
        Name of the time column in ``df``.
    tz_field: string
        Name of the time zone column in ``df``.

    Returns
    -------
    df: ``pandas.DataFrame``
        The data frame with a formatted 'datetime' column.

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
    """Append a UTC ``<prefix>DateTime`` column per Date/Time/TimeZone triplet.

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


def _raise_for_status(
    response: httpx.Response,
    *,
    detail_from: Callable[[httpx.Response], str | None] | None = None,
) -> None:
    """Raise the typed :class:`DataRetrievalError` for an HTTP error response.

    A success status returns ``None``. Shared by the legacy :func:`query` path
    (and ``streamstats`` / ``wateruse``). Delegates the status-to-type mapping to
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
    raise error_for_status(
        status,
        message,
        retry_after=parse_retry_after(response.headers.get("Retry-After")),
    )


def _single_request_policy() -> RetryPolicy:
    """Retry policy for the one-shot adapters (WQP, NLDI, StreamStats).

    These services answer a rejected query with a 500, so only the gateway
    statuses are worth re-sending; the Water Data chunker keeps the broader
    default, where a 5xx is an upstream hiccup worth riding out.
    """
    return RetryPolicy.from_env(retryable_statuses=_GATEWAY_STATUSES)


def _get_with_retry(
    url: str | httpx.URL,
    *,
    detail_from: Callable[[httpx.Response], str | None] | None = None,
    retry_policy: RetryPolicy | None = None,
    **kwargs: Any,
) -> httpx.Response:
    """GET with status mapping and bounded retry on typed transients."""

    def attempt() -> httpx.Response:
        response = _get(url, **kwargs)
        _raise_for_status(response, detail_from=detail_from)
        return response

    try:
        return retry_sync(
            attempt,
            _single_request_policy() if retry_policy is None else retry_policy,
        )
    except httpx.InvalidURL as exc:
        raise _url_too_long_error(f"httpx rejected the URL client-side: {exc}") from exc


def _query_impl(
    url: str,
    payload: dict[str, Any],
    delimiter: str = ",",
    ssl_check: bool = True,
    *,
    retry_policy: RetryPolicy,
) -> httpx.Response:
    """Send a query.

    Wrapper for ``httpx.get`` that handles errors, converts listed query
    parameters to comma-separated strings, and returns the response.

    Parameters
    ----------
    url: string
        URL to query.
    payload: dict
        Query parameters passed to ``httpx.get``.
    delimiter: string
        Delimiter to use with lists.
    ssl_check: bool
        Whether to check SSL certificates. Default is True.

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

    for key, value in payload.items():
        payload[key] = to_str(value, delimiter)
    # httpx serializes None params as ``foo=``; USGS rejects with 400.
    # Drop them. (``to_str`` returns None for non-iterable scalars like bools.)
    payload = {k: v for k, v in payload.items() if v is not None}

    user_agent = {"user-agent": USER_AGENT}

    response = _get_with_retry(
        url,
        params=payload,
        headers=user_agent,
        verify=ssl_check,
        retry_policy=retry_policy,
        **HTTPX_DEFAULTS,
    )

    # USGS waterservices signals an empty result with a 200 whose body starts
    # "No sites/data ..." (its legacy wording); surface it as NoSitesError.
    if response.text.startswith("No sites/data"):
        raise NoSitesError(response.url)

    return response


def query(
    url: str,
    payload: dict[str, Any],
    delimiter: str = ",",
    ssl_check: bool = True,
) -> httpx.Response:
    return _query_impl(
        url,
        payload,
        delimiter,
        ssl_check,
        retry_policy=RetryPolicy(max_retries=0),
    )


query.__doc__ = _query_impl.__doc__


def _query_with_retry(
    url: str,
    payload: dict[str, Any],
    delimiter: str = ",",
    ssl_check: bool = True,
) -> httpx.Response:
    """Active-service form of :func:`query` with bounded transient retry."""
    return _query_impl(
        url,
        payload,
        delimiter,
        ssl_check,
        retry_policy=_single_request_policy(),
    )
