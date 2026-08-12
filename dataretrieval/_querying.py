"""The one-shot HTTP query path behind the legacy service adapters.

"Compose a USGS query URL, send it, map the status, retry a transient" -- the
half of the old ``utils`` module that talks to the network, as used by ``nwis``,
``wqp``, ``nldi``, ``streamstats`` and ``nwdc``. Its other half (pandas
column munging) shared nothing with this but a filename: no caller wanted both,
and the two have disjoint dependencies -- this one needs ``exceptions`` and
``transport``, that one needs ``codes`` and pandas.

The module is private because the *names* are not: ``query`` and ``to_str`` keep
their documented ``dataretrieval.utils`` path, the way ``Ambient`` and
``BaseMetadata`` do from their own implementation leaves. This is legacy
machinery for the deprecated single-request adapters; new service code belongs
on the chunked transport instead.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

import httpx

from dataretrieval.exceptions import (
    NoSitesError,
    URLTooLong,
    error_for_status,
    parse_retry_after,
)
from dataretrieval.transport.http import HTTPX_DEFAULTS, USER_AGENT
from dataretrieval.transport.http import get as _get
from dataretrieval.transport.retry import (
    _GATEWAY_STATUSES,
    RetryPolicy,
    retry_sync,
)

__all__ = ["query", "to_str"]


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
    (and ``streamstats`` / ``nwdc``). Delegates the status-to-type mapping to
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


def _single_request_policy(adapter: str | None = None) -> RetryPolicy:
    """Retry policy for the one-shot adapters (WQP, NLDI, StreamStats).

    These services answer a rejected query with a 500, so only the gateway
    statuses are worth re-sending; the Water Data chunker keeps the broader
    default, where a 5xx is an upstream hiccup worth riding out.

    ``adapter`` names which settings table supplies ``retries`` and
    ``stall_timeout`` -- these three services share a retry *shape* but not
    a settings scope.
    """
    return RetryPolicy.from_settings(
        retryable_statuses=_GATEWAY_STATUSES, adapter=adapter
    )


def _get_with_retry(
    url: str | httpx.URL,
    *,
    detail_from: Callable[[httpx.Response], str | None] | None = None,
    retry_policy: RetryPolicy | None = None,
    adapter: str | None = None,
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
            _single_request_policy(adapter) if retry_policy is None else retry_policy,
        )
    except httpx.InvalidURL as exc:
        raise _url_too_long_error(f"httpx rejected the URL client-side: {exc}") from exc


def _query_with_retry(
    url: str,
    payload: dict[str, Any],
    delimiter: str = ",",
    ssl_check: bool = True,
    *,
    retry_policy: RetryPolicy | None = None,
    adapter: str | None = None,
) -> httpx.Response:
    """Send an active-service query with bounded transient retry by default."""

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
        adapter=adapter,
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
    return _query_with_retry(
        url,
        payload,
        delimiter,
        ssl_check,
        retry_policy=RetryPolicy(max_retries=0),
    )


# Preserve the documented function paths from the v1.2.0 utility API.
to_str.__module__ = "dataretrieval.utils"
query.__module__ = "dataretrieval.utils"
