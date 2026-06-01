"""HTTP plumbing for the Water Data internals.

Default request headers, the USGS-API-aware error-body formatter, the
``Retry-After`` parser, the typed non-200 raiser, the queryables/schema probe,
and the mid-pagination failure-message builder. Depends on
:mod:`dataretrieval.waterdata.utils.constants` for the OGC URL and on
:mod:`dataretrieval.waterdata.chunking` only for the exception *types* it
raises (``RateLimited`` / ``ServiceUnavailable``); ``chunking`` does not import
back, so the arrow points one way.
"""

from __future__ import annotations

import os

import httpx

from dataretrieval import __version__
from dataretrieval.utils import HTTPX_DEFAULTS
from dataretrieval.waterdata.chunking import (
    RateLimited,
    ServiceUnavailable,
)
from dataretrieval.waterdata.utils.constants import OGC_API_URL


def _default_headers():
    """
    Generate default HTTP headers for API requests.

    Returns
    -------
    dict
        A dictionary containing default headers including 'Accept-Encoding',
        'Accept', 'User-Agent', and 'lang'. If the environment variable
        'API_USGS_PAT' is set, its value is included as the 'X-Api-Key' header.
    """
    headers = {
        "Accept-Encoding": "compress, gzip",
        "Accept": "application/json",
        "User-Agent": f"python-dataretrieval/{__version__}",
        "lang": "en-US",
    }
    token = os.getenv("API_USGS_PAT")
    if token:
        headers["X-Api-Key"] = token
    return headers


def _check_ogc_requests(endpoint: str = "daily", req_type: str = "queryables"):
    """
    Sends an HTTP GET request to the specified OGC endpoint and request type,
    returning the JSON response.

    Parameters
    ----------
    endpoint : str, optional
        The OGC collection endpoint to query (default is "daily").
    req_type : str, optional
        The type of request to make. Must be either "queryables" or "schema"
        (default is "queryables").

    Returns
    -------
    dict
        The JSON response from the OGC endpoint.

    Raises
    ------
    ValueError
        If req_type is not "queryables" or "schema".
    RateLimited, ServiceUnavailable, RuntimeError
        From :func:`_raise_for_non_200` on any non-200 — same typed
        contract as the main data path so callers can use one
        ``except`` clause everywhere.
    """
    if req_type not in ("queryables", "schema"):
        raise ValueError(f"req_type must be 'queryables' or 'schema', got {req_type!r}")
    url = f"{OGC_API_URL}/collections/{endpoint}/{req_type}"
    resp = httpx.get(url, headers=_default_headers(), **HTTPX_DEFAULTS)
    _raise_for_non_200(resp)
    return resp.json()


def _error_body(resp: httpx.Response):
    """
    Build an informative error message from an HTTP response.

    Parameters
    ----------
    resp : httpx.Response
        The HTTP response object to extract the error message from.

    Returns
    -------
    str
        An error message string assembled per status code:

        * **429** — predefined message describing the rate-limit and pointing
          at the API-token path; the response body is not consulted.
        * **403** — predefined message describing the most common cause
          (query exceeding server limits); the response body is not
          consulted.
        * **other statuses** — attempts ``resp.json()`` and renders
          ``"<status>: <code>. <description>."`` from the JSON error
          envelope. If the body is not JSON (e.g. an HTML 502 from a
          gateway), falls back to ``"<status>: <reason>. <snippet>"`` with
          the first 200 characters of ``resp.text``; an empty body
          degrades to ``"<status>: <reason>."``.
    """
    status = resp.status_code
    if status == 429:
        return (
            "429: Too many requests made. Please obtain an API token "
            "or try again later."
        )
    elif status == 403:
        return (
            "403: Query request denied. Possible reasons include "
            "query exceeding server limits."
        )
    try:
        j_txt = resp.json()
    except ValueError:
        snippet = (resp.text or "").strip()[:200]
        reason = resp.reason_phrase or "Error"
        if snippet:
            return f"{status}: {reason}. {snippet}"
        return f"{status}: {reason}."
    return (
        f"{status}: {j_txt.get('code', 'Unknown type')}. "
        f"{j_txt.get('description', 'No description provided')}."
    )


def _parse_retry_after(value: str | None) -> float | None:
    """
    Parse a USGS ``Retry-After`` header into seconds.

    Parameters
    ----------
    value : str or None
        The raw header value, or ``None`` if absent.

    Returns
    -------
    float or None
        Non-negative delta-seconds, clamped at zero. ``None`` when the
        header is absent or unparseable; ``ChunkedCall`` treats
        ``None`` as "fall back to my own retry policy".

    Notes
    -----
    USGS sends ``Retry-After`` as integer delta-seconds (empirically
    verified — e.g. ``Retry-After: 2619``). The HTTP spec also allows
    HTTP-date form, but USGS doesn't use it, so this function doesn't
    bother parsing it.
    """
    if not value:
        return None
    try:
        return max(0.0, float(value.strip()))
    except ValueError:
        return None


def _raise_for_non_200(resp: httpx.Response) -> None:
    """
    Raise a typed exception for any non-200 response.

    Routes through :func:`_error_body` (USGS-API-aware: handles
    429/403 specially, extracts ``code``/``description`` from JSON
    error bodies) rather than ``Response.raise_for_status``, which
    raises ``HTTPStatusError`` with a generic message.

    Parameters
    ----------
    resp : httpx.Response
        The HTTP response to inspect.

    Raises
    ------
    RateLimited
        On HTTP 429 — typed so ``ChunkedCall`` can wrap as a resumable
        :class:`~dataretrieval.waterdata.chunking.QuotaExhausted`.
    ServiceUnavailable
        On HTTP 5xx — typed so ``ChunkedCall`` can wrap as a resumable
        :class:`~dataretrieval.waterdata.chunking.ServiceInterrupted`.
    RuntimeError
        On any other non-200 (4xx other than 429) — these are
        programmer errors that retry won't fix.
    """
    status = resp.status_code
    if status == 200:
        return
    body = _error_body(resp)
    retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
    if status == 429:
        raise RateLimited(body, retry_after=retry_after)
    if 500 <= status < 600:
        raise ServiceUnavailable(body, retry_after=retry_after)
    raise RuntimeError(body)


def _paginated_failure_message(pages_collected: int, cause: BaseException) -> str:
    """
    Build a user-facing message for a mid-pagination failure.

    The API exposes no resume cursor, so the caller's only recovery is
    to retry the whole call — the message lists the practical knobs,
    tailored to whether the failure was rate-limit (429) or something
    else.

    Parameters
    ----------
    pages_collected : int
        Number of pages successfully fetched before the failure.
    cause : BaseException
        The underlying exception that interrupted pagination.

    Returns
    -------
    str
        A message suitable for the ``RuntimeError`` that
        ``_walk_pages`` and ``get_stats_data`` raise from the
        original exception.
    """
    cause_str = str(cause).removesuffix(".")
    # Some ``httpx`` exceptions (e.g. ``TimeoutException()`` with no args)
    # stringify to empty; fall back to the class name so the
    # returned message is always informative.
    if not cause_str.strip():
        cause_str = type(cause).__name__
    if isinstance(cause, RateLimited):
        action = "wait for the rate-limit window to reset and retry"
    else:
        action = "retry the request (possibly after a short backoff)"
    return (
        f"Paginated request failed after collecting {pages_collected} "
        f"page(s): {cause_str}. To recover: {action}, reduce the "
        f"request size (e.g. fewer locations, a shorter time range, or "
        f"a smaller ``limit``), or obtain an API token."
    )
