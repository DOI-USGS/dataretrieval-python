"""HTTP error mapping for the OGC engine.

Translates a non-200 OGC response into the typed ``DataRetrievalError``
taxonomy (USGS-API-aware: special 429/403 messages, JSON error envelopes, and
``Retry-After`` parsing) and builds the user-facing message for a
mid-pagination failure. Changes here track the API's error contract.
"""

from __future__ import annotations

import httpx

from dataretrieval.exceptions import RateLimited, error_for_status


def _error_body(resp: httpx.Response) -> str:
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
    DataRetrievalError
        The typed subclass for the status (see
        :func:`dataretrieval.exceptions.error_for_status` for the mapping). The
        transient types (:class:`~dataretrieval.exceptions.TransientError`) are
        distinguished so ``ChunkedCall`` can wrap them as a resumable
        :class:`~dataretrieval.ogc.interruptions.QuotaExhausted` /
        :class:`~dataretrieval.ogc.interruptions.ServiceInterrupted`; a fatal
        :class:`~dataretrieval.exceptions.HTTPError` (not a ``TransientError``)
        the chunker won't resume.
    """
    status = resp.status_code
    if status < 400:
        return
    raise error_for_status(
        status,
        _error_body(resp),
        retry_after=_parse_retry_after(resp.headers.get("Retry-After")),
    )


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
        A message suitable for the ``DataRetrievalError`` that the
        paginated fetch paths raise from the original exception.
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
