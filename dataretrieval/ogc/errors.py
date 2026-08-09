"""HTTP error mapping for the OGC engine.

Translates a non-200 OGC response into the typed ``DataRetrievalError``
taxonomy (USGS-API-aware: special 429/403 messages, JSON error envelopes, and
``Retry-After`` parsing) and builds the user-facing message for a
mid-pagination failure. Changes here track the API's error contract.
"""

from __future__ import annotations

import httpx

from dataretrieval.exceptions import error_for_status
from dataretrieval.exceptions import parse_retry_after as _parse_retry_after


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
        :class:`~dataretrieval.ogc.interruptions.ServiceInterrupted`. The
        chunker won't resume a fatal
        :class:`~dataretrieval.exceptions.HTTPError` (not a ``TransientError``).
    """
    status = resp.status_code
    if status < 400:
        return
    raise error_for_status(
        status,
        _error_body(resp),
        retry_after=_parse_retry_after(resp.headers.get("Retry-After")),
    )
