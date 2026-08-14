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
        * **every other status** — a supported JSON error body (the USGS
          ``code``/``description`` envelope or a gateway ``message``) when
          present; otherwise ``"<status>: <reason>. <snippet>"`` with the first
          200 characters of ``resp.text``; an empty body degrades to
          ``"<status>: <reason>."``, except **403**, which falls back to
          :data:`_FORBIDDEN_CAUSES` so a credential problem is named.

    :func:`_raise_for_non_200` appends ``" (URL: ...)"`` to whatever this
    returns.
    """
    status = resp.status_code
    if status == 429:
        return (
            "429: Too many requests made. Please obtain an API token "
            "or try again later."
        )
    detail = _json_error_detail(resp)
    if detail is not None:
        return f"{status}: {detail}"
    snippet = (resp.text or "").strip()[:200]
    reason = resp.reason_phrase or "Error"
    if snippet:
        return f"{status}: {reason}. {snippet}"
    if status == 403:
        return f"403: {_FORBIDDEN_CAUSES}"
    return f"{status}: {reason}."


#: What a 403 means when the service sends no error envelope. Both causes are
#: named because the credential one is far more common and was omitted.
_FORBIDDEN_CAUSES = (
    "Query request denied. The API key may be missing, expired, or revoked "
    "(see API_USGS_PAT), or the query may exceed server limits."
)


def _json_error_detail(resp: httpx.Response) -> str | None:
    """Render a supported JSON error body, or ``None`` for another shape."""
    try:
        body = resp.json()
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None

    candidate = body.get("error")
    if not isinstance(candidate, dict):
        candidate = body

    def clean(value: object | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip().rstrip(".")
        return text or None

    code = clean(candidate.get("code"))
    detail = clean(candidate.get("description")) or clean(candidate.get("message"))
    parts = [part for part in (code, detail) if part is not None]
    return ". ".join(parts) + "." if parts else None


def _url_suffix(resp: httpx.Response) -> str:
    """`` (URL: ...)``, or empty when no request is attached.

    ``httpx`` raises on ``.url`` for a hand-built response; an error path must
    not fail while reporting a failure.
    """
    try:
        return f" (URL: {resp.url})"
    except RuntimeError:
        return ""


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
        _error_body(resp) + _url_suffix(resp),
        retry_after=_parse_retry_after(resp.headers.get("Retry-After")),
    )
