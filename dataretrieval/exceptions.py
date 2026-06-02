"""Exception taxonomy for ``dataretrieval``.

A failed request from any service module (``nwis``, ``wqp``, ``waterdata``,
``nldi``, ...) raises a subclass of :class:`DataRetrievalError`, so a caller can
handle any request failure with a single ``except dataretrieval.DataRetrievalError``.

The tree has two intermediate bases a caller can catch to span a whole family:
:class:`RequestTooLarge` (the request can't fit, however it was issued) and
:class:`TransientError` (a temporary failure worth retrying).

This module deliberately has no third-party dependencies, so any module can
import it without pulling in pandas/httpx.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import httpx

__all__ = [
    "DataRetrievalError",
    "BadRequestError",
    "NotFoundError",
    "RequestTooLarge",
    "URLTooLong",
    "Unchunkable",
    "NoSitesError",
    "TransientError",
    "RateLimited",
    "ServiceUnavailable",
]


class DataRetrievalError(Exception):
    """Base class for errors raised when a request to a USGS or EPA web
    service fails.

    Every service module (``nwis``, ``wqp``, ``waterdata``, ``nldi``, ...)
    raises a subclass of this when a request fails, so a caller can handle any
    request failure uniformly::

        try:
            df, md = dataretrieval.wqp.get_results(...)
        except dataretrieval.DataRetrievalError:
            ...

    Subclasses also inherit from the built-in exception this package has
    historically raised for the condition's *kind* -- :class:`ValueError` for a
    request that can't succeed as written (bad params, too large), and
    :class:`RuntimeError` for a transient transport failure -- so existing
    ``except ValueError`` / ``except RuntimeError`` handlers keep working.
    """


# --- Fatal client errors -------------------------------------------------
# The request can't succeed as written; retrying it unchanged won't help. Each
# is also a ``ValueError`` -- the built-in the legacy ``query`` path has always
# raised -- so existing ``except ValueError`` handlers keep working.


class BadRequestError(DataRetrievalError, ValueError):
    """The service rejected the request parameters (HTTP 400)."""


class NotFoundError(DataRetrievalError, ValueError):
    """The requested resource was not found; often an empty query (HTTP 404)."""


class RequestTooLarge(DataRetrievalError, ValueError):
    """The request is too large for the service to satisfy.

    A base for the two ways a request can exceed what the service accepts;
    catch it to handle either. The concrete subclasses are :class:`URLTooLong`
    (a single request the server rejected) and :class:`Unchunkable` (the Water
    Data chunker could not split the call small enough to fit).
    """


class URLTooLong(RequestTooLarge):
    """A single request URL exceeded the service's limit (HTTP 414, or rejected
    client-side before it was sent).

    Raised by the legacy ``query`` path, which issues one request without
    chunking. Remediation: query fewer sites, or split the call manually.
    """


class Unchunkable(RequestTooLarge):
    """No chunking plan fits the URL byte limit.

    Raised by the Water Data chunker when even the smallest reducible plan
    (every list axis at one atom per sub-request, the filter at one clause per
    sub-request) still exceeds the server's byte limit -- so unlike
    :class:`URLTooLong`, automatic splitting has already been tried and
    exhausted. Shrink the input lists, simplify the filter, or split the call
    manually.
    """


class NoSitesError(DataRetrievalError):
    """The selection criteria matched no sites/data."""

    def __init__(self, url: httpx.URL) -> None:
        self.url = url

    def __str__(self) -> str:
        return (
            "No sites/data found using the selection criteria specified in "
            f"url: {self.url}"
        )


# --- Transient transport errors ------------------------------------------
# The service was reachable but temporarily refused the request; the same call
# may succeed if retried. Each is also a ``RuntimeError`` (the built-in the
# waterdata path has always raised). The Water Data chunker recognizes them via
# ``isinstance(exc, TransientError)`` and wraps them as resumable
# ``ChunkInterrupted`` subclasses.


class TransientError(DataRetrievalError, RuntimeError):
    """Base for transient HTTP failures that are worth an automatic retry.

    One subclass per recoverable HTTP status family (429 -> :class:`RateLimited`,
    5xx -> :class:`ServiceUnavailable`); the Water Data chunker recognizes them
    by this shared base and wraps them as resumable interruptions.

    Parameters
    ----------
    message : str
        Human-readable error message.
    retry_after : float, optional
        Seconds to wait before retrying, parsed from the ``Retry-After``
        response header; stored on the :attr:`retry_after` attribute (``None``
        when the header is absent or unparseable).
    """

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class RateLimited(TransientError):
    """A request was rejected with HTTP 429 (too many requests)."""


class ServiceUnavailable(TransientError):
    """A request was rejected with a server error (HTTP 5xx).

    Raised by both the legacy ``query`` path and the Water Data path, so a 5xx
    surfaces as one type regardless of which subsystem issued the request.
    """
