"""Exception taxonomy for ``dataretrieval``.

Every service module (``nwis``, ``wqp``, ``nldi``, ``waterdata``, ``nadp``,
``streamstats``) raises a subclass of :class:`DataRetrievalError` when a request
fails, so one ``except dataretrieval.DataRetrievalError`` catches them all --
including connection-level failures (timeouts, DNS, refused connections), which
are wrapped as :class:`NetworkError` with the underlying ``httpx`` exception on
``__cause__``.

Most failures are an :class:`HTTPError` carrying the response ``.status_code``,
of which :class:`TransientError` (429 / 5xx) is the retryable subset. The rest
aren't a plain status: :class:`RequestTooLarge` (with :class:`URLTooLong` /
:class:`Unchunkable`), :class:`NetworkError` (a failed connection, per above),
and :class:`NoSitesError`. :func:`error_for_status` maps a status to its type.

This module has no third-party runtime dependencies -- ``httpx`` is imported only
for type checking -- so any module can import it without pulling in pandas / httpx
and without risking an import cycle.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    import httpx

__all__ = [
    "DataRetrievalError",
    "HTTPError",
    "TransientError",
    "RateLimited",
    "ServiceUnavailable",
    "RequestTooLarge",
    "URLTooLong",
    "Unchunkable",
    "NetworkError",
    "NoSitesError",
    "error_for_status",
]


class DataRetrievalError(Exception):
    """Base class for every failed-request error in ``dataretrieval``.

    Catch it to handle any USGS or EPA service failure uniformly, and branch on
    the read-anywhere fields below without needing the concrete subclass::

        try:
            df, md = dataretrieval.waterdata.get_daily(...)
        except dataretrieval.DataRetrievalError as e:
            if e.retryable:  # 429 / 5xx / connection failure
                time.sleep(e.retry_after or backoff)
                ...  # re-issue the request
            elif e.status_code == 404:  # ``None`` unless an HTTP status error
                ...
            else:
                raise

    Connection-level failures (timeouts, DNS) are wrapped as
    :class:`NetworkError`, so this single clause covers them too.
    """

    #: HTTP status that triggered the error, or ``None`` for errors without one
    #: (connection failure, too-long URL, no data). Set by :class:`HTTPError`.
    status_code: int | None = None
    #: Seconds the server asked us to wait before retrying (its ``Retry-After``
    #: header), or ``None`` when it gave no hint. Set by :class:`TransientError`.
    retry_after: float | None = None
    #: Whether re-issuing the same request might succeed -- ``True`` for the
    #: transient HTTP statuses (429 / 5xx, :class:`TransientError`) and for
    #: connection failures (:class:`NetworkError`); ``False`` otherwise.
    retryable: ClassVar[bool] = False

    # These errors get pickled back across process boundaries (a lithops /
    # multiprocessing worker returns whatever it raises). Default ``BaseException``
    # pickling rebuilds via ``cls(*args)``, which these subclasses can't survive --
    # keyword-only constructor fields, and ``ChunkInterrupted`` builds its message
    # internally. So reconstruct via ``__new__`` + the standard getstate/setstate
    # protocol, bypassing ``__init__``; a subclass drops unpicklable state by
    # overriding ``__getstate__`` (see ``ChunkInterrupted``).
    def __reduce__(self) -> tuple[Any, ...]:
        return (_new_error, (self.__class__,), self.__getstate__())

    def __getstate__(self) -> dict[str, Any]:
        return {"args": self.args, **self.__dict__}

    def __setstate__(self, state: dict[str, Any] | None) -> None:
        state = state or {}
        self.args = state.pop("args", ())
        self.__dict__.update(state)


def _new_error(cls: type[DataRetrievalError]) -> DataRetrievalError:
    """Build a blank :class:`DataRetrievalError` for unpickling, bypassing
    ``__init__``; pickle then calls ``__setstate__`` to restore its state."""
    return cls.__new__(cls)


# --- HTTP status errors --------------------------------------------------


class HTTPError(DataRetrievalError):
    """The service returned an error HTTP status.

    The numeric status is on :attr:`status_code`; branch on it, e.g.
    ``except HTTPError as e: ... if e.status_code == 404``. :class:`TransientError`
    (429 / 5xx) is the retryable subset, and is itself an ``HTTPError``. The one
    exception to "a status is an ``HTTPError``" is a request the service rejects
    as too long: it surfaces as :class:`URLTooLong` (a :class:`RequestTooLarge`),
    *not* an ``HTTPError`` -- so catch :class:`DataRetrievalError` to be certain
    of spanning every failure. See :func:`error_for_status` for the full mapping.

    Parameters
    ----------
    message : str
        Human-readable error message.
    status_code : int
        The HTTP status the service returned.
    """

    def __init__(self, message: str, *, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class TransientError(HTTPError):
    """A 429 or 5xx the server may serve on a later try -- :class:`RateLimited`
    for 429, :class:`ServiceUnavailable` for 5xx.

    This only classifies the condition; it does not itself retry. Whether to
    retry is up to the calling path: a single-shot request raises it for the
    caller to handle (e.g. wait :attr:`retry_after` seconds, then re-issue),
    while the Water Data chunker retries and resumes automatically.

    Parameters
    ----------
    message : str
        Human-readable error message.
    status_code : int, optional
        The HTTP status the service returned. Defaults to the leaf's canonical
        code (429 / 503) when omitted; :func:`error_for_status` always passes the
        real status.
    retry_after : float, optional
        Seconds to wait before retrying, parsed from the ``Retry-After`` response
        header; ``None`` when the header is absent or unparseable.
    """

    retryable: ClassVar[bool] = True

    #: Canonical status a concrete transient stamps when built without an
    #: explicit ``status_code`` (:class:`RateLimited` = 429,
    #: :class:`ServiceUnavailable` = 503). ``TransientError`` itself is abstract
    #: and sets none, so constructing it bare requires ``status_code``.
    _DEFAULT_STATUS: ClassVar[int]

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        if status_code is None:
            status_code = getattr(self, "_DEFAULT_STATUS", None)
        if status_code is None:
            raise TypeError(
                f"{type(self).__name__} requires status_code "
                "(only the RateLimited / ServiceUnavailable leaves default it)"
            )
        super().__init__(message, status_code=status_code)
        self.retry_after = retry_after


class RateLimited(TransientError):
    """A request was rejected with HTTP 429 (too many requests)."""

    _DEFAULT_STATUS = 429


class ServiceUnavailable(TransientError):
    """A request was rejected with a server error (HTTP 5xx).

    Raised by both the legacy ``query`` path and the Water Data path, so a 5xx
    surfaces as one type whichever subsystem issued the request. ``.status_code``
    holds the actual 5xx; it falls back to 503 only on a bare hand-construction.
    """

    _DEFAULT_STATUS = 503


# --- Request can't fit (not necessarily an HTTP status) ------------------


class RequestTooLarge(DataRetrievalError):
    """The request is too large for the service to satisfy.

    Base for the two ways that happens; catch it to handle either:
    :class:`URLTooLong` (a single request rejected for length) and
    :class:`Unchunkable` (a Water Data call the chunker could not split small
    enough to fit).
    """


class URLTooLong(RequestTooLarge):
    """A single request URL was too long for the service.

    Raised on the legacy ``query`` path (which sends one un-chunked request),
    whether the URL is rejected client-side before sending or by the server
    (see :func:`error_for_status`). Remediation: query fewer sites, or split the
    call manually.
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


# --- Connection failure (no HTTP response) -------------------------------


class NetworkError(DataRetrievalError):
    """The request never completed a round-trip to the service -- a DNS
    failure, refused connection, or timeout -- so no HTTP response arrived to
    classify.

    Wraps the underlying ``httpx`` transport exception, preserved on
    ``__cause__``. Worth retrying (:attr:`~DataRetrievalError.retryable` is
    ``True``), but carries no ``.status_code`` because no response came back.
    """

    retryable: ClassVar[bool] = True


# --- Empty result --------------------------------------------------------


class NoSitesError(DataRetrievalError):
    """A request succeeded (HTTP 200) but matched no sites/data.

    A no-data result is normally **not** an error: the modern getters
    (``waterdata``, ``wqp``, ``nldi``) return an empty ``DataFrame``. Only the
    deprecated ``nwis`` (waterservices) path still raises this.
    """

    def __init__(self, url: httpx.URL) -> None:
        self.url = url

    def __str__(self) -> str:
        return (
            "No sites/data found using the selection criteria specified in "
            f"url: {self.url}"
        )


def error_for_status(
    status: int, message: str, *, retry_after: float | None = None
) -> DataRetrievalError:
    """Return the typed :class:`DataRetrievalError` for an HTTP error *status*.

    The one status-to-type mapping every request path shares (the legacy
    ``query`` path, ``waterdata``, ``nadp`` / ``streamstats``), so a given status
    becomes the same type everywhere:

    * **413, 414** -> :class:`URLTooLong` (a :class:`RequestTooLarge`) -- the
      "too long" semantic is more actionable than a bare status, and it matches
      the client-side over-long-URL case
    * **429** -> :class:`RateLimited`
    * **5xx** -> :class:`ServiceUnavailable`
    * **anything else** -> :class:`HTTPError`

    ``message`` is used verbatim; ``retry_after`` is attached only to the
    transient (:class:`TransientError`) types. *status* must be an error status
    (``>= 400``) -- classifying a success or redirect is a usage error and raises
    :class:`ValueError`.
    """
    if status < 400:
        raise ValueError(
            f"error_for_status expects an HTTP error status (>= 400), got {status}"
        )
    if status in (413, 414):
        return URLTooLong(message)
    if status == 429:
        return RateLimited(message, status_code=status, retry_after=retry_after)
    if 500 <= status < 600:
        return ServiceUnavailable(message, status_code=status, retry_after=retry_after)
    return HTTPError(message, status_code=status)
