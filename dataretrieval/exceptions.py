"""Exception taxonomy for ``dataretrieval``.

Every service module (``nwis``, ``wqp``, ``nldi``, ``waterdata``,
``streamstats``) raises a subclass of :class:`DataRetrievalError` when a request
fails, so one ``except dataretrieval.DataRetrievalError`` catches them all. That
includes connection-level failures (timeouts, DNS, refused connections), which
remain inside this taxonomy rather than leaking ``httpx`` exceptions. A
deterministic failure is :class:`NetworkError`; a recoverable failure that
exhausts retries during fan-out is a resumable ``ServiceInterrupted``.

Most failures are an :class:`HTTPError` carrying the response ``.status_code``,
of which :class:`TransientError` (429 / 5xx) is the retryable subset. The rest
aren't a plain status: :class:`RequestTooLarge` (with :class:`URLTooLong` /
:class:`Unchunkable`), :class:`NetworkError` (a failed connection, per above),
:class:`NoSitesError`, and :class:`ConfigurationError` for an unusable setting.
:func:`error_for_status` maps a status to its type. ``ConfigurationError`` is
the one member that is not a request failure at all: it reports an unusable
setting or config file, raised from wherever a setting is first resolved --
which, because resolution is lazy, is inside whichever getter runs first. The
*warning* side of the taxonomy lives here too: :class:`SkippedItemWarning`
(specialized by :class:`SkippedRatingWarning`) for a per-item skip inside a
batched retrieval, and :class:`DataCurrencyWarning` for an upstream dataset
that has stopped being updated.

This module has no third-party runtime dependencies -- ``httpx`` is imported only
for type checking. Any module can therefore import it without pulling in pandas
or httpx, and without risking an import cycle.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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
    "ConfigurationError",
    "DataCurrencyWarning",
    "SkippedItemWarning",
    "SkippedRatingWarning",
    "error_for_status",
    "parse_retry_after",
]


class DataRetrievalError(Exception):
    """Base class for every ``dataretrieval`` error.

    Almost every member is a failed request, and the read-anywhere fields below
    describe one. The exception is :class:`ConfigurationError`, which reports a
    configuration the library cannot use; it appears here because configuration
    is resolved lazily on the request path, so it surfaces from inside a getter
    and one ``except DataRetrievalError`` should cover it too. It carries no
    status and is not retryable, so the branching idiom below routes it to the
    final ``raise``.

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

    Connection-level failures (timeouts, DNS) remain subclasses of this base:
    :class:`NetworkError` when deterministic, or a resumable
    ``ServiceInterrupted`` when recoverable fan-out retries are exhausted.
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
    """Build a blank :class:`DataRetrievalError` for unpickling.

    Bypasses ``__init__``; pickle then calls ``__setstate__`` to restore state.
    """
    return cls.__new__(cls)


# --- HTTP status errors --------------------------------------------------


class HTTPError(DataRetrievalError):
    """The service returned an error HTTP status.

    The numeric status is on :attr:`status_code`; branch on it, e.g.
    ``except HTTPError as e: ... if e.status_code == 404``. :class:`TransientError`
    (429 / 5xx) is the retryable subset, and is itself an ``HTTPError``. The one
    exception to "a status is an ``HTTPError``" is a request the service rejects
    as too long: it surfaces as :class:`URLTooLong` (a :class:`RequestTooLarge`),
    *not* an ``HTTPError``. Catch :class:`DataRetrievalError` to be certain of
    spanning every failure. See :func:`error_for_status` for the full mapping.

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
    """A 429 or 5xx the server may serve on a later try.

    :class:`RateLimited` covers 429 and :class:`ServiceUnavailable` covers 5xx.

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
    (every list axis at one atom per chunk, the filter at one clause per
    chunk) still exceeds the server's byte limit. Unlike
    :class:`URLTooLong`, then, automatic splitting has already been tried and
    exhausted. Shrink the input lists, simplify the filter, or split the call
    manually.
    """


# --- Connection failure (no HTTP response) -------------------------------


class NetworkError(DataRetrievalError):
    """The request never completed a round-trip to the service.

    A DNS failure, refused connection, or timeout stopped it, so no HTTP
    response arrived to classify.

    Wraps the underlying ``httpx`` transport exception, preserved on
    ``__cause__``. Worth retrying (:attr:`~DataRetrievalError.retryable` is
    ``True``), but carries no ``.status_code`` because no response came back.
    """

    retryable: ClassVar[bool] = True


# --- Bad configuration ---------------------------------------------------


class ConfigurationError(DataRetrievalError, ValueError):
    """A ``dataretrieval`` setting holds a value that can't be used, so no
    request was issued -- an environment variable, a policy field, a malformed
    ``config.toml``, or a profile the file does not define.

    It is a :class:`DataRetrievalError` so ``except`` around a retrieval catches
    it rather than letting a bare ``ValueError`` escape a request path. That
    matters because settings resolve lazily, on the request path: a broken
    config file surfaces from inside whichever getter runs first, and belongs in
    the same handler as any other failure of that call. It is *also* a
    :class:`ValueError`, so code that already treats a bad setting as one keeps
    working whether the value came from the environment, a file, or a
    :func:`dataretrieval.configure` block.
    """


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


# --- Upstream data currency -----------------------------------------------


class DataCurrencyWarning(UserWarning):
    """An upstream dataset is frozen, retired, or no longer updated.

    Distinct from ``DeprecationWarning``, which promises that a *name in this
    package* is going away and gives the caller something to migrate to. Here
    the API is fine and there is nothing to migrate: the service's own data
    has stopped moving, and only the caller can judge whether that matters.

    It is a ``UserWarning`` for that reason. Emitting it as a
    ``DeprecationWarning`` meant a downstream project running
    ``-W error::DeprecationWarning`` -- ordinary CI hygiene -- could not call
    the affected getters with their default arguments at all.
    """


# --- Skipped work ---------------------------------------------------------


class SkippedItemWarning(UserWarning):
    """One item of a batched retrieval was skipped; the rest were returned.

    The policy for batch getters whose items are independent documents: an
    item that fails *deterministically* -- so retrying would reproduce the
    failure -- is dropped from the result under a warning naming it, because
    aborting would discard every other item's data over one bad entry.
    Transient failures (429 / 5xx / timeouts / connection drops) are never
    skipped -- they are retried and, if retries run out, raised as a
    resumable interruption. Rate limiting in particular is systematic, so
    skipping there would silently drop most of a batch; that silent loss is
    the failure mode this policy exists to prevent.

    A warning rather than a log line so it is visible by default. To make
    any skip fatal (strict all-or-nothing behavior)::

        warnings.filterwarnings("error", category=SkippedItemWarning)

    Getters emit a subclass naming their surface (e.g.
    :class:`SkippedRatingWarning`), so a filter can also target one getter.
    """


class SkippedRatingWarning(SkippedItemWarning):
    """A rating feature was skipped by
    :func:`dataretrieval.waterdata.get_ratings`.

    Emitted when a single STAC feature fails deterministically -- a stale
    catalog entry (404 on its data asset), a feature carrying no data asset,
    a malformed RDB file. The failed feature's id is absent from the returned
    dict. See :class:`SkippedItemWarning` for the policy and how to escalate
    a skip to an error.
    """


def error_for_status(
    status: int, message: str, *, retry_after: float | None = None
) -> DataRetrievalError:
    """Return the typed :class:`DataRetrievalError` for an HTTP error *status*.

    The one status-to-type mapping every request path shares (the legacy
    ``query`` path, ``waterdata``, ``streamstats``), so a given status
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


def parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header into seconds, or ``None`` for no usable hint.

    Both header forms mean the same thing and are treated the same way: the
    seconds are returned as given, however large. A value past what a caller will
    wait out inline stops the retry and surfaces a transient carrying the hint on
    ``.retry_after``, so a long wait becomes the caller's decision (and, for a
    chunked call, a resumable interruption) instead of being ignored.

    An over-long hint is honored rather than discarded. Dropping it would make
    the client retry *harder* against a service that just asked for a long
    pause, and would deny the caller the number it needs on ``.retry_after``.
    Clock skew can inflate a date-form hint, but trusting one costs a
    recoverable escalation while ignoring it costs hammering a service that is
    already asking for room.

    A date that has *already* passed yields no hint at all rather than ``0.0``.
    Read literally it says "retry now", but the likelier reading is that our
    clock runs ahead of the server's -- and acting on it would re-send almost
    immediately against a service that just asked for a pause. Falling back to
    our own bounded backoff is right under either reading. (Delta-seconds is
    clock-independent, so a literal ``Retry-After: 0`` is still honored as the
    instruction it is, floored by
    :meth:`~dataretrieval.transport.retry.RetryPolicy.backoff`'s jitter.)
    """
    if not value:
        return None
    raw = value.strip()
    try:
        seconds = float(raw)
    except ValueError:
        pass
    else:
        # ``inf``/``nan`` parse cleanly but poison every later comparison: an
        # infinite hint would refuse retry forever and travel to the caller on
        # ``.retry_after``. Treat them as no hint at all.
        return max(0.0, seconds) if math.isfinite(seconds) else None
    try:
        retry_at = parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
    return delay if delay > 0 else None
