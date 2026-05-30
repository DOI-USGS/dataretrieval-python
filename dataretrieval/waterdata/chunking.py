"""Joint URL-byte chunking for the Water Data OGC getters.

A Water Data query has several chunkable axes: every multi-value list
parameter (sites, parameter codes, …) plus the cql-text ``filter``,
which splits along its top-level OR clauses. Any of them can fan the
URL past the server's ~8 KB byte limit. ``ChunkPlan`` picks a fan-out
for each axis that minimizes total sub-requests while keeping every
sub-request URL under the budget; ``ChunkedCall`` fetches the resulting
cartesian product of chunks. Requests that already fit get a trivial
single-step plan — ``ChunkedCall`` has one code path either way.

Concurrency: ``multi_value_chunked`` fans every pending sub-request out
under one ``asyncio.gather`` sharing a single ``httpx.AsyncClient``;
concurrency is bounded purely by the client's connection pool
(``httpx.Limits(max_connections=N, max_keepalive_connections=N)``), so
the pool throttles. ``API_USGS_CONCURRENT`` resolves
``N``: an integer N > 1 caps connections at N; ``1`` pins a single
connection (one request at a time); the literal ``unbounded`` removes
the cap (``N=None``). The default (16) is the server-friendly sweet
spot; higher values can trip USGS burst-protection 5xx in practice. The
fan-out runs in a short-lived worker thread (an ``anyio`` blocking
portal), so it works whether or not the caller is already inside an
event loop (Jupyter / IPython / async apps).

Retries: each sub-request is retried on a transient failure (429,
5xx, connect/read timeout) with exponential backoff + full jitter,
honoring a server ``Retry-After`` when present. ``API_USGS_RETRIES``
sets the cap (default 4; ``0`` disables). A ``Retry-After`` longer
than the per-call ceiling escalates to a resumable interruption.

Interruption: any mid-stream transient failure — 429, 5xx, or a bare
transport error (connect/read timeout, oversize follow-up URL) — surfaces
as a ``ChunkInterrupted`` subclass: ``QuotaExhausted`` for 429,
``ServiceInterrupted`` for the rest. The exception carries ``.call``, a
``ChunkedCall`` handle that owns the already-completed sub-request
state (sparse-indexed, since gathered sub-requests complete out of
order). Call ``.call.resume()`` once the underlying condition clears;
only the still-pending sub-requests are re-issued. ``Retry-After`` (when
the server sets it) is surfaced on the exception as ``.retry_after``.

Dedup: list-axis chunks don't overlap; filter-axis chunks can, so
``_combine_chunk_frames`` dedupes by feature ``id``. ``properties``,
``bbox``, date intervals, ``limit``, ``skip_geometry``, and
``filter``/``filter_lang`` themselves are never sliced as list axes
(the filter is partitioned along its top-level OR axis instead).
"""

from __future__ import annotations

import asyncio
import copy
import functools
import itertools
import math
import os
import random
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, ClassVar
from urllib.parse import quote_plus

import httpx
import pandas as pd
from anyio.from_thread import start_blocking_portal

from dataretrieval.utils import HTTPX_DEFAULTS

from . import _progress
from .filters import (
    _check_numeric_filter_pitfall,
    _is_chunkable,
    _split_top_level_or,
)

# Empirically the API replies HTTP 414 above ~8200 bytes of full URL —
# matches nginx's default ``large_client_header_buffers`` of 8 KB. 8000
# leaves ~200 bytes for request-line framing and proxy variance.
_WATERDATA_URL_BYTE_LIMIT = 8000

# Any list-shaped kwarg with >1 element is chunked (comma-joined per
# sub-list in the URL); ~90 OGC params qualify, so we denylist the few
# exceptions rather than maintain a growing allowlist. Excluded because:
# ``properties`` defines the column schema; ``bbox`` is a fixed coord
# tuple; date/time params are intervals, not enumerable sets; ``filter``
# is handled as its own OR-axis in ``_extract_axes``; and ``limit`` /
# ``skip_geometry`` / ``filter_lang`` are scalar by contract.
_NEVER_CHUNK = frozenset(
    {
        "properties",
        "bbox",
        "datetime",
        "last_modified",
        "begin",
        "begin_utc",
        "end",
        "end_utc",
        "time",
        "filter",
        "filter_lang",
        "limit",
        "skip_geometry",
    }
)

# Response header USGS uses to advertise remaining hourly quota.
_QUOTA_HEADER = "x-ratelimit-remaining"

# Fan-out concurrency cap, read at call time (not import) so test
# ``monkeypatch.setenv`` applies. Value grammar in :func:`_read_concurrency_env`;
# the concurrency model is in the module docstring.
_CONCURRENCY_ENV = "API_USGS_CONCURRENT"
_CONCURRENCY_DEFAULT = 16
_CONCURRENCY_UNBOUNDED = "unbounded"


def _read_concurrency_env() -> int | None:
    """
    Resolve the ``API_USGS_CONCURRENT`` env var to a parallelism cap.

    Returns
    -------
    int or None
        ``1`` for a single connection; an integer >1 for bounded
        concurrency; ``None`` to disable the per-call cap entirely
        (``unbounded`` keyword). Unset → default of
        ``_CONCURRENCY_DEFAULT``.
    """
    raw = os.environ.get(_CONCURRENCY_ENV)
    if raw is None:
        return _CONCURRENCY_DEFAULT
    raw = raw.strip()
    if raw == "":
        return _CONCURRENCY_DEFAULT
    if raw.lower() == _CONCURRENCY_UNBOUNDED:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{_CONCURRENCY_ENV} must be a positive integer or "
            f"'{_CONCURRENCY_UNBOUNDED}'; got {raw!r}."
        ) from exc
    if value < 1:
        raise ValueError(
            f"{_CONCURRENCY_ENV} must be >= 1 (got {value}); use "
            f"'{_CONCURRENCY_UNBOUNDED}' to disable the cap."
        )
    return value


# Retry-with-backoff defaults for transient sub-request failures (429 /
# 5xx / connect-read timeouts): exponential backoff with full jitter, and
# honor a server ``Retry-After`` up to the cap below before escalating
# to a resumable interruption instead.
_RETRIES_ENV = "API_USGS_RETRIES"
_RETRIES_DEFAULT = 4
_RETRY_BASE_BACKOFF = 0.5
_RETRY_MAX_BACKOFF = 30.0
_RETRY_AFTER_CAP = 60.0


def _read_retries_env() -> int:
    """
    Resolve the ``API_USGS_RETRIES`` env var to a max-retry count.

    Returns
    -------
    int
        Number of retries after the first attempt; ``0`` disables
        retrying. Unset/blank → ``_RETRIES_DEFAULT``.
    """
    raw = os.environ.get(_RETRIES_ENV)
    if raw is None or raw.strip() == "":
        return _RETRIES_DEFAULT
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise ValueError(
            f"{_RETRIES_ENV} must be a non-negative integer (got {raw!r})."
        ) from exc
    if value < 0:
        raise ValueError(f"{_RETRIES_ENV} must be >= 0 (got {value}).")
    return value


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded retry-with-backoff config for transient sub-request failures.

    An immutable value object that owns the *timing* decisions; the
    exception taxonomy (which failures are retryable) lives in
    :func:`_retryable`. Backoff is exponential with **full jitter**
    (:func:`random.uniform` over ``[0, ceiling]``) so the concurrent
    fan-out's retries don't re-burst in lockstep. A server ``Retry-After``
    hint, when present, overrides the computed backoff — unless it exceeds
    :attr:`retry_after_cap`, in which case retrying stops and the failure
    surfaces as a resumable :class:`ChunkInterrupted` (a multi-minute
    quota-window reset shouldn't block the call inline).

    Attributes
    ----------
    max_retries : int
        Retries attempted after the first try; ``0`` disables retrying.
    base_backoff : float
        Seconds; the jitter ceiling for the first retry, doubled each
        subsequent attempt.
    max_backoff : float
        Upper bound on any single attempt's backoff ceiling.
    retry_after_cap : float
        Largest ``Retry-After`` (seconds) honored inline; longer hints
        escalate to a resumable interruption.
    """

    max_retries: int = _RETRIES_DEFAULT
    base_backoff: float = _RETRY_BASE_BACKOFF
    max_backoff: float = _RETRY_MAX_BACKOFF
    retry_after_cap: float = _RETRY_AFTER_CAP

    def __post_init__(self) -> None:
        # Catch invalid timing knobs here so a misconfiguration fails at
        # construction, not deep in a later ``time.sleep`` (ValueError on
        # a negative delay) or silently in ``asyncio.sleep`` (which
        # treats negative as zero).
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0 (got {self.max_retries}).")
        if self.base_backoff < 0 or self.max_backoff < 0 or self.retry_after_cap < 0:
            raise ValueError("retry backoff settings must be non-negative.")

    @classmethod
    def from_env(cls) -> RetryPolicy:
        """
        Build a policy from the module-level defaults, resolved now.

        Reads ``max_retries`` from ``API_USGS_RETRIES`` and the timing
        knobs from the ``_RETRY_*`` module constants at call time — not
        the dataclass field defaults (which freeze at class definition)
        — so test ``monkeypatch.setattr`` on the constants takes effect.

        Returns
        -------
        RetryPolicy
            A policy built from the module-level defaults resolved at
            call time.
        """
        return cls(
            max_retries=_read_retries_env(),
            base_backoff=_RETRY_BASE_BACKOFF,
            max_backoff=_RETRY_MAX_BACKOFF,
            retry_after_cap=_RETRY_AFTER_CAP,
        )

    def should_retry(self, attempt: int, retry_after: float | None) -> bool:
        """
        Whether a just-failed ``attempt`` (1-based) warrants another try.

        A ``Retry-After`` longer than ``retry_after_cap`` is *not* slept
        off inline — it returns ``False`` so the failure escalates to a
        resumable interruption instead of blocking the call for minutes.

        Parameters
        ----------
        attempt : int
            The just-failed attempt number (1-based).
        retry_after : float or None
            Seconds the server suggested waiting (``Retry-After`` hint),
            or ``None`` when no hint was given.

        Returns
        -------
        bool
            ``True`` if another try is warranted, ``False`` otherwise.
        """
        if attempt > self.max_retries:
            return False
        return retry_after is None or retry_after <= self.retry_after_cap

    def backoff(self, attempt: int, retry_after: float | None) -> float:
        """
        Seconds to wait before retry ``attempt`` (1-based).

        Parameters
        ----------
        attempt : int
            The retry attempt number (1-based).
        retry_after : float or None
            Seconds the server suggested waiting (``Retry-After`` hint),
            or ``None`` to use the computed exponential backoff instead.

        Returns
        -------
        float
            Seconds to wait before the retry.
        """
        if retry_after is not None:
            return retry_after
        ceiling = min(self.max_backoff, self.base_backoff * 2 ** (attempt - 1))
        return random.uniform(0.0, ceiling)


# Default for direct ``ChunkedCall`` / ``ChunkPlan.execute`` construction
# (and tests): no retrying. The production decorator path explicitly passes
# ``RetryPolicy.from_env()`` so retries are on by default there.
_NO_RETRY = RetryPolicy(max_retries=0)


# Shared per-call ``httpx.AsyncClient``, published via :func:`_publish`
# during ``ChunkedCall._run`` so paginated-loop helpers (``_walk_pages``)
# reuse the same connection pool across every sub-request. ``None``
# outside a chunked call — paginated helpers then open their own
# short-lived client.
_chunked_client: ContextVar[httpx.AsyncClient | None] = ContextVar(
    "_chunked_client", default=None
)


@contextmanager
def _publish(client: httpx.AsyncClient) -> Iterator[None]:
    """
    Publish ``client`` on the ``_chunked_client`` ContextVar so the
    paginated-loop helpers can borrow it via :func:`get_active_client`
    for the duration of the ``with`` block.

    Parameters
    ----------
    client : httpx.AsyncClient
        The client to publish.

    Yields
    ------
    None
        Yields once, for the duration of the bind.
    """
    token = _chunked_client.set(client)
    try:
        yield
    finally:
        _chunked_client.reset(token)


def get_active_client() -> httpx.AsyncClient | None:
    """
    Return the chunker's currently-published client, or ``None``.

    Used by the paginated-loop helpers (e.g.
    :func:`dataretrieval.waterdata.utils._client_for`) to reuse the
    per-call connection pool.

    Returns
    -------
    httpx.AsyncClient or None
        The client published via :func:`_publish` if currently inside a
        :class:`ChunkedCall` run; ``None`` otherwise.
    """
    return _chunked_client.get()


# Separators the two axis kinds use to join their atoms back into
# URL text. List axes comma-join values (``site=USGS-A,USGS-B``); the
# filter axis OR-joins clauses (``filter=a='1' OR a='2'``).
_LIST_SEP = ","
_OR_SEP = " OR "

# ``_Fetch`` is the per-sub-request fetcher the decorator wraps and
# ``ChunkedCall`` drives: an ``async def fetch(args) -> (df, response)``.
_Fetch = Callable[[dict[str, Any]], Awaitable[tuple[pd.DataFrame, httpx.Response]]]

# Caller-supplied transform applied to the combined chunk result, so a
# resumed call returns the same shape as an un-interrupted one rather than
# the chunker's raw ``(frame, httpx.Response)``. This keeps the chunker
# generic: the OGC getters inject their post-processing (type coercion,
# column arrangement, ``BaseMetadata``) through ``utils._finalize_ogc``.
# The default is identity, so direct ``ChunkedCall`` use is unaffected.
_Finalize = Callable[[pd.DataFrame, httpx.Response], tuple[pd.DataFrame, Any]]


def _passthrough_result(
    frame: pd.DataFrame, response: httpx.Response
) -> tuple[pd.DataFrame, Any]:
    """Default :data:`_Finalize`: return the raw combined pair unchanged."""
    return frame, response


class _RetryableTransportError(RuntimeError):
    """
    Base for typed HTTP transport failures the chunker recognizes as
    transient.

    Raised by :func:`dataretrieval.waterdata.utils._raise_for_non_200`
    and walked by :func:`_classify_chunk_error`. One subclass per
    recoverable HTTP status family (429 → :class:`RateLimited`,
    5xx → :class:`ServiceUnavailable`); ``ChunkedCall`` wraps them as
    resumable :class:`ChunkInterrupted` subclasses.

    Parameters
    ----------
    message : str
        Human-readable error message.
    retry_after : float, optional
        Seconds to wait before retrying, parsed from the
        ``Retry-After`` response header.

    Attributes
    ----------
    retry_after : float or None
        Seconds to wait before retrying, parsed from the
        ``Retry-After`` response header. ``None`` when the header was
        absent or unparseable.
    """

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class RateLimited(_RetryableTransportError):
    """
    A USGS Water Data API request was rejected with HTTP 429.

    Exposed as a typed exception so callers (notably the multi-value
    chunker) can detect rate-limit failures via ``isinstance`` instead
    of string-matching error messages.
    """


class ServiceUnavailable(_RetryableTransportError):
    """
    A USGS Water Data API request was rejected with HTTP 5xx.

    Surfaced as a typed exception (parallel to :class:`RateLimited`)
    so ``ChunkedCall`` can treat transient server failures as
    resumable interruptions rather than fatal programmer errors.
    """


class RequestTooLarge(ValueError):
    """
    No chunking plan fits the URL byte limit.

    Raised when even the smallest reducible plan (every list axis at
    singleton chunks and the filter at one clause per sub-request)
    still exceeds the server's byte limit. Shrink the input lists,
    simplify the filter, or split the call manually.
    """


class ChunkInterrupted(RuntimeError):
    """
    Base class for mid-stream chunk failures whose completed work is
    preserved and resumable.

    A ``ChunkInterrupted`` subclass means: a sub-request failed, but
    ``ChunkedCall`` still owns whatever completed successfully before
    the failure. Call ``self.call.resume()`` to pick up where the
    failure stopped you — only still-pending sub-requests are
    re-issued.

    Subclasses describe *why* ``ChunkedCall`` stopped so callers can
    pick a retry policy: :class:`QuotaExhausted` for 429 (wait for the
    rate-limit window), :class:`ServiceInterrupted` for 5xx (wait for
    the upstream to recover). The ``.call`` handle is the same object
    across every interruption of a single chunked call — frames
    accumulate across retries.

    Attributes
    ----------
    call : ChunkedCall or None
        Resumable handle into the ``ChunkedCall`` that raised this
        exception. ``None`` only on hand-constructed exceptions (test
        fixtures), where ``.call``-derived accessors degrade to
        empty/``None``.
    retry_after : float or None
        Seconds the server suggested waiting (``Retry-After`` header).
        ``None`` when the server gave no hint.
    completed_chunks : int
        Number of sub-requests successfully completed before the failure.
    total_chunks : int
        Total sub-requests in the plan.
    partial_frame : pandas.DataFrame
        Combined frame of work completed by the moment this exception
        was raised. Snapshot at raise time — does NOT advance on a
        later ``call.resume()`` (use ``exc.call.partial_frame`` for
        the live view).
    partial_response : httpx.Response or None
        Raw aggregate response covering the completed sub-requests at
        raise time; ``None`` if nothing had completed yet. Same snapshot
        semantics as ``partial_frame``. (Raw, not finalized — use
        ``exc.call.resume()`` for the finalized ``(df, metadata)`` result.)

    Examples
    --------
    Retry on any transient interruption, honoring the server's
    ``Retry-After`` hint when present and falling back to a fixed wait
    otherwise. Each new interruption keeps the already-completed work
    intact — only the still-pending sub-requests are re-issued.

    .. code-block:: python

        import time
        from dataretrieval.waterdata import get_daily
        from dataretrieval.waterdata.chunking import ChunkInterrupted

        try:
            df, md = get_daily(monitoring_location_id=long_list_of_sites)
        except ChunkInterrupted as exc:
            while True:
                time.sleep(exc.retry_after or 5 * 60)
                try:
                    df, md = exc.call.resume()
                    break
                except ChunkInterrupted as next_exc:
                    exc = next_exc
    """

    # Subclasses override with a ``str.format`` template; the format
    # call sees ``completed_chunks`` and ``total_chunks`` as kwargs.
    _MESSAGE_TEMPLATE: ClassVar[str] = (
        "Chunked request interrupted after {completed_chunks}/"
        "{total_chunks} sub-requests; call .call.resume() to continue."
    )

    def __init__(
        self,
        *,
        completed_chunks: int,
        total_chunks: int,
        call: ChunkedCall | None = None,
        retry_after: float | None = None,
        cause: BaseException | None = None,
    ) -> None:
        message = self._MESSAGE_TEMPLATE.format(
            completed_chunks=completed_chunks, total_chunks=total_chunks
        )
        if cause is not None:
            cause_msg = str(cause) or type(cause).__name__
            message = f"{message} Cause: {type(cause).__name__}: {cause_msg}"
        super().__init__(message)
        self.completed_chunks = completed_chunks
        self.total_chunks = total_chunks
        self.call = call
        self.retry_after = retry_after
        # Snapshot partial state at raise time so the exception's view stays
        # stable across later ``call.resume()`` advances (the live view is on
        # ``call.partial_frame`` / ``.partial_response``). ``.copy()`` guards
        # the single-chunk fast path, where the frame may be returned verbatim.
        if call is None:
            self.partial_frame: pd.DataFrame = pd.DataFrame()
            self.partial_response: httpx.Response | None = None
        else:
            self.partial_frame = call.partial_frame.copy()
            self.partial_response = call.partial_response


class QuotaExhausted(ChunkInterrupted):
    """
    A sub-request returned HTTP 429 — the per-key rate-limit window
    is exhausted. Subclass of :class:`ChunkInterrupted`.

    The completed sub-requests are preserved on ``.call``; once the
    rate-limit window resets, ``.call.resume()`` re-issues only the
    still-pending work. ``partial_frame`` holds what completed
    before the 429.
    """

    _MESSAGE_TEMPLATE = (
        "HTTP 429 after {completed_chunks}/{total_chunks} sub-requests; "
        "catch QuotaExhausted (or ChunkInterrupted) to access "
        ".partial_frame or .call.resume() once the rate-limit "
        "window has rolled over."
    )


class ServiceInterrupted(ChunkInterrupted):
    """
    A sub-request returned HTTP 5xx — the upstream service failed
    transiently. Subclass of :class:`ChunkInterrupted`.

    The completed sub-requests are preserved on ``.call``; once the
    upstream recovers, ``.call.resume()`` resumes only the
    still-pending work.
    """

    _MESSAGE_TEMPLATE = (
        "Service error after {completed_chunks}/{total_chunks} "
        "sub-requests; catch ServiceInterrupted (or ChunkInterrupted) "
        "and call .call.resume() once the upstream service recovers."
    )


def _request_bytes(req: httpx.Request) -> int:
    """
    Return the total bytes of an httpx request: URL + body.

    GET routes have empty ``.content`` and reduce to URL length. POST
    routes (CQL2 JSON body) need body bytes — the URL stays short
    regardless of payload, so URL-only sizing would underestimate the
    request and skip chunking when it's needed.

    Parameters
    ----------
    req : httpx.Request
        The request to size.

    Returns
    -------
    int
        ``len(str(req.url)) + len(req.content)``. ``httpx.URL`` doesn't
        support ``len()`` directly, so the str-coercion is required.
    """
    return len(str(req.url)) + len(req.content)


def _safe_request_bytes(
    build_request: Callable[..., httpx.Request],
    args: dict[str, Any],
    url_limit: int,
) -> int:
    """
    Size a candidate sub-request, treating ``httpx.InvalidURL`` as
    "still too large".

    ``httpx.URL`` enforces a hard 64 KB cap per URL component
    (``MAX_URL_LENGTH``) and raises ``httpx.InvalidURL`` for anything
    bigger. We report ``url_limit + 1`` on overflow so the greedy
    halving loop in :meth:`ChunkPlan._plan` keeps shrinking the
    largest axis until ``httpx.Request`` can be constructed at all.

    Parameters
    ----------
    build_request : Callable[..., httpx.Request]
        Factory that turns a kwargs dict into a sized request.
    args : dict[str, Any]
        Per-sub-request kwargs to pass through to ``build_request``.
    url_limit : int
        The chunker's byte budget; returned + 1 on overflow.

    Returns
    -------
    int
        Real byte count when the request builds, otherwise
        ``url_limit + 1`` so the planner's "too large" branch keeps
        halving.
    """
    try:
        req = build_request(**args)
    except httpx.InvalidURL:
        return url_limit + 1
    return _request_bytes(req)


def _safe_elapsed(response: httpx.Response) -> timedelta:
    """
    Read ``response.elapsed``, falling back to ``timedelta(0)`` when
    the attribute hasn't been populated.

    httpx only writes ``.elapsed`` when a response is closed through
    its normal transport path. ``MockTransport`` (used by
    ``pytest-httpx``) and hand-constructed ``httpx.Response`` objects
    leave the attribute unset, so accessing it raises ``RuntimeError``.
    Combining responses across chunks needs a defined duration, so we
    treat the missing attribute as zero elapsed.
    """
    try:
        return response.elapsed
    except RuntimeError:
        return timedelta(0)


def _set_response_url(response: httpx.Response, url: str | httpx.URL) -> None:
    """
    Overwrite the URL surfaced by a response without back-propagating
    the change into any aliased original.

    Try the direct assignment first: on lightweight test mocks ``.url``
    is a plain writable attribute. On real ``httpx.Response`` it's
    read-only (it resolves through the bound request), so swap in a
    fresh :class:`httpx.Request` carrying the new URL — mutating the
    existing one would leak through any shallow copy that shares the
    same ``.request``.
    """
    try:
        response.url = url  # type: ignore[misc]
    except AttributeError:
        target = httpx.URL(str(url))
        try:
            old = response.request
        except RuntimeError:
            # No request bound (some hand-built httpx.Response fixtures);
            # synthesize a minimal one to hold the URL.
            response.request = httpx.Request("GET", target)
            return
        response.request = httpx.Request(
            method=old.method, url=target, headers=old.headers
        )


@dataclass(frozen=True)
class _Axis:
    """
    A single chunkable axis of one user-level request — a list of
    atomic units and the separator that joins them in the URL.

    Both multi-value list parameters (``sites=[...]``, joiner ``","``)
    and the cql-text ``filter`` (split on top-level ``OR``, joiner
    ``" OR "``) fit this shape, so a single greedy halving loop in
    ``ChunkPlan._plan`` handles both — no need for two separate
    algorithms.

    Attributes
    ----------
    arg_key : str
        The args-dict key this axis substitutes back into when a
        sub-request is rendered.
    atoms : tuple of str
        The smallest indivisible units along this axis (one site, one
        OR-clause, …). A "chunk" is a contiguous slice of ``atoms``.
    joiner : str
        Separator placed between atoms when they are joined back into
        URL text — ``","`` for list axes, ``" OR "`` for the filter
        axis.
    """

    arg_key: str
    atoms: tuple[str, ...]
    joiner: str

    def chunk_bytes(self, chunk: list[str]) -> int:
        """
        Return the URL-encoded byte count this chunk contributes when
        substituted into the request.

        ``quote_plus`` is faithful to what the real URL builder
        produces, so values containing characters that expand under URL
        encoding (``%``, ``+``, ``/``, ``&``, …) can't be mis-ranked.

        Parameters
        ----------
        chunk : list of str
            A contiguous slice of ``self.atoms``.

        Returns
        -------
        int
            Length of ``quote_plus(self.joiner.join(chunk))``.
        """
        return len(quote_plus(self.joiner.join(map(str, chunk))))

    def render(self, chunk: list[str]) -> Any:
        """
        Convert a chunk into the form the URL builder expects.

        List axes yield a fresh list of atoms (``build_request`` will
        comma-join); the filter axis yields a pre-joined string (CQL
        doesn't take a list).

        Parameters
        ----------
        chunk : list of str
            A contiguous slice of ``self.atoms``.

        Returns
        -------
        list of str or str
            ``list(chunk)`` for list axes, ``self.joiner.join(chunk)``
            for the filter axis.
        """
        return list(chunk) if self.joiner == _LIST_SEP else self.joiner.join(chunk)


def _extract_axes(args: dict[str, Any]) -> list[_Axis]:
    """
    Build the chunkable-axis set from a request's args.

    Multi-value list params with more than one element each become an
    axis. The cql-text filter (when chunkable and split into more than
    one top-level OR-clause) becomes one too. Anything in
    ``_NEVER_CHUNK`` is excluded except ``filter`` itself, which is
    handled separately so its atoms are clauses not characters.

    Parameters
    ----------
    args : dict[str, Any]
        The user-level request kwargs (the same dict that would be
        passed to ``build_request``).

    Returns
    -------
    list[_Axis]
        Zero or more axes in insertion order: list axes first (one
        per eligible kwarg, in ``args`` order), then the filter axis
        if present.
    """
    axes: list[_Axis] = []
    for key, value in args.items():
        if key in _NEVER_CHUNK:
            continue
        if isinstance(value, (list, tuple)) and len(value) > 1:
            axes.append(_Axis(arg_key=key, atoms=tuple(value), joiner=_LIST_SEP))

    filter_expr = args.get("filter")
    if _is_chunkable(filter_expr, args.get("filter_lang")):
        _check_numeric_filter_pitfall(filter_expr)
        clauses = _split_top_level_or(filter_expr)
        if len(clauses) >= 2:
            axes.append(_Axis(arg_key="filter", atoms=tuple(clauses), joiner=_OR_SEP))
    return axes


class ChunkPlan:
    """
    Strategy for issuing one user-level request as a sequence of
    sub-requests whose URLs each fit ``url_limit``.

    Constructing a plan *is* planning:
    ``ChunkPlan(args, build_request, url_limit)`` extracts the
    chunkable axes, runs greedy halving on the biggest chunk across
    all axes, and stores the result.

    Passthrough requests (no chunkable axes, or already fitting) are
    represented as a trivial plan with empty ``axes`` / ``chunks`` and
    ``total == 1``; :meth:`iter_sub_args` yields the original args
    unchanged so the ``ChunkedCall`` loop is the same shape either
    way.

    Parameters
    ----------
    args : dict[str, Any]
        The user-level request kwargs.
    build_request : Callable[..., httpx.Request]
        Factory that turns a kwargs dict into a sized httpx request,
        e.g. ``_construct_api_requests``.
    url_limit : int
        Byte budget for the request (URL + body).

    Attributes
    ----------
    args : dict
        The original user-level args this plan was built for. Bound to
        the plan so :meth:`iter_sub_args` is self-contained.
    axes : list[_Axis]
        The chunkable axes of ``args``: each multi-value list
        parameter, plus the cql-text filter (if any) split on top-level
        OR. Empty in the passthrough case.
    chunks : dict[str, list[list[str]]]
        Per-axis partition: ``chunks[axis.arg_key]`` is the list of
        atom-sublists this axis is split into. Empty in passthrough.
    canonical_url : str or None
        URL of the user's original (un-chunked) request, used to
        overwrite a chunked response's ``.url`` so ``BaseMetadata``
        reflects the full query. ``None`` on the passthrough path
        and when no buildable URL exists.

    Raises
    ------
    RequestTooLarge
        If the request needs chunking but even the singleton plan
        doesn't fit ``url_limit``.
    """

    def __init__(
        self,
        args: dict[str, Any],
        build_request: Callable[..., httpx.Request],
        url_limit: int,
    ) -> None:
        self.args = args
        self.axes: list[_Axis] = []
        self.chunks: dict[str, list[list[str]]] = {}
        self.canonical_url: str | None = None

        axes = _extract_axes(args)
        # No chunkable axes → skip ``build_request`` entirely; the
        # common Water Data call shape shouldn't pay for an unused
        # request prep on the passthrough hot path. The fetcher
        # will run with the user's args verbatim; if that produces
        # an over-budget URL, the server (or httpx itself) rejects.
        if not axes:
            return

        # Constructing the initial request can itself trip
        # ``httpx.InvalidURL`` (URL > 64 KB) — that's the canonical
        # "needs chunking" signal, so swallow it and proceed to plan.
        # When the unchunked URL does build, preserve it as
        # ``canonical_url`` so ``BaseMetadata.url`` echoes the user's
        # original query verbatim; only fall back to a worst-case
        # sub-request URL when the URL itself can't be constructed.
        try:
            initial_request = build_request(**args)
        except httpx.InvalidURL:
            initial_request = None

        if initial_request is not None:
            self.canonical_url = str(initial_request.url)
            if _request_bytes(initial_request) <= url_limit:
                return

        self.axes = axes
        self.chunks = {axis.arg_key: [list(axis.atoms)] for axis in axes}
        self._plan(build_request, url_limit)

        if self.canonical_url is None:
            # Original URL was un-constructable (httpx.InvalidURL); fall
            # back to the worst-case sub-request URL so
            # ``BaseMetadata.url`` still surfaces something
            # informative. If even that overflows, leave canonical_url
            # as None (set above) and let the response's own URL stand.
            with suppress(httpx.InvalidURL):
                self.canonical_url = str(build_request(**self._worst_case_args()).url)

    def _plan(
        self,
        build_request: Callable[..., httpx.Request],
        url_limit: int,
    ) -> None:
        """
        Greedy-halve the biggest chunk across all axes until the
        worst-case sub-request URL fits ``url_limit``. Mutates
        ``self.chunks`` in place; treats list axes and the filter axis
        uniformly — each is just a list of atoms joined by its axis's
        separator.

        Raises
        ------
        RequestTooLarge
            If even the singleton plan (every axis at one atom per
            chunk) still exceeds ``url_limit``.
        """
        while True:
            worst = self._worst_case_args()
            if _safe_request_bytes(build_request, worst, url_limit) <= url_limit:
                return

            biggest_axis: _Axis | None = None
            biggest_idx = -1
            biggest_size = -1
            for axis in self.axes:
                for idx, chunk in enumerate(self.chunks[axis.arg_key]):
                    if len(chunk) <= 1:
                        continue
                    size = axis.chunk_bytes(chunk)
                    if size > biggest_size:
                        biggest_axis, biggest_idx, biggest_size = axis, idx, size

            if biggest_axis is None:
                raise RequestTooLarge(
                    f"Request exceeds {url_limit} bytes (URL + body) at the "
                    f"smallest reducible plan (every axis at one atom per "
                    f"sub-request). Reduce input sizes, shorten or simplify "
                    f"the filter, or split the call manually."
                )
            axis_chunks = self.chunks[biggest_axis.arg_key]
            chunk = axis_chunks[biggest_idx]
            mid = len(chunk) // 2
            axis_chunks[biggest_idx : biggest_idx + 1] = [chunk[:mid], chunk[mid:]]

    def _worst_case_args(self) -> dict[str, Any]:
        """
        Args dict representing the largest sub-request the current
        ``self.chunks`` partition will issue — each axis's longest
        (by URL-encoded bytes) chunk rendered back in.
        """
        out = dict(self.args)
        for axis in self.axes:
            worst = max(self.chunks[axis.arg_key], key=axis.chunk_bytes)
            out[axis.arg_key] = axis.render(worst)
        return out

    @property
    def total(self) -> int:
        """
        Total sub-request count: product of per-axis chunk counts.

        Returns
        -------
        int
            ``1`` for the passthrough plan, otherwise the cartesian
            product of ``len(chunks[ax.arg_key])`` across all axes.
        """
        return math.prod((len(self.chunks[ax.arg_key]) for ax in self.axes), start=1)

    def iter_sub_args(self) -> Iterator[dict[str, Any]]:
        """
        Yield substituted args for each sub-request, in deterministic
        order — cartesian product over axes in extraction order.

        The same plan yields the same sub-args sequence on every
        invocation, so resume is well-defined.

        Yields
        ------
        dict[str, Any]
            A copy of ``self.args`` with each axis's current chunk
            substituted under its ``arg_key``.
        """
        if not self.axes:
            yield dict(self.args)
            return
        chunk_lists = [self.chunks[ax.arg_key] for ax in self.axes]
        for combo in itertools.product(*chunk_lists):
            sub_args = dict(self.args)
            for axis, chunk in zip(self.axes, combo):
                sub_args[axis.arg_key] = axis.render(chunk)
            yield sub_args

    def execute(
        self,
        fetch: _Fetch,
        retry_policy: RetryPolicy = _NO_RETRY,
        finalize: _Finalize = _passthrough_result,
    ) -> tuple[pd.DataFrame, Any]:
        """
        Run the plan and return the combined, finalized result.

        Thin wrapper around ``ChunkedCall(self, fetch).resume()``;
        see :class:`ChunkedCall` for the per-sub-request semantics.

        Parameters
        ----------
        fetch : Callable
            ``async def`` that issues a single sub-request, given the
            substituted args dict, and returns ``(frame, response)``.
        retry_policy : RetryPolicy, optional
            Per-sub-request retry-with-backoff policy. Defaults to
            :data:`_NO_RETRY`; the decorator passes ``RetryPolicy.from_env()``.
        finalize : Callable, optional
            Transform applied to the combined ``(frame, response)`` (see
            :data:`_Finalize`). Defaults to :func:`_passthrough_result`.

        Returns
        -------
        df : pandas.DataFrame
            Combined data from every successful sub-request.
        response
            The finalized aggregate — a raw :class:`httpx.Response` by
            default, or whatever ``finalize`` produces.

        Raises
        ------
        ChunkInterrupted
            On a mid-stream transient failure
            (:class:`QuotaExhausted` for 429,
            :class:`ServiceInterrupted` for 5xx). The resumable handle
            is on ``exc.call``.
        """
        return ChunkedCall(self, fetch, retry_policy, finalize).resume()


def _classify_chunk_error(
    exc: BaseException,
) -> tuple[type[ChunkInterrupted], float | None] | None:
    """
    Classify a fetch error as a known transient (resumable) failure.

    Walks the ``__cause__`` chain of ``exc`` looking for a known typed
    transport failure. Returns the matching ``ChunkInterrupted``
    subclass and any ``Retry-After`` hint, or ``None`` if the error is
    not a recognized transient — in which case ``ChunkedCall``
    re-raises rather than wrapping (programmer errors and unknown
    failures shouldn't masquerade as resumable).

    Parameters
    ----------
    exc : BaseException
        The exception raised by a sub-request.

    Returns
    -------
    tuple[type[ChunkInterrupted], float or None] or None
        ``(interrupted_class, retry_after)`` for recognized transient
        failures; ``None`` otherwise.

    Notes
    -----
    ``_walk_pages`` re-wraps mid-pagination failures as
    ``RuntimeError`` with the typed transport exception linked as
    ``__cause__``, so this function must walk the chain rather than
    just ``isinstance`` the top-level exception.

    Bare ``httpx.HTTPError`` (``ConnectError``, ``TimeoutException``,
    etc.) and ``httpx.InvalidURL`` (server-supplied cursor URL too
    long, oversize follow-up) are also treated as transport failures
    and wrapped as :class:`ServiceInterrupted` — these don't inherit
    from ``RuntimeError`` (and ``InvalidURL`` doesn't even inherit
    from ``HTTPError``), so without explicit handling they would
    escape the chunker's catch with no resumable handle.
    """
    cur: BaseException | None = exc
    while cur is not None:
        if isinstance(cur, RateLimited):
            return QuotaExhausted, cur.retry_after
        if isinstance(cur, ServiceUnavailable):
            return ServiceInterrupted, cur.retry_after
        if isinstance(cur, (httpx.HTTPError, httpx.InvalidURL)):
            return ServiceInterrupted, None
        cur = cur.__cause__
    return None


def _retryable(exc: BaseException) -> tuple[bool, float | None]:
    """
    Decide whether ``exc`` is a transient worth an automatic retry.

    Only the *top-level* exception is inspected — unlike
    :func:`_classify_chunk_error`, which walks the ``__cause__`` chain.
    The distinction matters because ``_paginate`` raises an
    initial-request transient (429 / 5xx / :class:`httpx.TransportError`)
    *raw*, but wraps a mid-pagination failure as a ``RuntimeError``. So a
    raw transient means a sub-request that made no progress and is cheap to
    re-issue, whereas a mid-pagination failure is left to escalate to a
    resumable :class:`ChunkInterrupted` rather than re-walked from page 1
    (which would re-spend the quota just exhausted). ``httpx.InvalidURL``
    is never retried — a too-long cursor won't fix on a retry.

    Returns
    -------
    tuple[bool, float or None]
        ``(retryable, retry_after)`` — the server ``Retry-After`` hint
        (seconds) when the transient carried one, else ``None``.
    """
    if isinstance(exc, (RateLimited, ServiceUnavailable)):
        return True, exc.retry_after
    if isinstance(exc, httpx.TransportError):
        return True, None
    return False, None


def _retry_delay(exc: BaseException, attempt: int, policy: RetryPolicy) -> float | None:
    """
    Decide the backoff for a just-failed ``attempt`` (1-based), or ``None``
    to give up and re-raise.

    Returns ``None`` in three cases — the error isn't a retryable
    transient, the policy is exhausted, or the server's ``Retry-After``
    exceeds the cap (escalates to a resumable :class:`ChunkInterrupted`
    instead). Otherwise returns the seconds to wait and emits the
    progress-bar retry note.

    Parameters
    ----------
    exc : BaseException
        The exception raised by the just-failed attempt.
    attempt : int
        The just-failed attempt number (1-based).
    policy : RetryPolicy
        The retry-with-backoff policy governing the decision.

    Returns
    -------
    float or None
        Seconds to wait before retrying, or ``None`` to give up and
        re-raise.
    """
    retryable, retry_after = _retryable(exc)
    if not retryable or not policy.should_retry(attempt, retry_after):
        return None
    delay = policy.backoff(attempt, retry_after)
    # Surface the imminent retry on the active progress reporter, if any.
    reporter = _progress.current()
    if reporter is not None:
        reporter.note_retry(attempt=attempt, wait=delay)
    return delay


async def _retry(
    afn: Callable[[], Awaitable[tuple[pd.DataFrame, httpx.Response]]],
    policy: RetryPolicy,
) -> tuple[pd.DataFrame, httpx.Response]:
    """
    Call ``afn`` with bounded retry-with-backoff on transient failures.

    A non-retryable or policy-exhausted failure (see :func:`_retry_delay`)
    propagates unchanged so the caller's existing handling wraps it as a
    resumable :class:`ChunkInterrupted`. The whole retry *decision* lives
    in :func:`_retry_delay`; this driver only awaits the sleep between
    attempts.

    Parameters
    ----------
    afn : Callable
        Zero-arg awaitable callable that issues a single sub-request and
        returns ``(frame, response)``.
    policy : RetryPolicy
        The retry-with-backoff policy governing the retries.

    Returns
    -------
    tuple of (pandas.DataFrame, httpx.Response)
        The ``(frame, response)`` pair from the first successful call.
    """
    attempt = 0
    while True:
        try:
            return await afn()
        except Exception as exc:  # noqa: BLE001 — re-raised unless retryable
            attempt += 1
            delay = _retry_delay(exc, attempt, policy)
            if delay is None:
                raise
            await asyncio.sleep(delay)


def _combine_chunk_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Concatenate per-chunk frames, dropping empties and deduping by ``id``.

    Parameters
    ----------
    frames : list[pandas.DataFrame]
        One frame per completed sub-request.

    Returns
    -------
    pandas.DataFrame
        The concatenated, deduplicated result. Empty when every input
        frame is empty.

    Notes
    -----
    ``_get_resp_data`` returns a plain ``pd.DataFrame()`` on empty
    responses; concatenating it with real ``GeoDataFrame``s downgrades
    the result to plain ``DataFrame`` and strips geometry/CRS, so
    empties are dropped first. Dedup on the pre-rename feature ``id``
    keeps overlapping user OR-clauses from producing duplicate rows
    across chunks.

    Dedup is restricted to rows whose ``id`` is non-null. ``pandas``
    treats NaN==NaN as a duplicate for ``drop_duplicates``, so a
    blanket call would collapse every id-less row into a single one —
    silent data loss if any chunk emits features without an
    ``id`` field.
    """
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        # Preserve the frame type (GeoDataFrame vs DataFrame) of the
        # input even when every chunk is empty — ``_get_resp_data``
        # returns ``gpd.GeoDataFrame()`` on empty geopd responses, and
        # returning a plain ``pd.DataFrame()`` here would downgrade
        # the type a downstream ``pd.concat([result, geo_page])`` to a
        # plain DataFrame and strip geometry/CRS.
        return frames[0] if frames else pd.DataFrame()
    if len(non_empty) == 1:
        # Single-completed-chunk fast path. Return a copy so callers
        # who treat ``ChunkedCall.partial_frame`` as a fresh result
        # (the property docstring says "live; recomputed per access")
        # don't accidentally mutate ``_chunks[0][0]`` in place.
        return non_empty[0].copy()
    combined = pd.concat(non_empty, ignore_index=True)
    if "id" in combined.columns:
        has_id = combined["id"].notna()
        if has_id.all():
            combined = combined.drop_duplicates(subset="id", ignore_index=True)
        elif has_id.any():
            # Mixed: dedupe only the id-bearing rows; preserve id-less
            # rows verbatim (their order relative to id-bearing rows
            # may shift, which is acceptable — dedup can't be id-keyed
            # for rows without an id).
            id_rows = combined[has_id].drop_duplicates(subset="id")
            no_id_rows = combined[~has_id]
            combined = pd.concat([id_rows, no_id_rows], ignore_index=True)
    return combined


def _combine_chunk_responses(
    responses: list[httpx.Response], canonical_url: str | None
) -> httpx.Response:
    """
    Fold per-sub-request responses into a single aggregated response.

    For a multi-response input, returns a shallow copy of
    ``responses[0]`` with ``.headers`` set to the last response's (so
    ``x-ratelimit-remaining`` reflects current state), ``.elapsed`` set
    to total wall-clock across every response, and ``.url`` set to the
    canonical original-query URL (when supplied) so ``BaseMetadata``
    reflects the user's full request rather than the first chunk.

    For a single-response input with no canonical-URL override,
    ``responses[0]`` is returned unchanged to skip the copy on the
    passthrough hot path.

    Parameters
    ----------
    responses : list[httpx.Response]
        One response per completed sub-request, in execution order.
    canonical_url : str or None
        URL of the unchunked original request. ``None`` skips the URL
        override — used by the passthrough path (the fetcher's
        response already carries the original-query URL) and by the
        worst-case overflow path (no buildable canonical URL exists).

    Returns
    -------
    httpx.Response
        A shallow copy of the first response with aggregated
        ``headers``, ``elapsed``, and ``url``. The function is
        idempotent (the input responses' ``headers`` / ``elapsed`` /
        ``url`` are never mutated), so it's safe to call repeatedly
        via :attr:`ChunkedCall.partial_response` during error
        inspection or resume retries. ``headers`` on the returned
        object is a fresh ``httpx.Headers``, so mutations there don't
        back-propagate into any chunk's underlying response.
    """
    if len(responses) == 1 and canonical_url is None:
        return responses[0]

    # ``copy.copy`` lets repeated calls re-sum elapsed from scratch
    # rather than re-mutating ``responses[0]`` in place. The headers
    # dict is then rewrapped in a fresh ``httpx.Headers`` so the
    # aggregate's headers don't share identity with — or leak mutations
    # back into — any underlying response on ``ChunkedCall._chunks``.
    head = copy.copy(responses[0])
    if len(responses) > 1:
        head.headers = httpx.Headers(responses[-1].headers)
        head.elapsed = sum(
            (_safe_elapsed(r) for r in responses[1:]),
            start=_safe_elapsed(responses[0]),
        )
    else:
        head.headers = httpx.Headers(responses[0].headers)
    if canonical_url is not None:
        _set_response_url(head, canonical_url)
    return head


class ChunkedCall:
    """
    Stateful handle for a chunked call.

    Holds the in-flight state (per-sub-request frames and responses)
    and the async fetcher. A single :meth:`resume` entry point drives
    the call from wherever it is to completion — used both for the
    first invocation (from :meth:`ChunkPlan.execute`) and for subsequent
    retries after a :class:`ChunkInterrupted`.

    :meth:`_run` gathers every pending sub-request over one shared
    :class:`httpx.AsyncClient`, applies the failure-precedence rules, and
    combines; :meth:`resume` drives it through an ``anyio`` blocking
    portal so it works whether or not the caller is already inside an
    event loop. Concurrency is bounded purely by the client's connection
    pool, so a single connection (``API_USGS_CONCURRENT=1``) is just a
    degenerate gather.

    A ``ChunkedCall`` is created internally when a :class:`ChunkPlan`
    executes; callers reach it via :attr:`ChunkInterrupted.call` on
    the exception raised by a mid-stream failure.

    :meth:`resume` is idempotent: :meth:`_run` iterates
    :meth:`ChunkPlan.iter_sub_args` (deterministic order) and skips
    any index whose result is already in ``self._chunks``. The
    completion set is a sparse ``dict[int, (df, response)]`` so the
    gather can record scattered completions (e.g. indices [0, 2, 5]
    after siblings [1, 3, 4] failed) and a subsequent ``resume`` only
    re-issues the missing indices.

    Parameters
    ----------
    plan : ChunkPlan
        The chunking plan to execute.
    fetch : Callable
        ``async def`` that issues a single sub-request, given the
        substituted args dict, and returns ``(frame, response)``.

    Attributes
    ----------
    plan : ChunkPlan
        The plan being driven (read-only after construction).
    fetch : Callable
        The async per-sub-request fetch function.
    finalize : Callable
        Transform applied to the combined result (see :data:`_Finalize`) at
        the terminal :meth:`_run` return, so a completed call yields the
        caller's finished shape. The ``partial_*`` accessors deliberately
        skip it and stay raw.
    partial_frame : pandas.DataFrame
        Raw combined frame of completed sub-requests (live; recomputed per
        access). Not finalized — call :meth:`resume` for the finished shape.
    partial_response : httpx.Response or None
        Raw aggregate response (canonical URL restored), or ``None`` when
        nothing has completed yet (live; recomputed per access).
    """

    def __init__(
        self,
        plan: ChunkPlan,
        fetch: _Fetch,
        retry_policy: RetryPolicy = _NO_RETRY,
        finalize: _Finalize = _passthrough_result,
    ) -> None:
        self.plan = plan
        self.fetch = fetch
        self.retry_policy = retry_policy
        self.finalize = finalize
        # Completed (frame, response) pairs keyed by sub-args index; sparse
        # (gathered sub-requests complete out of order — see class docstring).
        # ``_run``'s ``track`` closure is the only writer, so ``dict`` insertion
        # order is completion order (relied on by :meth:`_combine_raw`).
        self._chunks: dict[int, tuple[pd.DataFrame, httpx.Response]] = {}

    def wrap_failure(self, exc: BaseException) -> ChunkInterrupted | None:
        """
        Build the matching :class:`ChunkInterrupted` carrying this
        call when ``exc`` is a recognized transient transport failure;
        return ``None`` for unrecognized failures so the caller can
        re-raise. Encapsulates the
        ``classify → instantiate-with-call-state`` recipe so
        :class:`ChunkedCall`'s private fields stay private.

        Parameters
        ----------
        exc : BaseException
            The exception raised by a sub-request.

        Returns
        -------
        ChunkInterrupted or None
            The matching :class:`ChunkInterrupted` subclass carrying this
            call for a recognized transient failure; ``None`` otherwise.
        """
        classification = _classify_chunk_error(exc)
        if classification is None:
            return None
        interrupted_class, retry_after = classification
        return interrupted_class(
            completed_chunks=self.completed_chunks,
            total_chunks=self.plan.total,
            call=self,
            retry_after=retry_after,
            cause=exc,
        )

    @property
    def completed_chunks(self) -> int:
        """Number of sub-requests completed so far."""
        return len(self._chunks)

    def _combine_raw(self) -> tuple[pd.DataFrame, httpx.Response]:
        """Assemble the raw ``(frame, response)`` from completed sub-requests,
        before :attr:`finalize` runs.

        Frames concatenate in sub-args *index* order (``sorted`` keys —
        deterministic, independent of parallel completion order). The
        aggregated response takes its headers from the most-recently-
        *completed* sub-request: the ``track`` closure in :meth:`_run`
        is the only writer of ``self._chunks`` and ``dict`` preserves
        insertion order, so the chunks' natural order is completion
        order and the last one carries the freshest
        ``x-ratelimit-remaining``.

        Returns
        -------
        tuple of (pandas.DataFrame, httpx.Response)
            The concatenated frame and the aggregated response, before
            :attr:`finalize` is applied.
        """
        frames = [self._chunks[i][0] for i in sorted(self._chunks)]
        responses = [response for _, response in self._chunks.values()]
        return (
            _combine_chunk_frames(frames),
            _combine_chunk_responses(responses, self.plan.canonical_url),
        )

    @property
    def partial_frame(self) -> pd.DataFrame:
        """
        Raw combined frame of sub-requests that have completed so far.

        Live — recomputed on each access so it reflects current state
        across resume attempts. Deliberately the *raw* combined frame
        (``_combine_raw``), NOT the finalized result: this is a cheap,
        side-effect-free snapshot for inspecting partial progress, so
        reading it (or building a :class:`ChunkInterrupted` around it)
        never triggers ``finalize`` work — which for OGC getters includes
        a schema network fetch on an empty frame. Use ``call.resume()``
        for the finalized result.

        Returns
        -------
        pandas.DataFrame
            Combined frame of completed sub-requests, or an empty
            ``DataFrame`` when nothing has completed.
        """
        if not self._chunks:
            return pd.DataFrame()
        return self._combine_raw()[0]

    @property
    def partial_response(self) -> httpx.Response | None:
        """
        Raw aggregate response with the canonical URL restored to the
        user's full original query.

        Live — recomputed on each access. Like :attr:`partial_frame`, this
        is the *raw* aggregate (an :class:`httpx.Response`), not the
        finalized result, so inspecting it is side-effect-free.

        Returns
        -------
        httpx.Response or None
            Aggregated response when at least one sub-request has
            completed, ``None`` otherwise.
        """
        if not self._chunks:
            return None
        return self._combine_raw()[1]

    def _pending(self) -> Iterator[tuple[int, dict[str, Any]]]:
        """
        Yield ``(index, sub_args)`` for sub-requests not yet completed.

        Walks :meth:`ChunkPlan.iter_sub_args` in deterministic order
        and skips any index already in ``self._chunks``. :meth:`_run`
        uses this to pick up exactly the sub-requests it still owes —
        first run and every resume alike.

        Yields
        ------
        tuple of (int, dict)
            The sub-args ``index`` and its ``sub_args`` dict for each
            sub-request not yet completed.
        """
        for index, sub_args in enumerate(self.plan.iter_sub_args()):
            if index not in self._chunks:
                yield index, sub_args

    def resume(self) -> tuple[pd.DataFrame, Any]:
        """
        Drive the chunked call to completion and return the combined result.

        Runs :meth:`_run` through an ``anyio`` blocking portal (a
        short-lived worker thread), so it works whether or not the caller
        is already inside an event loop (Jupyter / IPython / async apps).
        The portal copies the calling context, so the active progress
        reporter still reaches the sub-requests.

        Idempotent: only sub-requests whose index isn't already in
        ``self._chunks`` are re-issued. Sub-args order matches
        :meth:`ChunkPlan.iter_sub_args` and is deterministic, so a
        partial completion (sparse indices) resumes correctly.

        Returns
        -------
        df : pandas.DataFrame
            Combined data from every successful sub-request.
        response
            The finalized aggregate — a raw :class:`httpx.Response`
            (canonical URL, most-recently-completed sub-request's headers,
            cumulative elapsed time) by default, or whatever
            :attr:`finalize` produces (e.g. ``BaseMetadata`` for the OGC
            getters).

        Raises
        ------
        ChunkInterrupted
            On a mid-stream transient failure — 429, 5xx, or a bare
            transport error: :class:`QuotaExhausted` for 429,
            :class:`ServiceInterrupted` for the rest. The resumable
            handle is on ``exc.call`` — wait for the underlying
            condition to clear and call ``exc.call.resume()`` again.
        """
        concurrency = _read_concurrency_env()
        with start_blocking_portal() as portal:
            return portal.call(functools.partial(self._run, concurrency))

    async def _run(self, max_concurrent: int | None) -> tuple[pd.DataFrame, Any]:
        """
        Gather every pending sub-request over one shared
        :class:`httpx.AsyncClient` and return the combined, finalized result.

        Pending sub-requests (:meth:`_pending`) fan out under
        ``asyncio.gather`` with ``return_exceptions=True`` so completed
        sub-requests survive a sibling's transient failure. On a
        recognized transient (:class:`RateLimited`, :class:`ServiceUnavailable`,
        or a bare ``httpx.HTTPError`` / ``httpx.InvalidURL``) a
        :class:`ChunkInterrupted` subclass is raised carrying ``self`` on
        ``.call``; ``exc.call.resume()`` then re-issues only the unfinished
        indices through this same runner.

        Concurrency is bounded by the client's connection pool —
        ``httpx.Limits(max_connections=N, max_keepalive_connections=N)``
        where ``N = max_concurrent`` (``None`` for unbounded). The gather
        dispatches *every* pending sub-request and the pool throttles, so
        ``N=1`` is just a single-connection gather (one request at a time)
        and ``total <= 1`` is just a one-element gather.
        The shared client is published on :data:`_chunked_client` so
        the paginated-loop helpers reuse its connection pool.

        Parameters
        ----------
        max_concurrent : int or None
            Maximum simultaneous connections (the pool cap). ``None``
            disables the cap.

        Returns
        -------
        df : pandas.DataFrame
            Combined data from every sub-request.
        response
            The finalized aggregate — a raw :class:`httpx.Response`
            (canonical URL, most-recently-completed sub-request's headers,
            cumulative elapsed time) by default, or whatever
            :attr:`finalize` produces (e.g. ``BaseMetadata`` for OGC getters).

        Raises
        ------
        ChunkInterrupted
            On a transient sub-request failure. ``.call`` is ``self``,
            holding the sparse completed sub-requests; ``.call.resume()``
            re-issues the unfinished ones.
        """
        # ``httpx.Limits()`` defaults to ``max_connections=100`` — at higher
        # concurrency the pool would silently bottleneck the fan-out behind
        # that cap. Set it to the resolved concurrency so the pool *is* the
        # throttle (``None`` for truly unbounded).
        limits = httpx.Limits(
            max_connections=max_concurrent, max_keepalive_connections=max_concurrent
        )

        async with httpx.AsyncClient(limits=limits, **HTTPX_DEFAULTS) as client:
            with _publish(client):
                reporter = _progress.current()
                if reporter is not None:
                    reporter.set_chunks(self.plan.total)

                async def track(
                    index: int, args: dict[str, Any]
                ) -> tuple[pd.DataFrame, httpx.Response]:
                    """One sub-request (with retry) + result-store + progress tick."""
                    result = await _retry(lambda: self.fetch(args), self.retry_policy)
                    self._chunks[index] = result
                    if reporter is not None:
                        # Chunks finish out of order under gather, so tick the
                        # completed *count* rather than a positional index.
                        reporter.start_chunk(self.completed_chunks)
                    return result

                # Dispatch every pending sub-request concurrently; the
                # connection pool (``limits``) is the only throttle.
                # ``return_exceptions`` keeps completed pairs after a sibling
                # fails, so partial state stays recoverable via :meth:`resume`.
                # Failure precedence, in order:
                #   1. Cancellation / interrupt signals (CancelledError,
                #      KeyboardInterrupt, SystemExit — non-Exception) propagate
                #      unmodified; wrapping them as a transient would swallow
                #      the user's stop signal.
                #   2. A non-transient failure (a real bug — unrecognized by
                #      ``wrap_failure``) surfaces raw, so it isn't masked behind
                #      a resumable handle for a transient sibling that landed
                #      later.
                #   3. Only when every failure is a recognized transient do we
                #      raise the first as a resumable ``ChunkInterrupted``.
                results = await asyncio.gather(
                    *(track(index, args) for index, args in self._pending()),
                    return_exceptions=True,
                )
                failures = [r for r in results if isinstance(r, BaseException)]
                for exc in failures:
                    if not isinstance(exc, Exception):
                        raise exc
                first_transient: tuple[ChunkInterrupted, BaseException] | None = None
                for exc in failures:
                    interrupted = self.wrap_failure(exc)
                    if interrupted is None:
                        raise exc
                    if first_transient is None:
                        first_transient = (interrupted, exc)
                if first_transient is not None:
                    interrupted, exc = first_transient
                    raise interrupted from exc

        return self.finalize(*self._combine_raw())


def multi_value_chunked(
    *,
    build_request: Callable[..., httpx.Request],
    url_limit: int | None = None,
) -> Callable[[_Fetch], Callable[..., tuple[pd.DataFrame, Any]]]:
    """
    Decorate an async fetcher to transparently chunk over-budget requests.

    Returns a callable that builds a :class:`ChunkPlan` from ``args``,
    constructs a :class:`ChunkedCall` over the decorated
    ``async def fetch(args) -> (df, response)``, and drives it to
    completion via :meth:`ChunkedCall.resume`. The plan splits multi-value
    list params and the cql-text filter so each sub-request URL fits the
    byte limit; an already-fitting request is a one-step plan. See the
    module docstring for the concurrency model.

    Parameters
    ----------
    build_request : Callable[..., httpx.Request]
        Factory that turns a kwargs dict into a sized httpx request,
        e.g. ``_construct_api_requests``. Called during planning to
        measure each candidate plan.
    url_limit : int, optional
        Byte budget for the request (URL + body). When ``None``
        (default), the module-level ``_WATERDATA_URL_BYTE_LIMIT`` is
        resolved at call time so test patches via
        ``monkeypatch.setattr`` take effect.

    Returns
    -------
    Callable
        A *synchronous* wrapper ``wrapper(args, *, finalize=...) ->
        (df, response)`` that executes the underlying plan transparently
        over the decorated async fetcher.

    Raises
    ------
    RequestTooLarge
        If no plan can fit ``url_limit``.
    ChunkInterrupted
        On a mid-execution transient — 429, 5xx, or a bare transport
        error: :class:`QuotaExhausted` for 429, :class:`ServiceInterrupted`
        for the rest. See :class:`ChunkedCall` for the resume semantics.

    See Also
    --------
    ChunkPlan : Planning shape (axes, partitioning, passthrough).
    ChunkedCall : Per-sub-request execution and resume semantics.
    """

    def decorator(fetch: _Fetch) -> Callable[..., tuple[pd.DataFrame, Any]]:
        @functools.wraps(fetch)
        def wrapper(
            args: dict[str, Any],
            *,
            finalize: _Finalize = _passthrough_result,
        ) -> tuple[pd.DataFrame, Any]:
            limit = _WATERDATA_URL_BYTE_LIMIT if url_limit is None else url_limit
            plan = ChunkPlan(args, build_request, limit)
            retry_policy = RetryPolicy.from_env()
            # The connection-pool cap is resolved inside ``resume()`` from
            # ``API_USGS_CONCURRENT``; ``1`` is a single-connection gather,
            # ``total <= 1`` a one-element gather — no special branch.
            return plan.execute(fetch, retry_policy, finalize)

        return wrapper

    return decorator
