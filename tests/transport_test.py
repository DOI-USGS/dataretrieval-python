"""Component tests for the internal service-neutral transport layer."""

from __future__ import annotations

import asyncio
import datetime
import itertools
import socket
from unittest import mock

import httpx
import pandas as pd
import pytest

import dataretrieval.exceptions as exceptions
import dataretrieval.transport.liveness as liveness
import dataretrieval.transport.retry as retry
from dataretrieval._querying import _raise_for_status
from dataretrieval.exceptions import (
    ConfigurationError,
    DataRetrievalError,
    HTTPError,
    NetworkError,
    RateLimited,
    ServiceUnavailable,
)
from dataretrieval.transport.fanout import FanOut
from dataretrieval.transport.pagination import paginate


def _response(
    status: int = 200, *, url: str = "https://example.test/page"
) -> httpx.Response:
    return httpx.Response(status, request=httpx.Request("GET", url))


def test_paginate_follows_cursor_and_aggregates_response() -> None:
    first = _response(url="https://example.test/page/1")
    second = _response(url="https://example.test/page/2")
    first.headers["x-ratelimit-remaining"] = "9"
    second.headers["x-ratelimit-remaining"] = "8"
    client = mock.AsyncMock(spec=httpx.AsyncClient)
    client.send.return_value = first
    client.get.return_value = second

    cursors = {str(first.url): "next", str(second.url): None}

    def parse(response: httpx.Response) -> tuple[pd.DataFrame, str | None]:
        return pd.DataFrame({"value": [str(response.url)]}), cursors[str(response.url)]

    async def follow(cursor: str, session: httpx.AsyncClient) -> httpx.Response:
        assert cursor == "next"
        return await session.get("https://example.test/page/2")

    frame, response = asyncio.run(
        paginate(
            httpx.Request("GET", first.url),
            parse_response=parse,
            follow_up=follow,
            raise_for_status=_raise_for_status,
            client=client,
        )
    )

    assert frame["value"].tolist() == [str(first.url), str(second.url)]
    assert response.url == first.url
    assert response.headers["x-ratelimit-remaining"] == "8"


def test_paginate_stops_on_repeated_cursor_and_respects_row_cap() -> None:
    first = _response(url="https://example.test/page/1")
    second = _response(url="https://example.test/page/2")
    client = mock.AsyncMock(spec=httpx.AsyncClient)
    client.send.return_value = first
    client.get.return_value = second

    def parse(response: httpx.Response) -> tuple[pd.DataFrame, str]:
        return pd.DataFrame({"value": [1, 2]}), "same-cursor"

    async def follow(cursor: str, session: httpx.AsyncClient) -> httpx.Response:
        return await session.get(str(second.url))

    frame, _ = asyncio.run(
        paginate(
            httpx.Request("GET", first.url),
            parse_response=parse,
            follow_up=follow,
            raise_for_status=_raise_for_status,
            client=client,
            row_cap=3,
        )
    )

    assert frame["value"].tolist() == [1, 2, 1]
    assert client.get.await_count == 1


def test_retry_sync_retries_transient_then_succeeds(monkeypatch) -> None:
    attempts = 0
    slept: list[float] = []
    monkeypatch.setattr(retry.time, "sleep", slept.append)

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ServiceUnavailable("temporary")
        return "ok"

    result = retry.retry_sync(
        operation,
        retry.RetryPolicy(max_retries=1, base_backoff=0, max_backoff=0),
    )

    assert result == "ok"
    assert attempts == 2
    assert slept == [0]


def test_retry_sync_honors_cap_and_does_not_catch_cancellation(monkeypatch) -> None:
    sleep = mock.Mock()
    monkeypatch.setattr(retry.time, "sleep", sleep)
    policy = retry.RetryPolicy(max_retries=2, retry_after_cap=60)

    with pytest.raises(RateLimited):
        retry.retry_sync(
            lambda: (_ for _ in ()).throw(RateLimited("later", retry_after=61)),
            policy,
        )
    sleep.assert_not_called()

    with pytest.raises(KeyboardInterrupt):
        retry.retry_sync(
            lambda: (_ for _ in ()).throw(KeyboardInterrupt()),
            policy,
        )


def test_shared_status_mapping_preserves_retry_after() -> None:
    response = httpx.Response(
        429,
        headers={"Retry-After": "2.5"},
        request=httpx.Request("GET", "https://example.test"),
    )
    with pytest.raises(RateLimited) as exc_info:
        _raise_for_status(response)
    assert exc_info.value.retry_after == 2.5


def test_sync_bridge_runs_async_operation() -> None:
    """A one-item fan-out is the package's only sync-to-async bridge.

    Every retrieval path now enters through ``FanOut``, so a single request
    with nothing to chunk still reaches the network from synchronous caller
    code through the executor's blocking portal.
    """
    frame = pd.DataFrame({"value": ["ok"]})
    response = _response()

    async def operation(item: str) -> tuple[pd.DataFrame, httpx.Response]:
        assert item == "only"
        return frame, response

    returned, aggregated = FanOut(["only"], operation).resume()

    assert returned["value"].tolist() == ["ok"]
    assert aggregated is response


def test_retry_tunables_have_a_single_home() -> None:
    """Patching the tunables must reach the policy that reads them.

    Re-exporting them from ``ogc.retry`` would hand out copies taken at import
    time, so patching that path would change a value nothing consults. That
    module owns OGC classification only.
    """
    import dataretrieval.ogc.retry as ogc_retry

    assert not [name for name in vars(ogc_retry) if name.startswith("_RETRY")]
    assert set(ogc_retry.__all__) == {"_classify_chunk_error", "_classify_transient"}


def test_parse_retry_after_accepts_http_date() -> None:
    """A date in the future is honored; one already past is not a hint.

    Read literally an elapsed date says "retry now", but the likelier cause is
    our clock running ahead of the server's, and acting on it would re-send
    almost immediately against a service that just asked for a pause.
    """
    soon = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=30)
    parsed = exceptions.parse_retry_after(soon.strftime("%a, %d %b %Y %H:%M:%S GMT"))
    assert parsed is not None and 0 < parsed <= 30

    assert exceptions.parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") is None
    assert exceptions.parse_retry_after("not-a-date") is None
    # Delta-seconds is clock-independent, so a literal 0 stays an instruction.
    assert exceptions.parse_retry_after("0") == 0.0


def test_both_retry_after_forms_are_honored_alike() -> None:
    """The two header spellings mean the same thing and must behave the same.

    Discarding an over-long date hint (returning ``None``) made the client retry
    *harder* against a service asking for a long pause, and dropped the number
    the caller needs from ``.retry_after``.
    """
    far_future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        minutes=30
    )
    header = far_future.strftime("%a, %d %b %Y %H:%M:%S GMT")

    parsed = exceptions.parse_retry_after(header)
    assert parsed is not None and 1750 < parsed <= 1800
    assert exceptions.parse_retry_after("1800") == 1800.0
    # Either spelling, over the cap, stops the retry rather than being ignored.
    policy = retry.RetryPolicy(max_retries=4)
    assert not policy.should_retry(attempt=1, retry_after=parsed)
    assert not policy.should_retry(attempt=1, retry_after=1800.0)


def test_elapsed_retry_after_still_backs_off() -> None:
    """A ``Retry-After`` of zero must not become a zero-delay re-send."""
    policy = retry.RetryPolicy(base_backoff=0.5, max_backoff=30.0)

    assert policy.backoff(attempt=1, retry_after=0.0) > 0.0
    # The nudge is bounded by max_backoff, not this attempt's exponential
    # ceiling: keying it to the ceiling made it vanish whenever base_backoff was
    # zero -- exactly when a hint of 0 would become a zero-delay re-send.
    assert retry.RetryPolicy(base_backoff=0.0).backoff(attempt=1, retry_after=0.0) > 0.0
    # A server-named delay is honored, plus a small decorrelating nudge so
    # concurrent chunks handed the same hint do not all wake together --
    # and never enough to push the wait past the policy's own bounds.
    assert 5.0 < policy.backoff(attempt=1, retry_after=5.0) <= 6.0
    # A hint already at the cap is never nudged past it -- the jitter would
    # otherwise sleep longer than any bound the policy declares.
    at_cap = policy.backoff(attempt=8, retry_after=policy.retry_after_cap)
    assert at_cap == policy.retry_after_cap


def _dns_failure(errno: int) -> NetworkError:
    """A DNS failure shaped the way one actually reaches the retry loop.

    httpx and httpcore link their wrappers with ``__context__`` (implicit
    chaining), not ``__cause__``, so a walker following only explicit causes
    never reaches the ``gaierror``.
    """
    resolution_failed = socket.gaierror(errno, "name resolution failed")
    transport_failed = httpx.ConnectError("name resolution failed")
    transport_failed.__context__ = resolution_failed
    wrapped = NetworkError("could not reach host")
    wrapped.__context__ = transport_failed
    return wrapped


def test_deterministic_failures_are_not_retried() -> None:
    """Only failures a later attempt could survive are worth re-sending.

    The ``EAI_*`` values are platform-specific -- ``EAI_NONAME`` is 8 on
    macOS and -2 on Linux -- so these must come from :mod:`socket` rather
    than being written out, or the test only holds on the platform it was
    written on.
    """
    assert retry._retryable(_dns_failure(socket.EAI_NONAME)) == (False, None)
    assert retry._retryable(httpx.UnsupportedProtocol("no scheme")) == (False, None)
    assert retry._retryable(httpx.ConnectTimeout("timed out")) == (True, None)


def test_temporary_name_resolution_is_still_retried() -> None:
    """``gaierror`` is not one condition: ``EAI_AGAIN`` means "try again".

    A resolver still coming up, a VPN reconnect, or a laptop waking all
    surface this way, and they are exactly the failures retry exists for.
    """
    assert retry._retryable(_dns_failure(socket.EAI_AGAIN)) == (True, None)
    # An unrecognized code is retried too: a wasted attempt is cheaper than
    # dropping a call we could have recovered.
    assert retry._retryable(_dns_failure(0)) == (True, None)


def test_resolver_failure_found_past_an_unrelated_explicit_cause() -> None:
    """Both chain links are walked, not just the first one present.

    ``raise X from Y`` inside an ``except`` block leaves an explicit
    ``__cause__`` *and* an unrelated ``__context__`` on the same frame. Following
    only the cause walks off down the explicit branch and never reaches the
    ``gaierror``, so an unresolvable hostname spends the whole retry budget
    instead of failing fast.
    """
    failure = _dns_failure(socket.EAI_NONAME)
    failure.__cause__ = ValueError("an unrelated explicit cause")

    assert retry._retryable(failure) == (False, None)

    # The walk still distinguishes the temporary code on the same shape.
    temporary = _dns_failure(socket.EAI_AGAIN)
    temporary.__cause__ = ValueError("an unrelated explicit cause")
    assert retry._retryable(temporary) == (True, None)


def test_chain_walk_terminates_on_a_self_referential_cause() -> None:
    """A chain pointing back at itself must not hang the classifier."""
    looped = NetworkError("could not reach host")
    looped.__context__ = looped

    assert retry._retryable(looped) == (True, None)


def test_retryable_statuses_are_per_adapter() -> None:
    """A 500 means different things to different services, so the set differs.

    WQP answers an over-large query with a 500 and StreamStats answers
    out-of-network coordinates with one, so re-sending can never help there. The
    Water Data OGC API is a query interface where a 500 is an upstream hiccup, so
    the chunker keeps riding those out — applying WQP's rationale to it would
    quietly drop retries the chunked getters have always had.
    """
    rejected_query = ServiceUnavailable("bad query", status_code=500)
    gateway = ServiceUnavailable("bad gateway", status_code=502)

    # Default (Water Data chunker): every 5xx is worth another try.
    assert retry._retryable(rejected_query)[0]
    assert retry._retryable(gateway)[0]

    # One-shot adapters: only the gateway family.
    strict = retry._GATEWAY_STATUSES
    assert not retry._retryable(rejected_query, strict)[0]
    assert retry._retryable(gateway, strict)[0]
    assert retry._retryable(RateLimited("slow down", retry_after=1.0), strict) == (
        True,
        1.0,
    )
    # Never a plain client error, under either set.
    assert retry._retryable(HTTPError("not found", status_code=404)) == (False, None)


def test_stall_timeout_stops_a_silent_call(monkeypatch) -> None:
    """Retrying stops once a call has gone quiet for the whole budget.

    Without this, a request that times out is retried until the attempts run
    out, turning one 60 s timeout into minutes of apparent hang. The first
    retry is exempt (see below), so a silent call costs two attempts, not five.
    """
    attempts = 0

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        raise ServiceUnavailable("busy", status_code=503)

    # Every attempt appears to consume 100 s against a 60 s budget.
    clock = itertools.count(0.0, 100.0)
    monkeypatch.setattr(liveness.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(retry.time, "sleep", mock.Mock())

    with pytest.raises(ServiceUnavailable):
        retry.retry_sync(
            operation, retry.RetryPolicy(max_retries=4, stall_timeout=60.0)
        )

    assert attempts == 2, "first retry is exempt; the budget stops the rest"


def test_server_named_delay_does_not_consume_the_stall_budget(monkeypatch) -> None:
    """Honoring ``Retry-After`` must not cost a call its retries.

    The budget bounds *silence*; a delay the service named is the opposite of
    going quiet. Charging for it meant the more politely a service asked for
    room, the fewer retries it got: with the shipped defaults a
    ``Retry-After: 30`` against a 60 s budget allowed exactly one retry no
    matter what ``API_USGS_RETRIES`` said, silently capping the feature this
    layer exists to provide.
    """
    attempts = 0

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        raise RateLimited("slow down", status_code=429, retry_after=30.0)

    # A clock that advances by exactly what we sleep, so the only thing that can
    # exhaust the budget is the server-named wait itself.
    now = 0.0

    def sleep(seconds: float) -> None:
        nonlocal now
        now += seconds

    monkeypatch.setattr(liveness.time, "monotonic", lambda: now)
    monkeypatch.setattr(retry.time, "sleep", sleep)

    with pytest.raises(RateLimited):
        retry.retry_sync(
            operation,
            retry.RetryPolicy(max_retries=4, stall_timeout=60.0, retry_after_cap=60.0),
        )

    assert attempts == 5, "a sanctioned wait costs the no-progress budget nothing"


def test_credited_wait_never_credits_past_the_present(monkeypatch) -> None:
    """A wait longer than the budget must not disable the budget.

    ``credit_wait`` moves the progress stamp forward; without a ceiling at
    "now", one long queue wait pushed it into the future, made
    ``elapsed_since_progress`` negative, and -- since nothing ever pulls it back
    -- left that call exempt from the stall bound for the rest of its life.
    """
    now = 0.0
    monkeypatch.setattr(liveness.time, "monotonic", lambda: now)
    policy = retry.RetryPolicy(stall_timeout=60.0)

    liveness.note_progress()
    liveness.credit_wait(300.0)  # a deep-tail task queued past the whole budget
    assert liveness.elapsed_since_progress() == 0.0, "clamped to now, not negative"

    # The budget is spent again by real silence, not permanently disabled.
    now = 200.0
    assert not policy.allows_wait(5, 30.0, liveness.elapsed_since_progress())


def test_arriving_pages_restart_the_stall_budget(monkeypatch) -> None:
    """A slow but productive download keeps earning more time."""
    now = 0.0
    monkeypatch.setattr(liveness.time, "monotonic", lambda: now)
    policy = retry.RetryPolicy(stall_timeout=60.0)

    liveness.note_progress()
    now = 100.0
    assert not policy.allows_wait(2, 0.5, liveness.elapsed_since_progress())

    liveness.note_progress()  # a page arrived
    assert policy.allows_wait(2, 0.5, liveness.elapsed_since_progress())


def test_bad_retry_environment_raises_a_catchable_error(monkeypatch) -> None:
    """A typo in the environment must not escape as a bare ValueError.

    Every retrieval path builds its policy from the environment, so an
    unparseable value would otherwise bypass ``except DataRetrievalError`` in
    caller code and abort the run with an unrelated-looking error.
    """
    monkeypatch.setenv("API_USGS_RETRIES", "off")
    with pytest.raises(DataRetrievalError):
        retry.RetryPolicy.from_settings()

    monkeypatch.setenv("API_USGS_RETRIES", "2")
    monkeypatch.setenv("API_USGS_STALL_TIMEOUT", "none")
    with pytest.raises(ConfigurationError):
        retry.RetryPolicy.from_settings()

    monkeypatch.setenv("API_USGS_STALL_TIMEOUT", "10")
    assert retry.RetryPolicy.from_settings().stall_timeout == 10.0
    # Still a ValueError, so existing handling of a bad setting keeps working.
    assert issubclass(ConfigurationError, ValueError)


def test_queued_work_keeps_its_retries() -> None:
    """Time spent waiting for a concurrency slot is not silence.

    The no-progress budget starts when a retry loop is entered, but a fan-out
    task may sit behind a full semaphore long after that. Without excusing the
    wait, the tail of a wide fan-out enters its first attempt with the budget
    already spent, while the tasks dispatched ahead of it get the full
    allowance.
    """

    async def drive() -> dict[int, int]:
        gate = asyncio.Semaphore(1)
        attempts: dict[int, int] = {}
        policy = retry.RetryPolicy(
            max_retries=2, stall_timeout=1.0, base_backoff=0.001, max_backoff=0.001
        )

        async def one(index: int) -> None:
            attempts[index] = 0

            async def attempt() -> str:
                attempts[index] += 1
                if index == 0:
                    await asyncio.sleep(1.2)  # hold the gate past the budget
                    return "ok"
                raise ServiceUnavailable("busy", status_code=503)

            try:
                await retry.retry_async(attempt, policy, gate=gate)
            except ServiceUnavailable:
                pass

        await asyncio.gather(*(one(i) for i in range(3)))
        return attempts

    attempts = asyncio.run(drive())
    assert attempts[0] == 1
    # Queued behind a 1.2 s hold with a 1.0 s budget, these still get retried.
    assert attempts[1] == 3, attempts
    assert attempts[2] == 3, attempts


def test_gate_does_not_reset_silence_from_earlier_attempts() -> None:
    """Excusing the queue wait must not also forgive accumulated silence.

    The gated body is what the retry loop re-invokes, so stamping "now" on every
    slot acquisition would restart the clock each attempt and quietly turn a
    bound on *total* silence into a per-attempt latency bound -- five slow
    failures would each look brief while the call sat silent for their sum.
    """

    async def drive() -> int:
        gate = asyncio.Semaphore(4)  # never contended: no waiting to excuse
        attempts = 0
        policy = retry.RetryPolicy(
            max_retries=4, stall_timeout=1.0, base_backoff=0.001, max_backoff=0.001
        )

        async def attempt() -> str:
            nonlocal attempts
            attempts += 1
            await asyncio.sleep(0.4)  # each attempt is silent for 0.4 s
            raise ServiceUnavailable("gateway", status_code=504)

        try:
            await retry.retry_async(attempt, policy, gate=gate)
        except ServiceUnavailable:
            pass
        return attempts

    # 0.4 s per attempt against a 1.0 s budget: attempt 1 is exempt, attempt 2
    # accumulates past the budget. Five attempts would mean the budget stopped
    # counting across attempts.
    assert asyncio.run(drive()) == 3


def _wrapped_dns_failure(errno: int) -> NetworkError:
    """A DNS failure shaped the way one reaches the chunker.

    Our own wrapper uses ``raise ... from``, so the outer error links to the
    httpx one explicitly; only the layers beneath it chain implicitly. The
    chunker follows explicit links only, so this shape -- not
    :func:`_dns_failure`'s -- is the one that decides whether an unresolvable
    host is offered as resumable.
    """
    resolution_failed = socket.gaierror(errno, "name resolution failed")
    transport_failed = httpx.ConnectError("name resolution failed")
    transport_failed.__context__ = resolution_failed
    wrapped = NetworkError("Could not reach the service at https://nope.invalid")
    wrapped.__cause__ = transport_failed
    return wrapped


def test_deterministic_failures_are_not_offered_as_resumable() -> None:
    """ "Retryable" and "resumable" are one judgement and must agree.

    ``_retryable`` already refuses to re-send a hostname the resolver rejects
    outright. If the interruption classifier still mapped it to
    ``ServiceInterrupted``, the caller would be handed a ``.call.resume()``
    whose every attempt fails identically -- a resumable handle for something
    that cannot be resumed, hiding the ``NetworkError`` that actually explains
    the failure.
    """
    from dataretrieval.ogc.retry import _classify_chunk_error

    permanent = _wrapped_dns_failure(socket.EAI_NONAME)
    assert retry._retryable(permanent) == (False, None)
    assert _classify_chunk_error(permanent) is None

    unsupported = httpx.UnsupportedProtocol("no scheme")
    assert retry._retryable(unsupported) == (False, None)
    assert _classify_chunk_error(unsupported) is None

    # The converse still holds: a failure a later attempt could survive stays
    # both retryable and resumable. A temporary resolver failure is the sharp
    # case -- same exception type, same chain shape, opposite verdict, decided
    # only by the errno.
    temporary = _wrapped_dns_failure(socket.EAI_AGAIN)
    assert retry._retryable(temporary) == (True, None)
    assert _classify_chunk_error(temporary) is not None


def test_exception_chain_walk_terminates_on_a_self_referencing_chain() -> None:
    """Every question asked of a failure chain shares one guarded traversal.

    ``raise ... from`` accepts an exception already in the chain, so a retry
    loop that re-raises an earlier failure can close the cycle. Each of these
    walks reaches an answer by inspecting links, so an unguarded one would spin
    forever inside a request path rather than surface the failure. This fails by
    hanging, not by asserting -- pytest's timeout is the real assertion.
    """
    from dataretrieval.interruptions import (
        ServiceInterrupted,
        _classify_chunk_error,
        _deterministic_failure,
        _walk_causes,
    )

    first = RuntimeError("first")
    second = RuntimeError("second")
    first.__cause__ = second
    second.__cause__ = first

    assert {id(exc) for exc in _walk_causes(first)} == {id(first), id(second)}
    assert _classify_chunk_error(first) is None
    assert _deterministic_failure(first) is False
    # The status hunt in the interruption constructor walks the same chain.
    assert (
        ServiceInterrupted(completed_chunks=0, total_chunks=1, cause=first).status_code
        is None
    )
