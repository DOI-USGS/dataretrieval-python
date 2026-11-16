"""
Test scaffolding for the dataretrieval test suite.

* Defines the ``httpx_mock`` fixture, an in-house stand-in for
  ``pytest-httpx`` until that plugin supports httpx2
  (Colin-b/pytest_httpx#239).
* Pins the chunker env for every test (see ``_pin_chunker_env``), so
  chunk dispatch is deterministic and mocked retries measure attempt
  counts rather than wall clock. Concurrency and retry tests opt in by
  re-setting the env vars inside their body via ``monkeypatch.setenv``.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

import httpx2
import pytest

from dataretrieval import configuration


class HTTPXMock:
    """A small stand-in for pytest-httpx's ``httpx_mock`` fixture, built on httpx2.

    Tests register canned responses with ``add_response`` (or errors with
    ``add_exception``). Every request a real httpx2 transport would send is
    answered from that list instead of the network, and kept so a test can
    inspect it with ``get_requests``. Only the parts of the pytest-httpx API
    this suite uses are implemented; delete this class and the fixture below
    once pytest-httpx supports httpx2.

    Matching follows pytest-httpx run with its strict-mode checks off, so
    existing tests keep their meaning:

    * ``url`` is a string/``httpx2.URL`` (equal apart from query-parameter
      order) or a compiled regex (``re.match`` against the full URL).
      Omitted means any URL.
    * ``method`` is compared case-insensitively. Omitted means any method.
    * The first unused response that matches is sent. Once all matches are
      used, the last match is sent again.
    * A response that is never requested is not an error.
    * A request that matches nothing raises ``httpx2.TimeoutException``.
    """

    def __init__(self) -> None:
        self._mocks: list[
            tuple[Any, str | None, Callable[[httpx2.Request], httpx2.Response]]
        ] = []
        self._used: set[int] = set()
        self._requests: list[httpx2.Request] = []

    def add_response(
        self,
        *,
        url: Any = None,
        method: str | None = None,
        status_code: int = 200,
        is_optional: bool | None = None,
        is_reusable: bool | None = None,
        **response_kwargs: Any,
    ) -> None:
        # is_optional / is_reusable are accepted for compatibility: every response
        # here is already optional, and the last match is always reusable.
        def respond(request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(status_code, **response_kwargs)

        self._mocks.append((url, method, respond))

    def add_exception(
        self, exception: Exception, *, url: Any = None, method: str | None = None
    ) -> None:
        def respond(request: httpx2.Request) -> httpx2.Response:
            if isinstance(exception, httpx2.RequestError):
                exception.request = request
            raise exception

        self._mocks.append((url, method, respond))

    def get_requests(self) -> list[httpx2.Request]:
        return list(self._requests)

    def reset(self) -> None:
        self._mocks.clear()
        self._used.clear()
        self._requests.clear()

    @staticmethod
    def _url_matches(url: Any, request: httpx2.Request) -> bool:
        if url is None:
            return True
        if isinstance(url, re.Pattern):
            return url.match(str(request.url)) is not None
        expected = httpx2.URL(url)
        return dict(expected.params) == dict(request.url.params) and (
            expected.copy_with(query=None) == request.url.copy_with(query=None)
        )

    def _respond(self, request: httpx2.Request) -> httpx2.Response:
        self._requests.append(request)
        matches = [
            i
            for i, (url, method, _) in enumerate(self._mocks)
            if (method is None or method.upper() == request.method)
            and self._url_matches(url, request)
        ]
        if not matches:
            raise httpx2.TimeoutException(
                f"No response can be found for {request.method} request "
                f"on {request.url}",
                request=request,
            )
        unused = [i for i in matches if i not in self._used]
        chosen = unused[0] if unused else matches[-1]
        self._used.add(chosen)
        response = self._mocks[chosen][2](request)
        # httpx2.Response(content=...) reads and closes its body on creation, so
        # the client would never close the stream it wraps around it, and
        # ``response.elapsed`` would stay unset. Mark it unread, as pytest-httpx
        # does, so the client reads it the normal way.
        response.is_stream_consumed = False
        response.is_closed = False
        if hasattr(response, "_content"):
            del response._content
        if hasattr(response, "_decoder"):
            del response._decoder
        return response


@pytest.fixture
def httpx_mock(monkeypatch: pytest.MonkeyPatch) -> HTTPXMock:
    """Answer every httpx2 request in this test from registered mock responses."""
    mock = HTTPXMock()

    def handle_request(transport, request):
        request.read()  # load the body so tests can inspect POSTed CQL2 JSON
        return mock._respond(request)

    async def handle_async_request(transport, request):
        await request.aread()
        return mock._respond(request)

    # Patch the real network transports, as pytest-httpx does. Clients a test
    # builds with httpx2.MockTransport are a different class and are unaffected.
    monkeypatch.setattr(httpx2.HTTPTransport, "handle_request", handle_request)
    monkeypatch.setattr(
        httpx2.AsyncHTTPTransport, "handle_async_request", handle_async_request
    )
    return mock


@pytest.fixture(autouse=True)
def _pin_chunker_env(monkeypatch, tmp_path):
    """Pin every test to one connection, no retries, and no stall budget.

    Production defaults ``API_USGS_CONCURRENT`` to 32,
    ``API_USGS_RETRIES`` to 4, and ``API_USGS_STALL_TIMEOUT`` to 60 s.
    Pinning ``API_USGS_CONCURRENT=1`` keeps chunk dispatch
    deterministic for the mocked suite, and ``API_USGS_RETRIES=0`` makes
    a single transient is raised immediately rather than retried.
    Concurrency and retry tests opt in by overriding the env inside
    their body.

    ``API_USGS_STALL_TIMEOUT=0`` is pinned too so that an opting-in retry test measures
    the thing it names -- attempt counts -- and not the wall clock of the machine
    running it. Left at the production 60 s, a test that sets ``API_USGS_RETRIES`` would
    have its retries capped, without any signal, by whatever real time its mocked
    attempts consumed, which is both flaky on a busy CI machine and would let a passing
    retry test conceal a stall-budget bug. Tests of the budget itself set it explicitly.
    """
    monkeypatch.setenv("API_USGS_CONCURRENT", "1")
    monkeypatch.setenv("API_USGS_RETRIES", "0")
    monkeypatch.setenv("API_USGS_STALL_TIMEOUT", "0")
    # Point DATARETRIEVAL_CONFIG at a path that does not exist, so a developer's
    # real ~/.dataretrieval/config.toml -- which may hold an API key or a raised
    # concurrency -- can never influence a test run. Config tests opt in by
    # pointing the variable at a file they wrote.
    monkeypatch.setenv("DATARETRIEVAL_CONFIG", str(tmp_path / "no-such-config.toml"))
    configuration._reset_file_cache()
