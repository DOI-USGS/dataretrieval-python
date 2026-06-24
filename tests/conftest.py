"""
Test scaffolding for the dataretrieval test suite.

* Relaxes ``pytest-httpx``'s strict-mode flags so unconsumed mocks and
  unmatched requests don't fail the suite (keeps mocked-URL setup terse).
* Pins ``API_USGS_CONCURRENT=1`` and ``API_USGS_RETRIES=0`` for every
  test by default, so sub-request dispatch is deterministic and a single
  transient surfaces immediately (no backoff). Concurrency and retry
  tests opt in by re-setting the env vars inside their body via
  ``monkeypatch.setenv``.
"""

from __future__ import annotations

import pytest

#: Trace patterns that ``pytest-rerunfailures`` retries on the live-API test
#: modules: a transient upstream 429/5xx or dropped connection is retried,
#: deterministic failures (assertion errors, 4xx, etc.) are not. The OGC engine
#: renders a status error as ``"<status>: ..."`` while the legacy ``query`` path
#: renders ``"HTTP <status> ..."``, so the status pattern allows either shape;
#: the chunked fan-out wraps a transient sub-request as ``QuotaExhausted`` /
#: ``ServiceInterrupted``.
_TRANSIENT_RERUN_PATTERNS = [
    r"(?:RateLimited|ServiceUnavailable|RuntimeError):\s*(?:HTTP\s+)?(?:429|5\d\d)",
    r"(?:QuotaExhausted|ServiceInterrupted):",
    r"Connect(ion)?Error",  # requests' ConnectionError + httpx' ConnectError
    r"ReadTimeout|ConnectTimeout|Timeout",
    # ``dataretrieval`` wraps connection-level failures (timeout / DNS / refused)
    # in a typed ``NetworkError``; rerunfailures matches the crash line (the
    # ``NetworkError``), not the chained raw httpx exception, so match the
    # wrapper too -- otherwise a transient SSL/handshake timeout fails CI.
    r"NetworkError",
]

#: Apply to a test module (``pytestmark = flaky_api``) or class (``@flaky_api``)
#: that hits live USGS services, so a transient upstream failure is retried
#: instead of failing CI. Mocked tests are unaffected — the patterns match only
#: real round-trip error traces.
flaky_api = pytest.mark.flaky(
    reruns=2,
    reruns_delay=5,
    only_rerun=_TRANSIENT_RERUN_PATTERNS,
)


def pytest_collection_modifyitems(config, items):
    """Apply relaxed ``pytest-httpx`` strict-mode settings to every test
    so unconsumed mocks and unmatched requests don't fail the suite."""
    marker = pytest.mark.httpx_mock(
        assert_all_responses_were_requested=False,
        assert_all_requests_were_expected=False,
        can_send_already_matched_responses=True,
    )
    for item in items:
        item.add_marker(marker)


@pytest.fixture
def non_mocked_hosts() -> list[str]:
    """No hosts are exempted from mocking; every HTTP call must hit
    a mock registered through the ``httpx_mock`` fixture."""
    return []


@pytest.fixture(autouse=True)
def _pin_chunker_env(monkeypatch):
    """Pin every test to one connection and no retries.

    Production defaults ``API_USGS_CONCURRENT`` to 32 and
    ``API_USGS_RETRIES`` to 4. Pinning ``API_USGS_CONCURRENT=1`` keeps
    sub-request dispatch deterministic for the mocked suite, and
    ``API_USGS_RETRIES=0`` makes a single transient surface immediately
    rather than be retried. Concurrency and retry tests opt in by
    overriding the env inside their body.
    """
    monkeypatch.setenv("API_USGS_CONCURRENT", "1")
    monkeypatch.setenv("API_USGS_RETRIES", "0")
