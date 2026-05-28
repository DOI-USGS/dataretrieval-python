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

    Production defaults ``API_USGS_CONCURRENT`` to 16 and
    ``API_USGS_RETRIES`` to 4. Pinning ``API_USGS_CONCURRENT=1`` keeps
    sub-request dispatch deterministic for the mocked suite, and
    ``API_USGS_RETRIES=0`` makes a single transient surface immediately
    rather than be retried. Concurrency and retry tests opt in by
    overriding the env inside their body.
    """
    monkeypatch.setenv("API_USGS_CONCURRENT", "1")
    monkeypatch.setenv("API_USGS_RETRIES", "0")
