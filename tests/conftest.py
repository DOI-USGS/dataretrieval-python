"""
Test scaffolding for the dataretrieval test suite.

* Relaxes ``pytest-httpx``'s strict-mode flags so unconsumed mocks and
  unmatched requests don't fail the suite (keeps mocked-URL setup terse).
* Pins the chunker env for every test (see ``_pin_chunker_env``), so
  chunk dispatch is deterministic and mocked retries measure attempt
  counts rather than wall clock. Concurrency and retry tests opt in by
  re-setting the env vars inside their body via ``monkeypatch.setenv``.
"""

from __future__ import annotations

import pytest

from dataretrieval import settings


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
def _pin_chunker_env(monkeypatch, tmp_path):
    """Pin every test to one connection, no retries, and no stall budget.

    Production defaults ``API_USGS_CONCURRENT`` to 32,
    ``API_USGS_RETRIES`` to 4, and ``API_USGS_STALL_TIMEOUT`` to 60 s.
    Pinning ``API_USGS_CONCURRENT=1`` keeps chunk dispatch
    deterministic for the mocked suite, and ``API_USGS_RETRIES=0`` makes
    a single transient surface immediately rather than be retried.
    Concurrency and retry tests opt in by overriding the env inside
    their body.

    ``API_USGS_STALL_TIMEOUT=0`` is pinned too so that an opting-in retry
    test measures the thing it names -- attempt counts -- and not the wall
    clock of the machine running it. Left at the production 60 s, a test
    that sets ``API_USGS_RETRIES`` would have its retries silently capped
    by whatever real time its mocked attempts consumed, which is both flaky
    on a loaded CI box and a way for a stall-budget bug to hide behind a
    passing retry test. Tests of the budget itself set it explicitly.
    """
    monkeypatch.setenv("API_USGS_CONCURRENT", "1")
    monkeypatch.setenv("API_USGS_RETRIES", "0")
    monkeypatch.setenv("API_USGS_STALL_TIMEOUT", "0")
    # Point DATARETRIEVAL_CONFIG at a path that does not exist, so a developer's
    # real ~/.dataretrieval/config.toml -- which may hold an API key or a raised
    # concurrency -- can never influence a test run. Config tests opt in by
    # pointing the variable at a file they wrote.
    monkeypatch.setenv("DATARETRIEVAL_CONFIG", str(tmp_path / "no-such-config.toml"))
    settings._reset_file_cache()
