"""
Test scaffolding for the dataretrieval test suite.

* Relaxes ``pytest-httpx``'s strict-mode flags so unconsumed mocks and
  unmatched requests don't fail the suite (keeps mocked-URL setup terse).
* Pins the chunker env for every test (see ``_pin_chunker_env``), so
  chunk dispatch is deterministic and mocked retries measure attempt
  counts. Concurrency and retry tests opt in by re-setting the env vars
  inside their body via ``monkeypatch.setenv``.
"""

from __future__ import annotations

import pytest

from dataretrieval import configuration


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
    """No host is exempt from mocking; every HTTP call must match a mock registered
    through the ``httpx_mock`` fixture.
    """
    return []


@pytest.fixture(autouse=True)
def _pin_chunker_env(monkeypatch, tmp_path):
    """Pin every test to one connection, no retries, and no stall budget.

    Production defaults ``API_USGS_CONCURRENT`` to 32,
    ``API_USGS_RETRIES`` to 4, and ``API_USGS_STALL_TIMEOUT`` to 60 s.
    Pinning ``API_USGS_CONCURRENT=1`` keeps chunk dispatch
    deterministic for the mocked suite, and ``API_USGS_RETRIES=0``
    raises a single transient immediately. Concurrency and retry tests
    opt in by overriding the env inside their body.

    The fixture also pins ``API_USGS_STALL_TIMEOUT=0``, so an opting-in retry test
    measures attempt counts. Left at the production 60 s, the real time the mocked
    attempts consume would cap the retries of a test that sets ``API_USGS_RETRIES``,
    without any signal, which is flaky on a busy CI machine and lets a passing
    retry test conceal a stall-budget bug. Tests of the budget itself set it explicitly.
    """
    monkeypatch.setenv("API_USGS_CONCURRENT", "1")
    monkeypatch.setenv("API_USGS_RETRIES", "0")
    monkeypatch.setenv("API_USGS_STALL_TIMEOUT", "0")
    # Point DATARETRIEVAL_CONFIG at a path that does not exist, so a developer's
    # real ~/.dataretrieval/config.toml (which may hold an API key or a raised
    # concurrency) can never influence a test run. Config tests opt in by
    # pointing the variable at a file they wrote.
    monkeypatch.setenv("DATARETRIEVAL_CONFIG", str(tmp_path / "no-such-config.toml"))
    configuration._reset_file_cache()
