"""
Test scaffolding for the dataretrieval test suite.

Relaxes ``pytest-httpx``'s strict-mode flags so unconsumed mocks and
unmatched real requests don't fail the suite (matches the historical
``requests-mock``-style permissiveness the test code was written
against, and keeps mocked-URL setup terse).
"""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(config, items):
    """Apply relaxed ``pytest-httpx`` strict-mode settings to every
    test in the suite — matches the permissive defaults the historical
    tests were written against."""
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
