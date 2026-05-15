"""Auto-retry live-API tests on transient upstream errors.

After PR #273, paginated ``waterdata`` getters propagate mid-walk HTTP
errors (429 / 5xx / connection drops) instead of silently truncating the
result. That's the correct behavior for users but makes any CI test that
hits the live USGS Water Data API susceptible to flaking on a transient
upstream blip — e.g. the HTTP 502 Bad Gateway that broke CI on PR #273's
merge to main.

Heuristic: any test that does NOT request the ``requests_mock`` fixture
is treated as live and gets retried on transient-error patterns only.
Library bugs raising other exception types still fail on the first try.
"""

import pytest

# Anchored loosely — the failure trace embeds the exception message
# after a long preamble.
_TRANSIENT_PATTERNS = [
    r"RuntimeError:\s*(?:429|5\d\d):",  # _raise_for_non_200 output
    r"ConnectionError",  # requests/urllib3
    r"ReadTimeout|ConnectTimeout|Timeout",
]

# 5 seconds is generous enough for a USGS upstream replica to recover
# from a brief 5xx but short enough that CI doesn't drag.
_RETRY_DELAY_SEC = 5
_MAX_RERUNS = 2


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "requests_mock" in item.fixturenames:
            continue
        item.add_marker(
            pytest.mark.flaky(
                reruns=_MAX_RERUNS,
                reruns_delay=_RETRY_DELAY_SEC,
                only_rerun=_TRANSIENT_PATTERNS,
            )
        )
