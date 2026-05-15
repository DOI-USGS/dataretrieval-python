"""Auto-retry live-API tests on transient upstream errors.

After PR #273, paginated ``waterdata`` getters propagate mid-walk HTTP
errors (429 / 5xx / connection drops) instead of silently truncating —
the right behavior, but it makes any CI test that calls the live USGS
Water Data API susceptible to flaking on a transient upstream blip
(e.g. the HTTP 502 Bad Gateway that broke CI on PR #273's merge).

Scope: ``waterdata`` only. PR #273 specifically fixed the pagination
loops in ``dataretrieval.waterdata.utils`` (``_walk_pages`` /
``get_stats_data``); other modules (``wqp``, ``samples``, ``nadp``,
``streamstats``, ``nldi``, ``nwis``) reach the network through
different paths with their own error semantics and don't share PR
#273's regression.

Selection: a test is "live" iff (a) its file path is under
``tests/waterdata`` (other modules' tests are out of scope), AND (b)
it does not request the ``requests_mock`` fixture, AND (c) its
function body references one of the waterdata public user-facing
getters (the ``_PUBLIC_GETTERS`` set). The file-path scope is
necessary because some getter names (``get_ratings``, ``get_daily``)
collide with legacy ``nwis`` function names; without the scope, a
test of the legacy ``nwis.get_ratings`` would be retried under
waterdata's transient-error patterns. The function-body check skips
pure unit tests of internal helpers (``_get_args``, ``_check_*``,
``_normalize_*``, ``_construct_api_requests``) that share the test
file with live tests.

Retry: live tests are retried up to twice on a 5-second backoff,
but only when the failure trace matches a narrow transient-upstream
pattern. Library bugs raising other exception types still fail on
the first try.
"""

import inspect
import re

import pytest

# Public, network-doing entry points of the waterdata module.
# Other modules' getters are intentionally out of scope (see module
# docstring). Add new waterdata getters here.
_PUBLIC_GETTERS = frozenset(
    {
        "get_channel",
        "get_combined_metadata",
        "get_continuous",
        "get_daily",
        "get_field_measurements",
        "get_field_measurements_metadata",
        "get_latest_continuous",
        "get_latest_daily",
        "get_monitoring_locations",
        "get_nearest_continuous",
        "get_peaks",
        "get_ratings",
        "get_reference_table",
        "get_time_series_metadata",
        "get_stats_por",
        "get_stats_date_range",
    }
)

_PUBLIC_GETTER_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(n) for n in _PUBLIC_GETTERS) + r")\b"
)

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
        # Scope: waterdata test files only. See module docstring for why
        # this file-path filter is needed in addition to the name check.
        if "tests/waterdata" not in item.nodeid.replace("\\", "/"):
            continue
        if "requests_mock" in item.fixturenames:
            continue
        try:
            src = inspect.getsource(item.function)
        except (OSError, TypeError):
            continue
        if not _PUBLIC_GETTER_RE.search(src):
            continue
        item.add_marker(
            pytest.mark.flaky(
                reruns=_MAX_RERUNS,
                reruns_delay=_RETRY_DELAY_SEC,
                only_rerun=_TRANSIENT_PATTERNS,
            )
        )
