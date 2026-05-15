"""Auto-retry live-API tests on transient upstream errors.

After PR #273, paginated ``waterdata`` getters propagate mid-walk HTTP
errors (429 / 5xx / connection drops) instead of silently truncating —
the right behavior, but it makes any CI test that calls the live USGS
Water Data API susceptible to flaking on a transient upstream blip
(e.g. the HTTP 502 Bad Gateway that broke CI on PR #273's merge).

Selection: a test is "live" iff (a) it does not request the
``requests_mock`` fixture, AND (b) its function body references one of
the package's public user-facing getters (the ``_PUBLIC_GETTERS`` set).
This skips pure unit tests of internal helpers (``_get_args``,
``_check_*``, ``_normalize_*``, ``_construct_api_requests``) that
share the test file with live tests.

Retry: live tests are retried up to twice on a 5-second backoff,
but only when the failure trace matches a narrow transient-upstream
pattern. Library bugs raising other exception types still fail on
the first try.
"""

import inspect
import re

import pytest

# Public, network-doing entry points. The set is intentionally exhaustive
# so the source-inspection heuristic catches every test that exercises
# the library's user-facing API surface. Add new public getters here.
_PUBLIC_GETTERS = frozenset(
    {
        # waterdata
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
        # wqp / samples
        "get_results",
        "get_samples",
        "get_samples_summary",
        "what_activities",
        "what_activity_metrics",
        "what_detection_limits",
        "what_habitat_metrics",
        "what_organizations",
        "what_project_weights",
        "what_projects",
        "what_sites",
        # nadp
        "get_annual_MDN_map",
        "get_annual_NTN_map",
        "get_zip",
        # streamstats / nldi
        "get_basin",
        "get_features",
        "get_features_by_data_source",
        "get_flowlines",
        "get_sample_watershed",
        "get_watershed",
        # nwis (legacy)
        "get_discharge_measurements",
        "get_discharge_peaks",
        "get_dv",
        "get_gwlevels",
        "get_info",
        "get_iv",
        "get_pmcodes",
        "get_qwdata",
        "get_record",
        "get_stats",
        "get_water_use",
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
