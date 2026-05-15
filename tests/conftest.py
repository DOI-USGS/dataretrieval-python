"""Test infrastructure shared across all test modules.

After #273 the paginated ``waterdata`` getters surface mid-walk HTTP
errors (429 / 5xx / connection drops) to the caller instead of silently
truncating the result. That's the correct behavior for users — but it
makes any CI test that hits the live USGS Water Data API susceptible to
flaking on a transient upstream blip (e.g. the 502 Bad Gateway that
broke CI on #273's merge to main).

This file:

* registers a ``live`` pytest marker;
* auto-applies it to every test that does **not** take ``requests_mock``
  as a fixture — the existing convention in this repo for mock-driven
  tests, so the marker tracks "this test hits the network" without
  needing to decorate 35 functions by hand;
* via ``pytest-rerunfailures``, configures live-marked tests to retry
  up to twice (5-second backoff) ONLY when the failure trace matches a
  transient-upstream pattern: ``429:`` / ``5xx:`` prefixes that
  ``_raise_for_non_200`` produces, plus ``ConnectionError`` / timeout
  shapes from the ``requests`` library.

Library bugs that raise unrelated exception types are NOT retried —
the regex set deliberately omits generic ``RuntimeError`` matches.
"""

import pytest

# Match anywhere in the failure traceback. Anchor-free because the
# trace embeds the exception message after a long preamble.
_TRANSIENT_PATTERNS = [
    r"RuntimeError:\s*(?:429|5\d\d):",  # _raise_for_non_200 output
    r"ConnectionError",  # requests/urllib3
    r"ReadTimeout|ConnectTimeout|Timeout",
    r"timed out",
    r"Bad Gateway|Service Unavailable|Gateway Timeout",
]

_MAX_RERUNS = 2
_RERUN_DELAY_SEC = 5


def pytest_configure(config):
    """Register the ``live`` marker so pytest doesn't warn about it."""
    config.addinivalue_line(
        "markers",
        "live: marks tests that hit the live USGS Water Data API; "
        "retried up to twice on transient upstream HTTP errors.",
    )


def pytest_collection_modifyitems(config, items):
    """Auto-mark live-API tests with ``live`` + ``flaky`` retry config.

    Heuristic: any test that does NOT request the ``requests_mock``
    fixture is treated as live. This catches every test in
    ``waterdata_test.py`` and similar modules without needing to
    decorate them individually, and it auto-tracks new tests written
    in the same style.
    """
    for item in items:
        if "requests_mock" in getattr(item, "fixturenames", ()):
            continue
        item.add_marker(pytest.mark.live)
        item.add_marker(
            pytest.mark.flaky(
                reruns=_MAX_RERUNS,
                reruns_delay=_RERUN_DELAY_SEC,
                only_rerun=_TRANSIENT_PATTERNS,
            )
        )
