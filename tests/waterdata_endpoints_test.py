"""Live monitors for the API version each Water Data family serves.

``waterdata/endpoints.py`` puts a version in the OGC, STAC and statistics
paths. An old version keeps responding after USGS publishes a new one, so no
other test fails when that happens. (Samples and NGWMN have no version segment.)

Two conditions are checked, in separate tests for OGC and STAC and in one
test for statistics:

- **A new version is available**: the family's default is not the version the
  package requests.
- **The version the package requests stopped working**: it no longer returns
  data.

The OGC and STAC roots publish a ``self`` link naming their default version,
so the version is read from it. OGC cannot be probed, because it responds with
200 and an empty body for any version segment (checked 2026-09-22). Statistics
has no root document, so it is probed; a missing statistics version responds
with 404.
"""

import re

import httpx
import pytest

from dataretrieval.waterdata import endpoints

#: The families whose root document names the default version and that have a
#: ``/collections`` endpoint, with the function that builds the requested URL.
_FAMILIES = {
    "ogcapi": endpoints.ogc_api_url,
    "stac": endpoints.ratings_catalog_url,
}

#: A version segment anywhere in a path: ``/v0``, ``/v12/``, ``/v1?f=json``.
_VERSION_RE = re.compile(r"/(v\d+)(?=[/?#]|$)")

#: Fail a hung request before the scheduled job's own timeout does.
_TIMEOUT = 60


def _split_version(url: str) -> tuple[str, str]:
    """Split *url* into its unversioned root and its version segment."""
    match = _VERSION_RE.search(url)
    assert match is not None, f"no version segment in {url!r}"
    return url[: match.start()] + "/", match.group(1)


def _served_version(root: str) -> str:
    """The version in *root*'s ``self`` link, e.g. ``.../ogcapi/v1?f=json``."""
    response = httpx.get(root, timeout=_TIMEOUT, follow_redirects=True)
    response.raise_for_status()
    links = response.json().get("links") or []
    self_links = [link["href"] for link in links if link.get("rel") == "self"]
    assert self_links, f"{root} published no self link: {links}"
    return _split_version(self_links[0])[1]


@pytest.mark.live
@pytest.mark.parametrize("family", sorted(_FAMILIES))
def test_service_default_is_the_version_this_package_requests(family):
    root, requested = _split_version(_FAMILIES[family]())
    served = _served_version(root)

    assert served == requested, (
        f"the {family} API now serves {served} by default; this package requests "
        f"{requested}. Move the pin in waterdata/endpoints.py to {served}, after "
        f"checking the {served} release notes for dropped or renamed fields."
    )


@pytest.mark.live
def test_statistics_has_published_no_version_beyond_the_one_we_request():
    """``/statistics/vN`` responds with 404 even when vN exists, so the probe
    requests ``/statistics/vN/docs``."""
    url = endpoints.statistics_api_url()
    root, current = _split_version(url)
    following = f"v{int(current.removeprefix('v')) + 1}"

    assert httpx.get(f"{url}/docs", timeout=_TIMEOUT).status_code == 200, (
        f"the statistics service stopped serving {current}, which this package "
        "requests; check what replaced it."
    )

    probe = httpx.get(f"{root}{following}/docs", timeout=_TIMEOUT)
    assert probe.status_code == 404, (
        f"the statistics service now responds to {following}/docs with HTTP "
        f"{probe.status_code}; check whether the package should move to "
        f"{following}, and whether the service publishes a root document from "
        "which the version can be read instead of probed."
    )


@pytest.mark.live
@pytest.mark.parametrize("family", sorted(_FAMILIES))
def test_the_version_this_package_requests_still_returns_data(family):
    """Checked on content, not status: OGC responds with 200 and an empty body
    for a version that does not exist."""
    url = _FAMILIES[family]()
    response = httpx.get(f"{url}/collections", timeout=_TIMEOUT)
    response.raise_for_status()
    assert response.content and response.json().get("collections"), (
        f"{url}/collections returned no collections, so the "
        f"{_split_version(url)[1]} {family} API this package requests has stopped "
        "serving. Move the pin in waterdata/endpoints.py to the version the "
        "service now serves."
    )
