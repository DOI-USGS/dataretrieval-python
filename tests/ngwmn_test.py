"""Live tests for the NGWMN OGC getters (``dataretrieval.ngwmn``).

These hit the live NGWMN OGC API (``api.waterdata.usgs.gov/ngwmn/ogcapi``),
mirroring the integration-test style of ``waterdata_test.py``. The
``flaky`` marker only retries transient transport errors, so a real
behavior change still fails on the first run.
"""

import sys

import pytest
from pandas import DataFrame

if sys.version_info < (3, 10):
    pytest.skip("Skip entire module on Python < 3.10", allow_module_level=True)

from dataretrieval import ngwmn
from dataretrieval.utils import BaseMetadata

pytestmark = pytest.mark.flaky(
    reruns=2,
    reruns_delay=5,
    only_rerun=[
        r"(?:RateLimited|RuntimeError):\s*(?:429|5\d\d):",
        r"Connect(ion)?Error",
        r"ReadTimeout|ConnectTimeout|Timeout",
    ],
)

# A site with water-level, construction, and lithology records (per the R
# dataRetrieval NGWMN examples), plus a non-USGS-agency id to exercise the
# multi-agency identifier format NGWMN uses.
_SITE = "USGS-272838082142201"
_LITH_SITE = "AKDNR-535134236016630"


def test_get_sites():
    df, md = ngwmn.get_sites(state="Wisconsin", limit=10)
    assert isinstance(df, DataFrame)
    assert isinstance(md, BaseMetadata)
    assert len(df) > 0
    assert "monitoring_location_id" in df.columns
    # All returned sites are in the requested state.
    assert df["state_name"].dropna().eq("Wisconsin").all()
    # Sites carry geometry by default.
    assert "geometry" in df.columns
    assert "ngwmn/ogcapi/collections/sites" in str(md.url)


def test_get_sites_skip_geometry():
    df, _ = ngwmn.get_sites(monitoring_location_id=_SITE, skip_geometry=True)
    assert isinstance(df, DataFrame)
    assert "geometry" not in df.columns


def test_get_water_level():
    df, md = ngwmn.get_water_level(monitoring_location_id=_SITE)
    assert isinstance(df, DataFrame)
    assert len(df) > 0
    assert "sample_time" in df.columns
    assert (df["monitoring_location_id"] == _SITE).all()


def test_get_water_level_datetime_subsets():
    full, _ = ngwmn.get_water_level(monitoring_location_id=_SITE)
    windowed, _ = ngwmn.get_water_level(
        monitoring_location_id=_SITE, datetime=["2022-01-01", "2024-01-01"]
    )
    # A bounded window returns a strict subset of the full record.
    assert 0 < len(windowed) < len(full)


def test_get_providers():
    df, md = ngwmn.get_providers(state="WI")
    assert isinstance(df, DataFrame)
    assert len(df) > 0
    assert {"agency_code", "organization_type", "state"}.issubset(df.columns)
    # Providers have no geometry.
    assert "geometry" not in df.columns


def test_get_sites_state_accepts_name_postal_or_fips():
    """The single ``state`` parameter accepts a full name, postal code, or FIPS
    code; ``_resolve_state`` normalizes all three to the full ``state_name`` the
    ``sites`` collection queries on, so every encoding returns the same sites."""
    by_name, _ = ngwmn.get_sites(state="Wisconsin", skip_geometry=True)
    by_postal, _ = ngwmn.get_sites(state="WI", skip_geometry=True)
    by_fips, _ = ngwmn.get_sites(state="55", skip_geometry=True)
    assert len(by_name) > 0
    ids = set(by_name["monitoring_location_id"])
    assert set(by_postal["monitoring_location_id"]) == ids
    assert set(by_fips["monitoring_location_id"]) == ids


def test_get_providers_state_accepts_name_postal_or_fips():
    """``get_providers`` likewise normalizes any encoding to the uppercase
    postal code the ``providers`` collection queries on."""
    by_postal, _ = ngwmn.get_providers(state="WI")
    by_name, _ = ngwmn.get_providers(state="Wisconsin")
    by_fips, _ = ngwmn.get_providers(state="55")
    assert len(by_postal) > 0
    agencies = set(by_postal["agency_code"])
    assert set(by_name["agency_code"]) == agencies
    assert set(by_fips["agency_code"]) == agencies


def test_state_queryables_still_diverge_upstream():
    """The NGWMN ``sites`` and ``providers`` collections expose DIFFERENT state
    queryables (``sites`` -> ``state_name`` full name; ``providers`` ->
    ``state`` 2-letter code). The single-``state`` shim in
    ``ngwmn._resolve_state`` exists ONLY to paper over that asymmetry.

    If this test fails, the upstream API has unified the two queryables and the
    shim (``_resolve_state``) can be removed in favor of a single pass-through
    parameter.
    """
    import httpx

    from dataretrieval.ngwmn import NGWMN_OGC_API_URL
    from dataretrieval.ogc.engine import _default_headers

    headers = _default_headers()

    def queryables(collection):
        resp = httpx.get(
            f"{NGWMN_OGC_API_URL}/collections/{collection}/queryables",
            headers=headers,
            timeout=60,
        )
        resp.raise_for_status()
        return set(resp.json().get("properties") or {})

    sites_q = queryables("sites")
    providers_q = queryables("providers")
    assert "state_name" in sites_q and "state" not in sites_q, sites_q
    assert "state" in providers_q and "state_name" not in providers_q, providers_q


def test_get_lithology():
    df, _ = ngwmn.get_lithology(monitoring_location_id=_LITH_SITE)
    assert isinstance(df, DataFrame)
    assert len(df) > 0
    assert (df["monitoring_location_id"] == _LITH_SITE).all()


def test_get_well_construction():
    df, _ = ngwmn.get_well_construction(monitoring_location_id=_SITE)
    assert isinstance(df, DataFrame)
    assert len(df) > 0
    assert (df["monitoring_location_id"] == _SITE).all()


def test_multi_site_chunks_and_unions():
    """A multi-value ``monitoring_location_id`` fans out and unions the
    per-site results (the comma-join multi-value path), returning at least
    the single-site total."""
    one, _ = ngwmn.get_water_level(monitoring_location_id=_SITE)
    many, _ = ngwmn.get_water_level(
        monitoring_location_id=[_SITE, "USGS-404159100494601"]
    )
    assert len(many) >= len(one)
