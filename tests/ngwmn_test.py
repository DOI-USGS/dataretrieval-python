"""Live tests for the NGWMN OGC getters (``dataretrieval.waterdata.ngwmn``).

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
    df, md = ngwmn.get_sites(state_name="Wisconsin", limit=10)
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
