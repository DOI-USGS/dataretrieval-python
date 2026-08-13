"""Opt-in live conformance checks for OGC and shared pagination adapters.

These tests deliberately use small, fixed queries. They are deselected by the
project's default ``-m 'not live'`` configuration and run only in the scheduled
live workflow or explicitly with ``pytest -m live``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx
import pandas as pd
import pytest
from geopandas import GeoDataFrame

from dataretrieval import ngwmn, parallel_chunks, waterdata, wateruse

if TYPE_CHECKING:
    from dataretrieval.utils import BaseMetadata

pytestmark = pytest.mark.live

_WATERDATA_SITES = ["USGS-01646500", "USGS-05427718"]
_NGWMN_SITES = ["USGS-272838082142201", "USGS-404159100494601"]


def _assert_spatial(frame: pd.DataFrame) -> None:
    assert isinstance(frame, GeoDataFrame)
    assert frame.geometry.name == "geometry"
    assert frame.crs is not None
    assert frame.crs.to_epsg() == 4326


def _assert_metadata(metadata: BaseMetadata) -> None:
    assert metadata.url
    assert metadata.query_time.total_seconds() >= 0


def _assert_serial_parallel_equal(
    serial: pd.DataFrame,
    parallel: pd.DataFrame,
    *,
    id_column: str,
) -> None:
    """Compare fan-out values without conflating equivalent null sentinels."""
    assert type(serial) is type(parallel)
    assert list(serial.columns) == list(parallel.columns)

    left = serial.sort_values(id_column).reset_index(drop=True)
    right = parallel.sort_values(id_column).reset_index(drop=True)
    if isinstance(left, GeoDataFrame):
        assert left.crs == right.crs
        assert left.geometry.equals(right.geometry)
        left = left.drop(columns=left.geometry.name)
        right = right.drop(columns=right.geometry.name)

    left = left.astype(object).where(left.notna(), None)
    right = right.astype(object).where(right.notna(), None)
    pd.testing.assert_frame_equal(left, right, check_dtype=False)


def _record_live_requests(monkeypatch) -> list[httpx.Request]:
    """Capture real outgoing requests while preserving the live transport."""
    requests: list[httpx.Request] = []
    original_send = httpx.AsyncClient.send

    async def recording_send(self, request, *args, **kwargs):
        requests.append(request)
        return await original_send(self, request, *args, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "send", recording_send)
    return requests


def test_waterdata_spatial_and_skip_geometry_live():
    """The same spatial collection honors both requested frame families."""
    spatial, spatial_md = waterdata.get_monitoring_locations(
        monitoring_location_id=_WATERDATA_SITES[0]
    )
    tabular, tabular_md = waterdata.get_monitoring_locations(
        monitoring_location_id=_WATERDATA_SITES[0], skip_geometry=True
    )

    assert len(spatial) == len(tabular) == 1
    _assert_spatial(spatial)
    assert type(tabular) is pd.DataFrame
    assert "geometry" not in tabular.columns
    _assert_metadata(spatial_md)
    _assert_metadata(tabular_md)


def test_waterdata_nonspatial_reference_live():
    """Reference collections stay plain DataFrames on the raw-feature path."""
    frame, metadata = waterdata.get_reference_table(
        "parameter-codes", query={"id": "00060"}
    )

    assert type(frame) is pd.DataFrame
    assert frame["parameter_code"].tolist() == ["00060"]
    assert "geometry" not in frame.columns
    _assert_metadata(metadata)


def test_waterdata_empty_spatial_schema_live():
    """An empty spatial result is schema-complete and remains geospatial."""
    frame, metadata = waterdata.get_monitoring_locations(
        monitoring_location_id="USGS-999999999999999"
    )

    assert frame.empty
    _assert_spatial(frame)
    assert "monitoring_location_id" in frame.columns
    _assert_metadata(metadata)


def test_waterdata_pagination_and_max_rows_live():
    """A one-row page is followed once, then the raw-feature cap stops paging."""
    frame, metadata = waterdata.get_daily(
        monitoring_location_id=_WATERDATA_SITES[0],
        parameter_code="00060",
        time="2024-01-01/2024-01-03",
        limit=1,
        max_rows=2,
    )

    assert len(frame) == 2
    _assert_spatial(frame)
    _assert_metadata(metadata)


def test_waterdata_cql_post_parallel_chunks_live(monkeypatch):
    """A serial CQL2 POST and two-chunk fan-out return equivalent frames."""
    requests = _record_live_requests(monkeypatch)
    serial, _ = waterdata.get_monitoring_locations(
        monitoring_location_id=_WATERDATA_SITES
    )
    serial_item_requests = [
        request
        for request in requests
        if "/collections/monitoring-locations/items" in str(request.url)
    ]
    assert len(serial_item_requests) == 1
    assert serial_item_requests[0].method == "POST"
    requests.clear()

    monkeypatch.setenv("API_USGS_CONCURRENT", "2")
    with parallel_chunks(2):
        parallel, metadata = waterdata.get_monitoring_locations(
            monitoring_location_id=_WATERDATA_SITES
        )

    item_requests = [
        request
        for request in requests
        if "/collections/monitoring-locations/items" in str(request.url)
    ]
    assert len(item_requests) == 2
    _assert_serial_parallel_equal(
        serial,
        parallel,
        id_column="monitoring_location_id",
    )
    _assert_spatial(parallel)
    _assert_metadata(metadata)


def test_ngwmn_spatial_parallel_chunks_live(monkeypatch):
    """NGWMN site fan-out equals the serial completed spatial frame."""
    serial, _ = ngwmn.get_sites(monitoring_location_id=_NGWMN_SITES)

    requests = _record_live_requests(monkeypatch)
    monkeypatch.setenv("API_USGS_CONCURRENT", "2")
    with parallel_chunks(2):
        parallel, metadata = ngwmn.get_sites(monitoring_location_id=_NGWMN_SITES)

    item_requests = [
        request
        for request in requests
        if "/collections/sites/items" in str(request.url)
    ]
    assert len(item_requests) == 2
    assert all(request.method == "GET" for request in item_requests)
    _assert_serial_parallel_equal(
        serial,
        parallel,
        id_column="monitoring_location_id",
    )
    _assert_spatial(parallel)
    _assert_metadata(metadata)


def test_ngwmn_paginates_providers_live():
    """A partial second providers page exercises NGWMN next links."""
    frame, metadata = ngwmn.get_providers(state="WI", limit=35)

    assert len(frame) > 35
    assert type(frame) is pd.DataFrame
    assert "geometry" not in frame.columns
    _assert_metadata(metadata)


def test_ngwmn_providers_are_nonspatial_live():
    """An exact NGWMN provider query stays a plain DataFrame."""
    frame, metadata = ngwmn.get_providers(agency_code="USGS", limit=100)

    assert not frame.empty
    assert type(frame) is pd.DataFrame
    assert "geometry" not in frame.columns
    _assert_metadata(metadata)


def test_ngwmn_geometry_free_observations_live():
    """Observation features without geometry remain a plain DataFrame."""
    frame, metadata = ngwmn.get_water_level(
        monitoring_location_id=_NGWMN_SITES[0],
        limit=10000,
    )

    assert not frame.empty
    assert type(frame) is pd.DataFrame
    assert "geometry" not in frame.columns
    _assert_metadata(metadata)


def test_ngwmn_empty_spatial_live():
    """An empty NGWMN site result preserves its spatial frame contract."""
    frame, metadata = ngwmn.get_sites(monitoring_location_id="USGS-999999999999999")

    assert frame.empty
    _assert_spatial(frame)
    assert "monitoring_location_id" in frame.columns
    _assert_metadata(metadata)


def test_statistics_shared_paginator_live():
    """Statistics retains the shared paginator's default DataFrame path."""
    frame, metadata = waterdata.get_stats_por(
        monitoring_location_id=_WATERDATA_SITES[0],
        parameter_code="00060",
        computation_type="arithmetic_mean",
        normal_type="MOY",
        page_size=1,
    )

    assert not frame.empty
    assert isinstance(frame, pd.DataFrame)
    _assert_metadata(metadata)


def test_ratings_shared_paginator_live():
    """Ratings pages STAC search frames, then retrieves one small asset."""
    ratings = waterdata.get_ratings(
        monitoring_location_id="USGS-01104475",
        file_type="exsa",
        limit=1,
    )

    assert ratings
    assert all(type(frame) is pd.DataFrame for frame in ratings.values())
    assert all(not frame.empty for frame in ratings.values())


def test_wateruse_shared_paginator_live():
    """NWDC Water Use retains the shared paginator's DataFrame combiner."""
    frame, metadata = wateruse.get_wateruse(
        model="wu-public-supply-wd",
        variable="pswdtot",
        huc="010900020502",
        time_resolution="monthly",
        start_date="2020-01",
        end_date="2020-12",
        limit=1,
    )

    assert not frame.empty
    assert type(frame) is pd.DataFrame
    assert "huc12_id" in frame.columns
    _assert_metadata(metadata)


def test_wateruse_list_fan_out_live(monkeypatch):
    """NWDC Water Use combines two list-shaped location chunks."""
    monkeypatch.setenv("API_USGS_CONCURRENT", "2")
    frame, metadata = wateruse.get_wateruse(
        model="wu-public-supply-wd",
        variable="pswdtot",
        state=["RI", "DE"],
        time_resolution="monthly",
        start_date="2020-01",
        end_date="2020-01",
        limit=600,
    )

    assert not frame.empty
    assert type(frame) is pd.DataFrame
    assert "huc12_id" in frame.columns
    _assert_metadata(metadata)
