"""Offline unit tests for dataretrieval.waterdata.xarray converters.

These exercise the pure DataFrame -> xarray.Dataset converters with synthetic
frames, so they run without network access. Live end-to-end behavior is
covered by the waterdata getters' own tests.
"""

from types import SimpleNamespace

import pandas as pd
import pytest

xr = pytest.importorskip("xarray")
from dataretrieval.waterdata import xarray as wdx  # noqa: E402


def _meta(url="https://example.test/items"):
    return SimpleNamespace(url=url)


def _daily_frame(
    time_series_id="A",
    site="USGS-1",
    values=(100, 110),
    times=("2024-06-01", "2024-06-02"),
):
    n = len(values)
    return pd.DataFrame(
        {
            "time": list(times),
            "value": list(values),
            "monitoring_location_id": [site] * n,
            "parameter_code": ["00060"] * n,
            "statistic_id": ["00003"] * n,
            "unit_of_measure": ["ft^3/s"] * n,
            "qualifier": [None] * n,
            "approval_status": ["Approved"] * n,
            "time_series_id": [time_series_id] * n,
        }
    )


# series_meta is keyed by parameter_code and supplies only the readable name;
# units/statistic/parameter_code come from the values frame itself.
_DISCHARGE_META = {
    "00060": {
        "parameter_name": "Discharge",
        "parameter_description": "Discharge, cubic feet per second",
    }
}


def _temp_frame(times=("2024-06-02", "2024-06-03"), values=(18.0, 19.0)):
    """A water-temperature (00010, instantaneous) frame, parallel to _daily_frame."""
    n = len(values)
    return pd.DataFrame(
        {
            "time": list(times),
            "value": list(values),
            "monitoring_location_id": ["USGS-1"] * n,
            "parameter_code": ["00010"] * n,
            "statistic_id": ["00011"] * n,
            "unit_of_measure": ["degC"] * n,
            "qualifier": [None] * n,
            "approval_status": ["Approved"] * n,
            "time_series_id": ["B"] * n,
        }
    )


_TEMP_META = {
    "00010": {
        "parameter_name": "Temperature, water",
        "parameter_description": "Temperature, water, degrees Celsius",
    }
}


def test_build_timeseries_cf_attributes():
    ds = wdx._build_dense(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert isinstance(ds, xr.Dataset)
    assert "discharge" in ds.data_vars
    v = ds["discharge"]
    assert v.attrs["long_name"] == "Discharge, cubic feet per second"
    assert v.attrs["units"] == "ft3 s-1"  # UDUNITS-normalized from "ft^3/s"
    assert v.attrs["cell_methods"] == "time: mean"
    assert v.attrs["standard_name"] == "water_volume_transport_in_river_channel"
    assert v.attrs["usgs_parameter_code"] == "00060"
    assert v.attrs["usgs_statistic_id"] == "00003"
    # dataset-level provenance
    assert ds.attrs["Conventions"] == "CF-1.11"
    assert ds.attrs["featureType"] == "timeSeries"
    assert ds.attrs["references"] == "https://example.test/items"
    # site is the DSG instance dimension
    assert ds["monitoring_location_id"].attrs.get("cf_role") == "timeseries_id"
    assert ds.sizes["time"] == 2


def test_ancillary_variables_linked():
    ds = wdx._build_dense(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert "discharge_qualifier" in ds.data_vars
    assert "discharge_approval_status" in ds.data_vars
    assert ds["discharge"].attrs["ancillary_variables"] == (
        "discharge_qualifier discharge_approval_status"
    )


def test_scalarize_handles_list_array_nested_and_scalar():
    import numpy as np

    assert wdx._scalarize(["A", "e"]) == "A e"
    assert wdx._scalarize(("A", "e")) == "A e"
    assert wdx._scalarize(np.array(["A", "e"])) == "A e"  # numpy array, not list
    assert wdx._scalarize([]) is None  # empty -> missing
    assert wdx._scalarize(["A", None]) == "A"  # missing element dropped
    assert wdx._scalarize("A") == "A"  # scalar passes through
    assert wdx._scalarize(None) is None
    # a nested element must not raise (array-truth pitfall); it is stringified
    assert isinstance(wdx._scalarize([["A", "e"], "z"]), str)


def test_list_valued_qualifier_is_flattened_to_string():
    # The API returns ``qualifier`` as a list of codes per observation; the
    # ancillary variable must be flattened to a netCDF-encodable string (object
    # arrays of lists can't be written), empty lists -> missing.
    df = _daily_frame()
    df["qualifier"] = [["A", "e"], []]  # multi-code list, then empty list
    for build in (wdx._build_ragged, wdx._build_dense):
        ds = build(df, _meta(), service="daily", series_meta=_DISCHARGE_META)
        qual = ds["qualifier"] if "qualifier" in ds else ds["discharge_qualifier"]
        flat = qual.values.ravel().tolist()
        assert not any(isinstance(v, (list, tuple)) for v in flat)  # no list cells
        assert "A e" in flat  # multi-code list joined with a space


def test_unknown_unit_passes_through_verbatim():
    df = _daily_frame()
    df["unit_of_measure"] = "widgets/s"  # units are read from the frame
    ds = wdx._build_dense(df, _meta(), service="daily", series_meta=_DISCHARGE_META)
    assert ds["discharge"].attrs["units"] == "widgets/s"


def test_missing_standard_name_is_omitted():
    # parameter_code with no curated CF mapping -> no standard_name key
    meta = {"99999": {"parameter_name": "Mystery", "parameter_description": "Mystery"}}
    df = _daily_frame()
    df["parameter_code"] = "99999"
    ds = wdx._build_dense(df, _meta(), service="daily", series_meta=meta)
    assert "standard_name" not in ds["mystery"].attrs
    assert ds["mystery"].attrs["usgs_parameter_code"] == "99999"


def test_vertical_datum_distinguishes_stage_parameters():
    # 00065 (gage height) and 63160 (water level above NAVD88) share the CF
    # standard_name water_surface_height_above_reference_datum; the vertical_datum
    # attribute records the differing reference datum so they're distinguishable.
    for pcode, datum in (("00065", "local site datum"), ("63160", "NAVD88")):
        df = _daily_frame()
        df["parameter_code"] = pcode
        ds = wdx._build_ragged(df, _meta(), service="continuous", series_meta={})
        v = ds["value"]
        assert v.attrs["standard_name"] == "water_surface_height_above_reference_datum"
        assert v.attrs["vertical_datum"] == datum
    # a parameter with no datum mapping gets no vertical_datum attr
    ds = wdx._build_ragged(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert "vertical_datum" not in ds["value"].attrs


def test_multiple_parameters_outer_join_on_time():
    # discharge at t1,t2 ; temperature at t2,t3 -> union time, NaN fill
    q = _daily_frame(values=(100, 110), times=("2024-06-01", "2024-06-02"))
    t = _temp_frame()
    meta = {**_DISCHARGE_META, **_TEMP_META}
    ds = wdx._build_dense(
        pd.concat([q, t]), _meta(), service="continuous", series_meta=meta
    )
    assert {"discharge", "temperature_water"} <= set(ds.data_vars)
    assert ds.sizes["time"] == 3  # union of {t1,t2,t3}
    # temperature has no value at t1 -> NaN
    assert pd.isna(ds["temperature_water"].sel(time="2024-06-01").item())
    # cell_methods derived from statistic_id 00011 (instantaneous) -> point
    assert ds["temperature_water"].attrs["cell_methods"] == "time: point"


def test_collision_dedups_with_warning():
    # two values for the same (site, parameter, statistic, time) are ambiguous
    # without the hash key -> keep the smallest (deterministically) and warn;
    # site stays the dim. (test_dense_collision_dedup_is_order_independent pins
    # the order-independence; here 100 is both the first and the smallest.)
    a = _daily_frame(values=(100,), times=("2024-06-01",))
    b = _daily_frame(values=(200,), times=("2024-06-01",))
    with pytest.warns(UserWarning, match="multiple values per"):
        ds = wdx._build_dense(
            pd.concat([a, b]), _meta(), service="daily", series_meta=_DISCHARGE_META
        )
    assert "monitoring_location_id" in ds.dims
    assert ds.sizes["time"] == 1
    assert (
        ds["discharge"].sel(monitoring_location_id="USGS-1", time="2024-06-01").item()
        == 100
    )


def test_empty_frame_returns_dataset_with_conventions():
    ds = wdx._build_dense(pd.DataFrame(), _meta(), service="daily", series_meta={})
    assert isinstance(ds, xr.Dataset)
    assert list(ds.data_vars) == []
    assert ds.attrs["Conventions"] == "CF-1.11"


def test_build_stats_flat_dataset():
    df = pd.DataFrame(
        {
            "monitoring_location_id": ["USGS-1", "USGS-1"],
            "parameter_code": ["00060", "00060"],
            "month": [1, 2],
            "p50_va": [120.0, 130.0],
        }
    )
    ds = wdx._build_stats(df, _meta(), "statistics")
    assert isinstance(ds, xr.Dataset)
    assert "p50_va" in ds.data_vars
    assert ds.attrs["Conventions"] == "CF-1.11"
    # A flat percentile table is not a CF discrete-sampling geometry, so it must
    # NOT advertise featureType=timeSeries (which would mislead cf-xarray/CF
    # tooling into treating it as a timeseries DSG).
    assert "featureType" not in ds.attrs


def test_stats_empty_omits_feature_type():
    # The empty-stats path must also stay free of the timeSeries featureType.
    ds = wdx._build_stats(pd.DataFrame(), _meta(), "statistics")
    assert list(ds.data_vars) == []
    assert "featureType" not in ds.attrs
    # the series builders still advertise the DSG featureType
    series = wdx._build_ragged(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert series.attrs["featureType"] == "timeSeries"


def test_build_stats_drops_hash_columns():
    # The plain getters no longer drop hash IDs, so the flat stats builder must
    # drop the stats service's per-record / per-series UUIDs itself to keep the
    # CF dataset free of opaque coordinates.
    df = pd.DataFrame(
        {
            "monitoring_location_id": ["USGS-1"],
            "parameter_code": ["00060"],
            "computation_id": ["7d70379f-8452-44cd-b026-24dfa11f8503"],
            "parent_time_series_id": ["9cca880dec4846ec8cbdd05f3e22603e"],
            "time_series_id": ["b026-24dfa11f8503"],
            "p50_va": [120.0],
        }
    )
    ds = wdx._build_stats(df, _meta(), "statistics")
    for hash_col in ("computation_id", "parent_time_series_id", "time_series_id"):
        assert hash_col not in ds.variables
    assert "p50_va" in ds.data_vars


def test_ragged_omits_hash_columns():
    # The synthetic daily frame carries a time_series_id hash column; the ragged
    # builder whitelists the columns it converts, so the hash never surfaces in
    # the dataset (the timeseries path stays hash-free without the getter's
    # help).
    ds = wdx._build_ragged(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert "time_series_id" not in ds.variables


def test_public_wrappers_exist_and_are_documented():
    for name in [n for n in wdx.__all__ if n.startswith("get_")]:
        fn = getattr(wdx, name)
        assert callable(fn)
        # _make_getter carries the wrapped getter's name through, so the public
        # symbol, its __name__, and the docstring reference all agree.
        assert fn.__name__ == name
        assert "xarray wrapper" in (fn.__doc__ or "")
        assert f"dataretrieval.waterdata.{name}" in (fn.__doc__ or "")


def test_fetch_strips_include_hash():
    # The xarray path never surfaces hash columns, so include_hash must be
    # dropped before the underlying getter is called (no wasted fetch).
    captured = {}

    def fake_getter(*args, **kwargs):
        captured.update(kwargs)
        return "df", "meta"

    df, meta = wdx._fetch(
        fake_getter, (), {"include_hash": True, "parameter_code": "00060"}
    )
    assert (df, meta) == ("df", "meta")
    assert "include_hash" not in captured
    assert captured == {"parameter_code": "00060"}


def test_every_wrapper_routes_through_fetch(monkeypatch):
    # Pin the wiring: each public wrapper must delegate the underlying fetch to
    # _fetch (which is what strips include_hash). Guards against a wrapper
    # quietly reverting to calling the getter directly and leaking the flag.
    seen = []

    def spy(func, args, kwargs):
        seen.append(dict(kwargs))
        return pd.DataFrame(), SimpleNamespace(url=None)  # empty -> no network

    monkeypatch.setattr(wdx, "_fetch", spy)
    for name in [n for n in wdx.__all__ if n.startswith("get_")]:
        seen.clear()
        ds = getattr(wdx, name)(monitoring_location_id="USGS-1", include_hash=True)
        assert isinstance(ds, xr.Dataset)
        assert len(seen) == 1, f"{name} did not route through _fetch"
        # the wrapper hands include_hash to _fetch; _fetch is what drops it
        # (asserted in test_fetch_strips_include_hash)
        assert seen[0].get("include_hash") is True


def test_unparseable_time_dropped_with_warning():
    # A bad/missing time must be dropped explicitly (with a warning), not
    # silently swallowed by to_xarray.
    df = _daily_frame(
        values=(100, 110, 120),
        times=("2024-06-01", "not-a-date", "2024-06-03"),
    )
    with pytest.warns(UserWarning, match="unparseable or missing time"):
        ds = wdx._build_dense(df, _meta(), service="daily", series_meta=_DISCHARGE_META)
    assert ds.sizes["time"] == 2  # the bad-time row is gone, the good ones stay
    assert 110 not in ds["discharge"].values  # the dropped value did not survive


def test_all_unparseable_time_returns_empty_dataset():
    df = _daily_frame(values=(1, 2), times=("bad-a", "bad-b"))
    with pytest.warns(UserWarning, match="unparseable or missing time"):
        ds = wdx._build_dense(df, _meta(), service="daily", series_meta=_DISCHARGE_META)
    assert list(ds.data_vars) == []
    assert ds.attrs["Conventions"] == "CF-1.11"


def test_mixed_units_in_one_variable_warns():
    # Same (parameter, statistic) across two sites but different units -> one
    # variable can hold only one units attr; warn instead of silently mislabeling.
    a = _daily_frame(site="USGS-1", values=(100,), times=("2024-06-01",))
    b = _daily_frame(site="USGS-2", values=(3,), times=("2024-06-01",))
    b["unit_of_measure"] = "m3 s-1"
    with pytest.warns(UserWarning, match="spans multiple units"):
        ds = wdx._build_dense(
            pd.concat([a, b]), _meta(), service="daily", series_meta=_DISCHARGE_META
        )
    assert ds.sizes["monitoring_location_id"] == 2
    assert ds["discharge"].attrs["units"] == "ft3 s-1"  # first unit (ft^3/s)


def test_point_coords_from_list_geometry():
    # Without geopandas the OGC geometry is a plain [lon, lat] list, not a
    # shapely Point. lon/lat must still be extracted (regression: the old
    # ``.x``/``.y`` access raised AttributeError and silently dropped them).
    df = _daily_frame()
    df["geometry"] = [[-90.44, 43.19]] * len(df)
    ds = wdx._build_dense(df, _meta(), service="daily", series_meta=_DISCHARGE_META)
    assert "longitude" in ds.coords and "latitude" in ds.coords
    assert ds["longitude"].sel(monitoring_location_id="USGS-1").item() == -90.44
    assert ds["latitude"].sel(monitoring_location_id="USGS-1").item() == 43.19
    assert ds["longitude"].attrs["units"] == "degrees_east"
    assert ds["latitude"].attrs["standard_name"] == "latitude"


def test_point_coords_from_pointlike_geometry():
    # The shapely-Point path (geopandas installed) still works: any object
    # exposing .x/.y is read directly.
    df = _daily_frame()
    df["geometry"] = [SimpleNamespace(x=-90.44, y=43.19)] * len(df)
    ds = wdx._build_dense(df, _meta(), service="daily", series_meta=_DISCHARGE_META)
    assert ds["longitude"].sel(monitoring_location_id="USGS-1").item() == -90.44
    assert ds["latitude"].sel(monitoring_location_id="USGS-1").item() == 43.19


def test_non_point_geometry_skipped():
    # A non-point geometry (no .x/.y, not a [lon, lat] pair) is skipped, not
    # guessed -- no lon/lat coords are added.
    df = _daily_frame()
    df["geometry"] = [object()] * len(df)
    ds = wdx._build_dense(df, _meta(), service="daily", series_meta=_DISCHARGE_META)
    assert "longitude" not in ds.coords and "latitude" not in ds.coords


def test_date_modified_from_last_modified():
    # The newest last_modified becomes the dataset-level date_modified (ACDD).
    df = _daily_frame()
    df["last_modified"] = ["2024-06-01T00:00:00Z", "2024-06-10T12:00:00Z"]
    ds = wdx._build_dense(df, _meta(), service="daily", series_meta=_DISCHARGE_META)
    assert ds.attrs["date_modified"].startswith("2024-06-10")


def test_no_date_modified_without_last_modified():
    # The default frame has no last_modified column -> no date_modified attr.
    ds = wdx._build_dense(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert "date_modified" not in ds.attrs


def test_site_metadata_coordinates():
    # HUC / state_name ride along on the metadata call and become
    # instance-indexed auxiliary coordinates.
    site_meta = {
        "USGS-1": {"hydrologic_unit_code": "07070005", "state_name": "Wisconsin"}
    }
    ds = wdx._build_dense(
        _daily_frame(),
        _meta(),
        service="daily",
        series_meta=_DISCHARGE_META,
        site_meta=site_meta,
    )
    huc = ds["hydrologic_unit_code"].sel(monitoring_location_id="USGS-1").item()
    assert huc == "07070005"
    assert ds["state_name"].sel(monitoring_location_id="USGS-1").item() == "Wisconsin"
    assert ds["hydrologic_unit_code"].attrs["long_name"] == "hydrologic unit code (HUC)"


def test_site_metadata_absent_adds_no_coords():
    # No site_meta -> no HUC/state coords (back-compat with the old signature).
    ds = wdx._build_dense(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert "hydrologic_unit_code" not in ds.coords
    assert "state_name" not in ds.coords


# --- ragged layout (the default) -------------------------------------------


def test_ragged_structure_and_cf_attrs():
    # Single (site, parameter, statistic) -> one instance; value attrs are set
    # because the descriptors are homogeneous across the (one) instance.
    ds = wdx._build_ragged(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert set(ds.sizes) == {"obs", "timeseries"}
    assert ds.sizes == {"obs": 2, "timeseries": 1}
    assert ds["value"].dims == ("obs",)
    assert list(ds["value"].values) == [100, 110]
    assert ds["row_size"].dims == ("timeseries",)
    assert int(ds["row_size"][0]) == 2
    assert ds["row_size"].attrs["sample_dimension"] == "obs"
    # per-instance identity + cf_role on the synthesized timeseries_id
    assert ds["monitoring_location_id"].dims == ("timeseries",)
    assert ds["timeseries_id"].attrs["cf_role"] == "timeseries_id"
    assert ds["timeseries_id"].values[0] == "USGS-1:00060:00003"
    # homogeneous descriptors land on value
    v = ds["value"]
    assert v.attrs["long_name"] == "Discharge, cubic feet per second"
    assert v.attrs["units"] == "ft3 s-1"
    assert v.attrs["cell_methods"] == "time: mean"
    assert v.attrs["standard_name"] == "water_volume_transport_in_river_channel"
    assert v.attrs["ancillary_variables"] == "qualifier approval_status"
    assert ds.attrs["featureType"] == "timeSeries"
    # ancillary flags are per-observation; metadata is per-instance
    assert ds["qualifier"].dims == ("obs",)
    assert ds["approval_status"].dims == ("obs",)
    assert ds["parameter_code"].dims == ("timeseries",)
    assert ds["parameter_code"].values.tolist() == ["00060"]
    assert ds["statistic_id"].values.tolist() == ["00003"]
    assert ds["unit_of_measure"].values.tolist() == ["ft^3/s"]
    assert ds["parameter_code"].attrs["long_name"] == "USGS parameter code"


def test_ragged_stores_only_real_observations():
    # Two sites of very different length: obs == sum of lengths, no NaN fill.
    a = _daily_frame(
        site="USGS-A",
        values=(1, 2, 3),
        times=("2024-06-01", "2024-06-02", "2024-06-03"),
    )
    b = _daily_frame(site="USGS-B", values=(9,), times=("2024-06-03",))
    ds = wdx._build_ragged(
        pd.concat([a, b]), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert ds.sizes == {"obs": 4, "timeseries": 2}  # 3 + 1, not 2 x 3 grid
    assert not pd.isna(ds["value"].values).any()
    assert sorted(ds["row_size"].values.tolist()) == [1, 3]


def test_ragged_mixed_parameters_value_is_generic():
    # Mixed parameters/units -> value carries no single units/standard_name;
    # the per-instance parameter_code coordinate disambiguates.
    q = _daily_frame(values=(100, 110), times=("2024-06-01", "2024-06-02"))
    t = _temp_frame()
    meta = {**_DISCHARGE_META, **_TEMP_META}
    ds = wdx._build_ragged(
        pd.concat([q, t]), _meta(), service="continuous", series_meta=meta
    )
    assert ds.sizes == {"obs": 4, "timeseries": 2}
    assert ds["value"].attrs["long_name"] == "measured value"
    assert "units" not in ds["value"].attrs  # ft^3/s vs degC -> not homogeneous
    assert "standard_name" not in ds["value"].attrs
    assert set(ds["parameter_code"].values) == {"00060", "00010"}


def test_ragged_keeps_duplicate_observations_without_warning():
    # The dense path warns + dedups colliding (site, time); ragged just keeps
    # both observations -- no grid, so no ambiguity. Assert specifically that
    # neither dense-path diagnostic fires (not "no warning at all", so an
    # unrelated pandas warning can't fail the test for the wrong reason).
    a = _daily_frame(values=(100,), times=("2024-06-01",))
    b = _daily_frame(values=(200,), times=("2024-06-01",))
    import warnings as _w

    with _w.catch_warnings(record=True) as caught:
        _w.simplefilter("always")
        ds = wdx._build_ragged(
            pd.concat([a, b]), _meta(), service="daily", series_meta=_DISCHARGE_META
        )
    msgs = [str(w.message) for w in caught]
    assert not any(
        "multiple values per" in m or "spans multiple units" in m for m in msgs
    ), msgs
    assert ds.sizes["obs"] == 2
    assert sorted(ds["value"].values.tolist()) == [100, 200]


def test_ragged_lonlat_and_date_modified_per_instance():
    df = _daily_frame()
    df["geometry"] = [[-90.44, 43.19]] * len(df)
    df["last_modified"] = ["2024-06-01T00:00:00Z", "2024-06-10T12:00:00Z"]
    ds = wdx._build_ragged(df, _meta(), service="daily", series_meta=_DISCHARGE_META)
    assert ds["longitude"].dims == ("timeseries",)
    assert float(ds["longitude"][0]) == -90.44
    assert float(ds["latitude"][0]) == 43.19
    assert ds.attrs["date_modified"].startswith("2024-06-10")


def _instance_blocks(ds):
    """Map each instance's (parameter, statistic) -> (time-sorted values, unit)
    by walking the row_size offsets -- the reader's view of a ragged array."""
    starts, acc = [], 0
    for n in ds["row_size"].values.tolist():
        starts.append(acc)
        acc += int(n)
    starts.append(acc)
    blocks = {}
    for i in range(ds.sizes["timeseries"]):
        sl = slice(starts[i], starts[i + 1])
        key = (str(ds["parameter_code"].values[i]), str(ds["statistic_id"].values[i]))
        blocks[key] = (
            ds["value"].values[sl].tolist(),
            str(ds["unit_of_measure"].values[i]),
        )
    return blocks


def test_ragged_alignment_survives_interleaved_input():
    # The critical invariant: row_size, the contiguous obs blocks, and the
    # per-instance metadata must all stay aligned even when the input rows are
    # interleaved across instances and out of time order. A regression in the
    # sort/group ordering would silently map values to the wrong series.
    cols = [
        "monitoring_location_id",
        "parameter_code",
        "statistic_id",
        "unit_of_measure",
        "time",
        "value",
    ]
    # Values are deliberately NON-monotonic with time within each instance, so
    # a regression that ordered obs by value (instead of time) would produce a
    # different sequence and fail -- "sorted values" alone wouldn't catch that.
    rows = [
        ("USGS-1", "00060", "00003", "ft^3/s", "2024-06-02", 100),  # later, smaller
        ("USGS-1", "00010", "00011", "degC", "2024-06-01", 19),
        ("USGS-1", "00060", "00001", "ft^3/s", "2024-06-03", 500),
        ("USGS-1", "00060", "00003", "ft^3/s", "2024-06-01", 150),  # earlier, larger
        ("USGS-1", "00010", "00011", "degC", "2024-06-03", 18),
        ("USGS-1", "00060", "00001", "ft^3/s", "2024-06-01", 480),
    ]
    df = pd.DataFrame(rows, columns=cols)
    ds = wdx._build_ragged(df, _meta(), service="daily", series_meta={})

    assert ds.sizes == {"obs": 6, "timeseries": 3}
    assert int(ds["row_size"].sum()) == ds.sizes["obs"]
    blocks = _instance_blocks(ds)
    # each instance's values + unit are the ones that belong to it, in TIME order
    # (not value order: 150 precedes 100, and 19 precedes 18)
    assert blocks[("00060", "00003")] == ([150, 100], "ft^3/s")
    assert blocks[("00060", "00001")] == ([480, 500], "ft^3/s")
    assert blocks[("00010", "00011")] == ([19, 18], "degC")


def test_ragged_field_schema_without_statistic():
    # The field-measurements schema groups by parameter_code only (no
    # statistic_id): the instance has no statistic segment, and the service
    # default cell method fills in.
    df = pd.DataFrame(
        {
            "monitoring_location_id": ["USGS-1", "USGS-1"],
            "parameter_code": ["00060", "00060"],
            "time": ["2024-06-01", "2024-06-02"],
            "value": [100, 110],
            "unit_of_measure": ["ft^3/s", "ft^3/s"],
        }
    )
    ds = wdx._build_ragged(
        df,
        _meta(),
        service="field-measurements",
        schema=wdx._FIELD,
        series_meta=_DISCHARGE_META,
        default_cell_method="point",
    )
    assert ds.sizes == {"obs": 2, "timeseries": 1}
    assert "statistic_id" not in ds.coords
    assert ds["timeseries_id"].values[0] == "USGS-1:00060"  # no stat segment
    assert ds["value"].attrs["cell_methods"] == "time: point"
    assert ds["value"].attrs["long_name"] == "Discharge, cubic feet per second"


# --- select_series (label-based selection on the ragged layout) -------------


def _two_instance_ragged():
    """Ragged Dataset with two instances at one site: 00060 and 00010."""
    meta = {**_DISCHARGE_META, **_TEMP_META}
    return wdx._build_ragged(
        pd.concat([_daily_frame(), _temp_frame()]),
        _meta(),
        service="continuous",
        series_meta=meta,
    )


def test_select_series_returns_time_indexed_single_series():
    ds = _two_instance_ragged()
    s = wdx.select_series(ds, monitoring_location_id="USGS-1", parameter_code="00060")
    # time is now a real dimension (not the flat obs axis)
    assert set(s.sizes) == {"time"}
    assert s["value"].dims == ("time",)
    assert list(s["value"].values) == [100, 110]
    # the series identity rides along as scalar coordinates
    assert str(s["parameter_code"].values) == "00060"
    assert str(s["unit_of_measure"].values) == "ft^3/s"
    # ancillary flags follow the series; row_size is dropped
    assert "approval_status" in s.data_vars
    assert "row_size" not in s.variables
    # .sel(time=...) works now that time is the dimension
    assert s["value"].sel(time="2024-06-01").item() == 100


def test_dense_same_parameter_two_statistics_no_bare_name():
    # 00060 under both 00001 (max) and 00003 (mean): the bare 'discharge' name is
    # ambiguous, so BOTH variables are disambiguated by their cell method -- no
    # order-dependent bare 'discharge' that silently means one of them.
    mx = _daily_frame(values=(500,), times=("2024-06-01",))
    mx["statistic_id"] = "00001"
    mn = _daily_frame(values=(100,), times=("2024-06-01",))
    ds = wdx._build_dense(
        pd.concat([mx, mn]), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert "discharge" not in ds.data_vars  # no bare (ambiguous) name
    assert {"discharge_maximum", "discharge_mean"} <= set(ds.data_vars)
    assert ds["discharge_maximum"].attrs["cell_methods"] == "time: maximum"
    assert ds["discharge_mean"].attrs["cell_methods"] == "time: mean"


def test_dense_single_statistic_keeps_bare_name():
    # The common single-statistic case keeps the clean bare name.
    ds = wdx._build_dense(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert "discharge" in ds.data_vars


def test_select_series_matches_nan_instance_key():
    # An instance whose key is null (samples with no sample_fraction) must be
    # selectable by passing None, since `== NaN` never matches.
    df = _samples_frame()
    df["Result_SampleFraction"] = None
    ds = _samples_ds(df)
    s = wdx.select_series(ds, characteristic="Temperature, water", sample_fraction=None)
    assert set(s.sizes) == {"time"}
    assert "value" in s.data_vars


def test_select_series_ambiguous_raises():
    # selecting by site alone matches both instances -> ask for more keys
    ds = _two_instance_ragged()
    with pytest.raises(ValueError, match="2 time series match"):
        wdx.select_series(ds, monitoring_location_id="USGS-1")


def test_select_series_no_match_raises():
    ds = _two_instance_ragged()
    with pytest.raises(KeyError, match="no time series matches"):
        wdx.select_series(ds, parameter_code="99999")


def test_select_series_unknown_key_raises():
    ds = _two_instance_ragged()
    with pytest.raises(KeyError, match="not a per-series identity coordinate"):
        wdx.select_series(ds, bogus="x")
    # descriptor coords (lon/lat/unit/HUC/state) are not selectable identity keys
    with pytest.raises(KeyError, match="not a per-series identity coordinate"):
        wdx.select_series(ds, unit_of_measure="ft^3/s")


def test_select_series_on_dense_raises_helpful_error():
    # On a dense Dataset, parameters are named variables; select_series points
    # the user at ds[name].sel(...) instead of failing cryptically.
    dense = wdx._build_dense(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    with pytest.raises(ValueError, match="expects a ragged Dataset"):
        wdx.select_series(dense, monitoring_location_id="USGS-1")


def test_to_awkward_missing_dependency_raises_informative(monkeypatch):
    # awkward is NOT a dependency; calling to_awkward without it must raise a
    # clear, actionable error rather than a bare ImportError. (Simulated so the
    # test holds whether or not awkward happens to be installed.)
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "awkward":
            raise ModuleNotFoundError("No module named 'awkward'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    ds = wdx._build_ragged(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    with pytest.raises(ModuleNotFoundError, match="pip install awkward"):
        wdx.to_awkward(ds)


def test_to_awkward_converts_ragged_to_jagged_records():
    ak = pytest.importorskip("awkward")
    ds = _two_instance_ragged()  # two series at USGS-1: 00060 and 00010
    arr = wdx.to_awkward(ds)
    assert len(arr) == ds.sizes["timeseries"]  # one record per series
    # scalar identity fields + jagged observation fields
    assert {"monitoring_location_id", "parameter_code", "value", "time"} <= set(
        arr.fields
    )
    # faithful: per-series lengths == row_size, total obs preserved, no fill
    assert ak.num(arr.value).tolist() == ds["row_size"].values.tolist()
    assert int(ak.sum(ak.num(arr.value))) == ds.sizes["obs"]
    # per-series reductions vectorize across all series at once
    means = ak.mean(arr.value, axis=1)
    assert len(means) == len(arr)


def test_to_awkward_on_dense_raises():
    pytest.importorskip("awkward")
    dense = wdx._build_dense(
        _daily_frame(), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    with pytest.raises(ValueError, match="expects a ragged Dataset"):
        wdx.to_awkward(dense)


# --- ragged opt-out wiring --------------------------------------------------


def test_wrapper_defaults_to_dense_and_ragged_opt_out(monkeypatch):
    # Default wrapper output is the (site, time) grid; dense=False opts into the
    # ragged array.
    monkeypatch.setattr(wdx, "_fetch", lambda func, a, k: (_daily_frame(), _meta()))
    monkeypatch.setattr(wdx._TS_CACHE, "lookup", lambda site_ids: (_DISCHARGE_META, {}))

    dense = wdx.get_daily(monitoring_location_id="USGS-1")
    assert "discharge" in dense.data_vars
    assert "obs" not in dense.sizes

    ragged = wdx.get_daily(monitoring_location_id="USGS-1", dense=False)
    assert "obs" in ragged.sizes and "value" in ragged.data_vars
    assert "discharge" not in ragged.data_vars


def test_series_wrapper_signature_advertises_dense_and_dataset_return():
    # functools.wraps would otherwise carry over the getter's DataFrame
    # signature and hide ``dense``; _make_getter publishes an accurate one.
    # dense defaults to True (the grid is the default; dense=False -> ragged).
    import inspect

    sig = inspect.signature(wdx.get_daily)
    assert "monitoring_location_id" in sig.parameters  # real args stay visible
    dense = sig.parameters["dense"]
    assert dense.kind is inspect.Parameter.KEYWORD_ONLY
    assert dense.default is True
    assert "Dataset" in str(sig.return_annotation)
    assert wdx.get_daily.__annotations__["return"] == "xarray.Dataset"


def test_no_dense_wrappers_omit_dense_from_signature():
    # stats + samples have no dense layout, so ``dense`` must not be advertised
    # (the wrapper still accept-and-ignores it; see the dense-warning tests).
    import inspect

    for name in ("get_stats_por", "get_samples"):
        sig = inspect.signature(getattr(wdx, name))
        assert "dense" not in sig.parameters, name
        assert "Dataset" in str(sig.return_annotation), name


# --- water-quality samples --------------------------------------------------


def _samples_frame(characteristics=("Temperature, water",), units=("deg C",)):
    rows = []
    for ch, u in zip(characteristics, units):
        rows.append(
            {
                "Location_Identifier": "USGS-1",
                "Activity_StartDateTime": "2020-07-10T12:00:00Z",
                "Result_Characteristic": ch,
                "Result_SampleFraction": "Total",
                "Result_Measure": 12.5,
                "Result_MeasureUnit": u,
                "Result_ResultDetectionCondition": None,
                "Result_MeasureStatusIdentifier": "Provisional",
            }
        )
    return pd.DataFrame(rows)


def _samples_ds(frame):
    """Build a samples ragged Dataset the way the get_samples service does."""
    return wdx._build_ragged(
        frame,
        _meta(),
        service="samples",
        schema=wdx._SAMPLES,
        default_cell_method="point",
    )


def test_samples_surface_lonlat_from_location_columns():
    # Samples carry position as Location_Latitude/Location_Longitude (no OGC
    # geometry); the dataset must still get numeric longitude/latitude coords.
    frame = _samples_frame()
    frame["Location_Longitude"] = [-90.44]
    frame["Location_Latitude"] = [43.19]
    ds = _samples_ds(frame)
    assert "longitude" in ds.coords and "latitude" in ds.coords
    assert ds["longitude"].dtype.kind == "f"
    assert float(ds["longitude"].values[0]) == -90.44
    assert float(ds["latitude"].values[0]) == 43.19
    assert ds["longitude"].attrs["units"] == "degrees_east"


def test_build_samples_single_characteristic():
    ds = _samples_ds(_samples_frame())
    assert set(ds.sizes) == {"obs", "timeseries"}
    assert ds["value"].dims == ("obs",)
    assert ds["characteristic"].values[0] == "Temperature, water"
    assert ds["value"].attrs["long_name"] == "Temperature, water"
    assert ds["value"].attrs["cell_methods"] == "time: point"
    # censoring columns are ancillary, linked from value
    assert "detection_condition" in ds.variables and "status" in ds.variables
    assert ds["value"].attrs["ancillary_variables"] == "detection_condition status"
    assert ds["timeseries_id"].attrs["cf_role"] == "timeseries_id"


def test_build_samples_mixed_characteristics_generic_value():
    ds = _samples_ds(
        _samples_frame(("Temperature, water", "pH"), ("deg C", "std units"))
    )
    assert ds.sizes["timeseries"] == 2
    assert ds["value"].attrs["long_name"] == "measured value"
    assert set(ds["characteristic"].values) == {"Temperature, water", "pH"}


def test_get_samples_wrapper_builds_ragged(monkeypatch):
    monkeypatch.setattr(wdx, "_fetch", lambda func, a, k: (_samples_frame(), _meta()))
    ds = wdx.get_samples(monitoringLocationIdentifier="USGS-1", include_hash=True)
    assert "obs" in ds.sizes and "value" in ds.data_vars
    assert ds["characteristic"].values[0] == "Temperature, water"


def test_prepare_values_missing_required_column_returns_empty():
    # A frame lacking a mandatory column (here: no value) must degrade to an
    # empty Dataset, not raise KeyError from the column slim.
    df = _daily_frame().drop(columns=["value"])
    ds = wdx._build_ragged(df, _meta(), service="daily", series_meta=_DISCHARGE_META)
    assert isinstance(ds, xr.Dataset)
    assert list(ds.data_vars) == []
    assert ds.attrs["Conventions"] == "CF-1.11"


def test_build_samples_missing_value_returns_empty():
    # A non-result Samples profile (no Result_Measure -> no "value") must not
    # crash; it has nothing to convert.
    df = pd.DataFrame(
        {
            "Location_Identifier": ["USGS-1"],
            "Activity_StartDateTime": ["2020-07-10T12:00:00Z"],
            "Result_Characteristic": ["pH"],
        }
    )
    ds = _samples_ds(df)
    assert list(ds.data_vars) == []
    assert ds.attrs["Conventions"] == "CF-1.11"


def test_get_samples_ignores_dense_with_warning(monkeypatch):
    # dense=True is advertised generically; get_samples is always ragged, so it
    # must accept-and-ignore (with a warning) rather than leak dense= to the
    # underlying getter (which would TypeError).
    monkeypatch.setattr(wdx, "_fetch", lambda func, a, k: (_samples_frame(), _meta()))
    with pytest.warns(UserWarning, match="no dense layout"):
        ds = wdx.get_samples(monitoringLocationIdentifier="USGS-1", dense=True)
    assert "obs" in ds.sizes  # still ragged


def test_get_stats_ignores_dense_with_warning(monkeypatch):
    # The stats layout has no dense grid either; dense=True is ignored with the
    # same warning (consistent with samples), not silently swallowed.
    frame = pd.DataFrame({"monitoring_location_id": ["USGS-1"], "p50_va": [120.0]})
    monkeypatch.setattr(wdx, "_fetch", lambda func, a, k: (frame, _meta()))
    with pytest.warns(UserWarning, match="no dense layout"):
        ds = wdx.get_stats_por(monitoring_location_id="USGS-1", dense=True)
    assert isinstance(ds, xr.Dataset)


def test_timeseries_id_skips_missing_instance_key():
    # A NaN instance key (e.g. a characteristic with no sample fraction) must
    # not render as a literal "nan" token in the cf_role timeseries_id.
    df = _samples_frame()
    df["Result_SampleFraction"] = None
    ds = _samples_ds(df)
    assert all("nan" not in tid for tid in ds["timeseries_id"].values)
    assert ds["timeseries_id"].values[0] == "USGS-1:Temperature, water"


# --- metadata-lookup resilience + cache control -----------------------------


def test_metadata_lookup_failure_degrades_with_warning():
    # The metadata lookup is supplementary (only the readable name + site
    # descriptors); a fetch failure must warn and return empty rather than
    # discard already-fetched data -- and must NOT cache the failure, so a
    # later recovered call retries.
    def boom(monitoring_location_id):
        raise RuntimeError("network down")

    cache = wdx._MetadataCache(boom)
    with pytest.warns(UserWarning, match="metadata lookup failed"):
        param_meta, site_meta = cache.lookup(["USGS-1"])
    assert param_meta == {} and site_meta == {}
    assert len(cache) == 0  # failure not cached -> retryable


def test_wrapper_survives_metadata_failure(monkeypatch):
    # End to end on the DEFAULT (dense) layout: a failing metadata endpoint still
    # yields a CF dataset built from the data. With no metadata name the variable
    # falls back to the parameter code -- never the literal "nan" -- and only the
    # metadata-sourced long_name is missing.
    monkeypatch.setattr(wdx, "_fetch", lambda func, a, k: (_daily_frame(), _meta()))

    def boom(monitoring_location_id):
        raise RuntimeError("network down")

    monkeypatch.setattr(wdx._TS_CACHE, "_getter", boom)
    wdx._TS_CACHE.clear()
    with pytest.warns(UserWarning, match="metadata lookup failed"):
        ds = wdx.get_daily(monitoring_location_id="USGS-1")  # default (dense)
    assert "00060" in ds.data_vars  # observations survived under the code name
    assert "nan" not in ds.data_vars  # never a literal "nan" variable
    v = ds["00060"]
    assert "long_name" not in v.attrs  # name comes from metadata, which failed
    assert v.attrs["units"] == "ft3 s-1"  # units come from the frame


def test_nan_parameter_description_falls_back_to_name():
    # A present-but-null parameter_description (NaN is truthy) must not mask the
    # valid parameter_name; long_name falls back to the name.
    meta = {
        "00060": {"parameter_name": "Discharge", "parameter_description": float("nan")}
    }
    ds = wdx._build_dense(_daily_frame(), _meta(), service="daily", series_meta=meta)
    assert ds["discharge"].attrs["long_name"] == "Discharge"


def test_nan_parameter_name_uses_code_not_literal_nan():
    # A present-but-null parameter_name must fall back to the parameter code,
    # never produce a variable literally named "nan".
    meta = {"00060": {"parameter_name": float("nan")}}
    ds = wdx._build_dense(_daily_frame(), _meta(), service="daily", series_meta=meta)
    assert "nan" not in ds.data_vars
    assert "00060" in ds.data_vars


def test_dense_collision_dedup_is_order_independent():
    # Colliding (site, time) values must dedup deterministically (smallest),
    # independent of the upstream row order.
    a = _daily_frame(values=(100,), times=("2024-06-01",))
    b = _daily_frame(values=(200,), times=("2024-06-01",))
    kept = []
    for frame in (pd.concat([a, b]), pd.concat([b, a])):
        with pytest.warns(UserWarning, match="multiple values per"):
            ds = wdx._build_dense(
                frame, _meta(), service="daily", series_meta=_DISCHARGE_META
            )
        kept.append(
            ds["discharge"]
            .sel(monitoring_location_id="USGS-1", time="2024-06-01")
            .item()
        )
    assert kept == [100, 100]  # smallest value, both input orders


def test_dense_collision_kept_ancillary_is_order_independent():
    # Two rows collide on (site, time) with the SAME value but different
    # qualifiers; the retained flag must be deterministic (stable sort on value
    # then ancillary), not dependent on upstream row order.
    a = _daily_frame(values=(100,), times=("2024-06-01",))
    b = _daily_frame(values=(100,), times=("2024-06-01",))
    a["qualifier"] = ["X"]
    b["qualifier"] = ["Y"]
    kept = []
    for frame in (pd.concat([a, b]), pd.concat([b, a])):
        with pytest.warns(UserWarning, match="multiple values per"):
            ds = wdx._build_dense(
                frame, _meta(), service="daily", series_meta=_DISCHARGE_META
            )
        kept.append(
            ds["discharge_qualifier"]
            .sel(monitoring_location_id="USGS-1", time="2024-06-01")
            .item()
        )
    assert kept == ["X", "X"]  # smaller flag kept, both input orders


def test_partial_geometry_keeps_numeric_lonlat_coord():
    # When only some sites have point geometry, the lon/lat coordinate must stay
    # numeric (float, NaN-filled) -- not an object array with None, which is a
    # CF-invalid spatial coordinate.
    a = _daily_frame(site="USGS-A", values=(1, 2), times=("2024-06-01", "2024-06-02"))
    b = _daily_frame(site="USGS-B", values=(3, 4), times=("2024-06-01", "2024-06-02"))
    a["geometry"] = [[-90.44, 43.19]] * len(a)
    b["geometry"] = [None] * len(b)  # this site carries no point geometry
    ds = wdx._build_dense(
        pd.concat([a, b]), _meta(), service="daily", series_meta=_DISCHARGE_META
    )
    assert ds["longitude"].dtype.kind == "f"  # float, not object
    assert ds["longitude"].sel(monitoring_location_id="USGS-A").item() == -90.44
    assert pd.isna(ds["longitude"].sel(monitoring_location_id="USGS-B").item())


def test_metadata_cache_bounded_and_clearable():
    # The cache is bounded (FIFO eviction) so a long-running, many-site
    # process can't grow it without limit.
    def fake(monitoring_location_id):
        rows = [
            {
                "monitoring_location_id": s,
                "parameter_code": "00060",
                "parameter_name": f"name-{s}",
            }
            for s in monitoring_location_id
        ]
        return pd.DataFrame(rows), SimpleNamespace(url=None)

    cache = wdx._MetadataCache(fake, maxsize=3)
    for i in range(10):
        cache.lookup([f"S{i}"])
    assert len(cache) <= 3

    cache.lookup(["S99"])
    assert len(cache) > 0
    cache.clear()  # instance opt-out empties it
    assert len(cache) == 0

    # the public helper clears the real per-process caches
    wdx._TS_CACHE._entries["X"] = {"params": {}, "site": {}}
    wdx._FIELD_CACHE._entries["Y"] = {"params": {}, "site": {}}
    wdx.clear_metadata_cache()
    assert len(wdx._TS_CACHE) == 0 and len(wdx._FIELD_CACHE) == 0


def test_metadata_missing_site_is_not_negatively_cached():
    # A site the metadata endpoint returns nothing for must NOT be cached as an
    # empty entry (which would never be retried); a later call re-fetches it.
    calls = []

    def fake(monitoring_location_id):
        calls.append(list(monitoring_location_id))
        # respond only for S1, never for S2
        rows = [
            {
                "monitoring_location_id": s,
                "parameter_code": "00060",
                "parameter_name": s,
            }
            for s in monitoring_location_id
            if s == "S1"
        ]
        return pd.DataFrame(rows), SimpleNamespace(url=None)

    cache = wdx._MetadataCache(fake)
    cache.lookup(["S1", "S2"])
    cache.lookup(["S1", "S2"])
    # S1 cached (hit, not re-fetched); S2 never cached, so it is re-requested.
    assert calls[0] == ["S1", "S2"]
    assert calls[1] == ["S2"]  # only the still-uncached S2
    assert "S1" in cache._entries and "S2" not in cache._entries


def test_metadata_lookup_survives_within_batch_eviction():
    # A single pull whose site count exceeds maxsize must still return metadata
    # for every requested site, even though the bounded cache can't hold them all.
    sites = ["S0", "S1", "S2", "S3", "S4"]

    def fake(monitoring_location_id):
        rows = [
            {
                "monitoring_location_id": s,
                "parameter_code": f"p{s}",  # distinct per site
                "parameter_name": f"name-{s}",
                "hydrologic_unit_code": f"huc-{s}",
            }
            for s in monitoring_location_id
        ]
        return pd.DataFrame(rows), SimpleNamespace(url=None)

    cache = wdx._MetadataCache(fake, maxsize=2)
    param_meta, site_meta = cache.lookup(sites)
    # every requested site's metadata is in the result even though the bounded
    # cache evicted most of the just-fetched batch.
    assert {f"p{s}" for s in sites} <= set(param_meta)
    assert set(site_meta) == set(sites)
    assert len(cache) <= 2  # cache stayed bounded
