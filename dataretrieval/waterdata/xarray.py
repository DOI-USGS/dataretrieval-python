"""xarray-returning wrappers for the Water Data getters.

Each public function here mirrors the same-named function in
:mod:`dataretrieval.waterdata`, but returns a CF-conventions
:class:`xarray.Dataset` instead of a :class:`pandas.DataFrame`.

By default the data is returned on a CF ``(monitoring_location_id, time)``
grid (``featureType = "timeSeries"``): one named data variable per parameter
(``discharge``, ``temperature_water``, ...), NaN where a series has no
observation. This is the ergonomic layout -- ``ds["discharge"].sel(
monitoring_location_id=..., time=...)`` just works -- and
``monitoring_location_id`` is the instance dimension carrying
``cf_role = "timeseries_id"``. The cost is the union time axis and NaN fill,
which grow for large, very ragged multi-site collections.

Pass ``dense=False`` for the alternative CF *contiguous ragged array*: every
observation is concatenated along a single ``obs`` dimension, and each
(monitoring location, parameter, statistic) series is one instance along a
``timeseries`` dimension whose ``row_size`` records how many observations it
contributes. This stores only real observations -- no NaN fill -- so it
scales to large multi-site pulls where record lengths differ by decades.
Parameter, statistic, unit, and location identity become per-instance
metadata and ``time`` is a 1-D coordinate along ``obs``, so to select one
series use :func:`select_series` (or regroup the flat ``obs`` via the offsets
implied by ``row_size``, e.g. with ``cf-xarray``).

Either way the CF metadata is derived from columns the getter already
returns (``unit_of_measure`` -> ``units``, ``statistic_id`` ->
``cell_methods``, ``parameter_code`` -> ``standard_name``), plus the
human-readable parameter name from a small cached metadata lookup. The
timeseries identity carries ``cf_role = "timeseries_id"`` -- the synthesized
``timeseries_id`` coordinate when ragged, ``monitoring_location_id`` when
dense -- each site has ``longitude`` / ``latitude`` (and
``hydrologic_unit_code`` / ``state_name`` when the metadata call already
provides them), and dataset attributes carry ``Conventions``, provenance,
the request URL, and ``date_modified``.

Internally the conversion is organized around three small object roles:

* a :class:`_Schema` value object describes a service's column dialect;
* a :class:`_DatasetBuilder` strategy turns a values frame into a Dataset --
  the time-series :class:`_RaggedBuilder` / :class:`_DenseBuilder` (sharing a
  :class:`_SeriesBuilder` base) and the flat :class:`_StatsBuilder`;
* a :class:`_MetadataCache` memoizes the supplementary metadata lookup.

A :class:`_Service` record wires those together for each public getter.

This module requires the optional ``xarray`` dependency::

    pip install dataretrieval[xarray]
"""

from __future__ import annotations

import datetime as _dt
import inspect as _inspect
import re as _re
import threading as _threading
import warnings as _warnings
from collections import Counter as _Counter
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from functools import wraps as _wraps

import numpy as _np
import pandas as _pd

try:
    import xarray as _xr
except ModuleNotFoundError as _exc:  # pragma: no cover - exercised only sans xarray
    raise ModuleNotFoundError(
        "dataretrieval.waterdata.xarray requires the optional 'xarray' "
        "dependency. Install it with:  pip install dataretrieval[xarray]"
    ) from _exc

from . import api as _api
from .nearest import get_nearest_continuous as _get_nearest_continuous
from .types import (
    CF_CELL_METHODS,
    CF_STANDARD_NAMES,
    CF_UNIT_MAP,
    CF_VERTICAL_DATUM,
)

__all__ = [
    "clear_metadata_cache",
    "select_series",
    "to_awkward",
    "get_continuous",
    "get_daily",
    "get_field_measurements",
    "get_latest_continuous",
    "get_latest_daily",
    "get_nearest_continuous",
    "get_peaks",
    "get_samples",
    "get_stats_date_range",
    "get_stats_por",
]


# === constants =============================================================

# The CF vocabulary lookups (USGS units -> UDUNITS, statistic_id ->
# cell_methods operator, parameter_code -> standard_name) are plain data and
# live in ``types`` -- imported as CF_UNIT_MAP / CF_CELL_METHODS /
# CF_STANDARD_NAMES at the top of this module.

# Columns kept off the value pivot but surfaced as ancillary (flag) variables.
_ANCILLARY = ("qualifier", "approval_status")

# The monitoring-location ("site") column. A site is the DSG location; a
# timeseries *instance* is a site plus its group columns (parameter/statistic),
# so one site can host several instances.
_SITE = "monitoring_location_id"

# Only the human-readable name is sourced from the metadata endpoint; units,
# statistic, and parameter code all come from the values frame itself.
_NAME_DESCRIPTORS = ("parameter_name", "parameter_description")

# Per-site descriptors that ride along on the same metadata call (no extra
# request) and are surfaced as per-site auxiliary coordinates. Only
# fields the endpoint already returns are used -- station name/altitude live on
# the monitoring-locations endpoint and would need a separate call, so they are
# intentionally not fetched here.
_SITE_DESCRIPTORS = ("hydrologic_unit_code", "state_name")

# CF attributes for the per-site coordinates built from the descriptors above.
_SITE_COORD_ATTRS = {
    "hydrologic_unit_code": {"long_name": "hydrologic unit code (HUC)"},
    "state_name": {"long_name": "state name"},
}

# Upper bound on the per-process metadata cache (see :class:`_MetadataCache`).
_CACHE_MAXSIZE = 4096


# === stateless helpers =====================================================


def _slug(name) -> str:
    """A lower_snake_case, identifier-safe variable name."""
    s = _re.sub(r"[^0-9a-zA-Z]+", "_", str(name).strip().lower()).strip("_")
    return s or "value"


def _first_present(frame, col):
    """First non-null value of ``col`` if the column is present, else None."""
    if col not in frame:
        return None
    nonnull = frame[col].dropna()
    return nonnull.iloc[0] if len(nonnull) else None


def _unique_present(frame, col):
    """Distinct non-null values of ``col`` if present, else an empty list."""
    return frame[col].dropna().unique() if col in frame else []


def _is_missing(value):
    """True for None / a *scalar* NaN; False for any non-scalar (list, array).

    ``_pd.notna`` raises on a list/array (it returns an element-wise mask), so
    guard with ``is_scalar`` before testing -- callers pass cells that may be
    sequences.
    """
    return value is None or (_pd.api.types.is_scalar(value) and _pd.isna(value))


def _none_if_nan(value):
    """Collapse a pandas NaN to None.

    A NaN is *truthy*, so ``meta.get(col) or fallback`` keeps the NaN instead of
    falling back. Metadata frames yield NaN for a column that is present but null
    for a given parameter, so normalize it to None before any such ``or`` chain.
    """
    return None if _is_missing(value) else value


def _scalarize(cell):
    """Collapse a sequence-valued flag cell into a single string; pass scalars through.

    The Water Data API returns some ancillary (flag) columns -- notably
    ``qualifier`` -- as a *list* of codes per observation. xarray/netCDF cannot
    encode an object array whose elements are sequences, so join the codes into
    one space-separated string (an empty/all-missing sequence becomes None).
    Handles lists, tuples, and numpy arrays; missing elements are dropped
    element-by-element without assuming the element is scalar.
    """
    if isinstance(cell, (list, tuple, _np.ndarray)):
        parts = [str(v) for v in cell if not _is_missing(v)]
        return " ".join(parts) if parts else None
    return cell


def _sites(df):
    """Unique monitoring-location ids present in a values frame."""
    if _SITE in df:
        return df[_SITE].unique()
    return []


def _date_modified(df):
    """Most recent upstream record-refresh time in the frame, ISO-8601 or None.

    ``last_modified`` reflects when each record was last refreshed in the USGS
    database; the maximum is surfaced as the dataset's ``date_modified`` (ACDD)
    so a reader knows how current the pull is.
    """
    if df is None or "last_modified" not in getattr(df, "columns", ()):
        return None
    ts = _pd.to_datetime(df["last_modified"], errors="coerce", utc=True).dropna()
    return ts.max().isoformat() if len(ts) else None


def _lonlat(geom):
    """``(lon, lat)`` from a geometry, or None if it isn't a point.

    Geometry comes back as a shapely ``Point`` when geopandas is installed but
    as a plain ``[lon, lat]`` list when it is not (the OGC GeoJSON coordinates
    are flattened into a list). Handle both so the spatial coordinates survive
    either way; anything else (a polygon, a malformed cell) is skipped rather
    than guessed.
    """
    x, y = getattr(geom, "x", None), getattr(geom, "y", None)
    if x is not None and y is not None:
        return x, y
    if isinstance(geom, (list, tuple)) and len(geom) >= 2:
        try:
            return float(geom[0]), float(geom[1])
        except (TypeError, ValueError):
            return None
    return None


def _point_coords(df, site):
    """lon/lat dicts keyed by site, or None.

    Reads either a ``geometry`` column (the time-series getters' OGC response) or
    explicit ``longitude`` / ``latitude`` columns (the Samples profile, mapped via
    :data:`_SAMPLES_RENAME`) -- so every service surfaces station coordinates.
    """
    # Both sources reduce to a per-row "lonlat-able" value that _lonlat decodes
    # (a (lon, lat) tuple for the explicit columns, the geometry object
    # otherwise), so the dedup/loop/coercion scaffolding is shared.
    if {"longitude", "latitude"}.issubset(df.columns):
        subset = ["longitude", "latitude"]

        def _geoms(g):
            return list(zip(g["longitude"].to_numpy(), g["latitude"].to_numpy()))
    elif "geometry" in df.columns:
        subset = ["geometry"]

        def _geoms(g):
            return g["geometry"].to_numpy()
    else:
        return None

    geo = df.dropna(subset=subset).drop_duplicates(site)
    if geo.empty:
        return None
    lon, lat = {}, {}
    for site_id, geom in zip(geo[site].to_numpy(), _geoms(geo)):
        xy = _lonlat(geom)  # skips non-point / unparseable rather than guessing
        if xy is not None:
            lon[site_id], lat[site_id] = xy
    return (lon, lat) if lon else None


def _prepare_values(df, group_cols, ancillary_cols):
    """Slim the frame and coerce types, shared by the dense / ragged builders.

    Keeps only the columns we convert, parses ``time`` to naive-UTC
    ``datetime64`` (xarray has no tz dtype), coerces ``value`` to numeric, and
    drops rows whose ``time`` is unparseable/missing (with a warning, so
    observations are never lost without a trace). Returns
    ``(work, group_cols, ancillary, has_unit)`` filtered to columns present.
    """
    group_cols = [c for c in group_cols if c in df.columns]
    ancillary = [c for c in ancillary_cols if c in df.columns]
    has_unit = "unit_of_measure" in df.columns
    cols = [_SITE, "time", "value", *group_cols, *ancillary]
    if has_unit:
        cols.append("unit_of_measure")
    present = [c for c in dict.fromkeys(cols) if c in df.columns]
    work = df.loc[:, present].copy()
    # Instance id, time, and value are mandatory to build a series. A response
    # that lacks any of them (e.g. a non-result Samples profile) has nothing to
    # convert, so return an empty frame -> empty Dataset rather than KeyError.
    if not {_SITE, "time", "value"}.issubset(work.columns):
        return work.iloc[0:0], group_cols, ancillary, has_unit
    # ``format="ISO8601"`` matches the Water Data API timestamps and avoids
    # pandas' slow per-element ``dateutil`` fallback (and its warning) on large
    # frames; the tz is normalized to UTC then dropped (xarray has no tz dtype).
    work["time"] = _pd.to_datetime(
        work["time"], format="ISO8601", errors="coerce", utc=True
    ).dt.tz_localize(None)
    work["value"] = _pd.to_numeric(work["value"], errors="coerce")
    # Flatten any sequence-valued flag cells (e.g. ``qualifier``) to strings so
    # the ancillary variables stay netCDF-encodable. Only the columns that are
    # actually sequence-valued pay the per-row map -- a flag column is uniformly
    # typed, so the first non-null cell decides (skips the common scalar case).
    for c in ancillary:
        nonnull = work[c].dropna()
        if len(nonnull) and isinstance(nonnull.iloc[0], (list, tuple, _np.ndarray)):
            work[c] = work[c].map(_scalarize)
    n_before = len(work)
    work = work[work["time"].notna()]
    dropped = n_before - len(work)
    if dropped:
        _warnings.warn(
            f"dropped {dropped} row(s) with an unparseable or missing time.",
            stacklevel=3,
        )
    return work, group_cols, ancillary, has_unit


def _cf_units(unit):
    """Map a USGS unit string to its CF / UDUNITS form, or pass it through."""
    return CF_UNIT_MAP.get(str(unit), str(unit))


def _var_attrs(desc, *, unit, pcode, stat, default_cell_method):
    """Build the CF attribute dict for one data variable.

    ``unit``, ``pcode`` and ``stat`` are read from the values frame; ``desc``
    supplies only the human-readable name from the metadata lookup. Linking the
    ``ancillary_variables`` is left to each builder, since the flag-variable
    names follow the (layout-specific) data-variable name.
    """
    attrs: dict[str, str] = {}
    long_name = _none_if_nan(desc.get("parameter_description")) or _none_if_nan(
        desc.get("parameter_name")
    )
    if long_name:
        attrs["long_name"] = str(long_name)

    if unit is not None and _pd.notna(unit):
        attrs["units"] = _cf_units(unit)

    op = (
        CF_CELL_METHODS.get(str(stat)) if stat is not None and _pd.notna(stat) else None
    )
    op = op or default_cell_method
    if op:
        attrs["cell_methods"] = f"time: {op}"

    if pcode is not None and _pd.notna(pcode):
        sn = CF_STANDARD_NAMES.get(str(pcode))
        if sn:
            attrs["standard_name"] = sn
        datum = CF_VERTICAL_DATUM.get(str(pcode))
        if datum:
            attrs["vertical_datum"] = datum
        attrs["usgs_parameter_code"] = str(pcode)

    if stat is not None and _pd.notna(stat):
        attrs["usgs_statistic_id"] = str(stat)
    return attrs


def _dataset_attrs(service, base_meta, *, feature_type):
    """Dataset-level provenance (CF + ACDD) attributes.

    ``feature_type`` is the CF discrete-sampling ``featureType`` to advertise, or
    None to omit it (sourced from the builder's ``feature_type`` class attr).
    The series layouts are genuine ``timeSeries`` geometries; the preliminary
    flat stats table is not a DSG at all, so it passes None rather than mislabel
    itself.
    """
    attrs = {
        "Conventions": "CF-1.11",
        "institution": "U.S. Geological Survey",
        "source": f"USGS Water Data API ({service})",
        "history": (
            f"{_dt.datetime.now(_dt.timezone.utc).isoformat(timespec='seconds')} "
            "created by dataretrieval.waterdata.xarray"
        ),
    }
    if feature_type:
        attrs["featureType"] = feature_type
    url = getattr(base_meta, "url", None)
    if url:
        attrs["references"] = str(url)
    return attrs


def _empty_dataset(service, base_meta, *, feature_type):
    """An attribute-only Dataset for an empty / unconvertible response."""
    ds = _xr.Dataset()
    ds.attrs = _dataset_attrs(service, base_meta, feature_type=feature_type)
    return ds


# === metadata cache ========================================================


class _MetadataCache:
    """Per-process, bounded, thread-safe cache of supplementary metadata.

    Keyed by monitoring location, this memoizes the human-readable parameter
    name and a couple of site descriptors that the values getters don't return.
    ``getter`` is called as ``getter(monitoring_location_id=[...])`` and must
    return ``(DataFrame, metadata)``; one fetch is issued per not-yet-cached
    batch of sites and reused thereafter.

    The metadata is supplementary, so a fetch failure degrades to "no metadata"
    with a warning rather than discarding the already-retrieved observations,
    and the failed sites are left uncached so a later, recovered call retries
    them. The cache is bounded (oldest-inserted entries are evicted past
    ``maxsize``) and its mutation is serialized, so a long-running, many-site,
    multi-threaded process stays both correct and bounded.
    """

    def __init__(self, getter, *, maxsize=_CACHE_MAXSIZE):
        self._getter = getter
        self._maxsize = maxsize
        self._entries: dict[str, dict] = {}
        self._lock = _threading.Lock()

    def lookup(self, site_ids):
        """``(param_meta, site_meta)`` for ``site_ids``, fetching any misses.

        ``param_meta`` is ``{parameter_code: {name descriptors}}`` (keyed by the
        stable parameter code, so no hash id is needed) and ``site_meta`` is
        ``{monitoring_location_id: {site descriptors present in the response}}``.
        """
        sites = sorted({str(s) for s in site_ids if _pd.notna(s)})
        # Racy read of the keys is fine: a concurrent miss just re-fetches (the
        # fetch is idempotent); only the writes in _store take the lock.
        todo = [s for s in sites if s not in self._entries]
        fresh: dict[str, dict] = {}
        if todo:
            try:
                meta, _ = self._getter(monitoring_location_id=todo)
            except Exception as exc:  # supplementary lookup; never lose the data
                _warnings.warn(
                    f"metadata lookup failed ({exc!r}); returning the dataset "
                    "without parameter names / site descriptors.",
                    stacklevel=2,
                )
            else:
                fresh = self._parse(meta, todo)
                self._store(fresh)
        param_meta: dict[str, dict] = {}
        site_meta: dict[str, dict] = {}
        with self._lock:
            for s in sites:
                # Prefer this call's freshly-parsed entry over the cache: the
                # bounded cache may have already evicted just-fetched sites when a
                # single pull's ``todo`` exceeds maxsize, but the current call
                # must still see every site it fetched.
                entry = fresh.get(s) or self._entries.get(s, {})
                param_meta.update(entry.get("params", {}))
                if entry.get("site"):
                    site_meta[s] = entry["site"]
        return param_meta, site_meta

    def clear(self):
        """Empty the cache (release memory / force a refresh)."""
        with self._lock:
            self._entries.clear()

    def __len__(self):
        return len(self._entries)

    def _parse(self, meta, todo):
        """Parse ``meta`` into per-site ``{params, site}`` entries (lock-free)."""
        fresh = {s: {"params": {}, "site": {}} for s in todo}
        if not meta.empty:
            name_cols = [c for c in _NAME_DESCRIPTORS if c in meta.columns]
            site_cols = [c for c in _SITE_DESCRIPTORS if c in meta.columns]
            has_pcode = "parameter_code" in meta.columns
            # to_dict("records") is markedly faster than iterrows() (no per-row
            # Series boxing) and the dict access below is identical.
            for row in meta.to_dict("records"):
                site = row.get("monitoring_location_id")
                if site not in fresh:
                    continue
                if has_pcode:
                    pcode = row.get("parameter_code")
                    if not _is_missing(pcode):
                        # Normalize NaN -> None at the source so every consumer of
                        # the name descriptors gets a clean ``or``-able value.
                        fresh[site]["params"][str(pcode)] = {
                            c: _none_if_nan(row.get(c)) for c in name_cols
                        }
                if not fresh[site]["site"]:
                    desc = {
                        c: row.get(c) for c in site_cols if not _is_missing(row.get(c))
                    }
                    if desc:
                        fresh[site]["site"] = desc
        return fresh

    def _store(self, fresh):
        """Merge non-empty entries into the bounded cache (FIFO eviction).

        Sites that came back with no metadata are *not* cached, so a later call
        retries them rather than being stuck with a sticky empty result; the
        current call still sees them via the freshly-parsed ``fresh`` dict.
        """
        keep = {s: e for s, e in fresh.items() if e["params"] or e["site"]}
        if not keep:
            return
        with self._lock:
            self._entries.update(keep)
            while len(self._entries) > self._maxsize:
                self._entries.pop(next(iter(self._entries)))


# One cache per metadata endpoint, shared across getters for the process life.
_TS_CACHE = _MetadataCache(_api.get_time_series_metadata)
_FIELD_CACHE = _MetadataCache(_api.get_field_measurements_metadata)


def clear_metadata_cache():
    """Empty the per-process metadata caches (parameter names, site descriptors).

    The xarray getters cache the supplementary metadata lookup per monitoring
    location for the life of the process. Long-running services can call this to
    release memory or to force a refresh after the upstream metadata changed.
    """
    _TS_CACHE.clear()
    _FIELD_CACHE.clear()


def select_series(ds, **keys):
    """Extract one time series from a ragged-layout Dataset by label.

    The ragged layout (``dense=False``) stores each series as a contiguous block
    along ``obs`` located by ``row_size``, so ``time`` is a flat ``obs`` axis and
    a single series is addressed by its per-series coordinates --
    ``parameter_code``, ``monitoring_location_id``, ``statistic_id`` (or, for
    samples, ``characteristic`` / ``sample_fraction``) -- rather than a named
    variable::

        ds = wdx.get_daily(
            monitoring_location_id=[...], parameter_code=[...], dense=False
        )
        q = select_series(
            ds, monitoring_location_id="USGS-05407000", parameter_code="00060"
        )
        q["value"].sel(time="2023-07")  # time is now a real dimension

    This is the ragged-layout counterpart to ``ds[name].sel(...)`` on the default
    dense Dataset. It selects the one matching instance and returns its
    observations as a 1-D, time-indexed ``xarray.Dataset`` (``value`` plus any
    ancillary flag variables), with the series' identity carried as scalar
    coordinates. Raises ``KeyError`` if no instance matches and ``ValueError``
    if more than one does (add keys to disambiguate).
    """
    if "row_size" not in ds.variables or "obs" not in ds.dims:
        raise ValueError(
            "select_series expects a ragged Dataset (from dense=False); the "
            "default dense Dataset already exposes one variable per parameter, "
            "so select by name instead, e.g. "
            "ds[variable].sel(monitoring_location_id=...)."
        )
    # Selectable keys are the series *identity* coordinates only -- exclude the
    # per-series descriptors (lon/lat are a float-equality footgun; unit/HUC/state
    # are not series identifiers).
    descriptors = {"longitude", "latitude", "unit_of_measure", *_SITE_DESCRIPTORS}
    inst_coords = [
        c for c in ds.coords if ds[c].dims == ("timeseries",) and c not in descriptors
    ]
    mask = _np.ones(ds.sizes["timeseries"], dtype=bool)
    for key, value in keys.items():
        if key not in inst_coords:
            raise KeyError(
                f"{key!r} is not a per-series identity coordinate; choose from "
                f"{inst_coords}."
            )
        arr = ds[key].to_numpy()
        # NaN never equals anything, so match a missing instance key (e.g. a
        # characteristic with no sample fraction) by null-ness instead.
        mask &= _pd.isna(arr) if _is_missing(value) else (arr == value)
    matches = _np.flatnonzero(mask)
    if matches.size == 0:
        raise KeyError(f"no time series matches {keys}.")
    if matches.size > 1:
        raise ValueError(
            f"{matches.size} time series match {keys}; add more of "
            f"{inst_coords} to select exactly one."
        )
    i = int(matches[0])
    # Slice this instance's contiguous obs block (the CF sample_dimension link),
    # then promote time from an obs coordinate to the dimension.
    starts = _np.concatenate([[0], _np.cumsum(ds["row_size"].to_numpy())])
    block = slice(int(starts[i]), int(starts[i + 1]))
    series = ds.isel(timeseries=i, obs=block).swap_dims(obs="time")
    return series.drop_vars("row_size", errors="ignore")


def to_awkward(ds):
    """Convert a ragged (``dense=False``) Dataset to a per-series ``awkward.Array``.

    The CF contiguous-ragged layout (``row_size`` offsets + a flat ``obs``
    dimension) is structurally identical to awkward's jagged ``ListOffsetArray``,
    so this is a near-zero-copy re-view. Each timeseries instance becomes one
    record: its per-series identity/metadata as scalar fields
    (``monitoring_location_id`` / ``parameter_code`` / ``longitude`` / ...) plus
    a single ``obs`` field holding that series' observations as a
    variable-length list of ``{time, value, <flags>}`` records. No NaN fill, each
    series on its own time axis -- per-series operations then vectorize across
    every series at once, e.g. ``ak.mean(arr.obs.value, axis=1)``::

        ds = wdx.get_daily(..., dense=False)
        arr = wdx.to_awkward(ds)
        arr[0].monitoring_location_id  # one series' metadata
        arr.obs.value  # jagged values, all series
        ak.mean(arr.obs.value, axis=1)  # per-series means
        arr[arr.parameter_code == "00060"]  # filter series by metadata

    ``awkward`` is an optional dependency that is *not* installed with
    ``dataretrieval``; install it separately (``pip install awkward``).
    """
    try:
        import awkward as ak
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised only sans awkward
        raise ModuleNotFoundError(
            "to_awkward requires the optional 'awkward' dependency, which is not "
            "installed with dataretrieval. Install it with:  pip install awkward"
        ) from exc
    if "row_size" not in ds.variables or "obs" not in ds.dims:
        raise ValueError(
            "to_awkward expects a ragged Dataset (from dense=False); the default "
            "dense Dataset is already a (monitoring_location_id, time) grid."
        )
    counts = ds["row_size"].to_numpy()

    def _content(values):
        # awkward rejects numpy object dtype; route string/None columns through
        # from_iter (normalizing NaN -> missing), and keep numeric/datetime
        # content as numpy -- that part is the zero-copy re-view.
        if values.dtype == object:
            return ak.from_iter([_none_if_nan(v) for v in values.tolist()])
        return values

    # Per-series (timeseries-dim) coords -> scalar identity fields. Obs-dim
    # variables/coords -> a single jagged ``obs`` field whose elements are the
    # observation records, so each series is "metadata + a list of observations"
    # rather than several parallel jagged fields. ``row_size`` is the offsets,
    # already encoded by the unflatten.
    scalars, obs_fields = {}, {}
    for name in (*ds.data_vars, *ds.coords):
        if name == "row_size":
            continue
        da = ds[name]
        if da.dims == ("timeseries",):
            scalars[name] = _content(da.to_numpy())
        elif da.dims == ("obs",):
            obs_fields[name] = ak.unflatten(_content(da.to_numpy()), counts)
    # time first, then value, then any flags, for a readable observation record.
    order = [n for n in ("time", "value") if n in obs_fields]
    order += [n for n in obs_fields if n not in order]
    obs = ak.zip({n: obs_fields[n] for n in order})
    return ak.zip({**scalars, "obs": obs}, depth_limit=1)


# === column schemas ========================================================

# Water-quality samples (Samples DB / WQX) speak a different column vocabulary
# than the time-series getters; map their tidy backbone onto the canonical
# names the builders understand.
_SAMPLES_RENAME = {
    "Location_Identifier": _SITE,
    "Activity_StartDateTime": "time",
    "Result_Measure": "value",
    "Result_MeasureUnit": "unit_of_measure",
    "Result_Characteristic": "characteristic",
    "Result_SampleFraction": "sample_fraction",
    "Result_ResultDetectionCondition": "detection_condition",
    "Result_MeasureStatusIdentifier": "status",
    # Samples carry position as explicit columns (no OGC ``geometry``); map them
    # to the canonical names so _point_coords surfaces station lon/lat.
    "Location_Longitude": "longitude",
    "Location_Latitude": "latitude",
}
_CANONICAL_COORD_ATTRS = {
    "parameter_code": {"long_name": "USGS parameter code"},
    "statistic_id": {"long_name": "USGS statistic code"},
    "unit_of_measure": {"long_name": "unit of measurement"},
}
_SAMPLES_COORD_ATTRS = {
    "characteristic": {"long_name": "characteristic name"},
    "sample_fraction": {"long_name": "sample fraction"},
    "unit_of_measure": {"long_name": "unit of measurement"},
    "detection_condition": {"long_name": "result detection condition"},
    "status": {"long_name": "result status"},
}


@dataclass(frozen=True)
class _Schema:
    """How a service's columns map onto the canonical builder vocabulary.

    One object describes a column dialect so a single set of builders serves
    both the time-series getters and the (differently-named) Samples data:

    * ``rename`` -- source -> canonical column names (empty for the getters
      that already use the canonical names);
    * ``group_cols`` -- columns that identify one series (an instance);
    * ``ancillary`` -- per-observation flag columns carried alongside ``value``;
    * ``label_col`` -- the column whose value names the variable / ``long_name``
      (``parameter_code`` for the getters, ``characteristic`` for samples);
    * ``infer_standard_name`` -- whether ``label_col`` is a USGS parameter code
      eligible for a CF ``standard_name`` lookup (False for free-text
      characteristics);
    * ``coord_attrs`` -- ``long_name`` etc. for the per-instance metadata vars.
    """

    rename: dict = field(default_factory=dict)
    group_cols: tuple = ("parameter_code", "statistic_id")
    ancillary: tuple = _ANCILLARY
    label_col: str = "parameter_code"
    infer_standard_name: bool = True
    coord_attrs: dict = field(default_factory=dict)


_CANONICAL = _Schema(coord_attrs=_CANONICAL_COORD_ATTRS)
_FIELD = replace(_CANONICAL, group_cols=("parameter_code",))
_SAMPLES = _Schema(
    rename=_SAMPLES_RENAME,
    group_cols=("characteristic", "sample_fraction"),
    ancillary=("detection_condition", "status"),
    label_col="characteristic",
    infer_standard_name=False,
    coord_attrs=_SAMPLES_COORD_ATTRS,
)


# === dataset builders ======================================================


class _DatasetBuilder:
    """Strategy base: convert one values frame into a CF ``xarray.Dataset``.

    Holds only what every layout needs -- the frame, the request metadata, and
    the dataset-level provenance scaffolding -- and leaves :meth:`build` to the
    subclass. The series layouts share more state and live under
    :class:`_SeriesBuilder`; :class:`_StatsBuilder` extends this base directly.
    Construct via the :func:`_build_ragged` / :func:`_build_dense` /
    :func:`_build_stats` helpers rather than directly.
    """

    # The CF ``featureType`` this layout produces (None to omit it). The series
    # layouts are timeSeries DSGs; the flat stats table overrides this to None.
    feature_type = "timeSeries"

    def __init__(self, df, base_meta, *, service):
        self.df = df
        self.base_meta = base_meta
        self.service = service

    def build(self):
        raise NotImplementedError

    def _is_empty(self):
        return self.df is None or len(self.df) == 0

    def _empty(self):
        return _empty_dataset(
            self.service, self.base_meta, feature_type=self.feature_type
        )

    def _apply_provenance(self, ds):
        """Set the dataset-level CF/ACDD attributes and ``date_modified``."""
        ds.attrs = _dataset_attrs(
            self.service, self.base_meta, feature_type=self.feature_type
        )
        dm = _date_modified(self.df)
        if dm:
            ds.attrs["date_modified"] = dm
        return ds


class _SeriesBuilder(_DatasetBuilder):
    """Base for the time-series layouts (ragged / dense).

    Adds the column ``schema``, the metadata lookups, and the prepare /
    spatial-coordinate scaffolding the two series layouts share. :meth:`build`
    is a template: it renames the dialect, slims and type-checks the frame,
    short-circuits an empty result, and hands the cleaned frame to the subclass
    via :meth:`_build_series`.
    """

    def __init__(
        self,
        df,
        base_meta,
        *,
        service,
        schema=_CANONICAL,
        series_meta=None,
        site_meta=None,
        default_cell_method=None,
    ):
        # Rename the source dialect onto the canonical names once, up front, so
        # every downstream step speaks one vocabulary.
        if df is not None and schema.rename:
            df = df.rename(columns=schema.rename)
        super().__init__(df, base_meta, service=service)
        self.schema = schema
        self.series_meta = series_meta or {}
        self.site_meta = site_meta or {}
        self.default_cell_method = default_cell_method

    def build(self):
        if self._is_empty():
            return self._empty()
        work, group_cols, ancillary, has_unit = _prepare_values(
            self.df, self.schema.group_cols, self.schema.ancillary
        )
        if work.empty:
            return self._empty()
        return self._build_series(work, group_cols, ancillary, has_unit)

    def _build_series(self, work, group_cols, ancillary, has_unit):
        raise NotImplementedError

    def _add_spatial_coords(self, ds, dim, order):
        """Attach lon/lat and per-site descriptor coords along ``dim``.

        ``order`` is the list of site ids in ``dim`` order (one per instance),
        so the same logic serves both the dense (the site dimension) and ragged
        (the timeseries dimension) layouts.
        """
        coords = _point_coords(self.df, _SITE)
        if coords is not None:
            lon, lat = coords
            # Fill sites lacking point geometry with NaN (not None) so the
            # coordinate stays a numeric float array -- a CF longitude/latitude
            # coord must be numeric; an object array with None is CF-invalid.
            ds = ds.assign_coords(
                longitude=(dim, [lon.get(k, _np.nan) for k in order]),
                latitude=(dim, [lat.get(k, _np.nan) for k in order]),
            )
            ds["longitude"].attrs = {
                "standard_name": "longitude",
                "units": "degrees_east",
            }
            ds["latitude"].attrs = {
                "standard_name": "latitude",
                "units": "degrees_north",
            }
        # Instance-indexed site descriptors carried along on the metadata call
        # (HUC, state); added only where present so absent fields don't appear.
        if self.site_meta:
            for col, col_attrs in _SITE_COORD_ATTRS.items():
                vals = [self.site_meta.get(str(k), {}).get(col) for k in order]
                if any(v is not None for v in vals):
                    ds = ds.assign_coords({col: (dim, vals)})
                    ds[col].attrs.update(col_attrs)
        return ds


class _RaggedBuilder(_SeriesBuilder):
    """CF *contiguous ragged array* layout (the ``dense=False`` opt-in).

    All observations are concatenated into one ``obs`` dimension; each series (a
    ``schema.group_cols`` combination at a site) is an instance along
    ``timeseries`` with ``row_size`` giving its length. Only real observations
    are stored (no NaN fill), so this scales to large, very ragged multi-site
    pulls. Per-series parameter / statistic / unit are instance coordinates;
    descriptors homogeneous across instances are also written onto ``value``.
    """

    def _build_series(self, work, group_cols, ancillary, has_unit):
        inst_cols = [_SITE, *group_cols]
        ds, inst_frame = self._assemble(work, inst_cols, ancillary, has_unit)
        value_attrs = self._value_attrs(ds, inst_frame)
        ds = self._finalize(ds, inst_frame, ancillary, value_attrs)

        for col, col_attrs in self.schema.coord_attrs.items():
            if col in ds.variables:
                ds[col].attrs.update(col_attrs)
        order = inst_frame[_SITE].to_numpy()
        return self._add_spatial_coords(ds, "timeseries", order)

    def _assemble(self, work, inst_cols, ancillary, has_unit):
        """Cleaned values frame -> CF contiguous-ragged Dataset skeleton.

        Observations are concatenated along a single ``obs`` dimension, sorted
        so each instance's rows are contiguous; ``row_size`` records how many
        obs each instance contributes (the CF ``sample_dimension`` link).
        ``unit_of_measure`` (when present) is carried as one value per instance.
        Returns ``(ds, inst_frame)`` where ``inst_frame`` is one row/instance.
        """
        inst_first = ["unit_of_measure"] if has_unit else []
        work = work.sort_values([*inst_cols, "time"], kind="stable")
        grp = work.groupby(inst_cols, dropna=False, sort=False)
        row_size = grp.size()
        idx = row_size.index
        inst_frame = (
            idx.to_frame(index=False)
            if isinstance(idx, _pd.MultiIndex)
            else _pd.DataFrame({inst_cols[0]: idx})
        )
        data_vars = {
            "value": ("obs", work["value"].to_numpy()),
            # int64 (not int32): a single long, high-frequency series can exceed
            # 2^31 observations, and the select_series cumsum must not overflow.
            "row_size": ("timeseries", row_size.to_numpy().astype("int64")),
        }
        for c in ancillary:
            data_vars[c] = ("obs", work[c].to_numpy())
        coords = {"time": ("obs", work["time"].to_numpy())}
        for c in inst_cols:
            coords[c] = ("timeseries", inst_frame[c].to_numpy())
        for c in inst_first:
            coords[c] = ("timeseries", grp[c].first().to_numpy())
        return _xr.Dataset(data_vars, coords=coords), inst_frame

    def _value_attrs(self, ds, inst_frame):
        """CF attributes for ``value``.

        Homogeneous descriptors (a single parameter / statistic / unit across
        all instances) are written onto ``value``; otherwise ``value`` stays
        generic and the per-instance coordinates carry each series' identity.
        """
        schema = self.schema
        labels = _unique_present(inst_frame, schema.label_col)
        stats = _unique_present(inst_frame, "statistic_id")
        units = (
            ds["unit_of_measure"].to_series().dropna().unique()
            if "unit_of_measure" in ds.coords
            else []
        )
        unit = units[0] if len(units) == 1 else None
        if len(labels) == 1:
            if schema.infer_standard_name:
                desc, pcode = self.series_meta.get(str(labels[0]), {}), labels[0]
            else:  # free-text label (e.g. a characteristic): it *is* the name
                desc, pcode = {"parameter_name": str(labels[0])}, None
            return _var_attrs(
                desc,
                unit=unit,
                pcode=pcode,
                stat=stats[0] if len(stats) == 1 else None,
                default_cell_method=self.default_cell_method,
            )
        value_attrs = {
            "long_name": "measured value",
            "comment": (
                "multiple series with differing metadata are stacked here; see "
                "the per-timeseries coordinates for each series' identity"
            ),
        }
        if unit is not None:
            value_attrs["units"] = _cf_units(unit)
        # A service-wide cell method (e.g. samples are instantaneous grabs)
        # still applies when the statistic doesn't vary; the time-series getters
        # leave per-parameter cell methods to the per-instance coordinates.
        if self.default_cell_method and not schema.infer_standard_name:
            value_attrs["cell_methods"] = f"time: {self.default_cell_method}"
        return value_attrs

    def _finalize(self, ds, inst_frame, ancillary, value_attrs):
        """Attach the structural + provenance CF metadata for a ragged Dataset.

        Sets the ``value`` attrs (with ``ancillary_variables`` linked), dataset
        provenance, the ``row_size`` ``sample_dimension`` link, and a
        per-instance ``timeseries_id`` carrying ``cf_role`` (a site alone isn't
        unique once it has several series).
        """
        if ancillary:
            value_attrs = {**value_attrs, "ancillary_variables": " ".join(ancillary)}
        ds["value"].attrs = value_attrs
        ds = self._apply_provenance(ds)
        ds["time"].attrs.setdefault("standard_name", "time")
        ds["row_size"].attrs = {
            "long_name": "number of observations per time series",
            "sample_dimension": "obs",
        }
        # Join the instance keys into a cf_role id, skipping null parts so a
        # missing key (e.g. a characteristic with no sample fraction) doesn't
        # render as a literal "nan" token. Iterating the row arrays avoids the
        # per-instance Series boxing of ``apply(axis=1)``.
        ts_id = _np.array(
            [
                ":".join(str(x) for x in row if _pd.notna(x))
                for row in inst_frame.to_numpy()
            ],
            dtype=object,
        )
        ds = ds.assign_coords(timeseries_id=("timeseries", ts_id))
        ds["timeseries_id"].attrs["cf_role"] = "timeseries_id"
        ds[_SITE].attrs.setdefault("long_name", "monitoring location identifier")
        return ds


class _DenseBuilder(_SeriesBuilder):
    """Dense ``(monitoring_location_id, time)`` grid (the default): one named
    variable per parameter, NaN where a series has no observation. Ergonomic for
    a few overlapping series but memory-costly for large ragged collections; see
    :class:`_RaggedBuilder` for the ``dense=False`` scaling layout.
    """

    def _build_series(self, work, group_cols, ancillary, has_unit):
        # Outer join on time: parameters sampled on different clocks share a
        # union time axis, NaN where a given parameter has no observation.
        ds = _xr.merge(
            self._variable_datasets(work, group_cols, ancillary, has_unit),
            combine_attrs="drop_conflicts",
            join="outer",
        )
        ds = self._apply_provenance(ds)
        ds["time"].attrs.setdefault("standard_name", "time")
        if _SITE in ds.coords:
            ds[_SITE].attrs.setdefault("cf_role", "timeseries_id")
        order = list(ds[_SITE].values) if _SITE in ds.coords else []
        return self._add_spatial_coords(ds, _SITE, order)

    def _variable_datasets(self, work, group_cols, ancillary, has_unit):
        """One pivoted ``(site, time)`` Dataset per (parameter, statistic)."""
        # First pass: gather each group's identity and base name, so naming can
        # see the whole set (a bare name is only used when it is unambiguous).
        specs = []
        for _, group in work.groupby(group_cols, dropna=False):
            pcode = _first_present(group, "parameter_code")
            stat = _first_present(group, "statistic_id")
            desc = self.series_meta.get(str(pcode), {}) if pcode is not None else {}
            base = _slug(_none_if_nan(desc.get("parameter_name")) or pcode or "value")
            specs.append((group, pcode, stat, desc, base))
        names = self._disambiguate([s[4] for s in specs], [(s[1], s[2]) for s in specs])

        datasets = []
        for (group, pcode, stat, desc, _base), name in zip(specs, names):
            # Sort the units so the chosen label is deterministic across pulls
            # (values are not converted either way; see the multi-unit warning).
            group_units = (
                sorted(group["unit_of_measure"].dropna().unique()) if has_unit else []
            )
            unit = group_units[0] if group_units else None

            if len(group_units) > 1:
                # One variable can carry only one ``units`` attr; surface the
                # mix instead of silently labeling every value with the first.
                _warnings.warn(
                    f"'{name}' spans multiple units {list(group_units)}; labeling "
                    f"with '{unit}'. Filter by site/parameter to avoid mixing "
                    "units in one variable.",
                    stacklevel=3,
                )

            sub = group.set_index([_SITE, "time"])[["value", *ancillary]]
            if not sub.index.is_unique:
                _warnings.warn(
                    f"'{name}' has multiple values per (site, time) -- two series "
                    "share this (site, parameter, statistic); keeping the smallest "
                    "value. Filter the query to separate them.",
                    stacklevel=3,
                )
                # Sort by value then the flag columns, with a stable sort, so the
                # whole retained row (value *and* its ancillary flags) is
                # deterministic rather than dependent on the upstream row order.
                sub = sub.sort_values(["value", *ancillary], kind="stable")
                sub = sub[~sub.index.duplicated(keep="first")]
            ds_g = sub.to_xarray().rename(
                {"value": name, **{c: f"{name}_{c}" for c in ancillary}}
            )
            ds_g[name].attrs = _var_attrs(
                desc,
                unit=unit,
                pcode=pcode,
                stat=stat,
                default_cell_method=self.default_cell_method,
            )
            if ancillary:
                ds_g[name].attrs["ancillary_variables"] = " ".join(
                    f"{name}_{c}" for c in ancillary
                )
            datasets.append(ds_g)
        return datasets

    @staticmethod
    def _disambiguate(bases, keys):
        """Map per-group base slugs to unique, deterministic variable names.

        ``keys[i]`` is the group's ``(parameter_code, statistic_id)``. A base used
        by exactly one group stays bare (e.g. ``discharge``); a base shared by
        several groups is disambiguated for *all* of them -- by the statistic's
        cell-method operator (``discharge_maximum`` / ``discharge_mean``), falling
        back to the statistic id then the parameter code -- so a bare name never
        silently refers to an arbitrary one of several same-named series.
        """
        counts = _Counter(bases)
        names, used = [], set()
        for base, (pcode, stat) in zip(bases, keys):
            if counts[base] == 1:
                name = base
            else:
                # statistic cell-method (or raw id); if that doesn't yield a
                # fresh name, fall back to the parameter code.
                op = CF_CELL_METHODS.get(str(stat)) if stat is not None else None
                suffix = op or (str(stat) if stat is not None else None)
                name = f"{base}_{_slug(suffix)}" if suffix else base
                if name in used or suffix is None:
                    name = f"{base}_{_slug(pcode)}" if pcode is not None else base
            while name in used:  # final guard: append until unique
                name += "_x"
            used.add(name)
            names.append(name)
        return names


class _StatsBuilder(_DatasetBuilder):
    """Best-effort, preliminary conversion of the statistics tables.

    The statistics service returns percentile tables keyed by time-of-year
    rather than a (time, value) series, so this produces a flat Dataset (one
    variable per column over an ``index`` dimension) with dataset-level
    provenance only. A richer percentile / day-of-year layout is future work.
    """

    # Not a CF discrete-sampling geometry: a flat percentile table has no
    # obs/time/cf_role/sample_dimension, so it must not claim a ``featureType``.
    feature_type = None

    def build(self):
        if self._is_empty():
            return self._empty()
        # The series builders surface only the columns they convert, so opaque
        # hash IDs never reach those datasets. This flat path keeps every column,
        # so drop the stats service's hash-valued IDs (and geometry) here to keep
        # the CF dataset free of per-record UUID coordinates.
        drop = ("geometry", "computation_id", "parent_time_series_id", "time_series_id")
        flat = self.df.drop(columns=[c for c in drop if c in self.df.columns])
        ds = _xr.Dataset.from_dataframe(flat.reset_index(drop=True))
        ds = self._apply_provenance(ds)
        ds.attrs["comment"] = "preliminary flat conversion; see module docs"
        return ds


def _build_ragged(df, base_meta, **kwargs):
    """Functional entry point for the ragged layout (see :class:`_RaggedBuilder`)."""
    return _RaggedBuilder(df, base_meta, **kwargs).build()


def _build_dense(df, base_meta, **kwargs):
    """Functional entry point for the dense layout (see :class:`_DenseBuilder`)."""
    return _DenseBuilder(df, base_meta, **kwargs).build()


def _build_stats(df, base_meta, service):
    """Functional entry point for the stats layout (see :class:`_StatsBuilder`)."""
    return _StatsBuilder(df, base_meta, service=service).build()


# === public getters ========================================================


def _fetch(func, args, kwargs):
    """Call the underlying getter, dropping a stray ``include_hash`` kwarg.

    The xarray builders surface only the columns they convert, so the opaque
    hash-valued ID columns (per-record UUIDs, per-series join keys) never reach
    the dataset regardless of what the getter returns. ``include_hash`` is not a
    parameter of the plain getters, so it is swallowed here to keep passing it
    to an xarray wrapper harmless.
    """
    kwargs.pop("include_hash", None)
    return func(*args, **kwargs)


def _xr_doc(func, *, cf_metadata=True, allow_dense=True):
    """Prepend an xarray note to the wrapped getter's docstring.

    ``cf_metadata=False`` describes the preliminary stats path (a flat Dataset
    without per-variable CF attributes); ``allow_dense=False`` describes a
    ragged-only path (samples), where ``dense=`` does not apply.
    """
    if not cf_metadata:
        returns = (
            "a preliminary, flat ``xarray.Dataset`` (dataset-level provenance "
            "only; per-variable CF metadata is not yet populated)"
        )
    elif allow_dense:
        returns = (
            "a CF-conventions ``xarray.Dataset`` on a (site, time) grid with one "
            "named variable per parameter (pass ``dense=False`` for the "
            "contiguous ragged array that stores only real observations and "
            "scales to large, very ragged multi-site pulls)"
        )
    else:
        returns = (
            "a CF-conventions ``xarray.Dataset`` as a contiguous ragged array "
            "(always ragged; discrete samples are too sparse for a dense grid, "
            "so ``dense=`` does not apply)"
        )
    # A column-0 summary paragraph + a cleandoc'd body keeps the combined
    # docstring valid numpydoc (the wrapped getter's first line is unindented
    # but its body is source-indented, which would otherwise break dedent and
    # over-indent the Parameters/Returns sections).
    note = (
        "xarray wrapper: same arguments as "
        f"``dataretrieval.waterdata.{func.__name__}``, but returns {returns}. "
        "Hash-valued ID columns are always omitted here; the ``include_hash`` "
        "flag does not apply."
    )
    body = _inspect.cleandoc(func.__doc__) if func.__doc__ else ""
    return f"{note}\n\n{body}" if body else note


def _public_signature(wrapped, *, has_dense):
    """The signature ``help()`` / IDEs should show for a wrapper.

    Keeps the wrapped getter's own parameters (so its real arguments stay
    discoverable), adds the keyword-only ``dense`` toggle (default ``True``;
    pass ``dense=False`` for the ragged layout) where the layout offers it, and
    declares the ``xarray.Dataset`` return -- correcting the DataFrame signature
    that ``functools.wraps`` would otherwise carry over.
    """
    sig = _inspect.signature(wrapped)
    params = list(sig.parameters.values())
    if has_dense:
        cut = next(
            (
                i
                for i, p in enumerate(params)
                if p.kind is _inspect.Parameter.VAR_KEYWORD
            ),
            len(params),
        )
        params.insert(
            cut,
            _inspect.Parameter(
                "dense",
                _inspect.Parameter.KEYWORD_ONLY,
                default=True,
                annotation="bool",
            ),
        )
    return sig.replace(parameters=params, return_annotation="xarray.Dataset")


@dataclass(frozen=True)
class _Service:
    """Per-service configuration that drives one public xarray getter.

    The variation across services is data, not behaviour: which getter to call,
    which :class:`_MetadataCache` (if any) supplies the parameter names, the
    column ``schema``, the fallback ``cell_methods`` operator, whether the
    result is a time series or the preliminary stats table, and whether a dense
    grid is offered.
    """

    getter: Callable
    service: str
    metadata_cache: _MetadataCache | None = None
    schema: _Schema = _CANONICAL
    default_cell_method: str | None = None
    layout: str = "series"  # "series" | "stats"
    allow_dense: bool = True


def _make_getter(spec):
    """Build the public getter for one ``_Service`` spec (Factory)."""

    is_stats = spec.layout == "stats"
    has_dense = not is_stats and spec.allow_dense

    # The (site, time) grid is the default where it is offered; ``dense=False``
    # opts into the ragged array. Services without a grid (samples, stats)
    # default to their only layout, so a plain call never trips the warning.
    @_wraps(spec.getter)  # carry __name__/__wrapped__ through to help()/IDEs
    def getter(*args, dense=has_dense, **kwargs):
        if dense and not has_dense:
            _warnings.warn(
                f"{spec.getter.__name__} has no dense layout; ignoring dense=True.",
                stacklevel=2,
            )
            dense = False
        df, base_meta = _fetch(spec.getter, args, kwargs)
        if is_stats:
            return _build_stats(df, base_meta, spec.service)
        if spec.metadata_cache is not None:
            series_meta, site_meta = spec.metadata_cache.lookup(_sites(df))
        else:
            series_meta, site_meta = {}, {}
        build = _build_dense if dense else _build_ragged
        return build(
            df,
            base_meta,
            service=spec.service,
            schema=spec.schema,
            series_meta=series_meta,
            site_meta=site_meta,
            default_cell_method=spec.default_cell_method,
        )

    # ``@_wraps`` copied the getter's docstring, signature and annotations
    # verbatim -- which would advertise a DataFrame return and hide ``dense``.
    # Replace the docstring with the xarray note and publish an accurate
    # signature: the getter's own args, plus ``dense`` where it applies,
    # returning an ``xarray.Dataset``.
    getter.__doc__ = _xr_doc(
        spec.getter,
        cf_metadata=not is_stats,
        allow_dense=spec.allow_dense,
    )
    getter.__signature__ = _public_signature(spec.getter, has_dense=has_dense)
    getter.__annotations__ = {
        **getter.__annotations__,
        "return": "xarray.Dataset",
        **({"dense": "bool"} if has_dense else {}),
    }
    return getter


get_daily = _make_getter(_Service(_api.get_daily, "daily", metadata_cache=_TS_CACHE))
get_continuous = _make_getter(
    _Service(_api.get_continuous, "continuous", metadata_cache=_TS_CACHE)
)
get_latest_continuous = _make_getter(
    _Service(
        _api.get_latest_continuous,
        "latest-continuous",
        metadata_cache=_TS_CACHE,
        default_cell_method="point",
    )
)
get_latest_daily = _make_getter(
    _Service(
        _api.get_latest_daily,
        "latest-daily",
        metadata_cache=_TS_CACHE,
        default_cell_method="point",
    )
)
get_nearest_continuous = _make_getter(
    _Service(
        _get_nearest_continuous,
        "continuous",
        metadata_cache=_TS_CACHE,
        default_cell_method="point",
    )
)
get_peaks = _make_getter(
    _Service(
        _api.get_peaks,
        "peaks",
        metadata_cache=_TS_CACHE,
        default_cell_method="maximum",
    )
)
get_field_measurements = _make_getter(
    _Service(
        _api.get_field_measurements,
        "field-measurements",
        metadata_cache=_FIELD_CACHE,
        schema=_FIELD,
        default_cell_method="point",
    )
)
get_stats_por = _make_getter(_Service(_api.get_stats_por, "statistics", layout="stats"))
get_stats_date_range = _make_getter(
    _Service(_api.get_stats_date_range, "statistics", layout="stats")
)
get_samples = _make_getter(
    _Service(
        _api.get_samples,
        "samples",
        schema=_SAMPLES,
        default_cell_method="point",
        allow_dense=False,
    )
)
