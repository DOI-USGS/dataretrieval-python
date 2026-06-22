"""National Ground-Water Monitoring Network (NGWMN) getters.

The NGWMN exposes its data through a dedicated OGC API
(``https://api.waterdata.usgs.gov/ngwmn/ogcapi``) with five collections:
``sites``, ``waterLevelObs``, ``lithologyObs``, ``constructionObs``, and
``providers``. Each getter below delegates to the shared OGC engine
(:func:`~dataretrieval.ogc.engine.get_ogc_data`) with
``base_url=NGWMN_OGC_API_URL``, so multi-value chunking, pagination,
retry/resume, and result shaping all behave exactly as they do for the main
Water Data getters.

Unlike the main Water Data collections, NGWMN aggregates monitoring locations
from many agencies, so ``monitoring_location_id`` values use other agency
prefixes besides ``USGS-`` (e.g. ``MBMG-702934``, ``AKDNR-535134236016630``).

See https://api.waterdata.usgs.gov/ngwmn/ogcapi for the API reference.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd

from dataretrieval.codes.states import apply_state
from dataretrieval.ogc.engine import BASE_URL, OgcDialect, _get_args, get_ogc_data
from dataretrieval.utils import BaseMetadata

# The National Ground-Water Monitoring Network exposes its own OGC API at a
# separate, unversioned base.
NGWMN_OGC_API_URL = f"{BASE_URL}/ngwmn/ogcapi"

# --- state-filter shim -------------------------------------------------------
# NGWMN's collections expose DIFFERENT state queryables: ``sites`` filters on
# the full ``state_name`` (e.g. "Wisconsin"), while ``providers`` filters on the
# two-letter postal ``state`` (uppercase, e.g. "WI"). The state-aware getters
# take a single ``state`` parameter accepting any US-state encoding (full name,
# postal code, or FIPS code); ``_get`` resolves it into the one queryable each
# collection wants via the shared ``codes.states.apply_state``, keyed by
# ``_STATE_QUERYABLE`` below.
#
# This shim exists only to smooth over that upstream asymmetry.
# ``tests/ngwmn_test.py::test_state_queryables_still_diverge_upstream`` fails --
# the signal to remove it -- if the API ever unifies the two queryables.
_STATE_QUERYABLE = {
    # service -> ``apply_state`` kwargs (destination queryable + to_state format)
    "sites": {"into": "state_name", "to": "name"},
    "providers": {"into": "state", "to": "postal"},
}


# The NGWMN OGC API exposes the feature id under the generic ``id`` column
# (there is no service-specific id name as there is for the main collections).
_NGWMN_OUTPUT_ID = "id"

# NGWMN's request shape matches the generic OGC default (no CQL2-only or
# date-only collections), but its result columns need their own coercion and
# sort vocabulary: water-level observations are timestamped by ``sample_time``
# (not the Water Data ``time``) and report depths/levels in feet.
NGWMN_DIALECT = OgcDialect(
    time_cols=frozenset({"sample_time"}),
    numerical_cols=frozenset(
        {
            "water_depth_below_land_surface_ft",
            "water_level_above_site_datum_ft",
            "water_level_above_navd88_ft",
        }
    ),
    sort_cols=("sample_time", "monitoring_location_id"),
)


def _get(service: str, local_vars: dict[str, Any]) -> tuple[pd.DataFrame, BaseMetadata]:
    """Marshal a getter's arguments and dispatch to the shared OGC engine.

    Every NGWMN getter ends with this same call; centralizing it keeps the
    NGWMN base URL, output id, and dialect wired up in exactly one place.
    """
    queryable = _STATE_QUERYABLE.get(service)
    if queryable is not None:
        apply_state(local_vars, to=queryable["to"], into=queryable["into"])
    args = _get_args(local_vars)
    return get_ogc_data(
        args,
        service,
        output_id=_NGWMN_OUTPUT_ID,
        base_url=NGWMN_OGC_API_URL,
        dialect=NGWMN_DIALECT,
    )


def get_sites(
    monitoring_location_id: str | Iterable[str] | None = None,
    agency_code: str | Iterable[str] | None = None,
    monitoring_location_number: str | Iterable[str] | None = None,
    altitude: str | Iterable[str] | None = None,
    national_aquifer_code: str | Iterable[str] | None = None,
    national_aquifer_description: str | Iterable[str] | None = None,
    country_code: str | Iterable[str] | None = None,
    country_name: str | Iterable[str] | None = None,
    state: str | Iterable[str] | None = None,
    county_name: str | Iterable[str] | None = None,
    aquifer_name: str | Iterable[str] | None = None,
    site_type: str | Iterable[str] | None = None,
    aquifer_type_code: str | Iterable[str] | None = None,
    qw_sys_name: str | Iterable[str] | None = None,
    qw_sn_flag: str | Iterable[str] | None = None,
    qw_baseline_flag: str | Iterable[str] | None = None,
    qw_well_chars: str | Iterable[str] | None = None,
    qw_well_type: str | Iterable[str] | None = None,
    qw_well_purpose: str | Iterable[str] | None = None,
    wl_sys_name: str | Iterable[str] | None = None,
    wl_sn_flag: str | Iterable[str] | None = None,
    wl_baseline_flag: str | Iterable[str] | None = None,
    wl_well_chars: str | Iterable[str] | None = None,
    wl_well_type: str | Iterable[str] | None = None,
    wl_well_purpose: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    convert_type: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get NGWMN monitoring-location (site) metadata.

    Site records describe each NGWMN monitoring location — its identifier,
    responsible agency, location, aquifer, and whether it participates in the
    network's water-quality (``qw_*``) and water-level (``wl_*``) sub-networks.

    Parameters
    ----------
    monitoring_location_id : str or iterable of str, optional
        One or more agency-qualified site identifiers in ``AGENCY-ID`` form
        (e.g. ``"USGS-423114090161101"``, ``"MBMG-702934"``).
    agency_code : str or iterable of str, optional
        Code of the agency that manages the site.
    monitoring_location_number : str or iterable of str, optional
        Agency-assigned site number.
    altitude : str or iterable of str, optional
        Land-surface altitude at the site.
    national_aquifer_code, national_aquifer_description : str or iterable, optional
        National aquifer code / description.
    country_code, country_name : str or iterable, optional
        Country filters.
    state : str or iterable of str, optional
        State/territory filter. Accepts a full name (``"Wisconsin"``), a
        two-letter postal code (``"WI"``), or a two-digit ANSI/FIPS code
        (``"55"``).
    county_name : str or iterable of str, optional
        County name filter.
    aquifer_name, site_type, aquifer_type_code : str or iterable, optional
        Aquifer name, site type, and aquifer-type code.
    qw_sys_name, qw_sn_flag, qw_baseline_flag : str or iterable, optional
        Water-quality sub-network membership flags.
    qw_well_chars, qw_well_type, qw_well_purpose : str or iterable, optional
        Water-quality well characteristics, type, and purpose.
    wl_sys_name, wl_sn_flag, wl_baseline_flag : str or iterable, optional
        Water-level sub-network membership flags.
    wl_well_chars, wl_well_type, wl_well_purpose : str or iterable, optional
        Water-level well characteristics, type, and purpose.
    properties : str or iterable of str, optional
        Subset of columns to return. ``None`` (default) returns all columns.
    skip_geometry : bool, optional
        When ``True``, omit the geometry column. ``None`` (default) leaves the
        server default (geometry included).
    bbox : list of float, optional
        Bounding box ``[minx, miny, maxx, maxy]`` (CRS 4326) to spatially
        filter sites.
    limit : int, optional
        Per-page size; pagination still follows ``next`` links to completion.
    convert_type : bool, optional
        Whether to coerce column dtypes (default ``True``).

    Returns
    -------
    pandas.DataFrame or geopandas.GeoDataFrame
        Site metadata, one row per monitoring location.
    BaseMetadata
        Metadata object with the request URL and query time.

    Examples
    --------
    .. code::

        >>> # All NGWMN sites in Wisconsin
        >>> # state accepts a full name, postal code ("WI"), or FIPS ("55")
        >>> df, md = dataretrieval.ngwmn.get_sites(state="Wisconsin")

        >>> # Specific sites, geometry omitted
        >>> df, md = dataretrieval.ngwmn.get_sites(
        ...     monitoring_location_id=["USGS-423114090161101", "MBMG-702934"],
        ...     skip_geometry=True,
        ... )
    """
    return _get("sites", locals())


def get_water_level(
    monitoring_location_id: str | Iterable[str] | None = None,
    monitoring_location_obs_number: str | Iterable[str] | None = None,
    sample_time: str | Iterable[str] | None = None,
    data_provided_by: str | Iterable[str] | None = None,
    water_depth_below_land_surface_ft: str | Iterable[str] | None = None,
    water_level_above_site_datum_ft: str | Iterable[str] | None = None,
    monitoring_location_vertical_datum: str | Iterable[str] | None = None,
    water_level_above_navd88_ft: str | Iterable[str] | None = None,
    datetime: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    limit: int | None = None,
    convert_type: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get NGWMN water-level observations.

    Parameters
    ----------
    monitoring_location_id : str or iterable of str, optional
        One or more agency-qualified site identifiers (``AGENCY-ID`` form).
    monitoring_location_obs_number : str or iterable of str, optional
        Per-site observation number; use to subset a site's observations.
    sample_time : str or iterable of str, optional
        Exact sample-time value(s) to match. For a time *range*, use
        ``datetime`` instead.
    data_provided_by : str or iterable of str, optional
        Source organization for the observation.
    water_depth_below_land_surface_ft : str or iterable, optional
        Depth-to-water value filter (feet below land surface).
    water_level_above_site_datum_ft : str or iterable, optional
        Water-level value filter (feet above the site datum).
    water_level_above_navd88_ft : str or iterable, optional
        Water-level value filter (feet above NAVD 88).
    monitoring_location_vertical_datum : str or iterable of str, optional
        Vertical datum of the reported water level.
    datetime : str or iterable of str, optional
        Temporal filter — a single instant or a two-element ``[start, end]``
        range (ISO-8601 dates/datetimes); ``".."`` denotes an open end.
    properties : str or iterable of str, optional
        Subset of columns to return. ``None`` (default) returns all columns.
    limit : int, optional
        Per-page size; pagination still follows ``next`` links to completion.
    convert_type : bool, optional
        Whether to coerce column dtypes (default ``True``).

    Returns
    -------
    pandas.DataFrame
        Water-level observations, one row per measurement.
    BaseMetadata
        Metadata object with the request URL and query time.

    Examples
    --------
    .. code::

        >>> site = "USGS-272838082142201"
        >>> df, md = dataretrieval.ngwmn.get_water_level(
        ...     monitoring_location_id=site
        ... )

        >>> # Restrict to a date range
        >>> df, md = dataretrieval.ngwmn.get_water_level(
        ...     monitoring_location_id=site, datetime=["2022-01-01", "2024-01-01"]
        ... )

        >>> # Multiple sites across agencies
        >>> df, md = dataretrieval.ngwmn.get_water_level(
        ...     monitoring_location_id=["USGS-272838082142201", "MBMG-702934"]
        ... )
    """
    return _get("waterLevelObs", locals())


def get_lithology(
    monitoring_location_id: str | Iterable[str] | None = None,
    monitoring_location_obs_number: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    limit: int | None = None,
    convert_type: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get NGWMN lithology observations.

    Lithology records describe the geologic materials logged at a monitoring
    location, with depth intervals and controlled lithology concepts.

    Parameters
    ----------
    monitoring_location_id : str or iterable of str, optional
        One or more agency-qualified site identifiers (``AGENCY-ID`` form).
    monitoring_location_obs_number : str or iterable of str, optional
        Per-site observation number; use to subset a site's records.
    properties : str or iterable of str, optional
        Subset of columns to return. ``None`` (default) returns all columns.
    limit : int, optional
        Per-page size; pagination still follows ``next`` links to completion.
    convert_type : bool, optional
        Whether to coerce column dtypes (default ``True``).

    Returns
    -------
    pandas.DataFrame
        Lithology observations, one row per logged interval.
    BaseMetadata
        Metadata object with the request URL and query time.

    Examples
    --------
    .. code::

        >>> df, md = dataretrieval.ngwmn.get_lithology(
        ...     monitoring_location_id="AKDNR-535134236016630"
        ... )
    """
    return _get("lithologyObs", locals())


def get_well_construction(
    monitoring_location_id: str | Iterable[str] | None = None,
    monitoring_location_obs_number: str | Iterable[str] | None = None,
    material: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    limit: int | None = None,
    convert_type: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get NGWMN well-construction observations.

    Construction records describe a well's physical build-out — casing,
    screens, and similar elements — with depth intervals, materials, and
    diameters.

    Parameters
    ----------
    monitoring_location_id : str or iterable of str, optional
        One or more agency-qualified site identifiers (``AGENCY-ID`` form).
    monitoring_location_obs_number : str or iterable of str, optional
        Per-site observation number; use to subset a site's records.
    material : str or iterable of str, optional
        Construction-material filter.
    properties : str or iterable of str, optional
        Subset of columns to return. ``None`` (default) returns all columns.
    limit : int, optional
        Per-page size; pagination still follows ``next`` links to completion.
    convert_type : bool, optional
        Whether to coerce column dtypes (default ``True``).

    Returns
    -------
    pandas.DataFrame
        Well-construction observations, one row per construction element.
    BaseMetadata
        Metadata object with the request URL and query time.

    Examples
    --------
    .. code::

        >>> df, md = dataretrieval.ngwmn.get_well_construction(
        ...     monitoring_location_id="USGS-272838082142201"
        ... )
    """
    return _get("constructionObs", locals())


def get_providers(
    state: str | Iterable[str] | None = None,
    agency_code: str | Iterable[str] | None = None,
    organization_type: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    limit: int | None = None,
    convert_type: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get NGWMN data-provider records.

    Providers are the organizations that contribute data to the network.

    Parameters
    ----------
    state : str or iterable of str, optional
        State/territory filter. Accepts a full name (``"Wisconsin"``), a
        two-letter postal code (``"WI"``), or a two-digit ANSI/FIPS code
        (``"55"``). Only one state at a time — a multi-value state filter
        returns no records for this collection.
    agency_code : str or iterable of str, optional
        Provider agency code.
    organization_type : str or iterable of str, optional
        Provider organization type, e.g. ``"NWIS"``.
    properties : str or iterable of str, optional
        Subset of columns to return. ``None`` (default) returns all columns.
    limit : int, optional
        Per-page size; pagination still follows ``next`` links to completion.
    convert_type : bool, optional
        Whether to coerce column dtypes (default ``True``).

    Returns
    -------
    pandas.DataFrame
        Provider records, one row per provider.
    BaseMetadata
        Metadata object with the request URL and query time.

    Examples
    --------
    .. code::

        >>> df, md = dataretrieval.ngwmn.get_providers(state="WI")

        >>> # a full name (or FIPS code) works too
        >>> df, md = dataretrieval.ngwmn.get_providers(
        ...     organization_type="NWIS", state="Wisconsin"
        ... )
    """
    return _get("providers", locals())
