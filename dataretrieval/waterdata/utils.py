"""Water Data API layer over the generic OGC engine.

The API-agnostic OGC machinery (request construction, pagination, response
shaping, the chunked ``get_ogc_data`` entry point) lives in the
:mod:`dataretrieval.ogc` package — :mod:`~dataretrieval.ogc.engine` and its
sibling modules (``dates``, ``errors``, ``shaping``, ``chunking``). This
module is the Water-Data-specific layer
on top of it: it supplies the service-to-id map, the CQL2/date-only dialect,
profile validation, and a thin ``get_ogc_data`` wrapper that injects the
Water Data defaults. (The statistics path lives in its own
:mod:`dataretrieval.waterdata.stats` module.) Every engine symbol the Water Data
getters (``api.py``, ``ratings.py``, ``nearest.py``) and the test suite import
from here is re-exported below.
"""

from __future__ import annotations

import functools
import warnings
from collections.abc import Callable, Mapping
from typing import Any, TypeVar, get_args

import httpx
import pandas as pd

from dataretrieval.codes.states import apply_state
from dataretrieval.ogc import engine
from dataretrieval.ogc.dates import (
    _DATE_RANGE_PARAMS,
    _DURATION_RE,
    _format_api_dates,
)
from dataretrieval.ogc.engine import (
    BASE_URL,
    OGC_API_URL,
    OgcDialect,
    _as_str_list,
    _check_id_format,
    _check_monitoring_location_id,
    _check_ogc_requests,
    _construct_api_requests,
    _construct_cql_request,
    _default_headers,
    _next_req_url,
    _normalize_str_iterable,
    _paginate,
    _row_cap,
    _run_sync,
    _switch_properties_id,
    _walk_pages,
)
from dataretrieval.ogc.engine import (
    _get_args as _engine_get_args,
)
from dataretrieval.ogc.errors import (
    _error_body,
    _paginated_failure_message,
    _parse_retry_after,
    _raise_for_non_200,
)
from dataretrieval.ogc.shaping import (
    GEOPANDAS,
    _arrange_cols,
    _deal_with_empty,
    _get_resp_data,
    _to_snake_case,
)
from dataretrieval.ogc.shaping import (
    _finalize_ogc as _engine_finalize_ogc,
)
from dataretrieval.utils import BaseMetadata
from dataretrieval.waterdata.types import (
    PROFILE_LOOKUP,
    PROFILES,
    SERVICES,
)

SAMPLES_URL = f"{BASE_URL}/samples-data"

# Maps each OGC waterdata service to its user-facing ``id`` column (the name the
# typed getters rename the wire ``id`` to, e.g. ``daily`` -> ``daily_id``).
# ``get_cql`` validates its ``service`` argument against these keys and
# uses the value as the ``output_id`` for result shaping. Keep in sync with the
# ``types.WATERDATA_SERVICES`` Literal (same keys).
_OUTPUT_ID_BY_SERVICE: dict[str, str] = {
    "channel-measurements": "channel_measurements_id",
    "combined-metadata": "combined_meta_id",
    "continuous": "continuous_id",
    "daily": "daily_id",
    "field-measurements": "field_measurement_id",
    "field-measurements-metadata": "field_series_id",
    "latest-continuous": "latest_continuous_id",
    "latest-daily": "latest_daily_id",
    "monitoring-locations": "monitoring_location_id",
    "peaks": "peak_id",
    "time-series-metadata": "time_series_id",
}

# Every service's output id EXCEPT the two that are genuinely user-facing
# (``monitoring_location_id`` and ``time_series_id``). The rest are synthetic
# per-record ids that ``_arrange_cols`` moves to the end of a result frame.
# Derived from ``_OUTPUT_ID_BY_SERVICE`` so adding a service can't silently
# leave a stray id column at the front again.
_EXTRA_ID_COLS = frozenset(
    set(_OUTPUT_ID_BY_SERVICE.values()) - {"monitoring_location_id", "time_series_id"}
)

# The Water Data API dialect: ``monitoring-locations`` doesn't accept
# comma-separated multi-value GET params (so it must POST CQL2 JSON),
# ``daily`` renders its time arguments date-only (``YYYY-MM-DD``), and the
# ``time_cols``/``numerical_cols``/``sort_cols`` are the Water-Data column
# vocabulary used to coerce datetime/numeric columns and to sort results.
WATERDATA_DIALECT = OgcDialect(
    cql2_services=frozenset({"monitoring-locations"}),
    date_only_services=frozenset({"daily"}),
    time_cols=frozenset(
        {
            "begin",
            "begin_utc",
            "construction_date",
            "end",
            "end_utc",
            "last_modified",
            "time",
        }
    ),
    numerical_cols=frozenset(
        {
            "altitude",
            "altitude_accuracy",
            "contributing_drainage_area",
            "drainage_area",
            "hole_constructed_depth",
            "value",
            "well_constructed_depth",
        }
    ),
    sort_cols=("time", "monitoring_location_id"),
)

# Iterable-shaped params that ``_get_args`` must NOT push through
# ``_normalize_str_iterable`` (scalar non-string knobs are caught by runtime
# type, so only iterables with special handling need to be named here):
#   - date-range params may contain ``pd.NaT``/None or interval strings
#   - ``bbox``/``boundingBox`` are ``list[float]``, sometimes ``numpy.ndarray``
#   - ``get_peaks``'s int-valued filters (``water_year`` etc.) are ``list[int]``
#   - ``get_combined_metadata``'s ``thresholds`` is ``list[float]``
_NO_NORMALIZE_PARAMS = _DATE_RANGE_PARAMS | {
    "bbox",
    "boundingBox",
    "water_year",
    "year",
    "month",
    "day",
    "peak_since",
    "thresholds",
}


def _get_args(
    local_vars: dict[str, Any], exclude: set[str] | None = None
) -> dict[str, Any]:
    """Water-Data wrapper over :func:`engine._get_args`.

    Supplies the Water Data API's extended ``no_normalize`` set (numeric
    params such as ``water_year``, ``thresholds``, ``boundingBox``) so they
    keep their element types. See :func:`engine._get_args` for the full
    normalization contract.

    A getter's ``**queryables`` passthrough kwargs are collected by ``locals()``
    under the ``queryables`` key; they are flattened in here, so an extra
    server-side filter such as ``state_name="Wisconsin"`` is normalized and sent
    exactly like a named param. See
    :func:`dataretrieval.waterdata.get_queryables` for each collection's
    filterable properties (the service rejects an unknown one with a 400).
    """
    queryables = local_vars.pop("queryables", None)
    if queryables:
        local_vars.update(queryables)
    return _engine_get_args(local_vars, exclude, no_normalize=_NO_NORMALIZE_PARAMS)


def _with_state(local_vars: dict[str, Any], *, to: str, into: str) -> dict[str, Any]:
    """Resolve the unified ``state`` argument into an endpoint's native state
    queryable, returning the (mutated) args mapping.

    ``state`` is the canonical, format-flexible parameter (full name / postal /
    FIPS); it is normalized via :func:`~dataretrieval.codes.states.to_state` to
    the ``to`` representation and stored under ``into`` (the queryable this
    endpoint actually filters on). It is additive sugar over the native
    ``state_code`` / ``state_name`` parameters, which still accept the API's
    raw values (e.g. non-US FIPS); passing ``state`` together with either
    raises ``ValueError``.
    """
    return apply_state(
        local_vars, to=to, into=into, reject=("state_code", "state_name")
    )


def get_ogc_data(
    args: dict[str, Any],
    service: str,
    output_id: str | None = None,
    max_rows: int | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Water-Data wrapper over :func:`engine.get_ogc_data`.

    Defaults ``output_id`` from the Water Data service map when not given,
    and supplies the Water Data extra-id columns and dialect, so the typed
    getters in ``api.py`` call this unchanged. (Sibling OGC APIs such as
    NGWMN call ``engine.get_ogc_data`` directly with their own base URL and
    dialect rather than going through this Water Data wrapper.)

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of request arguments for the OGC service.
    service : str
        The OGC API collection name (e.g., ``"daily"``).
    output_id : str, optional
        The user-facing id column the wire ``id`` is renamed to. Defaults
        to ``_OUTPUT_ID_BY_SERVICE[service]``; pass it explicitly only for
        collections outside that map (e.g. reference-table collections).
    max_rows : int, optional
        Stop paginating once this many rows have been collected and
        truncate the result to exactly ``max_rows``. ``None`` (default)
        fetches the full result.

    Returns
    -------
    pd.DataFrame or gpd.GeoDataFrame
        A DataFrame containing the retrieved and processed OGC data.
    BaseMetadata
        A metadata object containing request information including URL and query time.
    """
    if output_id is None:
        output_id = _OUTPUT_ID_BY_SERVICE[service]
    return engine.get_ogc_data(
        args,
        service,
        output_id,
        max_rows=max_rows,
        base_url=OGC_API_URL,
        extra_id_cols=_EXTRA_ID_COLS,
        dialect=WATERDATA_DIALECT,
    )


def _finalize_ogc(
    frame: pd.DataFrame,
    response: httpx.Response,
    *,
    properties: list[str] | None,
    output_id: str,
    convert_type: bool,
    service: str,
    max_rows: int | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Water-Data wrapper over :func:`~dataretrieval.ogc.shaping._finalize_ogc`.

    Injects the Water Data ``extra_id_cols`` and ``dialect`` so a direct
    call (e.g. from ``get_cql``) orders synthetic id columns and coerces/
    sorts result columns identically to the typed getters. See
    :func:`~dataretrieval.ogc.shaping._finalize_ogc` for the full
    result-shaping contract.
    """
    return _engine_finalize_ogc(
        frame,
        response,
        properties=properties,
        output_id=output_id,
        convert_type=convert_type,
        service=service,
        max_rows=max_rows,
        extra_id_cols=_EXTRA_ID_COLS,
        dialect=WATERDATA_DIALECT,
    )


def _check_profiles(
    service: SERVICES,
    profile: PROFILES,
) -> None:
    """Check whether a service profile is valid.

    Parameters
    ----------
    service : string
        One of the service names from the "services" list.
    profile : string
        One of the profile names from "results_profiles",
        "locations_profiles", "activities_profiles",
        "projects_profiles" or "organizations_profiles".
    """
    valid_services = get_args(SERVICES)
    if service not in valid_services:
        raise ValueError(
            f"Invalid service: '{service}'. Valid options are: {valid_services}."
        )

    valid_profiles = PROFILE_LOOKUP[service]
    if profile not in valid_profiles:
        raise ValueError(
            f"Invalid profile: '{profile}' for service '{service}'. "
            f"Valid options are: {valid_profiles}."
        )


_R = TypeVar("_R")


def _accept_legacy_kwargs(
    mapping: Mapping[str, str],
) -> Callable[[Callable[..., _R]], Callable[..., _R]]:
    """Decorator: accept deprecated keyword-argument names, translating them
    to their modern equivalents and emitting a :class:`DeprecationWarning`.

    ``mapping`` maps each deprecated keyword name to the new keyword name the
    wrapped function expects (e.g. ``{"stateFips": "state_code"}``). When a
    caller passes a deprecated name, it is renamed to the new name before the
    wrapped function is invoked and a ``DeprecationWarning`` naming the
    replacement is emitted. Callers that already use the new names are
    unaffected (no warning, no overhead beyond the wrapper call).

    The wrapped function's return type is preserved; its parameter list is
    intentionally relaxed (the wrapper accepts the extra deprecated names),
    so static checkers won't flag legacy call sites.

    Raises
    ------
    TypeError
        If both a deprecated name and its modern equivalent are supplied for
        the same argument (ambiguous), mirroring Python's "got multiple
        values for argument" error.
    """

    def decorator(func: Callable[..., _R]) -> Callable[..., _R]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> _R:
            for old_name, new_name in mapping.items():
                if old_name not in kwargs:
                    continue
                if new_name in kwargs:
                    raise TypeError(
                        f"{func.__name__}() received both {old_name!r} "
                        f"(deprecated) and {new_name!r}; pass only {new_name!r}."
                    )
                warnings.warn(
                    f"The {old_name!r} argument is deprecated and will be "
                    f"removed in a future release; use {new_name!r} instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                kwargs[new_name] = kwargs.pop(old_name)
            return func(*args, **kwargs)

        return wrapper

    return decorator


__all__ = [
    "BASE_URL",
    "GEOPANDAS",
    "OGC_API_URL",
    "SAMPLES_URL",
    "WATERDATA_DIALECT",
    "_DATE_RANGE_PARAMS",
    "_DURATION_RE",
    "_EXTRA_ID_COLS",
    "_NO_NORMALIZE_PARAMS",
    "_OUTPUT_ID_BY_SERVICE",
    "_accept_legacy_kwargs",
    "_arrange_cols",
    "_as_str_list",
    "_check_id_format",
    "_check_monitoring_location_id",
    "_check_ogc_requests",
    "_check_profiles",
    "_construct_api_requests",
    "_construct_cql_request",
    "_deal_with_empty",
    "_default_headers",
    "_error_body",
    "_finalize_ogc",
    "_format_api_dates",
    "_get_args",
    "_get_resp_data",
    "_next_req_url",
    "_normalize_str_iterable",
    "_paginate",
    "_paginated_failure_message",
    "_parse_retry_after",
    "_raise_for_non_200",
    "_row_cap",
    "_run_sync",
    "_switch_properties_id",
    "_to_snake_case",
    "_walk_pages",
    "get_ogc_data",
]
