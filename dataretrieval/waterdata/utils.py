"""Water Data API layer over the generic OGC facade.

This module is the Water-Data-specific adapter: it supplies the
collection-to-id map, the CQL2/date-only dialect, and a
thin ``get_ogc_data`` wrapper that injects the Water Data defaults. The
statistics path lives in its own :mod:`dataretrieval.waterdata.stats`
module.

OGC machinery (request construction, pagination, response shaping, the
chunked ``get_ogc_data`` entry point) lives in :mod:`dataretrieval.ogc`
and its implementation submodules. This adapter consumes the public facade
for dialects, argument normalization, and retrieval; callers that need an OGC
implementation helper import its canonical module directly rather than using
this module as a re-export layer.
"""

from __future__ import annotations

import functools
import warnings
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, TypeVar

import pandas as pd

from dataretrieval.codes.states import apply_state
from dataretrieval.credentials import refuse_credential_keywords
from dataretrieval.ogc import OgcDialect, prepare_request_args
from dataretrieval.ogc import get_ogc_data as _facade_get_ogc_data

# Endpoint constants live in one place for the whole collection; they are re-bound
# here because ``waterdata.utils.OGC_API_URL`` is a documented path.
from dataretrieval.waterdata.endpoints import (
    BASE_URL,
    OGC_API_URL,
    SAMPLES_URL,
    redirected,
)

if TYPE_CHECKING:
    from dataretrieval._response_metadata import BaseMetadata

# Maps each OGC waterdata collection to its user-facing ``id`` column (the name the
# typed getters rename the wire ``id`` to, e.g. ``daily`` -> ``daily_id``).
# ``get_cql`` validates its ``collection`` argument against these keys and
# uses the value as the ``output_id`` for result shaping. Keep in sync with the
# ``types.WATERDATA_SERVICES`` Literal (same keys).
_OUTPUT_ID_BY_COLLECTION: dict[str, str] = {
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

# Every collection's output id EXCEPT the two that are genuinely user-facing
# (``monitoring_location_id`` and ``time_series_id``). The rest are synthetic
# per-record ids that ``_arrange_cols`` moves to the end of a result frame.
# Derived from ``_OUTPUT_ID_BY_COLLECTION`` so adding a collection can't silently
# leave a stray id column at the front again.
_EXTRA_ID_COLS = frozenset(
    set(_OUTPUT_ID_BY_COLLECTION.values())
    - {"monitoring_location_id", "time_series_id"}
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

# The Water-Data-specific *extras* on top of the engine's own no-normalize set
# (which already covers the date-range params and ``bbox``). Scalar non-string
# knobs are caught by runtime type, so only iterables with special handling
# need to be named here:
#   - ``boundingBox`` is ``list[float]``, sometimes ``numpy.ndarray``
#   - ``get_peaks``'s int-valued filters (``water_year`` etc.) are ``list[int]``
#   - ``get_combined_metadata``'s ``thresholds`` is ``list[float]``
_NO_NORMALIZE_PARAMS = frozenset(
    {
        "boundingBox",
        "water_year",
        "year",
        "month",
        "day",
        "peak_since",
        "thresholds",
    }
)


def _flatten_queryables(local_vars: dict[str, Any]) -> dict[str, Any]:
    """Merge a getter's ``**queryables`` passthrough kwargs into ``local_vars``.

    ``locals()`` collects them under the ``queryables`` key; this lifts them to
    top-level entries, so an extra server-side filter such as
    ``state_name="Wisconsin"`` is normalized, mutual-exclusion-checked, and sent
    exactly like a named param. See
    :func:`dataretrieval.waterdata.get_queryables` for each collection's
    filterable properties (the collection rejects an unknown one with a 400).

    ``**queryables`` always arrives as a dict (empty when unused) and the key is
    popped, so this is a no-op on getters without the passthrough and idempotent
    if called twice.
    """
    queryables = local_vars.pop("queryables", {})
    # A credential-shaped name would go out in the query string, which is the
    # one thing this passthrough must not forward. The predicate lives in the
    # credentials leaf rather than here: what motivates it -- ``api_key=`` being
    # a plausible guess now that ``configure()`` takes it -- is package-wide,
    # and WQP's ``**kwargs`` search filters read the same list.
    refuse_credential_keywords(queryables)
    local_vars.update(queryables)
    return local_vars


def _get_args(
    local_vars: dict[str, Any], exclude: set[str] | None = None
) -> dict[str, Any]:
    """Water-Data wrapper over :func:`~dataretrieval.ogc.prepare_request_args`.

    Adds the Water Data API's extra no-normalize params (numeric params such
    as ``water_year``, ``thresholds``, ``boundingBox``) so they keep their
    element types. Also flattens any ``**queryables`` passthrough (see
    :func:`_flatten_queryables`).
    """
    _flatten_queryables(local_vars)
    return prepare_request_args(
        local_vars, exclude, extra_no_normalize=_NO_NORMALIZE_PARAMS
    )


def _with_state(local_vars: dict[str, Any], *, to: str, into: str) -> dict[str, Any]:
    """Resolve the unified ``state`` argument into an endpoint's state queryable.

    Returns the (mutated) args mapping. ``state`` is the canonical,
    format-flexible parameter (full name / postal / FIPS); it is normalized via
    :func:`~dataretrieval.codes.states.to_state` to the ``to`` representation
    and stored under ``into`` (the queryable this endpoint actually filters on).
    It is additive sugar over the native ``state_code`` / ``state_name``
    parameters, which still accept the API's raw values (e.g. non-US FIPS);
    passing ``state`` together with either raises ``ValueError``.
    """
    # Flatten ``**queryables`` first so a native state param arriving that way
    # (e.g. ``get_time_series_metadata``'s ``state_code``, which isn't an
    # explicit parameter) is visible to apply_state's mutual-exclusion guard.
    # Otherwise ``state`` plus a passthrough ``state_code`` would slip past the
    # check and silently send both.
    _flatten_queryables(local_vars)
    return apply_state(
        local_vars, to=to, into=into, reject=("state_code", "state_name")
    )


def get_ogc_data(
    args: dict[str, Any],
    collection: str,
    output_id: str | None = None,
    max_rows: int | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Water-Data wrapper over :func:`~dataretrieval.ogc.get_ogc_data`.

    Defaults ``output_id`` from the Water Data collection map when not given,
    and supplies the Water Data extra-id columns and dialect, so the typed
    getters in ``api.py`` call this unchanged. (Sibling OGC APIs such as
    NGWMN call ``dataretrieval.ogc.get_ogc_data`` directly with their own
    base URL and dialect rather than going through this Water Data wrapper.)

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of request arguments for the OGC collection.
    collection : str
        The OGC API collection name (e.g., ``"daily"``).
    output_id : str, optional
        The user-facing id column the wire ``id`` is renamed to. Defaults
        to ``_OUTPUT_ID_BY_COLLECTION[collection]``; pass it explicitly only for
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
        A metadata object with request information, including the URL and
        query time.
    """
    if output_id is None:
        output_id = _OUTPUT_ID_BY_COLLECTION[collection]
    return _facade_get_ogc_data(
        args,
        collection,
        output_id,
        max_rows=max_rows,
        # ``redirected`` honors a ``WaterdataSettings(base_url=...)`` set
        # by an enclosing ``configure`` block, and is a no-op otherwise. Called
        # here rather than bound once at import because the block is scoped to
        # a ``with`` statement.
        base_url=redirected(OGC_API_URL),
        extra_id_cols=_EXTRA_ID_COLS,
        dialect=WATERDATA_DIALECT,
        # Which settings table these calls read. Declared here, in the one
        # wrapper every Water Data getter goes through, rather than derived
        # from ``base_url``: NGWMN is served from the same host, so a URL
        # cannot tell the two adapters apart (ADR 0010).
        adapter="waterdata",
    )


_R = TypeVar("_R")


def _accept_legacy_kwargs(
    mapping: Mapping[str, str],
    *,
    detail: str = "",
) -> Callable[[Callable[..., _R]], Callable[..., _R]]:
    """Accept deprecated keyword-argument names on the decorated function.

    Translates them to their modern equivalents and emits a
    :class:`DeprecationWarning`.

    ``mapping`` maps each deprecated keyword name to the new keyword name the
    wrapped function expects (e.g. ``{"stateFips": "state_code"}``). When a
    caller passes a deprecated name, it is renamed to the new name before the
    wrapped function is invoked and a ``DeprecationWarning`` naming the
    replacement is emitted. Callers that already use the new names are
    unaffected (no warning, no overhead beyond the wrapper call).

    The wrapped function's return type is preserved; its parameter list is
    intentionally relaxed (the wrapper accepts the extra deprecated names),
    so static checkers won't flag legacy call sites.

    ``detail`` appends a sentence to the warning. The default message says only
    that the name changed; a rename with a reason worth giving -- a spec that
    names the value differently, a removal date -- passes it here rather than
    hand-rolling the whole shim to carry one sentence.

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
                message = (
                    f"The {old_name!r} argument is deprecated and will be "
                    f"removed in a future release; use {new_name!r} instead."
                )
                warnings.warn(
                    f"{message} {detail}" if detail else message,
                    DeprecationWarning,
                    stacklevel=2,
                )
                kwargs[new_name] = kwargs.pop(old_name)
            return func(*args, **kwargs)

        return wrapper

    return decorator


__all__ = [
    "BASE_URL",
    "OGC_API_URL",
    "SAMPLES_URL",
    "WATERDATA_DIALECT",
    "_EXTRA_ID_COLS",
    "_NO_NORMALIZE_PARAMS",
    "_OUTPUT_ID_BY_COLLECTION",
    "_accept_legacy_kwargs",
    "_get_args",
    "_with_state",
    "get_ogc_data",
]
