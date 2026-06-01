"""Argument validation and normalization for the Water Data getters.

Normalizes the heterogeneous user inputs (bare strings, lists, ``pandas``
Series/Index, ``numpy`` arrays, generators) into the ``list[str]`` shapes the
request layer expects, enforces the AGENCY-ID format for monitoring-location
ids, validates service/profile pairs, and assembles the request-kwargs dict
from a getter's ``locals()``. Depends only on
:mod:`dataretrieval.waterdata.utils.constants` and
:mod:`dataretrieval.waterdata.types`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, get_args

from dataretrieval.waterdata.types import (
    PROFILE_LOOKUP,
    PROFILES,
    SERVICES,
)
from dataretrieval.waterdata.utils.constants import (
    _MONITORING_LOCATION_ID_RE,
    _NO_NORMALIZE_PARAMS,
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


def _normalize_str_iterable(
    value: str | Iterable[str] | None,
    param_name: str = "value",
) -> str | list[str] | None:
    """Validate that ``value`` is None, a string, or an iterable of strings.

    Non-string iterables (``list``, ``tuple``, ``pandas.Series``,
    ``pandas.Index``, ``numpy.ndarray``, generators) are materialized to a
    ``list`` so downstream code that branches on ``isinstance(v, (list,
    tuple))`` keeps working. ``Mapping`` types are rejected because
    iterating a mapping yields keys, not values.

    Parameters
    ----------
    value : None, str, or iterable of str
    param_name : str, optional
        Used in error messages. Defaults to ``"value"``.

    Returns
    -------
    None, str, or list of str

    Raises
    ------
    TypeError
        If the input isn't ``None``, ``str``, or a non-``Mapping``
        iterable; or if any iterable element isn't a string.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping) or not isinstance(value, Iterable):
        raise TypeError(
            f"{param_name} must be a string or iterable of strings, "
            f"not {type(value).__name__} (got {value!r})."
        )
    values: list[str] = []
    for v in value:
        if not isinstance(v, str):
            raise TypeError(
                f"{param_name} elements must be strings, "
                f"not {type(v).__name__} (got {v!r})."
            )
        values.append(v)
    return values


def _as_str_list(
    value: str | Iterable[str] | None,
    param_name: str = "value",
) -> list[str] | None:
    """Normalize ``value`` to ``list[str]`` (``None`` passes through).

    Wraps a bare ``str`` in a single-element list — so a later
    ``",".join(...)`` doesn't iterate it character-by-character — and
    materializes any other iterable via :func:`_normalize_str_iterable`.
    """
    return (
        [value]
        if isinstance(value, str)
        else _normalize_str_iterable(value, param_name)
    )


def _check_monitoring_location_id(
    monitoring_location_id: str | Iterable[str] | None,
) -> str | list[str] | None:
    """Validate and normalize a ``monitoring_location_id`` value.

    Combines :func:`_normalize_str_iterable` with the AGENCY-ID format
    check that is unique to ``monitoring_location_id`` (the OGC spec
    requires a hyphen separator, e.g. ``USGS-01646500``).

    Parameters
    ----------
    monitoring_location_id : None, str, or iterable of str
        See :func:`_normalize_str_iterable`. Each string is additionally
        required to match the AGENCY-ID hyphen-separated format.

    Returns
    -------
    None, str, or list of str

    Raises
    ------
    TypeError
        If the input isn't ``None``, ``str``, or a non-``Mapping``
        iterable; or if any iterable element isn't a string.
    ValueError
        If any identifier doesn't contain a hyphen separator
        (per the OGC API spec: AGENCY-ID format, e.g. ``USGS-01646500``).
    """
    try:
        value = _normalize_str_iterable(
            monitoring_location_id, "monitoring_location_id"
        )
    except TypeError as exc:
        # Re-raise with the AGENCY-ID hint the generic helper doesn't carry.
        raise TypeError(
            f"{exc} Expected 'AGENCY-ID' format, e.g., 'USGS-01646500'."
        ) from None
    if value is None:
        return None
    for item in (value,) if isinstance(value, str) else value:
        _check_id_format(item)
    return value


def _check_id_format(value: str) -> None:
    """Raise ``ValueError`` if ``value`` is not in ``AGENCY-ID`` format."""
    if not _MONITORING_LOCATION_ID_RE.fullmatch(value):
        raise ValueError(
            f"Invalid monitoring_location_id: {value!r}. "
            f"Expected 'AGENCY-ID' format, e.g., 'USGS-01646500'."
        )


def _get_args(
    local_vars: dict[str, Any], exclude: set[str] | None = None
) -> dict[str, Any]:
    """
    Build the API-request kwargs dict from a getter's ``locals()``.

    Drops bookkeeping keys (``service``, ``output_id``, anything in
    ``exclude``) and ``None``-valued kwargs, then normalizes the
    remaining values:

    - ``monitoring_location_id`` is validated against the AGENCY-ID
      format (per :func:`_check_monitoring_location_id`).
    - ``properties`` is materialized to ``list[str]`` (a bare string
      gets wrapped in a single-element list so downstream
      ``",".join(properties)`` doesn't iterate per character).
    - Any other ``Iterable[str]`` that isn't in ``_NO_NORMALIZE_PARAMS``
      is materialized to ``list[str]`` via
      :func:`_normalize_str_iterable` so downstream code that branches
      on ``isinstance(v, (list, tuple))`` works for ``pandas.Series``,
      ``numpy.ndarray``, generators, etc.
    - Scalars, strings, and ``_NO_NORMALIZE_PARAMS`` values pass through
      unchanged.

    Parameters
    ----------
    local_vars : dict[str, Any]
        Dictionary of local variables, typically from ``locals()``.
    exclude : set[str], optional
        Additional keys to exclude from the resulting dictionary.

    Returns
    -------
    dict[str, Any]
        Filtered and normalized arguments for API requests.
    """
    to_exclude = {"service", "output_id"}
    if exclude:
        to_exclude.update(exclude)

    args: dict[str, Any] = {}
    for k, v in local_vars.items():
        if k in to_exclude or v is None:
            continue
        if k == "monitoring_location_id":
            args[k] = _check_monitoring_location_id(v)
        elif k == "properties":
            args[k] = _as_str_list(v, k)
        elif (
            k in _NO_NORMALIZE_PARAMS
            or isinstance(v, str)
            or not isinstance(v, Iterable)
        ):
            args[k] = v
        else:
            args[k] = _normalize_str_iterable(v, k)
    return args
