"""Argument validation and normalization for the OGC API getters.

Identifier switching (``_switch_arg_id`` / ``_switch_properties_id``),
string-iterable normalization, and the ``locals()``-to-request-kwargs builder
(``_get_args``).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from dataretrieval.ogc._constants import (
    _MONITORING_LOCATION_ID_RE,
    _NO_NORMALIZE_PARAMS,
)


def _switch_arg_id(ls: dict[str, Any], id_name: str, service: str) -> dict[str, Any]:
    """
    Switch argument id from its package-specific identifier to the standardized "id" key
    that the API recognizes.

    If `ls` does not already have an "id" key, sets it from either the
    service-derived id key or the expected id column name. If neither key
    exists, "id" is left unset. The original service-specific id keys are
    removed regardless.

    Parameters
    ----------
    ls : Dict[str, Any]
        The dictionary containing identifier keys to be standardized.
    id_name : str
        The name of the specific identifier key to look for.
    service : str
        The service name.

    Returns
    -------
    Dict[str, Any]
        The modified dictionary with the "id" key set appropriately.

    Examples
    --------
    For service "time-series-metadata", the function will look for either
    "time_series_metadata_id" or "time_series_id" and change the key to simply
    "id".
    """

    service_id = service.replace("-", "_") + "_id"

    if "id" not in ls:
        if service_id in ls:
            ls["id"] = ls[service_id]
        elif id_name in ls:
            ls["id"] = ls[id_name]

    # Remove the original keys regardless of whether they were used
    ls.pop(service_id, None)
    ls.pop(id_name, None)

    return ls


def _switch_properties_id(
    properties: list[str] | None, id_name: str, service: str
) -> list[str]:
    """
    Build the wire ``properties`` list, dropping every id alias and
    ``geometry``.

    The feature ``id`` is always returned and is renamed to the
    service-specific id column (e.g. ``daily_id``) in post-processing, so
    it must not be requested as a property: several collections (e.g.
    ``daily``, ``continuous``) reject ``id`` in ``properties`` with an
    HTTP 400. ``geometry`` is likewise excluded because it is controlled
    by ``skip_geometry``. Any service-specific id name (``daily_id``,
    ``monitoring_location_id``, …) and the bare ``id`` are dropped, and
    remaining hyphens are normalized to underscores. Returns an empty
    list when `properties` is empty or None — the URL then omits the
    ``properties`` filter and the result is shaped by :func:`_arrange_cols`.

    Parameters
    ----------
    properties : Optional[List[str]]
        A list containing the properties or column names to be pulled from the
        service, or None.
    id_name : str
        The service-specific id column name to drop (e.g. ``daily_id``).
    service : str
        The service name.

    Returns
    -------
    List[str]
        The wire ``properties`` with id aliases and ``geometry`` removed
        and hyphens normalized.

    Examples
    --------
    For service "daily" with ``properties=["daily_id", "value", "geometry"]``,
    returns ``["value"]`` — ``daily_id`` and ``geometry`` are dropped, while
    the ``daily_id`` column still appears in the result, renamed from the
    always-returned feature ``id``.
    """
    if not properties:
        return []
    service_id = service.replace("-", "_") + "_id"
    # The feature ``id`` always comes back (renamed to the service id
    # downstream) and several collections reject it as a selectable
    # property; ``geometry`` is controlled by ``skip_geometry``. Drop both,
    # plus the service-specific id column (``id_name``) and the name derived
    # straight from the service (``service_id``).
    drop = {"id", "geometry", id_name, service_id}
    normalized = (p.replace("-", "_") for p in properties)
    return [p for p in normalized if p not in drop]


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
    normalized = _normalize_str_iterable(value, param_name)
    if isinstance(normalized, str):
        return [normalized]
    return normalized


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
    local_vars: dict[str, Any],
    exclude: set[str] | None = None,
    *,
    no_normalize: frozenset[str] | set[str] = _NO_NORMALIZE_PARAMS,
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
    - A non-string iterable in ``no_normalize`` (numeric params
      such as ``water_year``, ``bbox``, ``thresholds``) is materialized
      to a ``list`` with its element types preserved (no string
      normalization), so the GET comma-join and the chunker — which test
      ``list``/``tuple`` — handle it instead of ``str()``-ing the whole
      array.
    - Any other ``Iterable[str]`` (i.e. not in ``no_normalize``)
      is materialized to ``list[str]`` via
      :func:`_normalize_str_iterable` so downstream code that branches
      on ``isinstance(v, (list, tuple))`` works for ``pandas.Series``,
      ``numpy.ndarray``, generators, etc.
    - Scalars and strings pass through unchanged.

    Parameters
    ----------
    local_vars : dict[str, Any]
        Dictionary of local variables, typically from ``locals()``.
    exclude : set[str], optional
        Additional keys to exclude from the resulting dictionary.
    no_normalize : set[str], optional
        Iterable-shaped params whose element types must be preserved
        (no string normalization). Defaults to the generic date-range +
        ``bbox`` set; callers with extra numeric params pass a superset.

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
        elif k in no_normalize and isinstance(v, Iterable) and not isinstance(v, str):
            # Numeric params (water_year, bbox, thresholds, …) keep their
            # element types — no string-normalization — but a non-string
            # iterable (numpy array, pandas Series, generator) is materialized
            # to a list so the GET comma-join and the chunker, which test
            # ``list``/``tuple``, handle it instead of str()-ing the whole
            # array. ``.tolist()`` yields native int/float; ``list()`` covers
            # generators and other iterables. Scalars/strings fall through.
            args[k] = v.tolist() if hasattr(v, "tolist") else list(v)
        elif isinstance(v, str) or not isinstance(v, Iterable):
            args[k] = v
        else:
            args[k] = _normalize_str_iterable(v, k)
    return args
