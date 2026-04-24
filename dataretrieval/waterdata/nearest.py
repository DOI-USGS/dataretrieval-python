"""Nearest-timestamp convenience on top of ``get_continuous``.

This module exists purely for isolation: ``get_nearest_continuous``
is built on top of the CQL ``filter`` passthrough (see
``dataretrieval/waterdata/filters.py``) and has no meaning without
it, so the two features live in two sibling modules that can be
deleted together.

Rolling back the filter feature:

- Delete ``dataretrieval/waterdata/filters.py``,
  ``dataretrieval/waterdata/nearest.py``, and their test files.
- Drop the ``FILTER_LANG`` and ``get_nearest_continuous`` imports
  from ``waterdata/__init__.py`` (two lines).
- Drop the ``filter`` / ``filter_lang`` kwargs from the eight OGC
  getters in ``api.py``.

Only one name is imported from this module — ``get_nearest_continuous``
— and that import sits in ``__init__.py``. Everything else is
package-private.
"""

from __future__ import annotations

from typing import Literal

import pandas as pd

from dataretrieval.utils import BaseMetadata
from dataretrieval.waterdata.api import get_continuous


def get_nearest_continuous(
    targets,
    monitoring_location_id: str | list[str] | None = None,
    parameter_code: str | list[str] | None = None,
    *,
    window: str | pd.Timedelta = "PT7M30S",
    on_tie: Literal["first", "last", "mean"] = "first",
    **kwargs,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """For each target timestamp, return the nearest continuous observation.

    Builds one bracketed ``(time >= t-window AND time <= t+window)`` clause
    per target, joins them as a top-level CQL ``OR`` filter, and lets
    ``get_continuous`` (with its auto-chunking) fetch every observation
    that falls in any window. Then, per ``(monitoring_location_id, target)``
    pair, picks the single observation with the smallest ``|time - target|``.

    The USGS continuous endpoint matches ``time`` parameters exactly rather
    than fuzzily, and it does not implement ``sortby`` for arbitrary fields;
    this function is the single-round-trip way to ask "what reading is
    nearest this timestamp?" for many timestamps at once.

    Parameters
    ----------
    targets : list-like of datetime-convertible
        Target timestamps. Naive datetimes are treated as UTC. Accepts a
        list, ``pandas.Series``, ``pandas.DatetimeIndex``, ``numpy.ndarray``,
        or anything ``pandas.to_datetime`` consumes.
    monitoring_location_id : string or list of strings, optional
        Forwarded to ``get_continuous``.
    parameter_code : string or list of strings, optional
        Forwarded to ``get_continuous``.
    window : string or ``pandas.Timedelta``, default ``"PT7M30S"``
        Half-window around each target, as an ISO 8601 duration
        (``"PT7M30S"``, ``"PT15M"``, ``"PT1H"``, etc.). Also accepts
        any other form ``pandas.Timedelta`` parses — ``HH:MM:SS``
        (``"00:07:30"``), pandas shorthand (``"7min30s"``,
        ``"450s"``), or a ``pd.Timedelta`` directly. See the
        `pandas.Timedelta docs
        <https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html>`_
        for the full grammar.

        Must be small enough that every target's window captures
        roughly one observation at the service cadence. The default
        matches a 15-minute continuous gauge; widen (e.g.
        ``"PT15M"``) for irregular cadences or resilience to data
        gaps.
    on_tie : {"first", "last", "mean"}, default ``"first"``
        How to resolve ties when two observations are exactly equidistant
        from a target (which happens when the target falls at the midpoint
        between grid points — e.g. target ``10:22:30`` for a 15-minute
        gauge).

        - ``"first"``: keep the earlier observation.
        - ``"last"``:  keep the later observation.
        - ``"mean"``:  average numeric columns; set the ``time`` column to
          the target, since no real observation exists at the midpoint.

    **kwargs
        Additional keyword arguments forwarded to ``get_continuous``
        (e.g. ``statistic_id``, ``approval_status``, ``properties``).
        Passing ``time``, ``filter``, or ``filter_lang`` raises
        ``TypeError`` — this function builds those itself.

    Returns
    -------
    df : ``pandas.DataFrame``
        One row per ``(target, monitoring_location_id)`` combination that
        had at least one observation in its window. Rows are augmented
        with a ``target_time`` column indicating which target they
        correspond to. Targets with no observations in their window are
        silently dropped.
    md : :class:`~dataretrieval.utils.BaseMetadata`
        Metadata from the underlying ``get_continuous`` call.

    Notes
    -----
    *Window sizing and ties.* When ``window`` is exactly half the service
    cadence, most targets' windows contain a single observation and
    ``on_tie`` is moot. Ties arise only when a target sits exactly at the
    window edge — rare in practice but possible. Setting ``window`` to a
    full cadence (or larger) guarantees at least one observation per
    target in steady state at the cost of more bytes per response.

    *Why windowed CQL rather than sort+limit.* The API's advertised
    ``sortby`` parameter would make this a one-liner per target (``filter``
    by ``time <= t`` and ``limit 1``), but it is per-query — you would need
    one HTTP round-trip per target. The CQL ``OR``-chain approach folds
    all N targets into one request (auto-chunked when the URL is long).

    Examples
    --------
    .. code::

        >>> import pandas as pd
        >>> from dataretrieval import waterdata

        >>> # Pair three off-grid timestamps with nearby observations
        >>> targets = pd.to_datetime(
        ...     [
        ...         "2023-06-15T10:30:31Z",
        ...         "2023-06-15T14:07:12Z",
        ...         "2023-06-16T03:45:19Z",
        ...     ]
        ... )
        >>> df, md = waterdata.get_nearest_continuous(
        ...     targets,
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00060",
        ... )

        >>> # Widen the window for an irregular-cadence gauge
        >>> df, md = waterdata.get_nearest_continuous(
        ...     targets,
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00060",
        ...     window="PT30M",
        ...     on_tie="mean",
        ... )
    """
    _check_nearest_kwargs(kwargs, on_tie)
    targets = pd.DatetimeIndex(pd.to_datetime(targets, utc=True))
    window_td = pd.Timedelta(window)

    if len(targets) == 0:
        raise ValueError("targets must contain at least one timestamp")

    filter_expr = _build_window_or_filter(targets, window_td)
    df, md = get_continuous(
        monitoring_location_id=monitoring_location_id,
        parameter_code=parameter_code,
        filter=filter_expr,
        filter_lang="cql-text",
        **kwargs,
    )
    if df.empty:
        return _empty_nearest_result(df), md

    df = df.assign(time=pd.to_datetime(df["time"], utc=True))
    site_groups = (
        df.groupby("monitoring_location_id", sort=False)
        if "monitoring_location_id" in df.columns
        else [(None, df)]
    )

    selected = [
        row
        for _, site_df in site_groups
        for target in targets
        if (row := _pick_nearest_row(site_df, target, window_td, on_tie)) is not None
    ]
    if not selected:
        return _empty_nearest_result(df), md
    return pd.DataFrame(selected).reset_index(drop=True), md


_VALID_ON_TIE = ("first", "last", "mean")


def _check_nearest_kwargs(kwargs: dict, on_tie: str) -> None:
    """Reject kwargs the helper owns; validate ``on_tie``."""
    for forbidden in ("time", "filter", "filter_lang"):
        if forbidden in kwargs:
            raise TypeError(
                f"get_nearest_continuous constructs its own {forbidden!r}; "
                "do not pass it directly"
            )
    if on_tie not in _VALID_ON_TIE:
        raise ValueError(f"on_tie must be one of {_VALID_ON_TIE}; got {on_tie!r}")


def _build_window_or_filter(targets: pd.DatetimeIndex, window_td: pd.Timedelta) -> str:
    """Build the CQL OR-chain of ``time >= ... AND time <= ...`` windows.

    ``get_continuous`` auto-chunks the result if the full URL would
    exceed the server's length limit, so this is always safe to build
    as one string even for many targets.
    """
    return " OR ".join(
        f"(time >= '{(t - window_td).strftime('%Y-%m-%dT%H:%M:%SZ')}' "
        f"AND time <= '{(t + window_td).strftime('%Y-%m-%dT%H:%M:%SZ')}')"
        for t in targets
    )


def _pick_nearest_row(
    site_df: pd.DataFrame,
    target: pd.Timestamp,
    window_td: pd.Timedelta,
    on_tie: str,
) -> pd.Series | None:
    """Return the single row within ``window_td`` of ``target``, or ``None``.

    Resolves ties (two rows equidistant from ``target``) per ``on_tie``.
    The returned row carries a ``target_time`` column identifying which
    target it was selected for.
    """
    in_window = site_df[
        (site_df["time"] >= target - window_td)
        & (site_df["time"] <= target + window_td)
    ]
    if in_window.empty:
        return None
    deltas = (in_window["time"] - target).abs()
    candidates = in_window[deltas == deltas.min()].sort_values("time")

    if len(candidates) == 1 or on_tie == "first":
        row = candidates.iloc[0].copy()
    elif on_tie == "last":
        row = candidates.iloc[-1].copy()
    else:  # "mean" — synthesize a row whose numeric cols are averaged and
        # whose ``time`` is the target (no real observation sits at the midpoint).
        row = candidates.iloc[0].copy()
        for col in candidates.select_dtypes("number").columns:
            row[col] = candidates[col].mean()
        row["time"] = target

    row["target_time"] = target
    return row


def _empty_nearest_result(template: pd.DataFrame | None = None) -> pd.DataFrame:
    """Empty frame with a ``target_time`` column, for no-match cases.

    When ``template`` is provided, preserve its columns/dtypes so the
    returned frame matches the shape of a real ``get_continuous``
    response.
    """
    base = pd.DataFrame() if template is None else template.iloc[0:0].copy()
    base["target_time"] = pd.Series(dtype="datetime64[ns, UTC]")
    return base
