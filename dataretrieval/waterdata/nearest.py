"""``get_nearest_continuous``: nearest-timestamp convenience on top of
``get_continuous``. Built on the CQL ``filter`` passthrough; only
``get_nearest_continuous`` is public — everything else is package-private.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, get_args

import httpx
import pandas as pd

from dataretrieval._validation import require_one_of
from dataretrieval.interruptions import FanOutInterrupted
from dataretrieval.waterdata.time_series import get_continuous

if TYPE_CHECKING:
    from dataretrieval._response_metadata import BaseMetadata

__all__ = ["get_nearest_continuous"]


OnTie = Literal["first", "last", "mean"]
_VALID_ON_TIE: tuple[OnTie, ...] = get_args(OnTie)


class _ResumableCall(Protocol):
    """Structural subset of a fan-out call needed by the outer decorator."""

    @property
    def partial_frame(self) -> pd.DataFrame: ...

    @property
    def partial_response(self) -> httpx.Response | None: ...

    def resume(self) -> tuple[pd.DataFrame, BaseMetadata]: ...


class _MutableInterruption(Protocol):
    """Writable call slot exposed by fan-out interruption instances."""

    call: _ResumableCall | None


class _NearestCall:
    """Preserve nearest-result semantics around an interrupted inner call."""

    def __init__(
        self,
        inner: _ResumableCall,
        targets: pd.DatetimeIndex,
        window_td: pd.Timedelta,
        on_tie: OnTie,
    ) -> None:
        self._inner = inner
        self._targets = targets
        self._window_td = window_td
        self._on_tie = on_tie

    @property
    def partial_frame(self) -> pd.DataFrame:
        """Return the live partial rows in the outer getter's shape."""
        return _select_nearest_partial(
            self._inner.partial_frame,
            self._targets,
            self._window_td,
            self._on_tie,
        )

    @property
    def partial_response(self) -> httpx.Response | None:
        """Pass through the inner call's live aggregate response."""
        return self._inner.partial_response

    def resume(self) -> tuple[pd.DataFrame, BaseMetadata]:
        """Resume inner work and apply the outer getter's selection."""
        try:
            frame, metadata = self._inner.resume()
        except FanOutInterrupted as exc:
            _shape_interruption(exc, self._targets, self._window_td, self._on_tie)
            raise
        return (
            _select_nearest_rows(frame, self._targets, self._window_td, self._on_tie),
            metadata,
        )


def _shape_interruption(
    exc: FanOutInterrupted,
    targets: pd.DatetimeIndex,
    window_td: pd.Timedelta,
    on_tie: OnTie,
) -> None:
    """Decorate one inner interruption with the outer getter's semantics."""
    exc.partial_frame = _select_nearest_partial(
        exc.partial_frame, targets, window_td, on_tie
    )
    if exc.call is not None:
        cast("_MutableInterruption", exc).call = _NearestCall(
            exc.call, targets, window_td, on_tie
        )


def _select_nearest_partial(
    frame: pd.DataFrame,
    targets: pd.DatetimeIndex,
    window_td: pd.Timedelta,
    on_tie: OnTie,
) -> pd.DataFrame:
    """Select partial rows, including the no-completed-chunks empty shape."""
    if frame.empty and "time" not in frame.columns:
        return _empty_nearest_result(frame)
    return _select_nearest_rows(frame, targets, window_td, on_tie)


def get_nearest_continuous(
    targets: Iterable[Any],
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    *,
    window: str | pd.Timedelta = "PT7M30S",
    on_tie: OnTie = "first",
    **kwargs: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Return the nearest continuous observation to each target timestamp.

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
    monitoring_location_id : string or iterable of strings, optional
        Forwarded to ``get_continuous``.
    parameter_code : string or iterable of strings, optional
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
        matches a 15-minute continuous gage; widen (e.g.
        ``"PT15M"``) for irregular cadences or resilience to data
        gaps.
    on_tie : {"first", "last", "mean"}, default ``"first"``
        How to resolve ties when two observations are exactly equidistant
        from a target (which happens when the target falls at the midpoint
        between grid points — e.g. target ``10:22:30`` for a 15-minute
        gage).

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

    Raises
    ------
    FanOutInterrupted
        If the underlying fan-out is interrupted. ``partial_frame`` and
        ``call.partial_frame`` contain nearest-selected rows with
        ``target_time``; ``call.resume()`` returns that same public shape.

    Notes
    -----
    *Window sizing and ties.* When ``window`` is exactly half the service
    cadence, most targets' windows contain a single observation and
    ``on_tie`` is moot. Ties arise only when a target sits exactly at the
    midpoint between two grid observations — rare in practice but possible.
    Setting ``window`` to a full cadence (or larger) guarantees at least one
    observation per target in steady state at the cost of more bytes per
    response.

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

        >>> # Widen the window for an irregular-cadence gage
        >>> df, md = waterdata.get_nearest_continuous(
        ...     targets,
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00060",
        ...     window="PT30M",
        ...     on_tie="mean",
        ... )
    """
    _check_nearest_kwargs(kwargs, on_tie)
    target_index = _coerce_targets(targets)
    window_td = pd.Timedelta(window)

    if len(target_index) == 0:
        raise ValueError("targets must contain at least one timestamp")

    filter_expr = _build_window_or_filter(target_index, window_td)
    try:
        df, md = get_continuous(
            monitoring_location_id=monitoring_location_id,
            parameter_code=parameter_code,
            filter=filter_expr,
            filter_lang="cql-text",
            **kwargs,
        )
    except FanOutInterrupted as exc:
        _shape_interruption(exc, target_index, window_td, on_tie)
        raise
    return _select_nearest_rows(df, target_index, window_td, on_tie), md


def _select_nearest_rows(
    df: pd.DataFrame,
    targets: pd.DatetimeIndex,
    window_td: pd.Timedelta,
    on_tie: OnTie,
) -> pd.DataFrame:
    """Apply the public nearest-per-target shape to continuous rows."""
    if "time" not in df.columns:
        raise ValueError(
            "get_nearest_continuous requires a 'time' column in the response; "
            "if a `properties` kwarg was passed, include 'time' in it"
        )
    if df.empty:
        return _empty_nearest_result(df)

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
        return _empty_nearest_result(df)
    return pd.DataFrame(selected).reset_index(drop=True)


def _coerce_targets(targets: Any) -> pd.DatetimeIndex:
    """Accept anything ``pandas.to_datetime`` consumes, including a single value.

    A bare scalar (string, ``Timestamp``, ``datetime``, …) becomes a
    one-element ``DatetimeIndex``; an iterable (list, ``Series``, ``ndarray``)
    is wrapped directly so its elements are preserved.
    """
    parsed = pd.to_datetime(targets, utc=True)
    if pd.api.types.is_scalar(parsed):
        parsed = [parsed]
    return pd.DatetimeIndex(parsed)


def _check_nearest_kwargs(kwargs: dict[str, Any], on_tie: OnTie) -> None:
    """Reject kwargs the helper owns; validate ``on_tie``."""
    for forbidden in ("time", "filter", "filter_lang"):
        if forbidden in kwargs:
            raise TypeError(
                f"get_nearest_continuous constructs its own {forbidden!r}; "
                "do not pass it directly"
            )
    require_one_of(on_tie, _VALID_ON_TIE, name="on_tie")


def _build_window_or_filter(targets: pd.DatetimeIndex, window_td: pd.Timedelta) -> str:
    """Build the CQL OR-chain of ``time >= ... AND time <= ...`` windows.

    ``get_continuous`` auto-chunks the result if the full URL would
    exceed the server's length limit, so this is always safe to build
    as one string even for many targets.
    """
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    lowers = (targets - window_td).strftime(fmt)
    uppers = (targets + window_td).strftime(fmt)
    return " OR ".join(
        f"(time >= '{lo}' AND time <= '{up}')"
        for lo, up in zip(lowers, uppers, strict=False)
    )


def _pick_nearest_row(
    site_df: pd.DataFrame,
    target: pd.Timestamp,
    window_td: pd.Timedelta,
    on_tie: OnTie,
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
    else:  # "mean" — average numeric cols, set time to the target.
        row = candidates.iloc[0].copy()
        for col in candidates.select_dtypes("number").columns:
            row[col] = candidates[col].mean()
        row["time"] = target

    row["target_time"] = target
    return row


def _empty_nearest_result(template: pd.DataFrame) -> pd.DataFrame:
    """Empty frame matching ``template``'s columns plus a ``target_time``."""
    base = template.iloc[0:0].copy()
    base["target_time"] = pd.Series(dtype="datetime64[ns, UTC]")
    return base
