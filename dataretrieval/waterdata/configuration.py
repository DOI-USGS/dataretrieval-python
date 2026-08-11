"""The settings the Water Data adapter reads -- its configuration profile.

A file of its own because :mod:`dataretrieval.waterdata` is a package rather
than a single module; every other adapter declares its class in the module a
caller imports. Either way the point is the same: a setting's definition sits
with the code that reads it, so adding one no longer edits a service-neutral
file (ADR 0011).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from dataretrieval.configuration import _UNSET, BaseConfiguration, _register

__all__ = ["WaterdataConfiguration"]


@dataclass(frozen=True)
class WaterdataConfiguration(BaseConfiguration):
    """Settings for Water Data calls alone.

    Pass one to :func:`dataretrieval.configure` to narrow a setting to this
    service, leaving every other adapter on whatever the tiers below it
    resolve::

        with dataretrieval.configure(WaterdataConfiguration(concurrency=8)):
            df, md = waterdata.get_daily(monitoring_location_id=sites)

    Parameters
    ----------
    retries : int, optional
        Retries attempted after a transient failure; ``0`` disables retrying.
    stall_timeout : float, optional
        Seconds a call may go without receiving any data before retrying stops.
    base_url : str, optional
        Where to send Water Data requests, instead of the service's own base.
        Code only -- the file and the environment refuse it.
    concurrency : int or str, optional
        Cap on simultaneous sub-requests, or ``"unbounded"``.
    parallel_chunks : int, optional
        Baseline fan-out for multi-value queries. Each sub-request spends
        rate-limit quota, so raise it only for pulls you know are large.
    """

    adapter: ClassVar[str] = "waterdata"

    retries: int | None = _UNSET
    stall_timeout: float | int | None = _UNSET
    base_url: str | None = _UNSET
    # Water Data queries divide along a URL byte budget and are executed
    # concurrently, so both fan-out dials mean something here.
    concurrency: int | str | None = _UNSET
    parallel_chunks: int | None = _UNSET


_register(WaterdataConfiguration)
