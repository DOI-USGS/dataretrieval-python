"""The settings the Water Data adapter reads -- its configuration profile.

A file of its own because :mod:`dataretrieval.waterdata` is a package rather
than a single module; every other adapter declares its class in the module a
caller imports. Either way the point is the same: a setting's definition sits
with the code that reads it, so adding one no longer edits a service-neutral
file (ADR 0011).
"""

from __future__ import annotations

from typing import ClassVar

from dataretrieval.settings import (
    AdapterSettings,
    _Chunked,
    _Concurrent,
    _Redirectable,
    _register,
    _Retrying,
)

__all__ = ["WaterdataSettings"]


class WaterdataSettings(
    _Chunked, _Concurrent, _Redirectable, _Retrying, AdapterSettings
):
    """Settings for Water Data calls alone.

    Pass one to :func:`dataretrieval.configure` to narrow a setting to this
    service, leaving every other adapter on whatever the tiers below it
    resolve::

        with dataretrieval.configure(WaterdataSettings(concurrency=8)):
            df, md = waterdata.get_daily(monitoring_location_id=sites)

    Parameters
    ----------
    retries : int, optional
        Retries attempted after a transient failure; ``0`` disables retrying.
    stall_timeout : float, optional
        Seconds a call may go without receiving any data before retrying stops.
    base_url : str, optional
        Root to send Water Data requests to, instead of the service's own. The
        package appends its own paths, so one value moves all four families
        together -- ``/ogcapi/v0``, ``/samples-data``, ``/statistics/v0`` and
        ``/stac/v0``. Code only: the file and the environment refuse it. The
        API key is scoped to the host that honors it, so a redirected call
        carries no key.
    concurrency : int or str, optional
        Cap on simultaneous sub-requests, or ``"unbounded"``.
    parallel_chunks : int, optional
        Baseline fan-out for multi-value queries. Each sub-request spends
        rate-limit quota, so raise it only for pulls you know are large.
    """

    # The settings this service reads, named by the groups they come from:
    # every adapter's retry dials, a redirectable base, and -- because Water
    # Data queries divide along a URL byte budget and are executed concurrently
    # -- both fan-out dials. Each group declares the setting itself once, in
    # :mod:`dataretrieval.settings`, which is also where its grammar and
    # its coercion live.
    adapter: ClassVar[str] = "waterdata"


_register(WaterdataSettings)
