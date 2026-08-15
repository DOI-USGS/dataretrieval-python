"""Retrieve USGS water-use data from the NWDC web service.

The National Water Availability Assessment Data Companion (NWDC) web services
provide national-scale, USGS-modeled water-use data that underlie the `USGS
National Water Availability Assessment <https://water.usgs.gov/nwaa-data/>`_.
Estimates are served on a HUC12 (12-digit hydrologic unit) spatial grid and can
be queried for any county, state, or hydrologic unit. This is the modern
replacement for the defunct legacy NWIS water-use service
(``nwis.get_water_use``).

Unlike the main Water Data getters (:mod:`dataretrieval.waterdata`) and NGWMN
(:mod:`dataretrieval.ngwmn`), the NWDC is a plain CSV REST service rather than
an OGC API Features collection. This module supplies the NWDC-specific bits —
request building, CSV parsing, the ``Link``-header cursor, and the ``{detail}``
error envelope. The service-neutral transport layer supplies cursor pagination,
response aggregation, client lifecycle, and sync-from-async dispatch. The module
follows the same conventions: host-scoped request headers, the typed
:class:`~dataretrieval.exceptions.DataRetrievalError` taxonomy, and a
``(DataFrame, BaseMetadata)`` return.

See https://api.water.usgs.gov/docs/nwaa-data/ for the API reference and
https://water.usgs.gov/nwaa-data/ for the catalog of available models and
variables.

Examples
--------
.. code-block:: python

    from dataretrieval import nwdc

    # Monthly public-supply withdrawals for Rhode Island, 2020 onward.
    df, md = nwdc.get_wateruse(
        model="wu-public-supply-wd",
        variable=["pswdtot", "pswdgw", "pswdsw"],
        state="RI",
        start_date="2020-01",
        time_resolution="monthly",
    )

"""

from __future__ import annotations

import io
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, ClassVar

import httpx
import pandas as pd

from dataretrieval import configuration as _configuration
from dataretrieval._querying import _raise_for_status, to_str
from dataretrieval._response_metadata import BaseMetadata
from dataretrieval.codes.states import to_state
from dataretrieval.configuration import (
    BaseConfiguration,
    _Concurrent,
    _Redirectable,
    _register,
    _Retrying,
)
from dataretrieval.exceptions import DataRetrievalError
from dataretrieval.transport.http import default_headers
from dataretrieval.transport.links import resolve_next_url
from dataretrieval.transport.pagination import run_paginated

__all__ = [
    "NwdcConfiguration",
    "get_wateruse",
    "WATERUSE_URL",
    "MODELS",
    "TIME_RESOLUTIONS",
    "DEFAULT_CONCURRENT_REQUESTS",
]

WATERUSE_URL = "https://api.water.usgs.gov/nwaa-data/data"
_WATERUSE_HOST = httpx.URL(WATERUSE_URL).host
# Hosts a ``rel="next"`` cursor may name for this same service; each is
# rewritten to :data:`_WATERUSE_HOST` rather than followed as given.
_WATERUSE_HOST_ALIASES = frozenset({_WATERUSE_HOST, "water.usgs.gov"})

#: Water-use models (categories) served by the NWDC. The catalog at
#: https://water.usgs.gov/nwaa-data/ lists the variables available within each.
MODELS = (
    "wu-public-supply-wd",  # public-supply withdrawals
    "wu-public-supply-cu",  # public-supply consumptive use
    "wu-thermoelectric",  # thermoelectric-power water use
    "wu-irrigation-wd",  # irrigation withdrawals
    "wu-irrigation-cu",  # irrigation consumptive use
)

#: Temporal resolutions: monthly, annual calendar year, annual water year.
TIME_RESOLUTIONS = ("monthly", "annualcy", "annualwy")

#: This service's preferred in-flight cap when nothing is configured. Lower
#: than the package default of 32 because every location retries
#: independently, so a rate-limit episode bursts this number times the retry
#: count; the NWDC tolerates this level without rate-limit errors (verified by
#: stress test) and higher has not been tested. Any configured concurrency
#: overrides it -- see :func:`dataretrieval.configuration.concurrency` for why the
#: general setting outranks a module's default rather than the reverse.
DEFAULT_CONCURRENT_REQUESTS = 4

# Page responses carry the HUC12 identifier in this column; it must stay a
# string so leading zeros (e.g. "010900020502") survive the round trip.
_HUC12_COLUMN = "huc12_id"


def get_wateruse(
    model: str,
    variable: str | Iterable[str] | None = None,
    state: str | int | Iterable[str | int] | None = None,
    county: str | Iterable[str] | None = None,
    huc: str | Iterable[str] | None = None,
    time_resolution: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    intersection: str = "overlap",
    limit: int = 600,
    ssl_check: bool = True,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get USGS water-use data from the NWDC web service.

    Retrieves modeled water-use estimates from the USGS National Water
    Availability Assessment Data Companion. The area is given as exactly one of
    ``state``, ``county``, or ``huc``; results are always returned on a HUC12
    grid, in a long (tidy) frame with one row per HUC12 and time step. Large
    areas (e.g. a whole region or a populous state) are served across multiple
    pages; this function follows those pages transparently and concatenates
    them into one frame.

    Each selector also accepts a list of values. The NWDC queries one area per
    request, so a list is fanned out into one request per value — up to the
    effective ``concurrency`` setting in parallel, defaulting to
    :data:`DEFAULT_CONCURRENT_REQUESTS` for this service — and the results are
    concatenated in the order given. That cap resolves through the
    configuration chain, so it can be raised or lowered for this service alone
    (``configure(NwdcConfiguration(concurrency=2))``, or an ``[nwdc]`` table in
    the config file) as well as package-wide via ``API_USGS_CONCURRENT``; see
    :doc:`the configuration guide </userguide/configuration>`. A fan-out
    interrupted by a rate limit or an upstream fault raises a resumable
    :class:`~dataretrieval.interruptions.FanOutInterrupted`, whose
    ``.call.resume()`` re-issues only the locations that did not complete.

    Parameters
    ----------
    model : string
        Water-use category to query. See :data:`MODELS` for the available
        options (e.g. ``"wu-public-supply-wd"``). The full catalog of models
        and their variables is at https://water.usgs.gov/nwaa-data/.
    variable : string or iterable of strings, optional
        One or more variable IDs within ``model`` (e.g. ``"pswdtot"`` for total
        public-supply withdrawals, or ``["pswdgw", "pswdsw"]`` for the
        groundwater and surface-water components). Multiple variables are
        comma-joined into a single request. The service requires at least one
        variable; omitting it returns a 400 listing the model's valid variable
        IDs (surfaced as a :class:`~dataretrieval.exceptions.DataRetrievalError`).
    state : string, int, or iterable, optional
        One or more US states/territories to query. Each accepts a full name
        (``"Wisconsin"``), a two-letter postal code (``"WI"``), or a two-digit
        ANSI/FIPS code (``"55"`` or ``55``), mirroring
        :func:`dataretrieval.ngwmn.get_sites`.
    county : string or iterable, optional
        One or more five-digit county FIPS codes — state FIPS + county FIPS,
        e.g. ``"55025"`` for Dane County, Wisconsin.
    huc : string or iterable, optional
        One or more hydrologic unit codes. Each code's level is taken from its
        length: a 2-digit code queries a HUC2 region, 8-digit a HUC8 subbasin,
        12-digit a single HUC12, and so on (even lengths 2-12, e.g. ``"04"``,
        ``"07070005"``, ``"010900020502"``).

        Provide exactly one of ``state``, ``county``, or ``huc`` (each may be a
        single value or a list).
    time_resolution : string, optional
        Temporal resolution: ``"monthly"``, ``"annualcy"`` (annual, calendar
        year), or ``"annualwy"`` (annual, water year). See
        :data:`TIME_RESOLUTIONS`.
    start_date : string, optional
        Start of the query window, formatted ``"YYYY"`` for annual data or
        ``"YYYY-MM"`` for monthly data.
    end_date : string, optional
        End of the query window, in the same format as ``start_date``.
    intersection : string, optional
        How to select HUC12s that straddle the queried-area boundary:
        ``"overlap"`` (any overlap, the default) or ``"envelop"`` (fully
        enclosed).
    limit : int, optional
        Maximum number of HUC12s returned per page. Queries spanning more than
        ``limit`` HUC12s are split across pages and reassembled. Default 600.
    ssl_check : bool, optional
        If True (default), verify SSL certificates; set False to skip
        verification (e.g. behind a TLS-intercepting proxy).

    Returns
    -------
    df : ``pandas.DataFrame``
        Water-use estimates in long form: a ``huc12_id`` column (string,
        leading zeros preserved), a time column (``year_month`` for monthly
        data or ``year`` for annual data), and one value column per requested
        variable (suffixed with its unit, e.g. ``pswdtot_mgd`` for million
        gallons per day).
    md : :class:`dataretrieval.utils.BaseMetadata`
        Metadata describing the request (URL, query time, response headers).

    Raises
    ------
    ValueError
        If not exactly one of ``state``, ``county``, or ``huc`` is given, or a
        given selector is malformed (an unrecognized state, a county code that
        is not five digits, or a HUC of invalid length).
    DataRetrievalError
        On an HTTP error response, the typed subclass for the status (see
        :func:`dataretrieval.exceptions.error_for_status`). A transient 429,
        5xx, or recoverable connection failure that exhausts inline retries is
        raised as a resumable
        :class:`~dataretrieval.interruptions.FanOutInterrupted`; a deterministic
        connection failure (for example, a permanently unresolvable host)
        remains a :class:`~dataretrieval.exceptions.NetworkError`.

    Examples
    --------
    .. doctest::
        :skipif: True  # network

        >>> from dataretrieval import nwdc
        >>> df, md = nwdc.get_wateruse(
        ...     model="wu-public-supply-wd",
        ...     variable=["pswdtot", "pswdgw", "pswdsw"],
        ...     state="RI",
        ...     start_date="2020-01",
        ...     time_resolution="monthly",
        ... )

    """
    # The public parameters are idiomatic snake_case (consistent with
    # ``waterdata.get_samples``); the NWDC service expects compact lowercase
    # query names, so map to those here as the request is built.
    base_params: dict[str, Any] = {
        "format": "csv",
        "model": model,
        "variable": to_str(variable),
        "timeres": time_resolution,
        "startdate": start_date,
        "enddate": end_date,
        "intersection": intersection,
        "limit": limit,
    }
    # Drop params the caller left unset; the service rejects empty values.
    base_params = {k: v for k, v in base_params.items() if v is not None}

    # An ``NwdcConfiguration(base_url=...)`` from an enclosing block, or this
    # service's own endpoint. Resolved once per call -- the block is scoped to
    # a ``with`` statement -- and threaded through every request and the page
    # walk, so a redirected call cannot half-follow the redirect.
    service_url = _configuration.base_url(adapter="nwdc", default=WATERUSE_URL)

    # The NWDC queries one location per request, so fan a multi-value selector
    # out into one request per location, each handled by shared transport
    # pagination, and concatenate the results.
    headers = default_headers(service_url)
    requests = [
        httpx.Request(
            "GET",
            service_url,
            params={**base_params, "location": location},
            headers=headers,
        )
        for location in _resolve_locations(state, county, huc)
    ]
    return _fan_out(requests, headers, ssl_check, host=httpx.URL(service_url).host)


# Valid HUC code lengths (digits) → the hydrologic-unit level they query.
_HUC_LENGTHS = (2, 4, 6, 8, 10, 12)

# Maps each selector to the NWDC ``location=<type>:<id>`` value(s) it produces.
# A value may be a single code or a list; ``_as_list`` normalizes both (``state``
# additionally normalizes to the two-letter postal code, and ``to_state`` may
# itself return a scalar or list, which ``_as_list`` flattens the same way).
# Since NWDC takes one location per request, a list value fans out — one request
# per location (see :func:`_fan_out`).
_LOCATION_BUILDERS: dict[str, Callable[[Any], list[str]]] = {
    "state": lambda v: [f"stateCd:{c}" for c in _as_list(to_state(v, to="postal"))],
    "county": lambda v: [f"countyCd:{_validate_county(c)}" for c in _as_list(v)],
    "huc": lambda v: [f"huc{len(c)}:{c}" for c in map(_validate_huc, _as_list(v))],
}


def _resolve_locations(
    state: str | int | Iterable[str | int] | None,
    county: str | Iterable[str] | None,
    huc: str | Iterable[str] | None,
) -> list[str]:
    """Build the NWDC ``location=<type>:<id>`` value(s) from the selectors.

    Exactly one of ``state`` / ``county`` / ``huc`` must be given; each may be a
    single value or a list. ``state`` is normalized to the two-letter postal
    code ``stateCd`` requires; ``county`` is a five-digit FIPS code; and a
    ``huc`` code's length selects its level (``huc2`` … ``huc12``). Returns one
    location string per value — the caller issues one request per location.
    """
    selected = {
        name: value
        for name, value in (("state", state), ("county", county), ("huc", huc))
        if value is not None
    }
    if len(selected) != 1:
        raise ValueError(
            "Specify exactly one of state, county, or huc "
            f"(got: {', '.join(selected) or 'none'})."
        )
    [(name, value)] = selected.items()
    locations = _LOCATION_BUILDERS[name](value)
    if not locations:
        raise ValueError(
            "The chosen location selector is empty; pass at least one value."
        )
    return locations


def _as_list(value: object) -> list[Any]:
    """Normalize a value to a list.

    A scalar becomes a one-element list; any non-string iterable (list, tuple,
    Series, ndarray, generator) is materialized to a list. A string is treated
    as a scalar so it isn't exploded into characters.
    """
    if isinstance(value, Iterable) and not isinstance(value, str):
        return list(value)
    return [value]


def _validate_county(value: object) -> str:
    """Validate and normalize a five-digit state+county FIPS code."""
    code = str(value).strip()
    if not (code.isdigit() and len(code) == 5):
        raise ValueError(
            "county must be a five-digit state+county FIPS code "
            f"(e.g. '55025'), got {value!r}."
        )
    return code


def _validate_huc(value: object) -> str:
    """Validate a HUC code (even length 2-12 digits; level set by length)."""
    code = str(value).strip()
    if not (code.isdigit() and len(code) in _HUC_LENGTHS):
        raise ValueError(
            "huc must be a hydrologic unit code of even length 2-12 digits "
            f"(e.g. '04', '07070005', '010900020502'), got {value!r}."
        )
    return code


def _fan_out(
    requests: list[httpx.Request],
    headers: dict[str, str],
    ssl_check: bool,
    *,
    host: str = _WATERUSE_HOST,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Fetch every request (each paginated) over the shared fan-out executor.

    This function is only the NWDC-specific half: parse a CSV page and read
    its ``Link`` header cursor, follow that cursor, raise the typed error
    carrying the NWDC ``detail``, and shape the result.
    :func:`~dataretrieval.transport.pagination.run_paginated` owns the rest.

    The plan is the request list itself. The executor asks a plan only to be
    sized and iterable, and the NWDC accepts one ``location=`` per request, so
    the caller's locations arrive already separate -- there is nothing to
    divide and so nothing for a plan class to hold.

    The broad retry status set is on purpose: NWDC reports a bad query as a 400
    with a ``{"detail": ...}`` envelope, so unlike WQP and StreamStats its 5xx
    really is an upstream fault worth re-sending.
    """

    def parse(response: httpx.Response) -> tuple[pd.DataFrame, str | None]:
        return _read_csv_page(response), _next_page_url(response, host=host)

    async def follow(cursor: str, sess: httpx.AsyncClient) -> httpx.Response:
        return await sess.get(cursor, headers=headers)

    def raise_for_status(response: httpx.Response) -> None:
        _raise_for_status(response, detail_from=_nwdc_error_detail)

    def finalize(
        frame: pd.DataFrame, response: httpx.Response
    ) -> tuple[pd.DataFrame, BaseMetadata]:
        return frame, BaseMetadata(response)

    return run_paginated(
        requests,
        parse_response=parse,
        follow_up=follow,
        raise_for_status=raise_for_status,
        finalize=finalize,
        client_options={"verify": ssl_check},
        default_concurrent=DEFAULT_CONCURRENT_REQUESTS,
        service="nwdc",
        adapter="nwdc",
    )


def _read_csv_page(response: httpx.Response) -> pd.DataFrame:
    """Parse one CSV page; ``huc12_id`` stays a string to keep leading zeros."""
    try:
        return pd.read_csv(io.BytesIO(response.content), dtype={_HUC12_COLUMN: str})
    except pd.errors.EmptyDataError as exc:
        # NWDC normally signals "no data" with a 400 (handled above) or rows of
        # zeros, never an empty body — but keep the typed-error contract if it
        # ever returns one rather than leaking a bare pandas exception.
        raise DataRetrievalError(
            f"NWDC returned an empty response body (URL: {response.url})."
        ) from exc


def _next_page_url(
    response: httpx.Response, *, host: str = _WATERUSE_HOST
) -> str | None:
    """Return the absolute URL of the next page, or None if this is the last.

    Reads the standard ``Link: <...>; rel="next"`` header (parsed by httpx into
    ``response.links``). The cursor is normalized before it is trusted, because
    the service spells it inconsistently. A relative reference is resolved
    against the page it came from, and the bare ``water.usgs.gov`` host is
    rewritten to the public ``api.water.usgs.gov`` gateway (over https, whatever
    scheme the link used) so the follow-up request reaches the API. Only a
    cursor that still points somewhere else after that is refused -- following
    it would send Water Use requests, and any credentials on them, to a host the
    caller never asked for.

    ``host`` is the host this call is actually talking to, which is not the
    NWDC's when a ``configure`` block redirected the adapter. The alias list and
    the rewrite are facts about *this* service -- nothing else answers for
    ``water.usgs.gov`` -- so a redirected call gets the general rule instead:
    follow a link only back to the host that served the page. Applying the
    NWDC's rewrite there would send page two of a mirrored query to the USGS.
    """
    url = response.links.get("next", {}).get("url")
    if not url:
        return None
    if host != _WATERUSE_HOST:
        return resolve_next_url(url, response, service="Water Use")
    return resolve_next_url(
        url,
        response,
        service="Water Use",
        allowed_hosts=_WATERUSE_HOST_ALIASES,
        rewrite_host=_WATERUSE_HOST,
    )


def _nwdc_error_detail(response: httpx.Response) -> str | None:
    """Pull the ``detail`` message out of an NWDC JSON error envelope, if any.

    The NWDC reports errors as ``{"detail": "Invalid model name: ..."}``. Passed
    to :func:`~dataretrieval.utils._raise_for_status` as ``detail_from`` so the
    service's wording surfaces in the typed error message.
    """
    try:
        body = response.json()
    except ValueError:
        return None
    return body.get("detail") if isinstance(body, dict) else None


@dataclass(frozen=True)
class NwdcConfiguration(_Concurrent, _Redirectable, _Retrying, BaseConfiguration):
    """Settings for NWDC calls alone.

    No ``parallel_chunks``: the NWDC is a plain CSV service, so a query
    fans out per location rather than being divided along a URL byte
    budget. There is nothing for the planner to divide more finely.

    Lives here rather than in :mod:`dataretrieval.configuration` because
    *which* settings a service reads is the service's own knowledge (ADR
    0011); what each of them means is shared, so the fields come from the
    setting groups declared beside their grammar.

    Parameters
    ----------
    retries : int, optional
        Retries attempted after a transient failure; ``0`` disables retrying.
    stall_timeout : float, optional
        Seconds a call may go without receiving any data before retrying
        stops.
    base_url : str, optional
        Endpoint to send NWDC requests to, instead of the service's own
        (``WATERUSE_URL``). A ``rel="next"`` cursor is then followed only
        back to that host, since the service's own host aliases mean
        nothing there. Code only: the file and the environment refuse it.
    concurrency : int or str, optional
        Cap on simultaneous sub-requests, or ``"unbounded"``.
    """

    # One request per location, fanned out but never chunked, so this service
    # reads the retry dials, a redirectable base and ``concurrency`` -- but not
    # ``parallel_chunks``, which divides a query it never divides.
    adapter: ClassVar[str] = "nwdc"


_register(NwdcConfiguration)
