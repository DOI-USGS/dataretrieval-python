"""Item search over the Water Data STAC catalog.

The catalog at ``https://api.waterdata.usgs.gov/stac/v0`` indexes several
collections: ``ratings`` (RDB rating-curve files), and ``daily`` and
``continuous`` (one Parquet file set per monitoring location). Every getter
that reads the catalog searches it the same way -- a CQL2-text ``filter``, a
STAC ``datetime`` interval, a ``bbox``, and the ``next`` link followed until it
runs out -- so that walk is defined once here, behind :func:`search`.

The catalog's edge refuses a search URL over :data:`EDGE_URL_LIMIT`
characters. :func:`search` owns that limit: it splits a long
``monitoring_location_id`` list across concurrent searches that each fit, and
raises before any request when the rest of the query cannot fit. A caller
passes its identifiers and expressions and never sizes a URL itself.

Each search names its collection. ``/search`` spans every collection, and an
unscoped search for one monitoring location returns that location's rating
files and its ``daily`` and ``continuous`` items together.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any
from urllib.parse import quote_plus

import httpx
import pandas as pd

from dataretrieval.ogc.dates import _DURATION_RE, _coerce_to_list, _format_api_dates
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.ogc.filters import _cql_text_literal
from dataretrieval.transport.http import default_headers
from dataretrieval.transport.links import resolve_next_url
from dataretrieval.transport.pagination import run_paginated
from dataretrieval.waterdata.endpoints import ratings_catalog_url

__all__ = ["EDGE_URL_LIMIT", "format_time", "search"]

#: The largest page ``/search`` serves.
MAX_PAGE_SIZE = 10000

#: The longest ``/search`` URL, in characters, that the catalog's edge accepts.
#: Measured by bisection on 2026-11-22: a GET of 2,094 characters is served and
#: one of 2,095 is answered with HTTP 403 and an HTML "Forbidden" page. The
#: edge counts the whole URL, so the endpoint, ``collections``, ``limit``,
#: ``datetime``, ``bbox`` and the caller's ``filter`` all share it.
EDGE_URL_LIMIT = 2094

#: Characters kept free below :data:`EDGE_URL_LIMIT`, so that a small
#: difference in how a client escapes a query cannot push a request over.
_URL_HEADROOM = 64

_ID_PROPERTY = "monitoring_location_id"
_ID_REMEDY = "Pass an 'AGENCY-ID' id, e.g., 'USGS-01646500'."


def format_time(time: Any, *, service: str) -> str | None:
    """Return *time* as a STAC ``datetime`` interval, or ``None`` if not given.

    ``service`` names the catalog collection in the error message, because
    the caller called a getter for it, not this helper.

    Raises
    ------
    ValueError
        For an ISO 8601 duration, which the catalog does not accept, or for a
        value that is not a date.
    """
    if time is None:
        return None
    # ``_coerce_to_list`` keeps a lone ``date``/``Timestamp`` whole.
    values = _coerce_to_list(time, "time")
    if any(isinstance(v, str) and _DURATION_RE.match(v) for v in values):
        raise ValueError(
            "ISO 8601 durations (e.g. 'P7D') are not supported in `time` "
            f"for the {service} service. Provide a date or interval instead."
        )
    return _format_api_dates(
        time,
        name="time",
        single_value_hint="an instant ('2020-01-01')",
        # The STAC firewall rejects ``datetime=../2026-01-01`` with HTTP
        # 403; STAC treats an empty start (``/2026-01-01``) as open.
        open_start="",
    )


def _search_params(
    collection: str,
    filter_str: str | None,
    limit: int,
    *,
    time_str: str | None = None,
    bbox: list[float] | None = None,
) -> dict[str, Any]:
    """The query parameters of one ``/search`` request over *collection*."""
    query_params: dict[str, Any] = {
        "collections": collection,
        "limit": min(limit, MAX_PAGE_SIZE),
    }
    if filter_str is not None:
        query_params["filter"] = filter_str
    if time_str is not None:
        query_params["datetime"] = time_str
    if bbox is not None:
        query_params["bbox"] = ",".join(map(str, bbox))
    return query_params


def _search_request(params: dict[str, Any]) -> httpx.Request:
    url = f"{ratings_catalog_url()}/search"
    return httpx.Request("GET", url, params=params, headers=default_headers(url))


def _run_search(
    params: list[dict[str, Any]], *, ssl_check: bool, service: str
) -> tuple[list[dict[str, Any]], httpx.Response]:
    """Run one ``/search`` per entry of *params*, as chunks of one fan-out.

    Every request goes to a single
    :func:`~dataretrieval.transport.pagination.run_paginated` call, so the
    searches run concurrently, and each follows its own ``next`` links until
    they run out. That walk retries, reports progress, and raises a resumable
    interruption like every other multi-request path. Pages hold items rather
    than rows, so each page frame wraps the raw item dicts in a single
    ``feature`` column.

    Returns the items of every search, in request order, and the aggregate
    response, from which a getter builds its metadata.
    """

    def parse_response(resp: httpx.Response) -> tuple[pd.DataFrame, str | None]:
        body = resp.json()
        page = pd.DataFrame({"feature": body.get("features", [])})
        # The STAC ``next`` link is a complete GET href that carries the
        # limit/filter/bbox and a continuation token, so it becomes the
        # cursor verbatim -- after the shared safety policy: the href is
        # response data, so it is checked before it becomes a request. A link
        # to another host would send this request's API key to a host
        # other than the authorized one, and one with ``user:pass@`` would
        # produce an ``Authorization: Basic`` header the caller never configured.
        href = next(
            (lnk["href"] for lnk in body.get("links", []) if lnk.get("rel") == "next"),
            None,
        )
        cursor = None if href is None else resolve_next_url(href, resp, service=service)
        return page, cursor

    async def follow_up(cursor: str, sess: httpx.AsyncClient) -> httpx.Response:
        return await sess.get(cursor, headers=default_headers(cursor))

    df, response = run_paginated(
        [_search_request(p) for p in params],
        parse_response=parse_response,
        follow_up=follow_up,
        raise_for_status=_raise_for_non_200,
        client_options={"verify": ssl_check},
        service=service,
        adapter="waterdata",
    )
    # Every page frame is built with a ``feature`` column, and the combine
    # helpers preserve it, so the empty case needs no special branch.
    return list(df["feature"]), response


def search(
    collection: str,
    monitoring_location_id: str | Iterable[str] | None = None,
    *,
    before: str | None = None,
    after: str | None = None,
    time_str: str | None = None,
    bbox: list[float] | None = None,
    limit: int,
    ssl_check: bool,
    service: str,
    remedy: str,
) -> tuple[list[dict[str, Any]], httpx.Response]:
    """Search one collection of the catalog and return every matching item.

    The filter is ``monitoring_location_id IN (...)`` joined by ``AND`` to the
    CQL2-text expressions ``before`` and ``after``, which name where each one
    goes relative to the identifier list. Any of the three may be omitted; with
    none, the search has no filter.

    ``limit`` is the page size, clamped to the service maximum. Each search
    follows its ``next`` link until it runs out, so a result larger than one
    page is not truncated (see :func:`_run_search`). An identifier list too
    long for one URL is split across searches that run concurrently, and an
    item that more than one of them returned is kept once.

    Returns the items and the aggregate response, from which a getter builds
    its metadata.

    Raises
    ------
    ValueError
        Before any request, if the query without identifiers is already over
        :data:`EDGE_URL_LIMIT`, or an identifier cannot be written as a
        CQL-text literal. ``remedy`` ends the first message with the action
        the getter's caller can take.
    """
    if monitoring_location_id is None:
        filter_str = " AND ".join(part for part in (before, after) if part) or None
        _require_url_fits(
            filter_str, collection, limit, time_str=time_str, bbox=bbox, remedy=remedy
        )
        filters: list[str | None] = [filter_str]
    else:
        ids = (
            [monitoring_location_id]
            if isinstance(monitoring_location_id, str)
            else monitoring_location_id
        )
        filters = [
            *_chunk_in_filters(
                ids,
                collection,
                limit,
                lead=f"{before} AND " if before else "",
                tail=f" AND {after}" if after else "",
                time_str=time_str,
                bbox=bbox,
                remedy=remedy,
            )
        ]
    params = [
        _search_params(collection, f, limit, time_str=time_str, bbox=bbox)
        for f in filters
    ]
    items, response = _run_search(params, ssl_check=ssl_check, service=service)
    if len(params) > 1:
        unique = {(item.get("collection"), item.get("id")): item for item in items}
        items = list(unique.values())
    return items, response


def _url_length(params: dict[str, Any]) -> int:
    """Characters in the ``/search`` URL that *params* would produce."""
    return len(str(_search_request(params).url))


def _require_url_fits(
    filter_str: str | None,
    collection: str,
    limit: int,
    *,
    time_str: str | None,
    bbox: list[float] | None,
    remedy: str,
) -> None:
    """Raise ``ValueError`` if one search with *filter_str* is over the edge limit.

    Called before any request, because the edge answers an over-long URL with
    an HTTP 403 that names no cause. ``remedy`` ends the message with the
    action the caller can take.
    """
    params = _search_params(collection, filter_str, limit, time_str=time_str, bbox=bbox)
    length = _url_length(params)
    if length > EDGE_URL_LIMIT:
        raise ValueError(
            f"The catalog search URL would be {length:,} characters, over the "
            f"{EDGE_URL_LIMIT:,}-character limit the Water Data catalog "
            f"accepts. {remedy}"
        )


def _encoded_length(text: str) -> int:
    return len(quote_plus(text))


def _chunk_in_filters(
    ids: Iterable[str],
    collection: str,
    limit: int,
    *,
    lead: str = "",
    tail: str = "",
    time_str: str | None = None,
    bbox: list[float] | None = None,
    remedy: str,
) -> list[str]:
    """Split a ``monitoring_location_id IN (...)`` search into filters that fit.

    One filter is returned per chunk of the search, each of the form
    ``{lead}monitoring_location_id IN ('a', 'b'){tail}``. A chunk holds as many
    ids as keep the URL within :data:`EDGE_URL_LIMIT` less a small headroom,
    sized by each id's URL-encoded length (ids run 13 to 21 characters), and
    at most one page (``limit``) of them, so one page holds a chunk. A repeated
    id is sent once. An empty *ids* gives one filter with an empty list, as
    before.

    ``lead`` and ``tail`` carry the rest of the filter, and the endpoint,
    collection, page size, ``time_str`` and ``bbox`` are counted too.

    Raises
    ------
    ValueError
        If everything but the ids already leaves no room for one id, or an id
        cannot be written as a CQL-text literal. ``remedy`` ends the message.
    """
    unique = list(dict.fromkeys(str(i) for i in ids))
    page = max(1, min(limit, MAX_PAGE_SIZE))

    def build(chunk: Sequence[str]) -> str:
        quoted = ", ".join(_cql_text_literal(i, remedy=_ID_REMEDY) for i in chunk)
        return f"{lead}{_ID_PROPERTY} IN ({quoted}){tail}"

    _require_url_fits(
        build([]),
        collection,
        limit,
        time_str=time_str,
        bbox=bbox,
        remedy=remedy,
    )
    base = _url_length(
        _search_params(collection, build([]), limit, time_str=time_str, bbox=bbox)
    )
    budget = EDGE_URL_LIMIT - _URL_HEADROOM - base
    separator = _encoded_length(", ")

    chunks: list[list[str]] = [[]]
    used = 0
    for value in unique:
        cost = _encoded_length(_cql_text_literal(value, remedy=_ID_REMEDY)) + separator
        if chunks[-1] and (used + cost - separator > budget or len(chunks[-1]) >= page):
            chunks.append([])
            used = 0
        chunks[-1].append(value)
        used += cost
    return [build(chunk) for chunk in chunks]
