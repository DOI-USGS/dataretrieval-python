"""Item search over the Water Data STAC catalog.

The catalog at ``https://api.waterdata.usgs.gov/stac/v0`` indexes several
collections: ``ratings`` (RDB rating-curve files), and ``daily`` and
``continuous`` (one Parquet file set per monitoring location). Every getter
that reads the catalog searches it the same way -- a CQL2-text ``filter``, a
STAC ``datetime`` interval, a ``bbox``, and the ``next`` link followed until it
runs out -- so that walk is defined once here.

Each search names its collection. ``/search`` spans every collection, and an
unscoped search for one monitoring location returns that location's rating
files and its ``daily`` and ``continuous`` items together.
"""

from __future__ import annotations

from typing import Any

import httpx
import pandas as pd

from dataretrieval.ogc.dates import _DURATION_RE, _coerce_to_list, _format_api_dates
from dataretrieval.ogc.errors import _raise_for_non_200
from dataretrieval.transport.http import default_headers
from dataretrieval.transport.links import resolve_next_url
from dataretrieval.transport.pagination import run_paginated
from dataretrieval.waterdata.endpoints import ratings_catalog_url

__all__ = ["format_time", "search_items"]

#: The largest page ``/search`` serves.
MAX_PAGE_SIZE = 10000


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


def search_items(
    collection: str,
    *,
    filter_str: str | None,
    time_str: str | None,
    bbox: list[float] | None,
    limit: int,
    ssl_check: bool,
    service: str,
) -> tuple[list[dict[str, Any]], httpx.Response]:
    """Run STAC ``/search`` over one collection and return every matching item.

    ``limit`` is the page size, clamped to the service maximum. The ``next``
    link is followed until it runs out, so a result larger than one page is
    not truncated. The page walk runs on
    :func:`~dataretrieval.transport.pagination.run_paginated`, so it retries,
    reports progress, and raises a resumable interruption like every other
    multi-request path. Pages hold items rather than rows, so each page frame
    wraps the raw item dicts in a single ``feature`` column.

    Returns the items and the aggregate response, from which a getter builds
    its metadata.
    """
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

    url = f"{ratings_catalog_url()}/search"
    req = httpx.Request("GET", url, params=query_params, headers=default_headers(url))

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
        [req],
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
