"""HTTP client lifecycle, timeout defaults, and host-scoped authentication."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from typing import Any

import httpx

from dataretrieval.exceptions import NetworkError

try:
    _PACKAGE_VERSION = _pkg_version("dataretrieval")
except PackageNotFoundError:
    _PACKAGE_VERSION = "version-unknown"

USER_AGENT = f"python-dataretrieval/{_PACKAGE_VERSION}"

HTTPX_DEFAULTS: dict[str, Any] = {
    "follow_redirects": True,
    "timeout": httpx.Timeout(60.0, connect=10.0),
}

_AUTHORIZED_API_KEY_HOST = "api.waterdata.usgs.gov"


def accepts_api_key(target_url: str | httpx.URL | None) -> bool:
    """Whether ``target_url`` names the host that honors ``API_USGS_PAT``.

    The single answer to "does this destination get the key" -- used both when
    attaching the credential and when stripping it back off at redirect time, so
    the two can't drift apart. It is also what makes "get an API key" useful
    advice rather than noise: every other service this package talks to is on a
    different host and ignores the key entirely.
    """
    if target_url is None:
        return False
    try:
        url = target_url if isinstance(target_url, httpx.URL) else httpx.URL(target_url)
    except (httpx.InvalidURL, TypeError):
        return False
    return url.host == _AUTHORIZED_API_KEY_HOST


def default_headers(target_url: str | httpx.URL | None = None) -> dict[str, str]:
    """Build standard headers, scoping ``API_USGS_PAT`` to its authorized host."""
    headers = {
        "Accept-Encoding": "compress, gzip",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
        "lang": "en-US",
    }
    token = os.getenv("API_USGS_PAT")
    if token and accepts_api_key(target_url):
        headers["X-Api-Key"] = token
    return headers


def strip_api_key_from_untrusted_host(request: httpx.Request) -> None:
    """Remove Water Data credentials before sending to any other host."""
    if not accepts_api_key(request.url):
        request.headers.pop("X-Api-Key", None)


async def strip_api_key_from_untrusted_host_async(request: httpx.Request) -> None:
    """Async-client form of :func:`strip_api_key_from_untrusted_host`."""
    strip_api_key_from_untrusted_host(request)


HTTPX_ASYNC_DEFAULTS: dict[str, Any] = {
    **HTTPX_DEFAULTS,
    "event_hooks": {"request": [strip_api_key_from_untrusted_host_async]},
}


def network_error(url: str | httpx.URL, exc: httpx.TransportError) -> NetworkError:
    """Build a typed error for a failed round trip with no HTTP response."""
    detail = str(exc) or type(exc).__name__
    return NetworkError(f"Could not reach the service at {url}: {detail}")


def get(url: str | httpx.URL, **kwargs: Any) -> httpx.Response:
    """Issue one guarded synchronous GET and map transport failures."""
    client_options: dict[str, Any] = {
        key: kwargs.pop(key)
        for key in ("follow_redirects", "timeout", "transport", "verify")
        if key in kwargs
    }
    client_options["event_hooks"] = {"request": [strip_api_key_from_untrusted_host]}
    try:
        with httpx.Client(**client_options) as client:
            return client.get(url, **kwargs)
    except httpx.TransportError as exc:
        raise network_error(url, exc) from exc


@asynccontextmanager
async def open_async_client(**overrides: Any) -> AsyncIterator[httpx.AsyncClient]:
    """Open a short-lived async client with redirect-safe shared defaults."""
    options = {**HTTPX_ASYNC_DEFAULTS, **overrides}
    async with httpx.AsyncClient(**options) as client:
        yield client
