"""HTTP client lifecycle, timeout defaults, and host-scoped authentication."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from typing import Any

import httpx

from dataretrieval.credentials import (
    accepts_api_key,
    api_key,
    strip_api_key_from_untrusted_host,
    strip_api_key_from_untrusted_host_async,
)
from dataretrieval.exceptions import NetworkError

# Re-exported for the adapters that reach for credential policy through the
# transport surface they already import. ``dataretrieval.credentials`` is the
# single definition; these names are views on it, not copies of it.
__all__ = [
    "HTTPX_ASYNC_DEFAULTS",
    "HTTPX_DEFAULTS",
    "USER_AGENT",
    "accepts_api_key",
    "default_headers",
    "get",
    "network_error",
    "open_async_client",
    "strip_api_key_from_untrusted_host",
    "strip_api_key_from_untrusted_host_async",
]

try:
    _PACKAGE_VERSION = _pkg_version("dataretrieval")
except PackageNotFoundError:
    _PACKAGE_VERSION = "version-unknown"

USER_AGENT = f"python-dataretrieval/{_PACKAGE_VERSION}"

HTTPX_DEFAULTS: dict[str, Any] = {
    "follow_redirects": True,
    "timeout": httpx.Timeout(60.0, connect=10.0),
}


def default_headers(target_url: str | httpx.URL | None = None) -> dict[str, str]:
    """Build standard headers, scoping the API key to its authorized host.

    The host is checked *before* the key is resolved, and the key is resolved
    only for the authorized host. Order matters now that settings come from a
    layered chain: resolution reads the config file and can raise
    :class:`~dataretrieval.exceptions.ConfigurationError` for a malformed file or
    a profile it no longer defines. Resolving first would let a Water Data
    configuration problem break a legacy NWIS, WQP, or NGWMN call that would
    never have received the key.
    """
    headers = {
        "Accept-Encoding": "compress, gzip",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
        "lang": "en-US",
    }
    if accepts_api_key(target_url):
        token = api_key()
        if token:
            headers["X-Api-Key"] = token
    return headers


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
