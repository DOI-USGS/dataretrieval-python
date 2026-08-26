"""Tests for API-key host scoping in _default_headers."""

from __future__ import annotations

import asyncio
from unittest import mock

import httpx
import pytest

from dataretrieval.transport.http import HTTPX_ASYNC_DEFAULTS
from dataretrieval.utils import (
    _default_headers,
    _get,
)


class TestDefaultHeadersHostScoping:
    """_default_headers only sends X-Api-Key to the authorized Water Data host."""

    FAKE_TOKEN = "test-fake-token-abc123"

    @pytest.fixture(autouse=True)
    def _api_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Install one harmless token for every host-scoping behavior test."""
        monkeypatch.setenv("API_USGS_PAT", self.FAKE_TOKEN)

    def test_key_included_for_waterdata_host(self):
        """Key IS added when target URL matches api.waterdata.usgs.gov."""
        url = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items"
        headers = _default_headers(url)
        assert headers.get("X-Api-Key") == self.FAKE_TOKEN

    def test_key_excluded_for_external_host(self):
        """Key is NOT added for an external (non-USGS) host."""
        url = "https://nwis.waterservices.usgs.gov/nwis/iv/"
        headers = _default_headers(url)
        assert "X-Api-Key" not in headers

    def test_key_excluded_for_wateruse_host(self):
        """Key is NOT added for the NWDC water-use host (api.water.usgs.gov)."""
        url = "https://api.water.usgs.gov/nwaa-data/data"
        headers = _default_headers(url)
        assert "X-Api-Key" not in headers

    def test_key_excluded_for_rating_asset_host(self):
        """Key is NOT added for rating asset downloads (S3/external)."""
        url = "https://labs.waterdata.usgs.gov/sta/v1.1/Datastreams(123)/rating.rdb"
        headers = _default_headers(url)
        assert "X-Api-Key" not in headers

    def test_key_excluded_for_lookalike_host(self):
        """Key is NOT sent to a typosquatting/lookalike domain."""
        url = "https://api.waterdata.usgs.gov.evil.com/ogcapi/v0/daily/items"
        headers = _default_headers(url)
        assert "X-Api-Key" not in headers

    def test_key_excluded_when_no_url_provided(self):
        """Key is NOT added when target_url is None (legacy callers)."""
        headers = _default_headers(None)
        assert "X-Api-Key" not in headers

    def test_key_excluded_when_no_token_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No key header at all when API_USGS_PAT is not set."""
        monkeypatch.delenv("API_USGS_PAT")
        headers = _default_headers("https://api.waterdata.usgs.gov/ogcapi/v0/daily")
        assert "X-Api-Key" not in headers

    def test_non_auth_headers_always_present(self):
        """User-Agent, Accept, Accept-Encoding, lang are always present."""
        url = "https://example.com/any"
        headers = _default_headers(url)
        assert "User-Agent" in headers
        assert "Accept" in headers
        assert "Accept-Encoding" in headers
        assert "lang" in headers
        # Key should NOT be sent to example.com
        assert "X-Api-Key" not in headers

    def test_key_excluded_over_cleartext_on_the_authorized_host(self):
        """The right host over plain http is still the wrong destination.

        Matching on the host alone would send a bearer token in the clear on
        the strength of a hostname an attacker chose to keep -- reachable via a
        redirect or a server-supplied ``http://`` next-page link.
        """
        headers = _default_headers("http://api.waterdata.usgs.gov/ogcapi/v0/daily")
        assert "X-Api-Key" not in headers

    def test_sync_transport_withholds_key_on_downgrade_to_cleartext(self):
        """The guard runs at send time, not only where headers are built."""
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if len(seen) == 1:
                return httpx.Response(
                    302,
                    headers={"Location": "http://api.waterdata.usgs.gov/next"},
                    request=request,
                )
            return httpx.Response(200, request=request)

        url = "https://api.waterdata.usgs.gov/start"
        _get(
            url,
            headers=_default_headers(url),
            follow_redirects=True,
            transport=httpx.MockTransport(handler),
        )

        assert seen[0].headers.get("X-Api-Key") == self.FAKE_TOKEN
        assert seen[1].url.scheme == "http", "the redirect under test must downgrade"
        assert "X-Api-Key" not in seen[1].headers

    def test_generic_ogc_request_excludes_key_for_custom_host(self):
        """A caller-supplied OGC base URL never inherits Water Data auth."""
        from dataretrieval.ogc.requests import _construct_api_requests

        request = _construct_api_requests(
            "things", base_url="https://features.example.org/ogcapi"
        )
        assert "X-Api-Key" not in request.headers

    def test_rating_download_scopes_headers_to_asset_url(self):
        """The ratings adapter evaluates auth against each asset href."""
        import asyncio

        import pandas as pd

        import dataretrieval.waterdata.ratings as ratings

        asset_url = "https://objects.example.org/ratings/site.rdb"
        feature = {"id": "site.rdb", "assets": {"data": {"href": asset_url}}}
        client = mock.AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = mock.Mock(text="rating body")
        with (
            mock.patch.object(ratings, "active_client", return_value=client),
            mock.patch.object(ratings, "_raise_for_non_200"),
            mock.patch.object(ratings, "read_rdb", return_value=pd.DataFrame()),
            mock.patch.object(ratings, "extract_rdb_comment", return_value=""),
        ):
            asyncio.run(ratings._fetch_rating(feature, file_path=None))

        assert client.get.call_args.args[0] == asset_url
        assert "X-Api-Key" not in client.get.call_args.kwargs["headers"]

    def test_sync_redirect_strips_key_before_cross_host_request(self):
        """The synchronous transport guard runs again for redirects."""
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if len(seen) == 1:
                return httpx.Response(
                    302,
                    headers={"Location": "https://outside.example.org/next"},
                    request=request,
                )
            return httpx.Response(200, request=request)

        url = "https://api.waterdata.usgs.gov/start"
        _get(
            url,
            headers=_default_headers(url),
            follow_redirects=True,
            transport=httpx.MockTransport(handler),
        )

        assert seen[0].headers.get("X-Api-Key") == self.FAKE_TOKEN
        assert "X-Api-Key" not in seen[1].headers

    def test_async_redirect_strips_key_before_cross_host_request(self):
        """The shared asynchronous client policy guards redirects too."""
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if len(seen) == 1:
                return httpx.Response(
                    302,
                    headers={"Location": "https://outside.example.org/next"},
                    request=request,
                )
            return httpx.Response(200, request=request)

        async def run() -> None:
            url = "https://api.waterdata.usgs.gov/start"
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler),
                **HTTPX_ASYNC_DEFAULTS,
            ) as client:
                await client.get(url, headers=_default_headers(url))

        asyncio.run(run())

        assert seen[0].headers.get("X-Api-Key") == self.FAKE_TOKEN
        assert "X-Api-Key" not in seen[1].headers
