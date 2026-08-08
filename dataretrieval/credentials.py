"""Which host honors the USGS API key, and how it is attached and withheld.

One leaf owns every answer about the ``API_USGS_PAT`` credential: the host that
accepts it, whether a given destination qualifies, and how it is stripped back
off a request bound somewhere else. Splitting those answers across the layers
that happen to need them is how a credential reaches a host nobody authorized:
the code that attaches a key and the code that removes it have to agree, and the
only way to guarantee they agree is to have them read the same predicate.

This is deliberately a leaf. It sits below HTTP mechanics (which attaches the
header) and below progress reporting (which tells an unauthenticated caller where
to register), so neither has to depend on the other to learn the same fact.
"""

from __future__ import annotations

import os

import httpx

#: Environment variable holding the USGS Water Data personal access token.
API_KEY_ENV = "API_USGS_PAT"

#: Where to register for a key. Surfaced once, by the progress reporter, when a
#: query against the authorized host runs without one -- unauthenticated callers
#: hit much lower rate limits (see the ``API_USGS_PAT`` note in the README).
SIGNUP_URL = "https://api.waterdata.usgs.gov/signup/"

#: The only host that honors the key. Every other service this package talks to
#: ignores it, so sending it there would leak a credential for no benefit.
_AUTHORIZED_API_KEY_HOST = "api.waterdata.usgs.gov"

#: Origin of the Water Data API, built from the authorized host rather than
#: spelled again. The host that serves these endpoints and the host that honors
#: the key are the same fact, and the failure mode of keeping two copies is
#: silent: the endpoint moves, the predicate does not follow, and either the key
#: quietly stops attaching or it is sent somewhere nobody authorized. The
#: adapters import this instead of restating the authority (enforced by
#: ``test_credential_policy_has_one_definition``).
WATERDATA_BASE_URL = f"https://{_AUTHORIZED_API_KEY_HOST}"


def accepts_api_key(target_url: str | httpx.URL | None) -> bool:
    """Whether ``target_url`` names the host that honors :data:`API_KEY_ENV`.

    The single answer to "does this destination get the key" -- used when
    attaching the credential, when stripping it back off at redirect time, and
    when deciding whether "get an API key" is useful advice rather than noise, so
    the three can't drift apart.

    The scheme has to be ``https``, not just the host. A bearer token sent over
    cleartext is readable by anything on the path, and the destination that would
    receive it is reachable through data we do not control: a redirect, or a
    server-supplied next-page link naming ``http://`` on the very host that is
    otherwise authorized. Matching on the host alone would hand the key over in
    the clear on the strength of a hostname the attacker chose to keep.
    """
    if target_url is None:
        return False
    try:
        url = target_url if isinstance(target_url, httpx.URL) else httpx.URL(target_url)
    except (httpx.InvalidURL, TypeError):
        return False
    return bool(url.scheme == "https" and url.host == _AUTHORIZED_API_KEY_HOST)


def without_embedded_credentials(url: httpx.URL) -> httpx.URL:
    """Drop any ``user:pass@`` from a URL we were *handed* rather than built.

    A next-page link is data, not configuration. ``httpx`` derives an
    ``Authorization: Basic`` header from userinfo in a URL, so a poisoned link
    carrying ``user:pass@`` mints a credential the caller never configured and
    sends it onward -- next to the real API key, when the host still checks out
    and the host check therefore raises nothing. No USGS service authenticates
    that way, so stripping it costs a legitimate caller nothing.
    """
    return url.copy_with(userinfo=b"") if url.userinfo else url


def api_key() -> str | None:
    """The configured token, or ``None``.

    Read through a function rather than captured at import so a caller that sets
    the variable after import -- or a test that patches it -- is still honored.
    """
    return os.getenv(API_KEY_ENV)


def strip_api_key_from_untrusted_host(request: httpx.Request) -> None:
    """Remove Water Data credentials before sending to any other host."""
    if not accepts_api_key(request.url):
        request.headers.pop("X-Api-Key", None)


async def strip_api_key_from_untrusted_host_async(request: httpx.Request) -> None:
    """Async-client form of :func:`strip_api_key_from_untrusted_host`."""
    strip_api_key_from_untrusted_host(request)
