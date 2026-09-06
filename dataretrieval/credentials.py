"""Which host honors the USGS API key, and how it is attached and withheld.

One leaf owns every answer about the ``API_USGS_PAT`` credential: the host that
accepts it, whether a given destination qualifies, how it is stripped back off a
request bound somewhere else, and which keyword names are a caller *asking* to
send it. ADR 0006 assigns that sole ownership, and ADR 0010 keeps the key out
of every adapter's settings.

This sits below HTTP mechanics and below progress reporting in the layers
contract. Its only first-party dependency is
:mod:`dataretrieval.configuration` -- itself a standard-library-only leaf --
which supplies the key's *value*.
"""

from __future__ import annotations

from collections.abc import Iterable

import httpx

from dataretrieval import configuration as _configuration

#: Environment variable holding the USGS Water Data personal access token.
#: Taken from the chain that reads it rather than spelled again here -- the
#: same rule ``test_credential_policy_has_one_definition`` enforces for the
#: authorized host, and for the same reason: two copies stop agreeing silently.
API_KEY_ENV = _configuration.ENV_VARS["api_key"]

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

    The scheme has to be ``https``, not just the host (ADR 0009): a redirect or
    a server-supplied next-page link can name ``http://`` on the very host that
    is otherwise authorized.
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

    A next-page link is data, not configuration, and ``httpx`` derives an
    ``Authorization: Basic`` header from userinfo in a URL (ADR 0009). No USGS
    service authenticates that way, so stripping it costs a caller nothing.
    """
    return url.copy_with(userinfo=b"") if url.userinfo else url


# Credential-shaped keyword names must never reach a getter's generic query
# passthrough: URLs are retained by clients, proxies, logs, and response
# metadata. The predicate lives in this leaf rather than in any one adapter so
# that ten getters cannot drift into ten spellings of it (ADR 0006).
#
# Matched as *substrings* of the separator-stripped name, not as exact names:
# an exact-match list misses the spelling the library's own docs make most
# tempting -- ``x_api_key``, after the ``X-Api-Key`` header.
_CREDENTIAL_MARKERS = (
    "apikey",
    "authorization",
    "credential",
    "password",
    "passwd",
    "secret",
    "token",
)

# Whole names that are credentials on their own but too short to match as
# substrings without catching legitimate query parameters.
#
# ``session`` is deliberately absent from both lists: it carries no secret, so
# rejecting it with a credentials message reports an incorrect reason, and as a
# substring it claims part of a namespace the *server* owns -- any future query
# parameter containing it would be unreachable behind that message.
_CREDENTIAL_NAMES = frozenset({"auth", "key", "pat", "pw"})


def refuse_credential_keywords(names: Iterable[str]) -> None:
    """Raise ``TypeError`` if any of *names* reads as a request for the key.

    For the ``**kwargs`` passthroughs -- Water Data's ``**queryables`` and
    WQP's search filters -- where a name the caller invents is forwarded to the
    server as a query parameter. Both call this rather than each keeping its
    own list, so a spelling learned from one adapter's mistake is refused by
    the other on the same day.

    A usability check, not a security control (ADR 0009). It answers the
    caller who reasonably guesses that a credential goes here, with a
    ``TypeError`` naming ``with configure(Configuration(api_key=...)):``
    instead of a token in a URL -- the bare call is a no-op, since
    ``configure`` is a context manager.
    """
    forbidden = set()
    for name in names:
        flat = name.replace("_", "").replace("-", "").casefold()
        if flat in _CREDENTIAL_NAMES or any(m in flat for m in _CREDENTIAL_MARKERS):
            forbidden.add(name)
    if forbidden:
        spellings = ", ".join(f"{name}=" for name in sorted(forbidden))
        raise TypeError(
            f"Credentials cannot be passed as query parameters ({spellings}); "
            "wrap the call in `with dataretrieval.configure("
            "dataretrieval.Configuration(api_key=...)):`, or set the "
            f"{API_KEY_ENV} environment variable."
        )


def api_key() -> str | None:
    """The configured token, or ``None``.

    Lives here, next to the host check and
    :func:`strip_api_key_from_untrusted_host`, so reading the key and the rules
    governing where it may travel stay in one module. The value itself resolves
    through :func:`dataretrieval.configuration.api_key`, so host scoping applies
    identically no matter which source supplied the key.
    """
    return _configuration.api_key()


def strip_api_key_from_untrusted_host(request: httpx.Request) -> None:
    """Remove Water Data credentials before sending to any other host."""
    if not accepts_api_key(request.url):
        request.headers.pop("X-Api-Key", None)


async def strip_api_key_from_untrusted_host_async(request: httpx.Request) -> None:
    """Async-client form of :func:`strip_api_key_from_untrusted_host`."""
    strip_api_key_from_untrusted_host(request)
