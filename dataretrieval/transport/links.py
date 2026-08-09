"""One policy for the server-supplied next-page links every page walk follows.

A ``next`` href is response *data*, not configuration: it arrives over the wire
from the service (or from whatever answered for it) and then becomes the URL of
our next request, carrying that request's headers and API key. Three page walks
-- the OGC engine's ``links`` array, the ratings STAC walk, and Water Use's
``Link:`` header -- each need the same things of it before it is trusted: parse
it, resolve a relative reference against the page it came from, refuse a host
the caller never asked for, and drop any embedded ``user:pass@`` (which
``httpx`` would otherwise turn into an ``Authorization: Basic`` header).

They had three implementations of that policy, and the three disagreed: only two
resolved relative references, only two refused an unparseable link rather than
handing it back, and each worded its refusal differently. A security invariant
with three spellings is one that gets fixed in one place and stays broken in the
other two, so it lives here once and the walks pass in what genuinely differs --
which hosts are acceptable, and whether an accepted host is rewritten.
"""

from __future__ import annotations

import httpx

from dataretrieval.credentials import without_embedded_credentials
from dataretrieval.exceptions import DataRetrievalError

__all__ = ["resolve_next_url"]


def _page_url(response: httpx.Response) -> httpx.URL:
    """The URL *response* came from, as an ``httpx.URL``.

    Read lazily by :func:`resolve_next_url` (see there). The coercion is for
    callers holding a response-shaped stand-in whose ``url`` is a plain string.
    """
    url = response.url
    return url if isinstance(url, httpx.URL) else httpx.URL(str(url))


def resolve_next_url(
    href: str,
    response: httpx.Response,
    *,
    service: str,
    allowed_hosts: frozenset[str] | None = None,
    rewrite_host: str | None = None,
    error: type[Exception] = DataRetrievalError,
) -> str:
    """Return *href* as a URL safe to request, or raise if it is not.

    ``response.url`` is consulted only when it is actually needed -- to resolve a
    relative reference, or as the default acceptable host. A walk that names its
    own acceptable hosts and receives an absolute link never touches it, which
    keeps this usable on a response whose request was never attached.

    Parameters
    ----------
    href : str
        The next-page link exactly as the service supplied it.
    response : httpx.Response
        The page the link arrived on.
    service : str
        Name of the service, used in the error messages (e.g. ``"ratings"``).
    allowed_hosts : frozenset of str, optional
        Hosts the link may name. Defaults to just the responding host; pass a
        wider set only where the service is known to spell its own host several
        ways.
    rewrite_host : str, optional
        Rewrite an accepted link to this host over ``https``, dropping any
        explicit port. For a service whose links name a spelling of the host
        that does not serve the API.
    error : type of Exception, optional
        Exception type to raise. Defaults to
        :class:`~dataretrieval.exceptions.DataRetrievalError`; the OGC engine
        passes ``RuntimeError`` to keep the type it has always raised, until
        retyping it is a deliberate, released decision.

    Returns
    -------
    str
        An absolute URL on an acceptable host, carrying no embedded credentials.
    """
    try:
        target = httpx.URL(href)
    except (httpx.InvalidURL, TypeError) as exc:
        raise error(
            f"The {service} service returned an unusable next-page link: "
            f"{href!r}. The page walk cannot continue; report this if it "
            f"persists."
        ) from exc
    if not target.is_absolute_url:
        target = _page_url(response).join(target)
    expected = (
        allowed_hosts
        if allowed_hosts is not None
        else frozenset({_page_url(response).host})
    )
    if target.host not in expected:
        raise error(
            f"Refusing to follow a cross-host next-page link: the {service} "
            f"response points at {target.host} rather than "
            f"{rewrite_host or ' or '.join(sorted(expected))}. Following it "
            f"would send this request, and any credentials on it, to a host "
            f"you did not ask for. Retrying will not help; report this if it "
            f"persists."
        )
    if rewrite_host is not None:
        # The port goes with the scheme/host rewrite: one that went with the
        # link's original scheme (``http://...:8080``) would otherwise survive
        # into an https request and be dialed under TLS. ``userinfo`` goes for
        # the reason below -- ``copy_with`` is doing both jobs at once here.
        return str(
            target.copy_with(scheme="https", host=rewrite_host, port=None, userinfo=b"")
        )
    # A same-host link may still carry ``user:pass@``, which httpx turns into an
    # ``Authorization: Basic`` header on the follow-up request. The host check
    # passes in exactly that case, so strip it rather than trust the link.
    return str(without_embedded_credentials(target))
