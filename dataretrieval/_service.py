"""The value an adapter declares for its service (ADR 0014).

A dependency-free leaf, so configuration, execution, and protocol code can all
accept a :class:`Service` without importing an adapter.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar


@dataclass(frozen=True)
class Service:
    """One external system this package retrieves from.

    Attributes
    ----------
    name : str
        The adapter's name: the key of its configuration table and its
        ``Configuration`` class, and the name settings are resolved by. It must
        be listed in the configuration roster.
    default_url : str
        The URL used when no ``base_url`` setting replaces it.
    """

    name: str
    default_url: str


DialectT = TypeVar("DialectT")


@dataclass(frozen=True)
class Endpoint(Generic[DialectT]):
    """One API of a service, as a call reaches it (ADR 0014).

    An adapter builds one when a call starts, so its URL is the one the
    configuration in effect at that moment resolves; every chunk, and a later
    resume, use the same value.

    Attributes
    ----------
    service : Service
        The service this API belongs to; settings resolve by its name.
    url : str
        The API's base URL for this call.
    dialect : DialectT
        How this API departs from its protocol's standard. The type is the
        protocol's own dialect type.
    """

    service: Service
    url: str
    dialect: DialectT
