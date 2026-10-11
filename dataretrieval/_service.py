"""The value an adapter declares for its service (ADR 0014).

A dependency-free leaf, so configuration, execution, and protocol code can all
accept a :class:`Service` without importing an adapter.
"""

from __future__ import annotations

from dataclasses import dataclass


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
