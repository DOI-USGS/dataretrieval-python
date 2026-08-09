"""Environment parsing for the ``API_USGS_*`` numeric knobs.

A dependency-free leaf: every transport setting read from the environment
shares one grammar and one error voice, and no policy module has to be
imported to get at the parser.
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from typing import TypeVar

from dataretrieval.exceptions import ConfigurationError

_Number = TypeVar("_Number", int, float)


def _read_env_number(
    name: str,
    default: _Number,
    cast: Callable[[str], _Number],
    expected: str,
    *,
    minimum: float = 0,
    hint: str = "",
) -> _Number:
    """Read a bounded number from the environment, or ``default`` if unset.

    The single parser behind every ``API_USGS_*`` numeric knob, so they share
    one grammar and one error voice rather than each adapter hand-rolling the
    read-cast-validate sequence its own way.

    Raises :class:`~dataretrieval.exceptions.ConfigurationError` -- a
    ``DataRetrievalError`` *and* a ``ValueError`` -- for an unusable value, so a
    typo in the environment doesn't escape a request path as a bare
    ``ValueError`` that ``except DataRetrievalError`` misses. ``hint`` appends
    a sentence pointing at the fix when a setting has one (e.g. the keyword
    that disables a cap).
    """
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = cast(raw)
    except ValueError as exc:
        raise ConfigurationError(
            f"{name} must be {expected} (got {raw!r}).{hint}"
        ) from exc
    # ``nan`` passes every ordering test, so a bare ``< minimum`` guard lets it
    # through and then silently makes each budget comparison false.
    if not math.isfinite(value):
        raise ConfigurationError(f"{name} must be {expected} (got {raw!r}).{hint}")
    if value < minimum:
        raise ConfigurationError(f"{name} must be >= {minimum:g} (got {value}).{hint}")
    return value
