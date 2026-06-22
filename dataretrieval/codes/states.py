"""State code lookups and normalization, keyed by full state name.

``state_codes`` maps each state name to its two-letter postal abbreviation
(e.g. ``"Alabama": "al"``); ``fips_codes`` maps it to its two-digit FIPS
code (e.g. ``"Alabama": "01"``). :func:`to_state` normalizes a state
identifier -- a full name, postal code, or two-digit / ``US:``-prefixed FIPS
code (or an iterable of them) -- to a chosen representation, raising
``ValueError`` on an unrecognized value. Coverage is the 50 states plus the
District of Columbia.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

state_codes = {
    "Alabama": "al",
    "Alaska": "ak",
    "Arizona": "az",
    "Arkansas": "ar",
    "California": "ca",
    "Colorado": "co",
    "Connecticut": "ct",
    "Delaware": "de",
    "District of Columbia": "dc",
    "Florida": "fl",
    "Georgia": "ga",
    "Hawaii": "hi",
    "Idaho": "id",
    "Illinois": "il",
    "Indiana": "in",
    "Iowa": "ia",
    "Kansas": "ks",
    "Kentucky": "ky",
    "Louisiana": "la",
    "Maine": "me",
    "Maryland": "md",
    "Massachusetts": "ma",
    "Michigan": "mi",
    "Minnesota": "mn",
    "Mississippi": "ms",
    "Missouri": "mo",
    "Montana": "mt",
    "Nebraska": "ne",
    "Nevada": "nv",
    "New Hampshire": "nh",
    "New Jersey": "nj",
    "New Mexico": "nm",
    "New York": "ny",
    "North Carolina": "nc",
    "North Dakota": "nd",
    "Ohio": "oh",
    "Oklahoma": "ok",
    "Oregon": "or",
    "Pennsylvania": "pa",
    "Rhode Island": "ri",
    "South Carolina": "sc",
    "South Dakota": "sd",
    "Tennessee": "tn",
    "Texas": "tx",
    "Utah": "ut",
    "Vermont": "vt",
    "Virginia": "va",
    "Washington": "wa",
    "West Virginia": "wv",
    "Wisconsin": "wi",
    "Wyoming": "wy",
}

fips_codes = {
    "Alabama": "01",
    "Alaska": "02",
    "Arizona": "04",
    "Arkansas": "05",
    "California": "06",
    "Colorado": "08",
    "Connecticut": "09",
    "Delaware": "10",
    "District of Columbia": "11",
    "Florida": "12",
    "Georgia": "13",
    "Hawaii": "15",
    "Idaho": "16",
    "Illinois": "17",
    "Indiana": "18",
    "Iowa": "19",
    "Kansas": "20",
    "Kentucky": "21",
    "Louisiana": "22",
    "Maine": "23",
    "Maryland": "24",
    "Massachusetts": "25",
    "Michigan": "26",
    "Minnesota": "27",
    "Mississippi": "28",
    "Missouri": "29",
    "Montana": "30",
    "Nebraska": "31",
    "Nevada": "32",
    "New Hampshire": "33",
    "New Jersey": "34",
    "New Mexico": "35",
    "New York": "36",
    "North Carolina": "37",
    "North Dakota": "38",
    "Ohio": "39",
    "Oklahoma": "40",
    "Oregon": "41",
    "Pennsylvania": "42",
    "Rhode Island": "44",
    "South Carolina": "45",
    "South Dakota": "46",
    "Tennessee": "47",
    "Texas": "48",
    "Utah": "49",
    "Vermont": "50",
    "Virginia": "51",
    "Washington": "53",
    "West Virginia": "54",
    "Wisconsin": "55",
    "Wyoming": "56",
}

# Reverse lookups (built once): postal code -> name, FIPS code -> name, and a
# case-insensitive full-name index. ``state_codes`` and ``fips_codes`` share the
# same keys, so any name resolved here is valid in both.
_name_by_postal = {code: name for name, code in state_codes.items()}
_name_by_fips = {fips: name for name, fips in fips_codes.items()}
_name_by_lower = {name.lower(): name for name in state_codes}


def to_state(
    value: str | int | Iterable[str | int], to: str = "name"
) -> str | list[str]:
    """Normalize a US state/territory identifier to a chosen representation.

    ``value`` may be given as a full name (``"Wisconsin"``), a two-letter
    postal code (``"WI"``), or an ANSI/FIPS code as a string or integer
    (``"55"`` or ``55``), optionally ``US:``-prefixed (``"US:55"``). The
    encodings are unambiguous: a value prefixed ``US:`` or all-digits is a
    FIPS code, exactly two letters is a postal code, anything else is matched
    (case-insensitively) as a full name. An iterable of identifiers is
    resolved element-wise to a list.

    ``to`` selects the output representation:

    * ``"name"``    -> full name, e.g. ``"Wisconsin"``
    * ``"postal"``  -> uppercase two-letter code, e.g. ``"WI"``
    * ``"fips"``    -> two-digit ANSI/FIPS code, e.g. ``"55"``
    * ``"fips_us"`` -> ``"US:"`` + FIPS code, e.g. ``"US:55"``

    Coverage is the 50 states plus the District of Columbia. A ``value`` that
    isn't a recognized state in one of those encodings raises ``ValueError``
    (so a typo fails fast rather than silently matching nothing).
    """
    if isinstance(value, str):
        return _to_state_one(value, to)
    if isinstance(value, Iterable):
        return [_to_state_one(v, to) for v in value]
    return _to_state_one(value, to)


def _to_state_one(value: str | int, to: str) -> str:
    """Resolve a single state identifier; see :func:`to_state`."""
    s = str(value).strip()
    if s[:3].upper() == "US:":  # prefixed FIPS, e.g. "US:55"
        name = _name_by_fips.get(s[3:].strip().zfill(2))
    elif s.isdigit():  # bare FIPS, e.g. "55"
        name = _name_by_fips.get(s.zfill(2))
    elif len(s) == 2 and s.isalpha():  # postal, e.g. "WI"
        name = _name_by_postal.get(s.lower())
    else:  # full name (case-insensitive)
        name = _name_by_lower.get(s.lower())

    if name is None:
        raise ValueError(
            f"{value!r} is not a recognized US state or the District of "
            f'Columbia. Provide a full name ("Wisconsin"), a two-letter postal '
            f'code ("WI"), or a two-digit ANSI/FIPS code ("55").'
        )

    if to == "name":
        return name
    if to == "postal":
        return state_codes[name].upper()
    if to == "fips":
        return fips_codes[name]
    if to == "fips_us":
        return f"US:{fips_codes[name]}"
    raise ValueError(f"to must be 'name', 'postal', 'fips', or 'fips_us'; got {to!r}")


def apply_state(
    local_vars: dict[str, Any],
    *,
    to: str,
    into: str,
    reject: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Resolve a unified ``state`` kwarg into an endpoint's native queryable.

    Pops ``state`` from ``local_vars`` (a no-op when absent), normalizes it via
    :func:`to_state` to the ``to`` representation, and stores the result under
    ``into`` -- the queryable the endpoint actually filters on. ``reject`` names
    native state parameters that must not be combined with ``state``; passing
    ``state`` alongside any of them raises ``ValueError``. Returns the (mutated)
    ``local_vars``.
    """
    state = local_vars.pop("state", None)
    if state is None:
        return local_vars
    if any(local_vars.get(p) is not None for p in reject):
        raise ValueError(f"Pass `state`, or {'/'.join(reject)}, but not both.")
    local_vars[into] = to_state(state, to)
    return local_vars
