"""State code lookups and normalization, keyed by full state name.

``state_codes`` maps each state name to its two-letter postal abbreviation
(e.g. ``"Alabama": "al"``); ``fips_codes`` maps the same names to their
two-digit FIPS codes (e.g. ``"Alabama": "01"``). :func:`to_state` normalizes
a state identifier -- a full name, postal code, or two-digit /
``US:``-prefixed FIPS code (or an iterable of them) -- to a chosen
representation. An unrecognized value raises ``ValueError``. Coverage is the
50 states, the District of Columbia, and the five US territories.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from dataretrieval._validation import reject_together, require_one_of

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
    "American Samoa": "as",
    "Guam": "gu",
    "Northern Mariana Islands": "mp",
    "Puerto Rico": "pr",
    "US Virgin Islands": "vi",
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
    "American Samoa": "60",
    "Guam": "66",
    "Northern Mariana Islands": "69",
    "Puerto Rico": "72",
    "US Virgin Islands": "78",
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

    Coverage is the 50 states, DC, and the five US territories, each under its
    real ANSI/FIPS code. A ``value`` that isn't recognized in one of those
    encodings raises ``ValueError``, so a typo fails fast rather than
    silently matching nothing.
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
            f"{value!r} is not a recognized US state, district, or "
            f'territory. Provide a full name ("Wisconsin"), a two-letter '
            f'postal code ("WI"), or a two-digit ANSI/FIPS code ("55").'
        )

    return _format_state(name, to)


def _format_state(name: str, to: str) -> str:
    """Render a canonical state *name* in the ``to`` representation."""
    require_one_of(to, ("name", "postal", "fips", "fips_us"), name="to")
    if to == "name":
        return name
    if to == "postal":
        return state_codes[name].upper()
    if to == "fips":
        return fips_codes[name]
    return f"US:{fips_codes[name]}"


def apply_state(
    local_vars: dict[str, Any],
    *,
    to: str,
    into: str,
    reject: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Resolve a unified ``state`` kwarg into an API query parameter.

    Pops ``state`` from ``local_vars`` (a no-op when absent), normalizes it via
    :func:`to_state` to the ``to`` representation, and stores the result under
    ``into`` -- the API query parameter the endpoint filters on. ``reject`` names
    native state parameters that must not be combined with ``state``; passing
    ``state`` alongside any of them raises ``ValueError``. Returns the (mutated)
    ``local_vars``.

    An unrecognized ``state`` is re-raised naming the parameters in ``reject``,
    and only those. They are the endpoint's own state parameters as the public
    getter spells them: the mutual-exclusion guard below proves the getter
    accepts them as keyword arguments. ``into`` is deliberately not offered,
    because an API query parameter need not exist on the getter's signature --
    NGWMN's ``get_sites`` filters on ``state_name`` but accepts only ``state``,
    so naming ``into`` there produced a remedy that raises ``TypeError`` when
    followed. An endpoint with an empty ``reject`` has no alternative spelling
    to offer, so it appends nothing rather than pointing back at the argument
    that just failed.
    """
    state = local_vars.pop("state", None)
    if state is None:
        return local_vars
    reject_together(
        {"state": state, **{p: local_vars.get(p) for p in reject}},
        context="they filter on the same thing",
    )
    try:
        local_vars[into] = to_state(state, to)
    except ValueError as err:
        if not reject:
            # No native spelling of the getter's own to offer instead.
            raise
        # Only ``reject`` proves the getter accepts a spelling; ``into`` is the
        # API query parameter, so it leads only when it appears there too.
        offered = dict.fromkeys(n for n in (into, *reject) if n in reject)
        raise ValueError(
            f"{err} Pass {' or '.join(offered)} directly instead, using the "
            "API's native value."
        ) from err
    return local_vars
