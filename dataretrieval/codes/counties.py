"""County code lookups and normalization, keyed by five-digit FIPS code.

``counties`` maps each county or county equivalent -- parish, borough, census
area, independent city, municipio, Connecticut planning region -- to its name
as the Water Data API spells it (e.g. ``"55025": "Dane County"``).
:func:`to_county` normalizes a county identifier to a chosen representation,
and :func:`apply_county` resolves a getter's ``county`` argument into the
parameters its service filters on. An unrecognized value raises
``ValueError``.

Coverage matches :mod:`dataretrieval.codes.states`: the 50 states, the
District of Columbia, and the five US territories. The table is a snapshot of
the Water Data ``counties`` collection, which follows the Census Bureau's
current codes; ``tests/utils_test.py`` compares the two live.
"""

from __future__ import annotations

import difflib
from collections.abc import Callable, Iterable
from typing import Any

from dataretrieval._validation import reject_together, require_one_of

from ._county_table import counties
from .states import to_state

__all__ = ["apply_county", "counties", "to_county"]

#: Trailing words that name a county's kind rather than the county, longest
#: first so "City and Borough" is stripped before "Borough". A caller may omit
#: them: ``"Dane"`` matches ``"Dane County"`` when nothing else in the state
#: shares the base name.
_DESIGNATIONS = (
    "city and borough",
    "planning region",
    "census area",
    "municipality",
    "municipio",
    "borough",
    "county",
    "parish",
    "island",
    "district",
    "city",
)

_FORMATS = ("fips", "fips_us", "name")


def to_county(
    value: str | int | Iterable[str | int],
    to: str = "fips",
    *,
    state: str | int | Iterable[str | int] | None = None,
) -> str | list[str]:
    """Normalize a US county identifier to a chosen representation.

    ``value`` may be given as

    * a five-digit FIPS code, as a string or integer (``"55025"``, ``55025``);
    * a ``US:``-prefixed code with a colon between state and county
      (``"US:55:025"``), as the Water Data statistics service writes it;
    * with ``state``, a three-digit county code (``"025"``) or a name, matched
      case-insensitively, with or without its designation (``"Dane County"``
      or ``"Dane"``).

    County names and three-digit codes repeat across states, so those forms
    need ``state``, which takes any form :func:`~dataretrieval.codes.states.to_state`
    accepts. A full FIPS code already names its state; given both, they must
    agree. An iterable is resolved element-wise to a list.

    ``to`` selects the output representation:

    * ``"fips"``    -> five-digit FIPS code, e.g. ``"55025"``
    * ``"fips_us"`` -> ``"US:"`` + state + ``":"`` + county, e.g. ``"US:55:025"``
    * ``"name"``    -> the county's name, e.g. ``"Dane County"``

    Raises
    ------
    ValueError
        If a value is not a county in the table, a name or three-digit code
        has no ``state``, a bare name matches more than one county in the
        state, or a FIPS code lies outside ``state``.
    """
    require_one_of(to, _FORMATS, name="to")
    state_fips = None if state is None else _single_state_fips(state)
    if isinstance(value, (str, int)):
        return _format_county(_to_fips_one(value, state_fips), to)
    return [_format_county(_to_fips_one(v, state_fips), to) for v in value]


def apply_county(
    local_vars: dict[str, Any],
    *,
    render: Callable[[list[str]], dict[str, Any]],
    reject: tuple[str, ...],
) -> dict[str, Any]:
    """Resolve a getter's ``county`` argument into its service's parameters.

    Pops ``county`` from ``local_vars`` (a no-op when absent) together with
    ``state``, which then qualifies the counties instead of filtering on its
    own: a county lies in one state, so sending both would be redundant at
    best. The counties are resolved to five-digit FIPS codes, deduplicated in
    order, and passed to ``render`` as a list, which returns the parameters
    this service filters on; they are merged into ``local_vars``, which is
    returned.

    ``reject`` names the getter's native state and county parameters. Passing
    ``county`` with any of them raises ``ValueError``, and an unrecognized
    county is re-raised naming them as the way to send the service's own
    value. ``render`` may raise ``ValueError`` for a county its service cannot
    filter on.
    """
    county = local_vars.pop("county", None)
    if county is None:
        return local_vars
    state = local_vars.pop("state", None)
    reject_together(
        {"county": county, **{p: local_vars.get(p) for p in reject}},
        context="they filter on the same thing",
    )
    try:
        fips = to_county(county, "fips", state=state)
    except ValueError as err:
        raise ValueError(
            f"{err} Or pass {' or '.join(reject)} directly, using the API's "
            "native value."
        ) from err
    local_vars.update(render(list(dict.fromkeys(_as_list(fips)))))
    return local_vars


def _as_list(value: str | list[str]) -> list[str]:
    """``to_county``'s result as a list, whether it was given one value or many."""
    return value if isinstance(value, list) else [value]


def _single_state_fips(state: object) -> str:
    """The two-digit FIPS code of the one state qualifying ``county``."""
    if not isinstance(state, (str, int)):
        raise ValueError(
            "With county, state names the one state the counties are in, so it "
            f"must be a single state (got {state!r}). To select counties in "
            "several states, pass each as a five-digit FIPS code, e.g. "
            "county=['55025', '17031'], without state."
        )
    fips = to_state(state, "fips")
    assert isinstance(fips, str)
    return fips


def _to_fips_one(value: str | int, state_fips: str | None) -> str:
    """Resolve one county identifier to its five-digit FIPS code."""
    if isinstance(value, bool):
        raise _unrecognized(value)
    s = str(value).strip()
    fips = _parse_code(s, state_fips)
    if fips is None:
        if state_fips is None:
            raise ValueError(
                f"county={value!r} is a name, and county names repeat across "
                f"states. Pass the state too, e.g. county={value!r}, state='WI', "
                "or a five-digit FIPS code, e.g. county='55025'."
            )
        fips = _match_name(s, state_fips)
    if fips not in counties:
        raise _unrecognized(value, in_connecticut=fips.startswith("09"))
    if state_fips is not None and fips[:2] != state_fips:
        raise ValueError(
            f"county={value!r} is in {to_state(fips[:2], 'name')}, not "
            f"{to_state(state_fips, 'name')}. Drop state, or pass a county in "
            "that state."
        )
    return fips


def _parse_code(s: str, state_fips: str | None) -> str | None:
    """The FIPS code a code-shaped identifier spells, or ``None`` for a name.

    Not checked against the table; the caller does that once for every form.
    """
    if s[:3].upper() == "US:":
        return s[3:].replace(":", "")
    if not s.isdigit():
        return None
    if len(s) == 3 and state_fips is not None:
        return state_fips + s
    # An integer, or a string that lost its leading zero (``1001``). Any other
    # length is returned as given, so the table lookup rejects it.
    return s.zfill(5) if len(s) == 4 else s


def _normal(name: str) -> str:
    """``name`` lower-cased, without periods, with single spaces: ``"St Louis"``
    and ``"st. louis"`` compare equal."""
    return " ".join(name.lower().replace(".", "").split())


def _base(name: str) -> str:
    """``name`` normalized, without its trailing designation."""
    lowered = _normal(name)
    for word in _DESIGNATIONS:
        if lowered.endswith(f" {word}"):
            return lowered[: -len(word) - 1]
    return lowered


def _match_name(name: str, state_fips: str) -> str:
    """The FIPS code of the county called ``name`` in the state."""
    in_state = {f: n for f, n in counties.items() if f[:2] == state_fips}
    wanted = _normal(name)
    by_name = {_normal(n): f for f, n in in_state.items()}
    if wanted in by_name:
        return by_name[wanted]
    # The input may itself end in a designation word that is part of the
    # name ("Charles City" for Charles City County), so try it both ways.
    keys = {wanted, _base(wanted)}
    by_base = [f for f, n in in_state.items() if _base(n) in keys]
    if len(by_base) == 1:
        return by_base[0]
    state_name = to_state(state_fips, "name")
    if by_base:
        options = ", ".join(repr(in_state[f]) for f in by_base)
        raise ValueError(
            f"county={name!r} matches more than one county in {state_name}: "
            f"{options}. Pass the full name."
        )
    raise ValueError(
        f"county={name!r} is not a county in {state_name}."
        f"{_suggest(wanted, in_state.values())}"
    )


def _suggest(wanted: str, names: Iterable[str]) -> str:
    """A "Did you mean" hint naming the counties closest to ``wanted``."""
    bases: dict[str, list[str]] = {}
    for n in names:
        bases.setdefault(_base(n), []).append(n)
    close = difflib.get_close_matches(_base(wanted), list(bases))
    suggestions = [repr(n) for c in close for n in bases[c]]
    return f" Did you mean {', '.join(suggestions)}?" if suggestions else ""


def _unrecognized(value: object, *, in_connecticut: bool = False) -> ValueError:
    """The error for a code that names no county in the table."""
    hint = (
        " Connecticut's counties were replaced in 2022 by planning regions, "
        "codes 09110 to 09190."
        if in_connecticut
        else ""
    )
    return ValueError(
        f"{value!r} is not a recognized US county or county equivalent. Pass a "
        "five-digit FIPS code (county='55025'), or a name with its state "
        f"(county='Dane County', state='WI').{hint}"
    )


def _format_county(fips: str, to: str) -> str:
    """Render a five-digit FIPS code in the ``to`` representation."""
    if to == "fips":
        return fips
    if to == "fips_us":
        return f"US:{fips[:2]}:{fips[2:]}"
    return counties[fips]
