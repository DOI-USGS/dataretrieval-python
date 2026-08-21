"""Argument checks shared by every adapter.

Rejecting a value that is not in a closed vocabulary is the one validation
every adapter does, and it was written eleven times: eight message phrasings
for one concept, so each new check was a coin flip on wording. That is how
:func:`~dataretrieval.waterdata.get_reference_table` came to tell callers who
passed a bad ``collection`` that their *code service* was invalid -- the check
was copied from :mod:`~dataretrieval.waterdata.samples`, message and local
variable name included, and the noun was never changed.

This module owns the wording so a new check cannot invent its own. It is a
leaf with no first-party imports: the vocabularies it validates against live
with the adapters that define them, and only the rejection is shared.

Three checks recur across the adapters, and each has a message shape here: a
value outside a closed vocabulary (:func:`require_one_of`), an argument that is
missing (:func:`require_argument`, :func:`require_together`), and arguments that
cannot be combined (:func:`require_exactly_one`, :func:`reject_together`).

Every message states the problem and then the move that fixes it, in that
order. Most callers of this package are programs -- a script, a pipeline stage,
an agent -- and a program cannot infer from "Service not recognized" which
services exist. Naming the remedy is what lets the caller correct itself
without a human reading the source, so a check that cannot name one is a check
whose message is not finished.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import TypeVar

_T = TypeVar("_T")


def _render(options: Collection[object]) -> str:
    """Format *options* for a message: ``'a', 'b', 'c'``.

    Renders the values rather than their container so ``dict_keys([...])`` and
    a bare tuple read the same to a caller, who never sees the container.
    """
    return ", ".join(repr(option) for option in options)


def require_one_of(
    value: object,
    options: Collection[object],
    *,
    name: str,
    context: str = "",
) -> None:
    """Raise ``ValueError`` unless *value* is one of *options*.

    Parameters
    ----------
    value
        The argument the caller supplied.
    options
        The closed vocabulary it must belong to -- typically
        ``get_args(SomeLiteral)``, a module constant, or a mapping's keys.
        Rendered in iteration order, so pass a sorted view when the source is
        unordered and the order would otherwise be arbitrary.
    name
        What the value *is*, as the caller's parameter names it (``"service"``,
        ``"collection"``). It becomes the message's subject, so it must match
        the parameter the caller actually passed.
    context
        Optional qualifier for a vocabulary that depends on another argument,
        e.g. ``context="service 'wqp'"`` when the valid profiles differ per
        service.

    Raises
    ------
    ValueError
        If *value* is not in *options*.
    """
    if isinstance(options, str):
        # ``str`` is a Collection, so this type-checks -- and then ``in``
        # silently means "substring", accepting any fragment of a valid option.
        raise TypeError(f"options must be a collection of values, not {options!r}")
    if value in options:
        return
    qualifier = f" for {context}" if context else ""
    raise ValueError(
        f"Invalid {name}: {value!r}{qualifier}. Valid options are: {_render(options)}."
    )


def _render_names(names: Collection[str], *, conjunction: str = "and") -> str:
    """Format parameter *names* for a message: ``a``, ``a and b``, ``a, b and c``.

    Bare, not quoted: these are the caller's own parameter names, so they read
    as identifiers to paste back into the call rather than as data values --
    which is what :func:`_render` is for.
    """
    listed = list(names)
    if len(listed) <= 1:
        return "".join(listed)
    return f"{', '.join(listed[:-1])} {conjunction} {listed[-1]}"


def _supplied(values: Mapping[str, object]) -> tuple[list[str], list[str]]:
    """Split *values* into the names that were supplied and those that were not.

    ``None`` is the package's "not supplied" marker throughout the public
    signatures, so it is the one this module tests for. A caller whose sentinel
    differs -- an empty string that should count as missing -- normalizes to
    ``None`` before calling, rather than this module guessing which falsy values
    were meant.
    """
    supplied = [name for name, value in values.items() if value is not None]
    missing = [name for name, value in values.items() if value is None]
    return supplied, missing


def require_argument(
    name: str,
    value: _T | None,
    *,
    context: str = "",
    remedy: str = "",
) -> _T:
    """Return *value*, or raise ``ValueError`` if it was not supplied.

    Returns the value rather than ``None`` so the check also narrows the type:
    a caller that must hand an optional argument to something requiring a
    concrete one writes ``x = require_argument("x", x)`` and is done. The
    alternative -- validating here and re-testing for ``None`` to satisfy the
    type checker -- puts a second, unreachable message next to this one, and
    the two drift.

    Parameters
    ----------
    name
        The parameter as the caller spells it.
    value
        What they passed; ``None`` means not supplied.
    context
        When the requirement is conditional, the condition that triggered it --
        ``context="when comid is given"``. Omitted when the argument is always
        required.
    remedy
        What to do instead, when the default ("pass a value") is not enough to
        act on -- typically the accepted forms or an example value.

    Returns
    -------
    The supplied value, narrowed to non-``None``.

    Raises
    ------
    ValueError
        If *value* is ``None``.
    """
    if value is not None:
        return value
    when = f" {context}" if context else ""
    raise ValueError(f"{name} is required{when}. {remedy or f'Pass a {name} value.'}")


def require_together(
    values: Mapping[str, object],
    *,
    context: str = "",
    remedy: str = "",
) -> None:
    """Raise ``ValueError`` unless *values* are all supplied or all omitted.

    For arguments that only mean something as a set -- a ``lat``/``long`` pair,
    a ``feature_source``/``feature_id`` pair. Passing none of them is allowed:
    that is the caller declining the whole group, which is a different question
    from whether the group is complete.

    Parameters
    ----------
    values
        Parameter name to supplied value, in the order the message should
        list them.
    context
        Where the group applies, when more than one exists --
        ``context="for find='basin'"``.
    remedy
        Overrides the default remedy, which names the missing arguments to
        supply and the supplied ones to drop.

    Raises
    ------
    ValueError
        If some but not all of *values* were supplied.
    """
    supplied, missing = _supplied(values)
    if not supplied or not missing:
        return
    where = f" {context}" if context else ""
    fix = remedy or (
        f"Pass {_render_names(missing)}, or omit {_render_names(supplied)}."
    )
    raise ValueError(
        f"{_render_names(values)} must be given together{where}. "
        f"Missing: {_render_names(missing)}. {fix}"
    )


def require_exactly_one(
    values: Mapping[str, object],
    *,
    context: str = "",
    remedy: str = "",
) -> None:
    """Raise ``ValueError`` unless exactly one of *values* was supplied.

    For a choice between alternatives that are each sufficient on their own --
    the origin of an NLDI navigation, the location selector of an NWDC query.
    Both failure directions are reported by the same check because they have
    the same fix from opposite sides: supply one, or drop the rest.

    Parameters
    ----------
    values
        Parameter name to supplied value, in the order the message should
        list them.
    context
        What the choice is for, when the parameter names do not say --
        ``context="as the query's location"``.
    remedy
        Overrides the default remedy, which is derived from which way the
        check failed.

    Raises
    ------
    ValueError
        If none of *values* were supplied, or more than one was.
    """
    supplied, _ = _supplied(values)
    if len(supplied) == 1:
        return
    where = f" {context}" if context else ""
    if supplied:
        fix = remedy or f"Drop all but one of {_render_names(supplied)}."
        got = _render_names(supplied)
    else:
        fix = remedy or f"Pass one of {_render_names(values, conjunction='or')}."
        got = "none"
    raise ValueError(
        f"Provide exactly one of {_render_names(values, conjunction='or')}"
        f"{where}. Supplied: {got}. {fix}"
    )


def reject_together(
    values: Mapping[str, object],
    *,
    context: str = "",
    remedy: str = "",
) -> None:
    """Raise ``ValueError`` if more than one of *values* was supplied.

    The permissive sibling of :func:`require_exactly_one`: it rejects the
    combination without requiring that anything be supplied at all, for
    arguments that conflict but are jointly optional.

    Parameters
    ----------
    values
        Parameter name to supplied value, in the order the message should
        list them.
    context
        Why they conflict, when the names do not make it evident --
        ``context="they name different origins"``.
    remedy
        Overrides the default remedy, which names the supplied arguments to
        choose between.

    Raises
    ------
    ValueError
        If two or more of *values* were supplied.
    """
    supplied, _ = _supplied(values)
    if len(supplied) < 2:
        return
    why = f" -- {context}" if context else ""
    fix = remedy or f"Pass only one of {_render_names(supplied, conjunction='or')}."
    raise ValueError(f"{_render_names(supplied)} cannot be combined{why}. {fix}")
