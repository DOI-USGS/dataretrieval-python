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
"""

from __future__ import annotations

from collections.abc import Collection


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
