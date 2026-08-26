"""Dependency-free scoped context values used by concurrent internals."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Generic, TypeVar

_T = TypeVar("_T")


class Ambient(Generic[_T]):
    """A :class:`~contextvars.ContextVar` paired with a scoping contextmanager.

    Bundles the var and its set/reset-token dance into one object, so an ambient
    value needs a single declaration instead of a ``var`` + setter-function pair.
    Read the current value with :meth:`get`; set it for a ``with`` block by
    calling the instance. The previous value is restored on exit::

        _base_url = Ambient("ogc_base_url", DEFAULT)
        with _base_url(other):
            _base_url.get()  # -> other
    """

    def __init__(self, name: str, default: _T) -> None:
        self._var: ContextVar[_T] = ContextVar(name, default=default)

    def get(self) -> _T:
        """Return the current value, or the default outside an active scope."""
        return self._var.get()

    @contextmanager
    def __call__(self, value: _T) -> Iterator[None]:
        """Set the value for the duration of the ``with`` block."""
        token = self._var.set(value)
        try:
            yield
        finally:
            self._var.reset(token)


# Preserve the documented class path from the v1.2.0 utility API.
Ambient.__module__ = "dataretrieval.utils"
