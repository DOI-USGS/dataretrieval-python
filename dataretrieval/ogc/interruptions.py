"""Deprecated alias for :mod:`dataretrieval.interruptions`.

The classes live in :mod:`dataretrieval.interruptions`, where the base class is
named :class:`~dataretrieval.interruptions.FanOutInterrupted`. That move, and
``ChunkInterrupted`` staying a permanent alias rather than a shim, are ADR 0008.
This path is the one v1.2.0 published, when the classes were defined here.

Importing this module emits a :class:`DeprecationWarning` and re-exports the
taxonomy. The re-exported objects are the *same objects*, not copies, so
``ogc.interruptions.ChunkInterrupted is dataretrieval.ChunkInterrupted`` and
``except`` clauses behave identically through either spelling. Only the module
*path* is deprecated.

``dataretrieval.ogc.__init__`` deliberately does not import this module, so
``import dataretrieval`` and ``import dataretrieval.ogc`` stay silent. The
warning fires only for code that names ``ogc.interruptions`` itself.
"""

from __future__ import annotations

from dataretrieval._deprecation import REMOVALS, warn_deprecated
from dataretrieval.interruptions import (
    ChunkInterrupted,
    FanOutInterrupted,
    QuotaExhausted,
    ServiceInterrupted,
)

#: When the alias may be deleted. Read from the shared horizon table rather
#: than spelled here, so it is audited and bumped with every other published
#: removal.
OGC_INTERRUPTIONS_REMOVAL_DATE = REMOVALS["ogc.interruptions"]

__all__ = [
    "ChunkInterrupted",
    "FanOutInterrupted",
    "QuotaExhausted",
    "ServiceInterrupted",
]

warn_deprecated(
    "`dataretrieval.ogc.interruptions`",
    replacement="`dataretrieval.interruptions`, or the top level "
    "(`from dataretrieval import ChunkInterrupted`)",
    removal=OGC_INTERRUPTIONS_REMOVAL_DATE,
    detail="The exception classes are unchanged and are the same objects; "
    "only this import path is going away. Removal is planned for a future "
    "major release.",
    # 1 lands the warning on the line that imported this module -- an import
    # has no deeper user frame to point at.
    stacklevel=1,
)
