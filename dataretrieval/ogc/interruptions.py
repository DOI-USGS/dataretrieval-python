"""Compatibility re-export: the interruption taxonomy moved to a top-level leaf.

The resume contract is no longer OGC-specific — Water Use raises it too — so the
classes live in :mod:`dataretrieval.interruptions`, where the base class is
named :class:`~dataretrieval.interruptions.FanOutInterrupted`. This path is kept
because it is what existing code and tests import; new code should import from
the leaf, or the top level
(``from dataretrieval import FanOutInterrupted``).
"""

from __future__ import annotations

from dataretrieval.interruptions import (
    ChunkInterrupted,
    FanOutInterrupted,
    QuotaExhausted,
    ServiceInterrupted,
)

__all__ = [
    "ChunkInterrupted",
    "FanOutInterrupted",
    "QuotaExhausted",
    "ServiceInterrupted",
]
