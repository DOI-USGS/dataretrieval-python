"""Compatibility re-export: interruption classification moved to the taxonomy leaf.

Turning a transport failure into a resumable
:class:`~dataretrieval.interruptions.FanOutInterrupted` was never OGC-specific --
it keys off the shared ``RateLimited``/``TransientError`` taxonomy and httpx --
so it now lives beside the classes it produces, in
:mod:`dataretrieval.interruptions`.

The retry *policy* -- backoff, bounds, classification of what is transient --
still belongs to :mod:`dataretrieval.transport.retry`, which callers import
directly; re-exporting its tunables here would hand out stale copies that
patching cannot reach.
"""

from __future__ import annotations

from dataretrieval.interruptions import _classify_chunk_error, _classify_transient

__all__ = [
    "_classify_chunk_error",
    "_classify_transient",
]
