"""OGC interruption classification over API-neutral transport retry policy.

Only the OGC-specific half of retry lives here: turning a transport failure into
the resumable :class:`~dataretrieval.ogc.interruptions.ChunkInterrupted` the
chunker reports. The policy itself -- backoff, bounds, classification of what is
transient -- belongs to :mod:`dataretrieval.transport.retry`, which callers
import directly; re-exporting its tunables here would hand out stale copies that
patching cannot reach.
"""

from __future__ import annotations

import httpx

from dataretrieval.exceptions import RateLimited, TransientError
from dataretrieval.ogc.interruptions import (
    ChunkInterrupted,
    QuotaExhausted,
    ServiceInterrupted,
)


def _classify_transient(
    exc: BaseException,
) -> tuple[type[ChunkInterrupted], float | None] | None:
    """Classify one failure as a resumable OGC interruption."""
    if isinstance(exc, RateLimited):
        return QuotaExhausted, exc.retry_after
    if isinstance(exc, TransientError):
        return ServiceInterrupted, exc.retry_after
    if isinstance(exc, (httpx.HTTPError, httpx.InvalidURL)):
        return ServiceInterrupted, None
    return None


def _classify_chunk_error(
    exc: BaseException,
) -> tuple[type[ChunkInterrupted], float | None] | None:
    """Walk a wrapped pagination failure for a resumable transport cause."""
    current: BaseException | None = exc
    while current is not None:
        result = _classify_transient(current)
        if result is not None:
            return result
        current = current.__cause__
    return None


__all__ = [
    "_classify_chunk_error",
    "_classify_transient",
]
