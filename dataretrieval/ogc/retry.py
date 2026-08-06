"""OGC interruption classification over service-neutral transport retry policy.

Only the OGC-specific half of retry lives here: turning a transport failure into
the resumable :class:`~dataretrieval.ogc.interruptions.ChunkInterrupted` the
chunker reports. The policy itself -- backoff, bounds, classification of what is
transient -- belongs to :mod:`dataretrieval.transport.retry`, which callers
import directly; re-exporting its tunables here would hand out stale copies that
patching cannot reach.

"Should we retry this?" and "can the caller resume it?" are the same question
asked twice, so both answers come from one place in transport. Keeping a second
copy here is how they would end up disagreeing -- refusing to retry a failure
while still telling the caller it can be resumed.
"""

from __future__ import annotations

import httpx

from dataretrieval.exceptions import RateLimited, TransientError
from dataretrieval.ogc.interruptions import (
    ChunkInterrupted,
    QuotaExhausted,
    ServiceInterrupted,
)
from dataretrieval.transport.retry import _deterministic_failure


def _classify_transient(
    exc: BaseException,
) -> tuple[type[ChunkInterrupted], float | None] | None:
    """Classify one failure as a resumable OGC interruption."""
    if isinstance(exc, RateLimited):
        return QuotaExhausted, exc.retry_after
    if isinstance(exc, TransientError):
        return ServiceInterrupted, exc.retry_after
    if isinstance(exc, (httpx.HTTPError, httpx.InvalidURL)):
        # Some failures will fail the same way every time -- a bad scheme, a
        # hostname that doesn't resolve. Offering to resume one would just
        # hide the real error behind a retry that can never work.
        if _deterministic_failure(exc):
            return None
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
