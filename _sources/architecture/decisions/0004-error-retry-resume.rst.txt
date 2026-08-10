ADR 0004: Use typed failures and bounded recovery
=================================================

Status
------

Accepted

Context
-------

Remote hydrologic services fail through HTTP statuses, rate limits, timeouts,
invalid payloads, and mid-pagination interruptions. Returning partial data after
an undetected page failure is worse than raising. At the same time, retrying
without limits can stall callers, multiply quota usage, or hide persistent
faults.

Decision
--------

All request failures exposed by public service modules derive from
``DataRetrievalError`` and provide uniform ``status_code``, ``retry_after``, and
``retryable`` attributes. Status mapping lives in one policy function.

Where automatic recovery is supported, retries are bounded, use exponential
backoff with full jitter, honor only bounded ``Retry-After`` delays, and preserve
cancellation. OGC fan-out retains completed chunks and raises a typed
``ChunkInterrupted`` with a handle that resumes only missing work. Fatal or
unknown failures are not disguised as resumable transients.

The shared transport layer supplies bounded retry and callback-driven cursor
pagination, but each adapter opts in only where its requests are idempotent and
its protocol exposes a cursor. Chunk planning and resumable partial state remain
OGC-specific capabilities rather than assumptions imposed on every service.

Consequences
------------

- Callers can catch one stable base error and still branch on useful fields.
- Mid-pagination failure cannot silently look like a complete dataset.
- Retry can increase latency and request quota, so policy and defaults are part
  of observable behavior.
- Partial OGC state requires careful serialization and finalization tests.
- Expanding retry to another service requires service-specific idempotency and
  failure-contract tests.

Compliance
----------

Tests cover status-to-type mapping, uniform fields, transport wrapping,
pagination failure, retry exhaustion and jitter bounds, ``Retry-After`` limits,
resume equivalence, partial-state stability, pickling, and cancellation
precedence.
