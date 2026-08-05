ADR 0006: Use an API-neutral transport layer
============================================

Status
------

Accepted

Context
-------

Several service adapters need the same low-level capabilities: guarded HTTP
clients, cursor pagination, bounded retry, response aggregation, progress, and a
sync-over-async bridge. Locating those capabilities inside a protocol package
would make unrelated services depend on protocol-specific implementation
details. Duplicating them would allow authentication, timeout, retry, and
failure behavior to drift.

Decision
--------

``dataretrieval.transport`` is the internal API-neutral execution layer. It owns:

- synchronous and asynchronous HTTP client lifecycle and timeout defaults;
- host-scoped API-key construction and redirect-time credential stripping;
- callback-driven cursor pagination;
- bounded retry with exponential backoff, full jitter, capped ``Retry-After``
  handling, and a no-progress budget bounding how long a call may receive
  nothing at all;
- DataFrame and HTTP-response aggregation;
- best-effort progress reporting; and
- the sync-over-async blocking-portal bridge.

Transport depends only on stable package leaves and third-party infrastructure.
It must not import OGC modules or service adapters. Service adapters inject
request construction, response parsing, cursor extraction, and API-specific
error details.

OGC retains its protocol concerns: dialects, CQL2, request construction, feature
shaping, URL-byte chunk planning, resumable ``ChunkedCall`` state, and typed
interruption handles. Thin imports at previous private OGC and utility paths
preserve compatibility where a consumer still uses them; a path no consumer
imports is deleted rather than kept as a module that exists to satisfy its own
test. Tunables are never re-exported by value: a copy taken at import time is
one a caller can patch without reaching the policy that reads it, so
``transport.retry`` is the single place they are read from.

Automatic retry is enabled only on active, idempotent request paths, and only
for failures a later attempt could survive -- rate limiting, gateway 5xx, and
transport failures that are not settled before the request leaves. A server
error reporting that *this* request was rejected is surfaced on the first
attempt rather than multiplied against an already-failing service. Deprecated
NWIS calls retain their compatibility behavior. A failed pagination or fan-out
operation raises rather than returning successful siblings as an apparently
complete result.

Two independent bounds limit retry: an attempt count and a no-progress budget
measured in seconds since data last arrived. Attempts alone leave elapsed time
unbounded, since each attempt may itself block until its timeout; the budget
alone would cut short a slow but productive download. Receiving a page restarts
the budget, and an attempt already in flight is never interrupted.

Consequences
------------

- Water Use has no dependency on OGC implementation modules.
- OGC and non-OGC adapters share authentication, timeout, retry, pagination,
  aggregation, progress, and sync-dispatch policy where their semantics match.
- Service-specific request and result contracts remain explicit instead of
  being forced into a universal adapter abstraction.
- Retry can increase latency and quota consumption, so attempt counts, waits,
  and total silent time remain bounded and cancellation signals are never
  wrapped.
- Guidance the transport layer prints is gated on the host it applies to, so a
  service that cannot use an API key is not told to obtain one.
- The transport package is internal infrastructure, not a new public API
  promise.

Compliance
----------

``tests/architecture_test.py`` enforces transport dependency direction, an
acyclic transport graph, and Water Use isolation from OGC. Component and adapter
tests cover cursor termination, row caps, response aggregation, retry
exhaustion, ``Retry-After`` limits, the no-progress budget, which failures are
re-sent, cancellation, no-partial fan-out behavior, and credential host scoping.
