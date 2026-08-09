ADR 0006: Use a service-neutral transport layer
===============================================

Status
------

Accepted

Context
-------

Several service adapters need the same low-level capabilities: guarded HTTP
clients, cursor pagination, bounded retry, response aggregation, progress, and a
sync-over-async bridge. Locating those capabilities inside a protocol package
would make unrelated services depend on protocol-specific implementation
details -- Water Use previously imported its page walker and sync bridge from
``ogc.engine``, a dependency with no conceptual basis. Duplicating them would
allow authentication, timeout, retry, and failure behavior to drift.

"Neutral" here means neutral across the USGS services this package talks to, not
across HTTP APIs in general. The layer knows the ``API_USGS_*`` environment
variables and the quota header USGS returns. Claiming broader neutrality than
that invites generality no caller needs.

Decision
--------

``dataretrieval.transport`` is the internal service-neutral execution layer --
neutral across the USGS services this package talks to, not across HTTP APIs in
general. It owns:

- synchronous and asynchronous HTTP client lifecycle and timeout defaults;
- attaching the API key and stripping it at redirect time, over the predicate
  ``dataretrieval.credentials`` defines;
- callback-driven cursor pagination;
- bounded retry with exponential backoff, full jitter, capped ``Retry-After``
  handling, and a no-progress budget bounding how long a call may receive
  nothing at all; and
- the sync-over-async blocking-portal bridge.

Three concerns are deliberately *outside* it, as top-level leaves, because they
are not HTTP execution policy and every adapter needs them whether or not it goes
through transport:

- ``dataretrieval.credentials`` -- which host honors the key, whether a
  destination qualifies, and how the key is withheld. One definition, so the code
  that attaches a credential and the code that removes it cannot disagree.
- ``dataretrieval.progress`` -- terminal rendering. Transport reports *into* it.
- ``dataretrieval.combining`` -- pandas frame and response assembly. Transport
  returns results *through* it.

Transport depends only on stable package leaves and third-party infrastructure.
It must not import OGC modules or service adapters. Service adapters inject
request construction, response parsing, cursor extraction, and API-specific
error details.

OGC retains its protocol concerns: dialects, CQL2, request construction, feature
shaping, URL-byte chunk planning, resumable ``ChunkedCall`` state, and typed
interruption handles. Thin imports at previous private OGC and utility paths
preserve compatibility where a consumer still uses them. A path no consumer
imports is deleted rather than kept as a module that exists to satisfy its own
test. Tunables are never re-exported by value: a caller can patch a copy taken
at import time without reaching the policy that reads it, so
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
  and total silent time remain bounded, and cancellation signals are never
  wrapped.
- Guidance the progress reporter prints is gated on the host it applies to, so a
  service that cannot use an API key is not told to obtain one.
- The transport package is internal infrastructure, not a new public API
  promise.
- Keeping presentation and frame assembly out means transport is roughly 570
  lines across five modules, each recognizably HTTP execution policy. Retry is
  the one intricate module, and it is intricate because two independent bounds
  are what make retry safe against a slow service.

Compliance
----------

``.importlinter`` enforces transport dependency direction, an acyclic runtime
graph, and Water Use isolation from OGC. ``tests/architecture_test.py`` covers
what the import graph cannot see: that presentation and frame-assembly modules
do not reappear inside transport, and that only ``dataretrieval.credentials``
names the API-key host. Component and adapter
tests cover cursor termination, row caps, response aggregation, retry
exhaustion, ``Retry-After`` limits, the no-progress budget, which failures are
re-sent, cancellation, no-partial fan-out behavior, and credential host scoping.
