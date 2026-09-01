ADR 0006: Use a service-neutral transport layer
===============================================

Status
------

Accepted. The clause assigning resumable ``ChunkedCall`` state to OGC is
superseded by :doc:`0008-fan-out-execution`, which moves fan-out *execution*
into transport and leaves chunk *planning* in OGC. The rest stands.

Amended after acceptance under :doc:`0000-documenting-decisions`; the
``Notes`` section records every clause added or corrected.

Context
-------

Several service adapters need the same low-level capabilities: guarded HTTP
clients, cursor pagination, bounded retry, response aggregation, progress, and a
sync-over-async bridge. Locating those capabilities inside a protocol package
would make unrelated services depend on protocol-specific implementation
details -- Water Use previously imported its page walker and sync bridge from
``ogc.engine``. Duplicating them would allow authentication, timeout, retry,
and failure behavior to drift.

"Neutral" here means neutral across the USGS services this package talks to, not
across HTTP APIs in general. The layer knows the ``API_USGS_*`` environment
variables and the quota header USGS returns. Claiming broader neutrality than
that invites generality no caller needs.

Decision
--------

``dataretrieval.transport`` is the internal service-neutral execution layer. It
owns:

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
for failures a later attempt could survive -- rate limiting, server errors, and
transport failures that are not deterministic. Which server errors qualify is
per adapter: a fanned-out call re-sends any 5xx, riding out a transient
upstream failure, while a single-shot adapter re-sends only the gateway
statuses, because its service answers a *rejected query* with a 500, and
re-sending that would spend a caller's quota on a request that cannot succeed.
Both sets are narrower than ``DataRetrievalError.retryable``, deliberately:
that field tells a caller re-issuing might work, where spending someone's quota
unasked needs a stricter bar. Deprecated NWIS calls retain their compatibility
behavior. A failed pagination or fan-out operation raises rather than returning
successful siblings as an apparently complete result -- with one narrow
carve-out for a fan-out over independent items, recorded in
:doc:`0004-error-retry-resume`.

Two independent bounds limit retry: an attempt count and a no-progress budget
measured in seconds since data last arrived. Attempts alone leave elapsed time
unbounded, since each attempt may itself block until its timeout; the budget
alone would cut short a slow but productive download. Receiving a page restarts
the budget, and an attempt already in flight is never interrupted.

**Waiting is not the same as silence.** The budget bounds time the *service*
left the caller with nothing, so time the package chose to spend is credited
back by the measured amount: a wait the server named in ``Retry-After``, and
time a chunk spent queued behind the concurrency gate. The first retry is
exempt outright. Without these exemptions a policy that honors a server's hint
would spend its own budget obeying it, and a call would lose retries for being
throttled by settings the caller chose. Credit is never stamped into the
future -- a timestamp ahead of now would make elapsed silence negative and
silently disable the bound. Because half of that accounting is the retry
driver's, the concurrency gate is acquired *per attempt* inside the retry
driver rather than held by the caller across one.

**A server-supplied next-page link is untrusted response data.** One shared
policy parses it, resolves it against the request, refuses a host the caller
did not ask for, and strips embedded credentials (ADR 0009) before it becomes a
request; a page walk injects only which hosts are acceptable. Three walks
follow such links -- OGC ``links``, the ratings STAC search, and Water Use's
``Link`` header -- and a link is the same attacker-influenced input in all
three, so a fourth parse outside that policy is a defect rather than a
variation.

**Refusing credential-shaped keywords is the credentials leaf's job.** ADR 0009
owns the rule that a wide ``**kwargs`` or ``**queryables`` passthrough refuses
such names; what belongs here is where the predicate lives. It is the fourth
question that leaf answers, alongside which host honors the key, whether a
destination qualifies, and how the key is withheld -- one definition, so ten
getters cannot drift into ten spellings of the same check.

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
  the one complex module, because two independent bounds are what make retry
  safe against a slow service.

Compliance
----------

``.importlinter`` enforces transport dependency direction and Water Use
isolation from OGC. ``tests/architecture_test.py`` covers what the import graph
cannot see: that presentation and frame-assembly modules do not reappear inside
transport, and that only ``dataretrieval.credentials`` names the API-key host.
Component and adapter tests cover cursor termination, row caps, response
aggregation, retry exhaustion, ``Retry-After`` limits, the no-progress budget,
which failures are re-sent, cancellation, no-partial fan-out behavior, and
credential host scoping. The exemptions above are covered by the liveness and
retry tests over credited waits and the first-attempt case. Next-page link
validation is covered by the shared link-policy tests over foreign hosts and
embedded userinfo.

Notes
-----

The waiting-is-not-silence, next-page-link, and credentials-leaf clauses were
added after the original decision, consolidating under ADR 0000 the rules the
code was carrying in prose -- the budget exemptions were argued in five places
across ``transport/retry.py``, ``transport/liveness.py``, and
``transport/fanout.py``.

One sentence of the original Decision was also corrected rather than added to:
it scoped automatic retry to "gateway 5xx", which was never true of a fanned-out
call -- those re-send any 5xx, and only the single-shot adapters are limited to
the gateway statuses. The decision is unchanged; the sentence now describes it.
