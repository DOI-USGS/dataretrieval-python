ADR 0008: Separate fan-out execution from chunk planning
========================================================

Status
------

Accepted. Supersedes the clause of :doc:`0006-service-neutral-transport`
assigning "resumable ``ChunkedCall`` state" to OGC's protocol concerns; the rest
of ADR 0006 stands.

Context
-------

Two services turn one logical query into several requests, for unrelated
reasons. A Water Data or NGWMN query whose URL exceeds the server's byte limit
is split along its multi-value axes. A Water Use query naming several locations
is split because the NWDC accepts one ``location=`` per request -- its URLs run
around 63 bytes against an 8000-byte budget, so the byte limit has nothing to do
with it.

Chunking is how you divide the data structurally; fan-out is how you distribute
the work operationally. The two are orthogonal, and only the first is protocol
knowledge: dividing a query needs the byte budget, the CQL2 grammar, and which
parameters are list-valued, while distributing the pieces needs none of it.

The package had not drawn that line. ``ChunkPlan`` (division) and
``ChunkedCall`` (distribution) sat side by side in ``dataretrieval.ogc`` as
siblings, and ADR 0006 grouped them together deliberately. That grouping was
correct while a byte plan was the only thing anyone fanned out over. It stopped
being correct once Water Use fanned out too: unable to reach an OGC-internal
executor, ``wateruse._fan_out`` re-implemented the semaphore, the
``asyncio.gather``, and the cancellation-beats-HTTP-error failure precedence,
with a comment naming ``ChunkedCall._run`` as the original. One subtle rule,
two copies, synchronized by prose.

The duplicate was not merely redundant. It lacked resume, so a rate limit
partway through discarded every location that had already succeeded -- against
an hourly quota, on fan-outs that reach into the hundreds. It reported no
progress. And it read its own module-global concurrency cap, so a user setting
``API_USGS_CONCURRENT`` to be polite to the service found one adapter ignoring
them.

Decision
--------

``dataretrieval.transport.fanout`` owns fan-out execution for every service:
bounded concurrency, per-attempt retry, deterministic failure precedence, sparse
completion tracking, and resume. It names no protocol concept. An adapter
supplies a ``FanOutPlan`` and an ``async def fetch(item) -> (df, response)``.

``FanOutPlan`` is a ``Protocol`` of ``__len__`` and ``__iter__``, generic in the
item type -- a sized, iterable collection of chunk descriptions, and
nothing more. The executor passes each item to the adapter's own ``fetch``
without inspecting it, so the item type is the adapter's business: the OGC
getters yield kwargs dicts, Water Use yields ready ``httpx.Request`` objects.

The standard protocols, rather than bespoke members, are a deliberate choice.
A plan declaring ``total`` and ``iter_chunk_args()`` would be stating ``len``
twice under a private name: the two could then report different counts, and a
test would have to assert they agree. Every adapter whose chunks are
already a list would also need a wrapper class whose only job is renaming
``len``.

With the standard names a plain ``list`` is a plan, which is exactly what Water
Use passes. ``ChunkPlan`` keeps ``total`` and ``iter_chunk_args`` as its own
vocabulary and defines the dunders to delegate to them, so the two cannot
disagree.

The protocol is structural rather than nominal for the original reason:
``ChunkPlan`` derives chunks from a byte budget over multi-value axes,
a list of requests derives nothing, and so there is no shared implementation an
abstract base could hold.

The identity of the query as a whole is *not* part of the plan. ``canonical_url``
is a value stamped on the combined response, not a property of how the work
divides, so it is an argument to ``FanOut``. ``ChunkPlan`` computes one while
planning and the OGC call site passes it through; Water Use passes its first
location's URL, since the service has no request expressing "all of these".

``dataretrieval.ogc`` keeps chunk planning: the byte budget, the axis
partitioning, the CQL2 filter split, the ``parallel_chunks`` dial. Those are
division, and division is protocol-specific.

The interruption taxonomy moves to ``dataretrieval.interruptions``, a top-level
leaf, for the reason ADR 0006 gives for ``combining``, ``progress``, and
``credentials``: adapters need it whether or not they went through transport,
and an exception taxonomy is not HTTP execution policy. Its base class is
renamed ``FanOutInterrupted`` because the failure interrupts a query's
*execution* rather than its division -- both Water Data and Water Use chunk,
for different reasons, but what fails is the fan-out. ``ChunkInterrupted`` is retained as a permanent alias of the same
class object -- not a shim scheduled for deletion -- because it is the name
published in the user guide and caught in user code. The subclasses
(``QuotaExhausted``, ``ServiceInterrupted``) were already neutral and are
unchanged.

Concurrency is one general setting with per-service defaults.
``API_USGS_CONCURRENT`` applies to every fanned-out call; a service may declare
a different default for when it is unset. The precedence is deliberate: an
explicitly set environment variable outranks a service default, never the
reverse. A service that could override the general setting would make
``API_USGS_CONCURRENT=1`` a lie. Service defaults say "absent instruction, this
service prefers N"; they do not say "this service knows better than you".

Consequences
------------

- Water Use gains resume, progress reporting, and the shared concurrency
  setting, and sheds roughly 75 lines of duplicated orchestration.
- One implementation of failure precedence, so cancellation-beats-error and
  deterministic failure ordering cannot drift between services.
- **Breaking:** a Water Use fan-out interrupted by a 5xx, 429, or recoverable
  connection failure now raises ``ServiceInterrupted`` / ``QuotaExhausted``
  rather than ``ServiceUnavailable`` / ``RateLimited`` / ``NetworkError``. All
  remain ``DataRetrievalError``, so broad handlers are unaffected, but a narrow
  handler around a Water Use call must widen. This is convergence, not novelty
  -- it is what the OGC getters have always done -- and it is what makes the
  failure resumable. Deterministic connection failures remain ``NetworkError``.
- **Breaking:** ``wateruse.MAX_CONCURRENT_REQUESTS`` is removed in favor of
  ``API_USGS_CONCURRENT`` and ``wateruse.DEFAULT_CONCURRENT_REQUESTS``.
- Resume re-issues a failed location's entire page walk, so pages fetched before
  the failure are fetched again. This already applied to OGC -- a partial walk
  never enters the completion map -- and is a cost, not a correctness problem.
- Water Use frames carry ``huc12_id``, not ``id``, so ``_combine_chunk_frames``
  concatenates them without deduplicating. Correct, because locations partition
  by construction, but the executor's dedup safety net does not apply there.
- ``transport`` is no longer purely leaf-shaped: ``fanout`` is a composite that
  drives retry, pagination-borrowed clients, and combining. It remains HTTP
  execution policy, which is the test the package applies.

Compliance
----------

``tests/architecture_test.py`` asserts three things. That ``nwdc``
(named ``wateruse`` when this decision was taken) contains no ``asyncio.gather``, ``Semaphore``, or ``TaskGroup``, so the
duplication cannot return. That both plan types are sized and *repeatably*
iterable -- resume keys completed work by position, so a generator mistaken for
a collection would re-issue the wrong chunks. And that an interruption
taxonomy does not reappear inside ``transport``.

Adapter tests cover Water Use resume re-issuing only unfinished locations,
progress ticks, and the concurrency precedence rule.

Conformance itself is left to the type checker rather than asserted at runtime:
with the protocol reduced to ``__len__`` and ``__iter__``, a missing member is a
``mypy`` error at the call site, not an ``AttributeError`` discovered mid-fan-out.
