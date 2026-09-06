ADR 0004: Use typed failures and bounded recovery
=================================================

Status
------

Accepted. The clause assigning resumable partial state to OGC is superseded by
:doc:`0008-fan-out-execution`, which moves fan-out *execution* into transport
and gives every fanned-out service resume. The rest stands.

Amended after acceptance under :doc:`0000-documenting-decisions`; the
``Notes`` section records every clause added or corrected.

Context
-------

Remote hydrologic services fail through HTTP statuses, rate limits, timeouts,
invalid payloads, and mid-pagination interruptions. Returning partial data after
an undetected page failure leaves the caller a truncated result they cannot tell
from a complete one. At the same time, retrying without limits can stall
callers, multiply quota usage, or hide persistent faults.

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

**Warnings carry the advisories that are not failures.** The taxonomy above
covers what stops a call; two things that do not stop one are decided here as
well, because getting either wrong turns a condition that should not stop a call
into one that does.

A fan-out over *independent* items may skip one. Where a query asks for many
items that do not compose into a single answer, an item failing
deterministically is dropped under a warning naming it, and counts as complete
so a resume does not re-attempt it. A transient failure is never skipped: it
retries, and exhausts into a resumable interruption like any other. This is the
deliberate exception to the rule that a failed fan-out raises rather than
returning successful siblings; it applies only where the items are independent,
never to the pages of one query.

An advisory about *upstream data* -- a dataset the service has stopped updating
-- is a ``UserWarning``, never a ``DeprecationWarning``. Downstream projects run
their suites under ``-W error::DeprecationWarning``, and spelling "this data is
stale" as a deprecation makes their build fail over something no code change of
theirs can fix. ``DeprecationWarning`` means *this package's* API is going away.

**A ``Retry-After`` hint is honored as information even when it is not
actionable.** A hint too long to wait for, or expressed as a date, is parsed and
surfaced on the failure rather than discarded, so a caller can see what the
service asked for; the retry policy separately declines to wait beyond its cap.
A date already in the past yields no hint at all rather than a zero-second one,
which would read as "retry immediately" -- the opposite of what the header said.

**Every error must survive a process boundary.** Reconstruction goes through
``__new__`` plus ``__getstate__``/``__setstate__`` rather than ``cls(*args)``,
because these errors carry fields whose values are not the constructor's
arguments. A subclass holding an unpicklable handle -- a client, a task -- must
shed it in ``__getstate__``. Without this a failure raised inside a worker
process is replaced by a pickling error on the way out, losing the diagnosis
exactly when it is hardest to reproduce.

Consequences
------------

- Callers can catch one stable base error and still branch on ``status_code``,
  ``retry_after``, and ``retryable``.
- Mid-pagination failure cannot silently look like a complete dataset.
- Retry can increase latency and request quota, so policy and defaults are part
  of observable behavior.
- Partial OGC state requires serialization and finalization tests.
- Expanding retry to another service requires service-specific idempotency and
  failure-contract tests.

Compliance
----------

Tests cover status-to-type mapping, uniform fields, transport wrapping,
pagination failure, retry exhaustion and jitter bounds, ``Retry-After`` limits,
resume equivalence, partial-state stability, pickling, and cancellation
precedence. The skip policy is covered by
``tests/waterdata_ratings_test.py::test_get_ratings_deterministic_download_failure_warns_and_skips``
and its sibling for a feature with no asset; the warning categories by
``tests/deprecation_test.py``, which asserts ``DataCurrencyWarning`` is not a
subclass of ``DeprecationWarning``; the hint-parsing rules by the
``Retry-After`` date and over-cap cases; and the process boundary by
round-tripping error subclasses through ``pickle``.

Notes
-----

The warning, hint-parsing, and pickling clauses were added after the original
decision. They record, under ADR 0000, rules the code was carrying in prose. The
skip clause records a carve-out that previously read as contradicting this
record and :doc:`0006-service-neutral-transport`; 0006 now points here for it.

The ``Status`` line was also annotated retroactively: this record assigned
resumable partial state to OGC, which :doc:`0008-fan-out-execution` superseded
without noting it here. The supersession is 0008's; only the backlink is new.
