ADR 0002: Keep synchronous public APIs over async internals
===========================================================

Status
------

Accepted

Context
-------

The established public API consists of synchronous functions used heavily in
scripts, notebooks, pandas workflows, and teaching examples. OGC pagination and
chunk fan-out benefit from asynchronous I/O, but exposing only async functions
would be a broad breaking change and would complicate common notebook use.

Calling ``asyncio.run`` directly is also unsafe when a caller already has a
running event loop, as Jupyter commonly does.

Decision
--------

Keep public service getters synchronous. Async-capable implementations may run
inside a short-lived anyio blocking portal and use ``httpx.AsyncClient`` for
pagination and bounded fan-out. Internal async functions are implementation
details, not a second public API promise.

Ambient per-call policy must propagate into the worker context. A resumable OGC
call captures the context needed to rebuild its remaining requests after the
original getter has returned.

Consequences
------------

- Existing scripts and notebooks retain simple blocking call sites.
- Concurrent network waits improve large paginated downloads.
- Each top-level async-backed call pays worker-thread and portal startup cost.
- Cancellation, context propagation, and client ownership need explicit tests.
- A future public async API, if justified, should be additive and share the same
  lower-level contracts rather than duplicate behavior.

Compliance
----------

Tests exercise calls inside an already running event loop, ambient context
capture on resume, cancellation precedence, bounded in-flight work, and shared
client ownership.
