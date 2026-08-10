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

Ambient per-call policy (the progress reporter) must propagate into the worker
context. A resumable OGC call binds the state needed to rebuild its remaining
requests -- base URL, dialect, row cap -- into its fetch closures, so a resume
fired after the original getter has returned rebuilds against the values the
call was created with.

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

Tests exercise calls inside an already running event loop, creation-time
binding on resume, cancellation precedence, bounded in-flight work, and shared
client ownership.
