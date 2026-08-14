.. _handling-errors:

===============
Handling errors
===============

Every failed request raises a subclass of
:class:`~dataretrieval.exceptions.DataRetrievalError`, so a single ``except``
clause handles any failure regardless of which service you called:

.. code-block:: python

    import dataretrieval

    try:
        df, md = dataretrieval.waterdata.get_daily(
            monitoring_location_id="USGS-05427718"
        )
    except dataretrieval.DataRetrievalError:
        ...  # any request failure: error status, connection loss, too-large, ...

Connection-level failures (timeouts, DNS, refused connections) remain inside
the package taxonomy, so the clause above covers them -- you never have to catch
an ``httpx`` exception. A deterministic connection failure is a
:class:`~dataretrieval.exceptions.NetworkError`; a recoverable one that exhausts
inline retries during fan-out is a resumable ``ServiceInterrupted``. A *no-data*
result is **not** an error: the modern getters return an empty ``DataFrame`` when
nothing matches, so check ``df.empty`` rather than catching anything.

Branch without knowing the concrete type
=========================================

Every :class:`~dataretrieval.exceptions.DataRetrievalError` exposes three
read-anywhere fields, so you rarely need to import the specific subclasses:

* ``.status_code`` -- the HTTP status, or ``None`` when the failure carried no
  response (a connection error, an over-long URL, ...).
* ``.retry_after`` -- seconds the server asked you to wait (its ``Retry-After``
  header), or ``None``.
* ``.retryable`` -- ``True`` when re-issuing the same request might succeed (a
  429 / 5xx, or a connection failure); ``False`` otherwise.

.. code-block:: python

    except dataretrieval.DataRetrievalError as e:
        if e.status_code == 404:
            ...            # not found
        elif e.retryable:
            ...            # transient -- see the retry recipe below
        else:
            raise

Retry transient failures with backoff
=====================================

``.retryable`` and ``.retry_after`` make a backoff loop type-agnostic: one loop
covers rate limits (429), server errors (5xx), and connection failures alike,
and honors the server's ``Retry-After`` hint when present:

.. code-block:: python

    import time
    import dataretrieval

    for attempt in range(5):
        try:
            df, md = dataretrieval.waterdata.get_continuous(
                monitoring_location_id=sites
            )
            break
        except dataretrieval.DataRetrievalError as e:
            if not e.retryable or attempt == 4:
                raise
            time.sleep(e.retry_after or 2 ** attempt)

Resume an interrupted request
=============================

Some requests become several: the Water Data and NGWMN getters split an
over-large request into chunks, and a Water Use call with several locations
becomes one request per location. When a transient failure interrupts one
mid-stream, the work already completed is preserved: catch
``FanOutInterrupted`` and call ``exc.call.resume()`` once the condition clears
-- only the unfinished chunks are re-issued.

(``ChunkInterrupted`` is the same class under its original name; either works.)

.. code-block:: python

    import time
    from dataretrieval import FanOutInterrupted
    from dataretrieval.waterdata import get_daily

    try:
        df, md = get_daily(monitoring_location_id=long_list_of_sites)
    except FanOutInterrupted as exc:
        while True:
            time.sleep(exc.retry_after or 5 * 60)
            try:
                df, md = exc.call.resume()
                break
            except FanOutInterrupted as again:
                exc = again

The same loop works for ``wateruse.get_wateruse`` with a list of states,
counties, or HUCs.

Large Water Data pulls are paged in parallel
==============================================

Water Data can compute page URLs with ``offset``, so pages of a large result are
fetched in ramped concurrent waves by default. There is no parallel-chunk knob
to enable: the old ``parallel_chunks(n)`` context manager was removed because
it split fitting queries into extra requests and could not help a single-site
query.

At a fixed page ``limit`` the walk overlaps requests it was already going to
make, apart from bounded probes in the final speculative wave. Reducing
``limit`` creates more pages and consumes more quota. The default limit is
50,000, above Water Data's 40,000 offset ceiling, so material speedups require
an explicit smaller limit; for example:

.. code-block:: python

    from dataretrieval import waterdata

    df, md = waterdata.get_daily(
        monitoring_location_id="USGS-01646500",
        parameter_code="00060",
        limit=2000,
    )

``API_USGS_CONCURRENT`` (default 32) bounds both chunk fan-out and the page-wave
width. Set it to ``1`` to use standard cursor pagination sequentially:

.. code-block:: python

    import os

    os.environ["API_USGS_CONCURRENT"] = "1"

``offset`` is a Water Data extension, not part of OGC API - Features. If a
server ignores it, the client detects identical pages before returning rows and
re-runs the query through standard ``next`` links. At Water Data's 40,000-row
offset ceiling, it rewinds one page and cursor-walks the tail so the result has
neither a gap nor duplicate rows.

Byte-driven chunking is unchanged: a multi-value request above the service's
~8 KB request limit is still split for correctness. That division is separate
from page-level parallelism.

The full taxonomy
=================

See :doc:`/reference/exceptions` for the complete class tree and per-type
details.
