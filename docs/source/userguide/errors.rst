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

Connection-level failures (timeouts, DNS, refused connections) are wrapped as
:class:`~dataretrieval.exceptions.NetworkError`, so the clause above covers them
too -- you never have to catch an ``httpx`` exception. A *no-data* result is **not** an
error: the modern getters return an empty ``DataFrame`` when nothing matches, so
check ``df.empty`` rather than catching anything.

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

``.retryable`` and ``.retry_after`` make a backoff loop type-agnostic -- it
covers rate limits (429), server errors (5xx), and connection failures alike,
honoring the server's ``Retry-After`` hint when present:

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

Resume a large Water Data request
=================================

The Water Data getters transparently split an over-large request into chunks.
When a transient failure interrupts one mid-stream, the work already completed
is preserved: catch ``ChunkInterrupted`` and call ``exc.call.resume()`` once the
condition clears -- only the unfinished sub-requests are re-issued.

.. code-block:: python

    import time
    from dataretrieval import ChunkInterrupted
    from dataretrieval.waterdata import get_daily

    try:
        df, md = get_daily(monitoring_location_id=long_list_of_sites)
    except ChunkInterrupted as exc:
        while True:
            time.sleep(exc.retry_after or 5 * 60)
            try:
                df, md = exc.call.resume()
                break
            except ChunkInterrupted as again:
                exc = again

The full taxonomy
=================

See :doc:`/reference/exceptions` for the complete class tree and per-type
details.
