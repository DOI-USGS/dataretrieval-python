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
-- only the unfinished sub-requests are re-issued.

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

Chunk a large request more finely
=================================

By default the getters split an over-large request only as much as the
server's ~8 KB URL limit forces -- the fewest sub-requests. Because each
sub-request paginates, splitting a large result further costs little or no
extra quota *as long as each sub-request still spans many pages*. (Ten states
pulled as one request then page nearly as many times as ten per-state requests
would; a split that leaves each sub-request only a page or two adds its partial
final page.) So if you *know* your pull is large, ask for a finer split with
``parallel_chunks(n)``: you trade roughly the same pages for more, smaller
sub-requests, which gives smoother progress, more even concurrency, and a
smaller unit of retry/resume. ``parallel_chunks`` is a scoped ``with`` block, so
an aggressive setting can't leak into unrelated calls and accidentally spend
quota:

.. code-block:: python

    from dataretrieval import waterdata

    with waterdata.parallel_chunks(32):
        df, md = waterdata.get_daily(
            monitoring_location_id=many_sites, parameter_code="00060"
        )

``n`` is a positive integer (e.g. ``2``, ``8``, ``32``) -- the number of
sub-requests to fan the call out into; a non-integer or non-positive value
raises ``ValueError`` at the ``with``. ``n`` caps the *total* sub-request count
across every multi-value argument combined (not per argument), bounded below by
what the byte limit already forces and above by how many values there are to
split. Several multi-value arguments therefore can't multiply past it, and
``n=1`` asks for no extra fan-out. Each sub-request costs a request against your
hourly rate limit. How many run *at once* is capped separately by
``API_USGS_CONCURRENT`` (default 32), so an ``n`` beyond that adds quota without
adding parallelism -- the useful range is roughly ``2`` up to
``API_USGS_CONCURRENT``. There is no "off" level: don't enter the block
unless you already expect a large, multi-page result -- on a query that would
have fit in a single page, extra chunks only burn quota.

The full taxonomy
=================

See :doc:`/reference/exceptions` for the complete class tree and per-type
details.
