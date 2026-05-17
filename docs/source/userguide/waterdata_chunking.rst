.. _waterdata-chunking-resume:

Chunked Queries and Resuming After Failure
------------------------------------------

The OGC ``waterdata`` getters (``get_daily``, ``get_continuous``,
``get_field_measurements``, and the other multi-value-capable
functions) transparently split requests whose URLs would otherwise
exceed the USGS Water Data API's ~8 KB byte limit. A heavy chained
query — e.g. *"pull every stream site in Ohio, then their daily
discharge for the last week"* — fans out into many sub-requests under
the hood and returns one combined DataFrame.

Long-running chunked calls can fail partway through. Two common
causes:

- **Quota exhaustion.** The API rate-limits each HTTP request
  (including pagination). The chunker monitors the
  ``x-ratelimit-remaining`` header between sub-requests and aborts
  before issuing the next one if the budget drops below the safety
  floor.
- **Transient upstream errors.** A single sub-request can hit a 5xx,
  a network blip, or a mid-pagination 429.

Both cases raise :class:`~dataretrieval.waterdata.chunking.PartialResult`
(the quota case raises its subclass
:class:`~dataretrieval.waterdata.chunking.QuotaExhausted`). The
exception carries the combined partial DataFrame and a
:class:`~dataretrieval.waterdata.chunking.ChunkManifest` that records
how many sub-requests of the cartesian-product plan completed.

The same getter accepts the partial metadata back via a
``resume_from=`` kwarg. The chunker validates that the freshly-planned
chunk layout matches the saved manifest, then issues only the
outstanding sub-requests.

The Resume Pattern
******************

The canonical idiom: a loop that retries on ``PartialResult``,
accumulates each call's partial DataFrame, and threads the latest
metadata back into the next attempt as ``resume_from=``. The USGS API
rate-limit window is one hour, so a total retry deadline of one hour
is a sensible ceiling — anything longer means the failure is
structural, not transient, and the loop should surface the error
rather than spin forever.

.. code:: python

    import time
    import pandas as pd
    from dataretrieval import waterdata
    from dataretrieval.waterdata import PartialResult

    sites_df, _ = waterdata.get_monitoring_locations(
        state_name="Ohio",
        site_type="Stream",
    )
    sites = sites_df["monitoring_location_id"].tolist()

    deadline = time.monotonic() + 3600  # one-hour cap
    partials = []
    md = None         # carries the latest chunk_manifest between attempts
    attempt = 0

    while True:
        try:
            df, md = waterdata.get_daily(
                monitoring_location_id=sites,
                parameter_code="00060",
                time="P7D",
                resume_from=md,   # None on the first attempt
            )
            break  # full result fetched
        except PartialResult as exc:
            partials.append(exc.partial_frame)
            md = exc.partial_metadata
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Could not complete chunked query within one hour "
                    f"({md.chunk_manifest.completed}/"
                    f"{md.chunk_manifest.total} chunks done)."
                ) from exc
            attempt += 1
            # Exponential backoff capped at 10 minutes. Quota-reset
            # failures benefit from a longer wait; transient transport
            # errors clear quickly. The outer deadline still bounds total
            # wait time at one hour.
            time.sleep(min(60 * 2 ** (attempt - 1), 600))

    # Each partial frame plus the final ``df`` is disjoint, so a single
    # ``concat`` reconstructs what a successful one-shot call would have
    # returned.
    full = pd.concat([*partials, df], ignore_index=True)

How Resume Validates the Plan
*****************************

``ChunkManifest`` pins the *normalized cartesian-product plan*, not
just the input kwargs. If a caller changes their inputs between the
original failure and the retry — even in ways that look equivalent —
the freshly-computed plan would differ from the saved one, and
silently re-fetching would interleave data from two incompatible
queries. The chunker raises ``ValueError`` instead, with one of four
explicit messages:

- ``"resume_from has no chunk_manifest"`` — the metadata is from a
  call that wasn't chunked (or from a different source entirely).
- ``"do not produce a chunk plan"`` — the current args fit in one
  round-trip, so there is no plan to skip against.
- ``"manifest does not match the current chunk plan"`` — the input
  list changed between calls.
- ``"already complete"`` — the saved manifest is fully consumed;
  drop ``resume_from``.

Inspecting the Manifest on Success
**********************************

The manifest is also attached to ``BaseMetadata.chunk_manifest`` on
successful chunked calls, so callers can log fan-out information
without catching anything:

.. code:: python

    df, md = waterdata.get_daily(
        monitoring_location_id=sites,
        parameter_code="00060",
        time="P7D",
    )
    if md.chunk_manifest is not None:
        m = md.chunk_manifest
        logger.info("query fanned out across %d sub-requests", m.total)

For calls that did not need chunking, ``md.chunk_manifest`` is
``None``.
