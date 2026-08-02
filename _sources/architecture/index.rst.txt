Architecture
============

Purpose and scope
-----------------

``dataretrieval`` is a Python client library for discovering and retrieving
hydrologic data from several independently operated USGS and partner services.
It is a modular monolith: one installable distribution with public service
facades, service- and protocol-specific adapters, and shared infrastructure.
The architecture favors incremental evolution and stable user-facing functions
over a framework that forces unlike upstream APIs into one shape.

This document records the current structure, the rules contributors should
preserve, and known variances that later changes may remove. Rationale for
architecturally significant choices is recorded in
:doc:`Architecture Decision Records <decisions/index>`.

Architecture characteristics
----------------------------

The characteristics below are prioritized in descending order when trade-offs
are necessary:

#. **Installability and artifact integrity.** A built wheel must contain every
   supported package and work outside a source checkout.
#. **Public API compatibility.** Established imports, function signatures,
   return shapes, metadata, warnings, and exception types should remain stable;
   intentional changes follow the project's deprecation policy.
#. **Correctness and data integrity.** Pagination and fan-out must not silently
   return truncated or duplicated data after a failure.
#. **Resilience.** Retry, rate-limit, interruption, and resume behavior must be
   explicit and bounded. Capabilities may differ where upstream protocols do.
#. **Maintainability.** Modules should have cohesive responsibilities and
   dependencies should point toward stable policies rather than service details.
#. **Bounded performance and resource use.** Concurrency, connection pools,
   retries, and page accumulation require explicit limits.
#. **Observability.** Logs, metadata, progress, and typed failures should make
   remote-service behavior diagnosable without changing results.
#. **Portability.** Supported Python and operating-system combinations must work;
   optional geospatial dependencies must remain isolated.

Context view
------------

The package sits between Python callers and remote hydrologic services::

    Python user / notebook / batch process
                    |
                    v
        dataretrieval public service modules
                    |
          HTTP(S), JSON, CSV, RDB, GeoJSON
                    |
                    v
    USGS Water Data APIs, NGWMN, NWDC Water Use,
    Water Quality Portal, NLDI, StreamStats, legacy NWIS

The remote services own their schemas, paging mechanisms, rate limits, and
availability. The library adapts those differences to documented Python
contracts but does not hide meaningful service-specific behavior.

Composition and dependency view
-------------------------------

Public service facades
^^^^^^^^^^^^^^^^^^^^^^

``dataretrieval.waterdata``
    Modern USGS Water Data API facade. Typed getters delegate to the shared OGC
    subsystem, with separate modules for statistics, ratings, and nearest-value
    operations.

``dataretrieval.ngwmn``
    NGWMN facade. Reuses the OGC subsystem with an NGWMN-specific base URL,
    output identifiers, state translation, and :class:`OgcDialect`.

``dataretrieval.wateruse``
    NWDC Water Use facade. Builds CSV requests and follows ``Link`` headers.
    It currently reuses generic pagination and response-combining helpers from
    private OGC modules; this is an explicitly recorded variance.

``dataretrieval.wqp``, ``dataretrieval.nldi``, and ``dataretrieval.streamstats``
    Service-specific adapters over the synchronous request infrastructure in
    ``dataretrieval.utils``. Their return types intentionally reflect their
    upstream data models.

``dataretrieval.nwis``
    Deprecated legacy NWIS facade, scheduled for removal on or after
    2027-05-06. Modern code must not depend on it.

Shared components
^^^^^^^^^^^^^^^^^

``dataretrieval.ogc``
    Protocol subsystem for Water Data and NGWMN. ``engine`` orchestrates
    requests and pagination; ``planning`` determines chunk boundaries;
    ``chunking`` executes plans and retains resumable state; ``retry`` owns the
    bounded retry policy; ``combining`` assembles results; and ``shaping``,
    ``dates``, ``filters``, ``errors``, and ``progress`` isolate their named
    concerns.

``dataretrieval.exceptions``
    Stable error-policy leaf. It has no runtime third-party dependency and may
    be imported by every service without creating an infrastructure cycle.

``dataretrieval.utils``
    Shared metadata, data-shaping helpers, ambient context support, and the
    legacy synchronous request path. Its broad responsibility is known debt;
    new service-specific behavior should not be added there by default.

``dataretrieval.codes`` and ``dataretrieval.rdb``
    State/time-zone code conversion and RDB parsing leaves.

The intended direction is::

    public facade -> service/protocol adapter -> shared policy/infrastructure
                                             -> third-party library / network

Dependencies must not point from shared infrastructure back to a public service
adapter. The executable checks in ``tests/architecture_test.py`` enforce the
rules that hold today and explicitly list temporary variances.

Interface view
--------------

The primary API is a collection of synchronous functions grouped by data
portal. Most tabular download functions return ``(DataFrame, metadata)``.
NLDI and StreamStats retain service-specific geospatial or response-object
contracts; consistency alone is not sufficient reason for a breaking change.

Failed requests derive from ``dataretrieval.DataRetrievalError``. Callers can
inspect ``status_code``, ``retry_after``, and ``retryable`` without knowing the
concrete subtype. OGC calls may raise ``ChunkInterrupted`` subclasses carrying a
resumable call handle and completed partial state.

The public surface is defined by package/module exports and documentation.
Underscore-prefixed symbols are implementation details even where existing
internal adapters currently import them; those imports are known variances, not
new extension points.

Interaction view
----------------

A typical OGC-backed call follows this sequence::

    synchronous public getter
        -> normalize and validate arguments
        -> select OGC dialect and build a chunk plan
        -> enter a short-lived anyio blocking portal
        -> execute subrequests through a shared httpx.AsyncClient
        -> paginate each subrequest
        -> retry bounded transient failures
        -> combine and deduplicate pages/chunks
        -> shape columns and types
        -> return DataFrame and BaseMetadata

A transient failure after some chunks complete raises a resumable interruption.
The retained ``ChunkedCall`` reissues only missing chunks and applies the same
finalization path when resumed. Cancellation and non-transient programming
errors take precedence over retry/resume wrapping.

Non-OGC services use simpler request paths where their protocols do not provide
the same paging or resume semantics. Later transport consolidation must preserve
those public contracts and must not invent unsupported upstream capabilities.

Resource and configuration view
-------------------------------

``API_USGS_PAT``
    Optional USGS API token. Authentication must be scoped to appropriate USGS
    hosts and never forwarded to WQP or another unrelated endpoint.

``API_USGS_CONCURRENT``
    OGC subrequest concurrency cap; defaults to 32, ``1`` is sequential, and
    ``unbounded`` removes the explicit cap. A semaphore, not pool waiting, is
    the execution throttle.

``API_USGS_RETRIES``
    Number of OGC retries after the first attempt; defaults to four. Backoff is
    exponential with full jitter and honors bounded ``Retry-After`` values.

``API_USGS_PROGRESS``
    Controls best-effort progress display. Reporting failures must never change
    retrieval results.

HTTP timeouts and connection limits are centralized for existing paths.
``wateruse`` currently has its own smaller fan-out cap. These differences must
remain visible until a shared transport policy replaces them deliberately.

Known architectural debt
------------------------

This view records categories and representative locations of debt. The fitness
functions in ``tests/architecture_test.py`` are authoritative for exact current
dependency allowlists.

- ``waterdata.utils`` re-exports many underscore-prefixed OGC helpers.
- ``wateruse`` depends on private generic helpers located under ``ogc`` even
  though NWDC is not an OGC service.
- ``ogc.shaping`` uses lazy engine imports for empty-result schema lookup and a
  default dialect, creating a logical cycle.
- ``waterdata/api.py`` and ``ogc/engine.py`` contain multiple reasons to change.
- Active non-OGC services do not yet share OGC's retry/resume capabilities.
- ``utils.py`` combines metadata, shaping, configuration, and transport duties.

These are documented so guardrails distinguish accepted current dependencies
from new erosion. They should be removed through small, test-protected changes,
not a rewrite.

Change process
--------------

Architecturally significant changes should:

#. add or supersede an ADR;
#. identify affected characteristics and trade-offs;
#. add or update an executable fitness function;
#. preserve public contracts or provide a deprecation path; and
#. update this view when component responsibilities or dependency rules change.

.. toctree::
   :maxdepth: 1

   decisions/index
