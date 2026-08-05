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
    Modern USGS Water Data API facade. Its generic adapter uses the four-symbol
    OGC facade; internal Water Data modules import protocol helpers from their
    canonical OGC modules. Water-Data-specific utilities own service policy and
    wrappers without re-exporting private OGC helpers. Statistics, ratings, and
    nearest-value operations live in separate modules.

``dataretrieval.ngwmn``
    NGWMN facade. Its only OGC dependency is the public OGC facade, which it
    configures with an NGWMN-specific base URL, output identifiers, state
    translation, and :class:`OgcDialect`.

``dataretrieval.wateruse``
    NWDC Water Use facade. Builds CSV requests, follows ``Link`` headers, and
    uses API-neutral transport for bounded fan-out, retry, pagination, response
    aggregation, and synchronous dispatch. It does not depend on OGC modules.

``dataretrieval.wqp``, ``dataretrieval.nldi``, and ``dataretrieval.streamstats``
    Service-specific adapters over shared synchronous HTTP and bounded retry
    policy. Their return types intentionally reflect their upstream data models.

``dataretrieval.nwis``
    Deprecated legacy NWIS facade, scheduled for removal on or after
    2027-05-06. Modern code must not depend on it.

Shared components
^^^^^^^^^^^^^^^^^

``dataretrieval.ogc``
    Protocol subsystem for Water Data and NGWMN. A small facade
    (``__init__.py``) exposes the service-adapter seam: ``OgcDialect``,
    ``prepare_request_args``, ``get_ogc_data``, and ``fetch_ogc_request``.
    Internally, ``policy`` defines the dialect type and endpoint constants
    (depends only on stdlib); ``requests`` owns request construction, argument
    normalization, and queryables/schema lookup; ``engine`` supplies OGC cursor
    and response strategies to transport pagination; ``planning`` determines
    chunk boundaries; ``chunking`` executes plans and retains resumable state;
    ``interruptions`` defines the resumable failure contract; ``retry``
    classifies failures into OGC interruption types; and ``shaping``, ``dates``,
    ``filters``, and ``errors`` isolate their named protocol concerns. The full
    runtime OGC graph, including the facade, is acyclic — enforced by
    ``tests/architecture_test.py``.

``dataretrieval.transport``
    Internal API-neutral execution layer. Owns guarded client lifecycle and
    timeouts, host-scoped authentication, cursor pagination, bounded retry,
    response aggregation, progress, and sync-over-async dispatch. Internally,
    ``liveness`` is a stdlib-only leaf recording when data last arrived, so the
    page loop that observes progress and the retry loop that acts on it both
    depend on it rather than on each other. It imports no service adapter or OGC
    protocol module, and it is not exposed as a public framework API.

``dataretrieval.exceptions``
    Stable error-policy leaf. It has no runtime third-party dependency and may
    be imported by every service without creating an infrastructure cycle.

``dataretrieval.utils``
    Shared metadata, data-shaping helpers, ambient context support, legacy
    request composition, and compatibility imports for transport names that
    historically lived here. New service-specific behavior should not be added
    there by default.

``dataretrieval.codes`` and ``dataretrieval.rdb``
    State/time-zone code conversion and RDB parsing leaves.

The intended direction is::

    public facade -> service/protocol adapter -> API-neutral transport
                                             -> stable policy/infrastructure
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

Non-OGC services use the same transport policy only where their protocols have
matching semantics. Retry and cursor pagination remain explicit adapter choices;
chunk planning and resumable interruptions remain OGC capabilities rather than
invented features of upstream APIs that do not provide them.

Resource and configuration view
-------------------------------

``API_USGS_PAT``
    Optional USGS API token. It is attached only to requests for
    ``api.waterdata.usgs.gov``. Shared synchronous and asynchronous clients
    re-check every redirected request and strip the token before following a
    link to any other host, including external rating assets.

``API_USGS_CONCURRENT``
    OGC subrequest concurrency cap; defaults to 32, ``1`` is sequential, and
    ``unbounded`` removes the explicit cap. A semaphore, not pool waiting, is
    the execution throttle.

``API_USGS_RETRIES``
    Number of retries after the first attempt on supported active request paths;
    defaults to four. Backoff is exponential with full jitter and honors bounded
    ``Retry-After`` values. Only failures a later attempt could survive are
    re-sent: 429 and gateway 5xx, not a 500 rejecting the query itself, and not a
    transport failure that is settled before the request leaves (unresolvable
    host, unsupported scheme). Deprecated NWIS compatibility paths do not opt in.

``API_USGS_STALL_TIMEOUT``
    Seconds a call may go without receiving any data before retrying stops and
    the failure surfaces; defaults to 60, and ``0`` disables the bound. It
    complements ``API_USGS_RETRIES``, which caps attempts rather than elapsed
    time: without it, four retries of a request that times out after a minute is
    four silent minutes. Progress restarts the budget — a page received, or a
    queued sub-request acquiring its concurrency slot — so neither a slow but
    productive download nor the tail of a wide fan-out is cut short, and an
    attempt already in flight is never interrupted. The first retry is never
    withheld by this bound, so one slow attempt cannot disable retry by itself;
    the budget decides whether to continue after that. A dead connection
    therefore costs about two read timeouts rather than five attempts' worth.

``API_USGS_PROGRESS``
    Controls best-effort progress display. Reporting failures must never change
    retrieval results.

HTTP timeout, redirect, and authentication policy is centralized in
``dataretrieval.transport``. OGC subrequest fan-out and Water Use location
fan-out retain separate explicit concurrency caps because their upstream costs
and request shapes differ.

Known architectural debt
------------------------

This view records categories and representative locations of debt. The fitness
functions in ``tests/architecture_test.py`` are authoritative for exact current
dependency allowlists.

- ``waterdata/api.py`` and ``ogc/engine.py`` contain multiple reasons to change.
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
