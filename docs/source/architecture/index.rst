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
    Modern USGS Water Data API facade. ``waterdata.api`` is a logic-free
    compatibility facade over collection-family modules: ``time_series``,
    ``metadata``, ``measurements``, ``reference``, ``samples``, and ``cql``.
    Focused modules own ratings, nearest-value selection, statistics execution,
    shared service policy, and type vocabularies. Internal modules import
    protocol helpers from their canonical OGC modules rather than re-exporting
    them through Water Data utilities.

``dataretrieval.ngwmn``
    NGWMN facade. Its only OGC dependency is the public OGC facade, which it
    configures with an NGWMN-specific base URL, output identifiers, state
    translation, and :class:`OgcDialect`.

``dataretrieval.nwdc``
    NWDC Water Use facade. Builds CSV requests, follows ``Link`` headers, and
    uses service-neutral transport for bounded fan-out, retry, pagination, response
    aggregation, and synchronous dispatch. It does not depend on OGC modules.

``dataretrieval.wqp``, ``dataretrieval.nldi``, and ``dataretrieval.streamstats``
    Service-specific adapters over shared synchronous HTTP and bounded retry
    policy. Their return types intentionally reflect their upstream data models.

``dataretrieval.nwis``
    Deprecated legacy NWIS facade, scheduled for removal on or after
    2027-05-06. Modern code must not depend on it.

Shared components
^^^^^^^^^^^^^^^^^

``dataretrieval.settings``
    Settings leaf, built on ``pydantic-settings`` (ADR 0012): it imports no
    adapter and nothing first-party but the exceptions taxonomy, so any module
    may depend on it without an import cycle. It resolves scoped overrides,
    environment variables, a TOML file with optional profiles, and built-in
    defaults in
    that order. Service and protocol modules may depend on it; it must not
    depend back on them. Scoped overrides use ``ContextVar`` so concurrent
    threads and asyncio tasks can carry distinct credentials.

``dataretrieval.ogc``
    Protocol subsystem for Water Data and NGWMN. A small facade
    (``__init__.py``) exposes the service-adapter seam: ``OgcDialect``,
    ``prepare_request_args``, ``get_ogc_data``, and ``fetch_ogc_request``.
    Internally, ``policy`` defines the dialect type, control validation, and
    endpoint constants; ``requests`` owns argument normalization and HTTP
    request construction, taking the target ``base_url`` and ``dialect`` as
    explicit parameters; ``schema`` executes queryables/schema requests;
    ``engine`` supplies OGC cursor and response strategies to transport
    pagination and binds per-call state into its fetch closures;
    ``planning`` determines chunk boundaries; ``chunking`` connects those plans
    to the shared fan-out executor and retains compatibility aliases;
    ``interruptions`` and ``retry`` re-export their moved compatibility
    surfaces; and ``shaping``, ``dates``, ``filters``, and ``errors`` isolate
    their named protocol concerns. The full runtime OGC graph, including the
    facade, is acyclic -- enforced by the package-wide fitness function in
    ``tests/architecture_test.py``.

``dataretrieval.transport``
    Internal service-neutral execution layer. Owns guarded client lifecycle and
    timeouts, host-scoped authentication, cursor pagination, bounded retry,
    response aggregation, fan-out execution, progress integration, and
    sync-over-async dispatch. ``fanout`` drives an injected plan and fetch
    callback, owning bounded concurrency, deterministic failure precedence,
    sparse completion state, resume, and the progress line. It is also the one
    entry point from synchronous getter code into the async internals: a query
    with nothing to divide runs as a one-item fan-out rather than crossing a
    separate bridge. Internally, ``liveness`` is a stdlib-only leaf recording
    when data last arrived, so the page loop that observes progress and the
    retry loop that acts on it depend on ``liveness`` rather than on each other.
    Transport imports no service adapter or OGC protocol module, and is not
    exposed as a public framework API.

``dataretrieval.interruptions``
    Shared resumable fan-out failure contract. It owns
    ``FanOutInterrupted`` and its subclasses; ``ChunkInterrupted`` remains a
    permanent alias for compatibility. The module is outside transport because
    adapters catch these errors independently of how they execute requests.

``dataretrieval.exceptions``
    Stable error-policy leaf. It has no runtime third-party dependency, and
    every service can import it without creating an infrastructure cycle.

``dataretrieval._response_metadata``
    ``BaseMetadata``, the second half of every getter's ``(DataFrame,
    metadata)`` return contract. A dependency-free leaf: nearly every service
    module needs this class, and while it lived in ``utils`` beside the legacy
    query machinery, importing it pulled that module's whole HTTP stack in
    transitively. The implementation module is private; the established public
    class path remains ``dataretrieval.utils.BaseMetadata``.

``dataretrieval._ambient``
    Dependency-free implementation of scoped context values used by concurrent
    OGC internals. ``dataretrieval.utils.Ambient`` remains the stable public
    import and resolves to this same class.

``dataretrieval.utils``
    Data-shaping helpers, plus compatibility imports for names that
    historically lived here (including ``Ambient``, ``BaseMetadata``, ``query``
    and ``to_str``, so their original import paths keep working). OGC does not
    depend on this legacy module; by default, do not add new service-specific
    behavior there.

``dataretrieval._querying``
    The one-shot HTTP query path the single-request adapters (``nwis``,
    ``wqp``, ``nldi``, ``streamstats``, ``nwdc``) use: compose the URL, send
    it, map the status, retry a transient. It left ``utils`` because the two
    halves shared only a filename -- this one depends on ``exceptions`` and
    ``transport``, the shaping half on ``codes`` and pandas, and no caller
    wanted both. The implementation module is private; the established public
    function paths remain ``dataretrieval.utils.query`` and
    ``dataretrieval.utils.to_str``.

``dataretrieval.codes`` and ``dataretrieval.rdb``
    State/time-zone code conversion and RDB parsing leaves.

The intended direction is::

    public facade -> service/protocol adapter -> service-neutral transport
                                             -> stable policy/infrastructure
                                             -> third-party library / network

Dependencies must not point from shared infrastructure back to a public service
adapter. ``.importlinter`` declares this as a layer stack and ``lint-imports``
checks it over the transitive import graph, so a violation routed through an
intermediary fails as surely as a direct one. The stack is exhaustive: a new
top-level module fails the contract until it is placed, so where a module
belongs is decided when it is added rather than inferred later.

``tests/architecture_test.py`` complements those contracts without repeating
their direction rules. It covers which symbols cross a seam, declared
``__all__`` surfaces, the AST shape of a facade, imports that must exist rather
than be forbidden, and full-graph cycle detection (see ADR 0003).

Interface view
--------------

The primary API is a collection of synchronous functions grouped by data
portal. Most tabular download functions return ``(DataFrame, metadata)``.
NLDI and StreamStats retain service-specific geospatial or response-object
contracts; consistency alone is not sufficient reason for a breaking change.

Failed requests derive from ``dataretrieval.DataRetrievalError``. Callers can
inspect ``status_code``, ``retry_after``, and ``retryable`` without knowing the
concrete subtype. A fanned-out call -- an over-large OGC request, or a Water Use
query naming several locations -- may raise ``FanOutInterrupted`` subclasses
(formerly, and still aliased as, ``ChunkInterrupted``) carrying a resumable call
handle and completed partial state.

Package/module exports and documentation define the public surface.
Underscore-prefixed symbols are implementation details even where existing
internal adapters currently import them; those imports are known variances, not
new extension points.

Service return contracts
------------------------

The library preserves meaningful upstream differences rather than forcing every
service into one return shape:

- Water Data, NGWMN, and Water Use tabular getters return ``(DataFrame,
  BaseMetadata)``. Geometry-bearing Water Data and NGWMN results may use a
  ``GeoDataFrame`` in the first position when geopandas is installed.
  ``BaseMetadata`` carries request URL, elapsed query time, response headers,
  and comments where the upstream format provides them.
- WQP getters return ``(DataFrame, WQP_Metadata)``; the service-specific
  metadata extends ``BaseMetadata`` with WQP query parameters and site lookup.
- ``waterdata.get_ratings`` returns a mapping of feature IDs to parsed rating
  ``DataFrame`` objects by default, or the raw STAC feature list when downloads
  are disabled.
- NLDI navigation functions return ``GeoDataFrame`` objects directly, or raw
  GeoJSON-like dictionaries when ``as_json=True``; they do not add a metadata
  tuple.
- StreamStats functions return raw ``httpx.Response`` objects or the
  service-specific ``Watershed`` domain object, depending on the requested
  format.
- Deprecated NWIS functions retain their established DataFrame and legacy
  metadata contracts through the published deprecation window.

Changing one of these shapes is a public compatibility change and requires the
project's deprecation process; consistency alone is not sufficient reason.

Interaction view
----------------

A typical OGC-backed call follows this sequence::

    synchronous public getter
        -> normalize and validate arguments
        -> select OGC dialect and build a chunk plan
        -> enter a short-lived anyio blocking portal
        -> execute chunks through a shared httpx.AsyncClient
        -> paginate each chunk
        -> retry bounded transient failures
        -> combine and deduplicate pages/chunks
        -> shape columns and types
        -> return DataFrame and BaseMetadata

A transient failure after some chunks complete raises a resumable
interruption. The retained ``FanOut`` (available as ``ChunkedCall`` on the OGC
compatibility path) reissues only missing work and applies the same finalization
path when resumed. Cancellation and non-transient programming errors take
precedence over retry/resume wrapping.

Non-OGC services use the same transport policy only where their protocols have
matching semantics. Retry, cursor pagination, and fan-out remain explicit
adapter choices. Chunk planning remains OGC-specific; resumable fan-out is also
used by Water Use because the NWDC accepts only one location per request.

Resource and configuration view
-------------------------------

Every setting resolves per key through an active ``configure()`` block, its
environment variable when one exists, the adapter's own table and the top-level
values in ``~/.dataretrieval/config.toml``, then its built-in default.
``configure()`` takes settings profiles -- a package-wide ``Settings``
and at most one per adapter -- and each adapter's class is defined in the module
that reads those settings, so ``settings`` stays a leaf holding only the
adapter roster. ``show_settings()`` reports the effective source while
redacting credentials.

The settings themselves -- names, defaults, environment variables, and the
config-file format -- are catalogued once in the
:doc:`settings guide </userguide/settings>`. What matters
architecturally is the behavior around them:

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
    time: without this bound, four retries of a request that times out after a
    minute add up to four silent minutes. Progress restarts the budget — a page
    received, or a queued chunk acquiring its concurrency slot. Neither a
    slow but productive download nor the tail of a wide fan-out is cut short,
    and an attempt already in flight is never interrupted. This bound never
    withholds the first retry, so one slow attempt cannot disable retry by
    itself; after that, the budget decides whether to continue. A dead
    connection therefore costs about two read timeouts rather than five
    attempts' worth.

``API_USGS_PROGRESS``
    Controls best-effort progress display. Reporting failures must never change
    retrieval results.
* The API token is attached only to requests for ``api.waterdata.usgs.gov``.
  Shared synchronous and asynchronous clients re-check every redirected request
  and strip the token before following a link to any other host, including
  external rating assets.
* A semaphore, not connection-pool waiting, is the execution throttle for
  sub-request concurrency.
* Retry backoff is exponential with full jitter and honors bounded
  ``Retry-After`` values.
* Progress reporting is best-effort: a reporting failure must never change
  retrieval results.
* ``dataretrieval.settings`` imports no adapter and nothing first-party but the
  exceptions taxonomy, so any module may depend on it without an import cycle.

``dataretrieval.transport`` centralizes HTTP timeout, redirect, and
authentication policy. OGC chunk fan-out and Water Use location
fan-out retain separate explicit concurrency caps because their upstream costs
and request shapes differ.

Known architectural debt
------------------------

This view records categories and representative locations of debt.
``.importlinter`` is authoritative for exact current dependency allowlists.

- ``ogc/engine.py`` retains a compatibility pagination wrapper alongside OGC
  orchestration. The sync-dispatch wrapper is gone: every retrieval path now
  enters through ``transport.fanout.FanOut``.
- ``utils.py`` combines shaping with compatibility imports for metadata,
  ambient configuration, transport, and the query path.
- ``waterdata/utils.py`` combines endpoint constants, argument normalization,
  and the OGC engine wrappers.

These are documented so guardrails distinguish accepted current dependencies
from new erosion. They should be removed through small, test-protected changes,
not a rewrite.

Change process
--------------

Architecturally significant changes should:

#. add or supersede an ADR;
#. identify affected characteristics and trade-offs;
#. add or update an executable fitness function, and the matching contract in
   ``.importlinter`` when the change moves a dependency boundary;
#. preserve public contracts or provide a deprecation path; and
#. update this view when component responsibilities or dependency rules change.

.. toctree::
   :maxdepth: 1

   decisions/index
