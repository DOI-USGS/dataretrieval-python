ADR 0014: Declare services, endpoints, and dialects as values
=============================================================

Status
------

Proposed. This record is revised as its implementation lands, one pull request
per step; ``Notes`` records each revision. It is accepted when the last step
merges and the record describes what was built.

Context
-------

An adapter tells shared code about its service in three unconnected ways.

**A name.** Six adapters pass a string such as ``adapter="waterdata"`` to
configuration, retry policy, pagination, and fan-out -- 18 call sites in 10
files, each spelling the name again, plus the ``adapter`` attribute of each
``Configuration`` class. The same name is the key of the adapter's table in the
configuration file.

**A URL.** Each adapter resolves its base URL from configuration in its own
way, and Water Data alone has four APIs under one root: OGC API Features, a
STAC catalog (ratings), Samples, and Statistics. Nothing in the code says these
are four endpoints of one service.

**Hard-coded facts.** The shared OGC code lists Water Data's parameter names as
"the OGC date parameters", exempts ``last_modified`` by name from date-only
rendering, passes durations through for every service because one accepts
them, and assumes every service spells an open range start ``..``. ADR 0003
keeps shared code from *importing* a service; it says nothing about *naming*
one, and a string literal is not an import. The services differ:

.. list-table::
   :header-rows: 1

   * - Behavior (observed 2026-10-11)
     - Water Data OGC
     - NGWMN
     - Water Data STAC
   * - Date parameters
     - ``time``, ``begin``, ``end``, ``last_modified``, ``datetime``
     - ``datetime``
     - ``datetime``
   * - A lone duration, ``P7D``
     - accepted
     - HTTP 400
     - HTTP 400
   * - An open start, ``../2016-01-01``
     - accepted
     - HTTP 403 from the firewall
     - HTTP 403 from the firewall
   * - An empty start, ``/2016-01-01``
     - HTTP 400 (500 on ``continuous``)
     - accepted
     - accepted

Pull request #443 formatted interval strings to the one open-start spelling the
OGC code knew, which broke those requests on NGWMN and STAC. Its review missed
it because no rule said which facts belong to a service.

Dates are also read and written in one function. Reading what a caller passed
(a string, a ``date``, a ``Timestamp``, ``None``, local-time rules) is package
policy; writing it as an ISO 8601 interval is protocol grammar. Every date
defect fixed in #440 through #443 was where those two meet.

Decision
--------

Six abstractions, each answering one question:

.. list-table::
   :header-rows: 1

   * - Abstraction
     - Answers
     - Kind
   * - **Service**
     - Which external system: its name, and the URL its ``base_url`` setting
       replaces.
     - a value
   * - **Endpoint**
     - Which API of that service, at what URL, speaking which protocol with
       which dialect.
     - a value
   * - **Protocol**
     - How to talk to a standard API: request grammar, paging, filters,
       chunking.
     - functions
   * - **Dialect**
     - Where one endpoint departs from its protocol's standard.
     - a value, one type per protocol
   * - **Execution**
     - How requests run: retry, fan-out, progress, quota (ADR 0006).
     - functions
   * - **Adapter**
     - What the public sees: getters, parameter names, translation, docs.
     - a module

**An adapter declares one** ``Service`` **.** It holds the adapter's name and
its default URL, and it is the only place the adapter writes either.
Configuration and execution keep resolving settings by the service's name,
because that is the key a configuration file uses: they take ``service.name``,
read from the declaration, never a literal. Only resolving the URL takes the
value itself.

**Each API an adapter calls is an** ``Endpoint`` **of its service.** An
endpoint names its service, its URL, and the dialect of its protocol. The
adapter builds it when a call starts, from the configuration in effect, so
every chunk and a later resume use the same URL. Water Data is one service with
four endpoints.

**A protocol is implemented as the standard is written.** Protocol code is
functions that receive an endpoint. Each dialect field records one way an
endpoint *extends* the standard (Water Data accepts a lone duration),
*restricts* it (NGWMN refuses ``../``), or *renames* it (Water Data filters on
``time``), and its default is the standard. A default dialect therefore
describes an endpoint that follows the standard. A field's docstring records the
observed server behavior that justifies it, and when it was observed.

**Shared code names no service.** Nothing a ``Service``, ``Endpoint``, or
dialect declares -- a name, a parameter, a collection, a column -- appears as a
literal in protocol or execution code; that code reads it from the declaration.
A protocol's own standard names (``datetime``, ``bbox``, ``limit``) are not
deviations. A comment may cite a service as evidence for why the code tolerates
a variation; the behavior may not depend on which service it is.

**Dialect fields are grouped by the code that reads them.** OGC's dialect has a
date group, a request-construction group, and a result-shaping group. A group is
created when one part of the code reads several fields together.

**A date range is read once, at the public boundary, and written by each
protocol.** One package-wide value holds what the caller meant. The ISO 8601
interval grammar shared by OGC and STAC is written by one module, adjusted by a
``DateDialect``; another protocol writes the value in its own format.

**Values are data; protocols and execution are functions.** ``Service``,
``Endpoint``, and the dialect types are frozen dataclasses with no behavior.
They do not send requests or read configuration, and nothing subclasses them or
a protocol's code. A method that sends requests, or an override, could
change behavior for one service without declaring a field, which is the problem
this record exists to prevent.

**Facts about one collection travel with the call.** Which column the wire
``id`` is renamed to, and whether a collection has geometry, are arguments of
each call, not fields of an endpoint: reference tables and ``get_cql`` reach
collections outside any adapter's map.

**A protocol module exists only when two endpoints share the protocol.** OGC
API Features has two (Water Data, NGWMN). The interval grammar has three (those
two and STAC). WQP, NLDI, NWDC, and StreamStats keep their request code in their
adapters until a second endpoint shares it.

Consequences
------------

- Adding a service means declaring values, not editing shared code, and a new
  endpoint of an existing protocol starts from the standard's behavior.
- Everything shared code knows about a service is in one place, next to the
  evidence for each deviation.
- Inside ``ogc``, one endpoint argument replaces the ``base_url``, ``dialect``,
  and ``adapter`` arguments that are passed together today.
- A deviation the protocol code does not model needs a new field first. That
  costs one field and its default, deliberately.
- Behavior change, when the date step lands: a lone duration sent to NGWMN
  raises ``ValueError`` before sending instead of returning HTTP 400.

Alternatives rejected:

- *Per-service conditionals in shared code.* The current state.
- *A client class per service, or methods such as* ``WATERDATA.get_data()``.
  A method that sends requests puts the protocol behind the value, which would
  make the module that defines dialects import the OGC engine -- forbidden for
  the modules that need the type -- and invites per-service overrides.
- *One OGC-specific service object holding name, URL, and dialect.* Name and
  default URL are not OGC-specific: every adapter has them. One service can have
  endpoints in several protocols.
- *A flat dialect.* The date fields are read together and are used without the
  rest by STAC.
- *Probing a service at runtime for what it accepts.* That adds a request per
  call to learn a fact that changes rarely, and some of these behaviors come
  from a firewall the API does not describe.

Compliance
----------

``tests/architecture_test.py`` fails if a service's name appears as a string
literal anywhere in the package except the configuration roster and that
service's declaration. Once endpoints and dialects land, it also fails if a
parameter, collection, or column a shipped dialect lists appears as a literal
in protocol code. Docstrings and comments are excluded.

Each dialect field that records a server behavior has a ``@pytest.mark.live``
test that sends the request the field prevents and the request it produces, so
the scheduled live workflow fails when a deployment changes.

``CONTEXT.md`` defines *Endpoint*, *Protocol*, and *Dialect*, and says how a
*Service* is declared.

Deviations still in the OGC protocol code, each removed when another endpoint
needs a different value or the code holding it next changes:

- ``monitoring_location_id`` is validated by name against the ``AGENCY-ID`` form
  both OGC services use.
- ``skipGeometry`` is pygeoapi's spelling; the standard has no such parameter.
- ``limit`` is capped at 50000, Water Data's largest page.
- An unquoted number in a CQL text filter raises, because every Water Data
  queryable is typed as a string.

Notes
-----

Prompted by the review of #442 and #443. Supersedes the narrower proposal in
#444, which bundled name, URL, and dialect into one OGC-specific value. The
behaviors in the Context table were observed by sending each request to each
service on 2026-10-11.

Implementation steps, each its own pull request: declare a ``Service`` per
adapter; endpoints for the OGC getters; endpoints for the remaining adapters;
the package-wide date range; the shared interval grammar and ``DateDialect``;
the OGC code naming nothing a dialect declares; acceptance.

Revisions:

- *Service declarations.* Each adapter declares its ``Service`` in the module
  that owns its URL (Water Data's in ``waterdata.endpoints``, shared by its four
  APIs). The type is a dependency-free leaf, ``dataretrieval._service``.
  Configuration, execution, and the OGC code keep taking the service's *name*
  rather than the value: settings are keyed by name, and a value would add
  nothing they read. ``configuration.service_url`` resolves a service's URL. The
  name check excludes ``__all__``, whose entries name modules that share a
  service's spelling.
- *Endpoints for the OGC getters.* An endpoint holds a resolved URL rather than
  a function that resolves one: the adapter builds it at the start of each
  call, which gives the same once-per-call guarantee with no behavior in the
  value. The OGC engine, request builder, and result finalizer take one
  ``endpoint`` where they took ``base_url``, ``dialect``, and ``adapter``;
  functions that need only the URL (schema and CQL requests) still take it
  alone. ``DEFAULT_DIALECT`` is removed, since every call now supplies a
  dialect through its endpoint.
