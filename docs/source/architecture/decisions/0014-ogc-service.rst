ADR 0014: Describe each OGC service with one declared value
===========================================================

Status
------

Proposed

Context
-------

``dataretrieval.ogc`` serves two services from one code path, Water Data and
NGWMN, and ``waterdata.get_ratings`` reuses its date formatting against a
third, the Water Data STAC catalog. All three implement the same standards --
OGC API Features, and STAC's item search, which shares its ``datetime`` grammar
-- and each departs from them differently:

.. list-table::
   :header-rows: 1

   * - Behavior (observed 2026-10-11)
     - Water Data
     - NGWMN
     - STAC ratings
   * - Date parameters
     - ``time``, ``begin``, ``end``, ``last_modified``, and ``datetime``
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

An adapter tells the shared OGC code about its service in two ways today, and
neither has a rule.

**By argument.** Every call to ``get_ogc_data`` passes six values describing
the service and the collection: ``adapter`` (which settings apply, ADR 0010),
``base_url``, ``dialect``, ``extra_id_cols``, ``output_id``, and ``spatial``.
Three of them -- ``adapter``, ``base_url``, ``dialect`` -- are passed on 13
more times inside ``ogc``, to the request builder, the result finalizer, the
schema request, the chunk planner, and the retry policy. They always travel
together but are not one thing in the code.

**By hard-coding.** Some facts about a service are not passed at all. The core
lists Water Data's parameter names as "the OGC date parameters", exempts
``last_modified`` by name from date-only rendering, passes durations through for
every service because one accepts them, and assumes every service spells an
open start ``..``. Each looks like protocol code because it sits in the
protocol package. ADR 0003 keeps ``ogc`` from *importing* a service; it says
nothing about *naming* one, and a string literal is not an import, so no
dependency check can find these. Pull request #443 formatted interval strings
to the one open-start spelling the core knew, which broke requests to the two
services that reject it. Its review missed this because no rule said which
facts belong to a service.

``OgcDialect`` existed as the place for per-API differences, but was defined by
example rather than by a rule.

Decision
--------

**An adapter declares one** ``OgcService`` **, and that is everything the
shared OGC code knows about the service.** An ``OgcService`` has three fields:

- ``adapter`` -- whose settings apply (ADR 0010), and the label on its
  progress line.
- ``base_url`` -- a function returning the API's base URL. It is a function
  because the URL comes from configuration, which a ``configure()`` block can
  change for the length of a call (ADR 0011). The core calls it once when a
  call starts, so every chunk and a later resume use the same URL.
- ``dialect`` -- how the service departs from the standard.

The core's entry points take the service as their first argument:
``get_ogc_data(service, collection, args, ...)``.

**The core implements the standard as written.** ``dataretrieval.ogc``
implements OGC API Features and the parts of CQL2 the adapters use, and
formats values the way the standard specifies. ``DEFAULT_DIALECT`` describes a
service that implements the standard and nothing more; a behavior the standard
does not define is off by default.

**Each deviation is one dialect field, whose default is the standard.** A
service may *extend* the standard (Water Data accepts a lone duration),
*restrict* it (NGWMN refuses ``../``), or *rename* it (Water Data filters on
``time`` where the standard says ``datetime``). Each is a field the adapter
sets. The field's docstring records the observed server behavior that
justifies it and when it was observed, because these are facts about a
deployment, and deployments change.

**The core names no service.** Nothing an ``OgcService`` declares -- an
adapter name, a parameter, a collection, a column -- appears as a literal in
the core; the core reads it from the declaration. The standard's own names
(``datetime``, ``bbox``, ``limit``, ``properties``, ``filter``, ``filter-lang``)
are not deviations and may appear. A comment in the core may cite a service as
evidence for why the core tolerates a variation, such as a page without
``numberReturned``; the core's behavior does not depend on which service it is
talking to.

**Dialect fields are grouped by the part of the core that reads them.**

- ``dates``, a ``DateDialect``, is read by date formatting: which parameters
  carry dates, which collections take dates without a time, which parameters
  keep their time regardless, how an open start is spelled, and whether a lone
  duration is sent. A service that shares the grammar without being an OGC
  Features API -- the STAC catalog -- uses a ``DateDialect`` alone.
- ``cql2_collections`` is read by request construction.
- ``time_cols``, ``numerical_cols``, ``sort_cols``, and ``extra_id_cols`` are
  read by result shaping. They are the service's column vocabulary.

A group is created when one part of the core reads several fields together, not
to make the dialect look symmetrical.

**Facts about a collection travel with the call.** ``output_id`` (the column
the wire ``id`` is renamed to) and ``spatial`` (whether the collection has
geometry) stay arguments of each call. A service declaration is fixed, while
reference tables and ``get_cql`` reach collections outside any adapter's map.

**Services and dialects are values; the core is functions that receive them.**
``OgcService`` and the dialect types are frozen dataclasses with no methods
that send requests, and an adapter does not subclass them or any core class. A
method or an override could change the core's behavior for one service without
declaring a field, which is the problem this record exists to prevent. Keeping
the types as data also keeps ``ogc.policy``, where they are defined, free of
imports from the rest of ``ogc``, so every module can use them.

**Date arguments are formatted once, before the query is planned.** They are
rendered to their wire form as a call enters the engine, so the chunk planner
sees one interval string, not a two-element range it might split. The planner
then needs no list of date parameters, and an unreadable date fails before any
request is sized or sent.

Consequences
------------

- Adding an OGC service means declaring its ``OgcService``, not editing the
  core. A new adapter starts from the standard's behavior rather than another
  service's.
- An adapter's facts about its service are in one place, next to the evidence
  for each. Reviewing a change to what is sent to one service means reading that
  service's declaration.
- Signatures inside ``ogc`` lose parameters: one value replaces three that are
  passed together.
- ``base_url`` is the one field that is a function rather than data. That is
  accepted because it only looks up configuration; it cannot change what the core
  does with the URL. The alternative, a URL template the core fills in, would put
  Water Data's version segment into the core.
- A deviation the core does not model needs a new field before an adapter can use
  it. That costs one field and its default, deliberately: the alternative is a
  special case in shared code.
- Behavior change: NGWMN stops passing a lone duration, which its service answers
  with HTTP 400, and raises ``ValueError`` before sending, as ``get_ratings``
  already does. The ``get_ratings`` message is reworded, since one message now
  serves both.
- ``get_ratings`` drops its own duration check; its ``DateDialect`` replaces it.
- ``OgcDialect.cql2_services`` and ``date_only_services`` become
  ``cql2_collections`` and ``DateDialect.date_only_collections``, which
  resolves the last part of the ``service`` entry in *Known legacy names*. ``OgcDialect`` appears on no reference
  page, so it is not public API under ADR 0012 and the renames need no
  deprecation.

Alternatives rejected:

- *Per-service conditionals in the core.* The current state, and what produced
  the #443 regression.
- *A flat dialect.* The date fields are read together by one module and are
  useful without the rest, as the STAC catalog shows.
- *Methods on* ``OgcService`` *(* ``WATERDATA.get_data(...)`` *).* No capability
  is gained over a function. A method that sends requests would make
  ``ogc.policy`` import the engine, which the internal-seams contract in
  ``.importlinter`` forbids for the modules that need the type, and it invites
  per-service overrides.
- *A client class per service, subclassing a core client.* An override can
  change any behavior without declaring it.
- *Collection facts in the service declaration.* ``output_id`` and ``spatial``
  would need lookup functions to cover collections outside the adapter's map.
- *Probing the service at runtime for what it accepts.* That adds a request to
  every call to learn a fact that changes rarely, and some of these behaviors
  come from a firewall rather than from anything the API describes.

Compliance
----------

``tests/architecture_test.py`` fails if anything a shipped ``OgcService``
declares -- its adapter name, or a parameter, collection, or column its dialect
lists -- appears as a string literal in code under ``dataretrieval/ogc``.
Docstrings and comments are excluded, since the rule binds behavior.

Each dialect field that records a server behavior has a ``@pytest.mark.live``
test. It sends the request the field prevents and asserts the service still
rejects it, then sends the request the field produces and asserts the service
accepts it. The scheduled live workflow therefore fails when a deployment
changes, which is when a field needs revisiting.

``CONTEXT.md`` defines *OGC service* and states the rule in its *Dialect*
entry.

Four deviations stay in the core as recorded variances. The first is a name a
dialect lists, so the fitness function allowlists it by module and an unlisted
one fails; the other three are not names a dialect lists, so this list is their
record:

- ``monitoring_location_id`` is validated by name against the ``AGENCY-ID``
  form both services use. ``waterdata.ratings`` uses the same check, so moving it
  needs a validator shared with a caller that has no ``OgcService``.
- ``skipGeometry`` is pygeoapi's spelling, which both services run; the standard
  has no such parameter.
- ``limit`` is capped at 50000, Water Data's largest page, for every service.
- An unquoted number in a CQL text filter raises, because every Water Data
  queryable is typed as a string. The check runs in the chunk planner.

Each is removed when another service needs a different value, or when the code
holding it is next changed for another reason.

Notes
-----

Prompted by the review of #442 and #443. The behaviors in the Context table were
observed by sending each request to each service on 2026-10-11. The count of
parameters passed together in ``ogc`` was taken at ``b3675aab``.

This record is proposed ahead of its implementation so that the design can be
reviewed on its own. The implementation is added to the same pull request,
which is where the record is accepted.
