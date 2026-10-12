ADR 0014: Declare how each service departs from its protocol as data
====================================================================

Status
------

Proposed. Implemented in three pull requests, listed under ``Notes``. It is
accepted when the last one merges.

Context
-------

Water Data and NGWMN share the OGC API Features code in ``dataretrieval.ogc``,
and Water Data's ratings catalog uses STAC, which writes dates in the same ISO
8601 interval grammar. The three services do not handle the standards the same
way:

.. list-table::
   :header-rows: 1

   * - Request (observed 2026-10-11)
     - Water Data OGC
     - NGWMN
     - Water Data STAC
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
   * - A comma-joined list, ``site=A,B``
     - read as one value, except for a few identifiers and codes
     - read as a list
     - not used

The shared code handles these differences in two ways. Some are fields of
``OgcDialect``, which each adapter sets: ``lists_as_cql2`` (#446) and the open
start (#443) were added that way while fixing bugs. Others are written into the
shared code: it lists Water Data's date parameter names, exempts
``last_modified`` by name from date-only writing, passes a duration through
for every service because Water Data accepts one, and passes each adapter's
name as a string literal to configuration, retry, and fan-out. ADR 0003 keeps
shared code from *importing* a service; it does not stop shared code from
*naming* one.

Dates were also read and written in one function. Reading what a caller passed
(a string, a ``date``, a ``Timestamp``, ``None``, the time-zone rule) is
package policy; writing it as an ISO 8601 interval is protocol grammar, and
differs per service. Each of the date defects fixed in #440 through #443 was in
that one function.

Decision
--------

**A service's departure from its protocol's standard is a dialect field.** A
dialect is a frozen dataclass with one type per protocol. Each field records
one way a service extends the standard (Water Data accepts a lone duration),
restricts it (NGWMN rejects ``../``), or renames it (Water Data filters dates on
``time``). Each field's default is the standard, so a default dialect describes
a service that follows the standard. Where an adapter sets a field, a comment
records the observed server behavior that requires it, and a live test sends
both the request the field prevents and the request it produces.

**Shared code names no service.** A parameter, collection, or column that a
dialect declares does not appear as a literal in protocol code, and a service
name does not appear as a literal outside the configuration roster and the
service's own declaration. A protocol's own standard names (``datetime``,
``bbox``, ``limit``) are not departures. A comment may cite a service as the
reason the code accepts a variation.

**A date argument is read once and written by each protocol.**
``dataretrieval._dates`` reads a date argument into a ``DateRange``: an
instant, a range whose ``None`` bound is open, or a duration, with every bound
in UTC (a naive value is UTC, as in pandas). ``dataretrieval._intervals``
writes a ``DateRange`` in the ISO 8601 interval grammar, adjusted by a
``DateDialect`` (``open_start``, ``durations``). It sits below ``ogc`` because
STAC also uses it.

**Values are data; protocol code is functions.** Dialects, and the ``Service``
and ``Endpoint`` values below, are frozen dataclasses with no methods that send
requests or read configuration, and nothing subclasses them. A method or an
override could change one service's behavior without adding a field.

**The OGC engine receives one endpoint value.** An ``Endpoint`` holds a
``Service`` (the adapter's name, which configuration resolves settings by, and
its default URL), the URL resolved when the call starts, and the dialect. The
OGC engine, request builder, and result finalizer take it in place of the
``base_url``, ``dialect``, and adapter-name arguments they take today.

**A protocol module exists only when two services share the protocol.** OGC
API Features has two (Water Data, NGWMN); the interval grammar has three (those
two and STAC). WQP, NLDI, NWDC, StreamStats, and Water Data's Samples and
Statistics APIs keep their request code in their adapters, and get no dialect
or endpoint value until a second service shares their protocol.

Consequences
------------

- Adding an OGC or STAC service means setting dialect fields, not editing
  shared code. A service that follows the standard needs none.
- What the shared code knows about a service is in one declaration, next to the
  evidence for each field, and the scheduled live workflow fails when a
  service's behavior changes.
- A departure that the protocol code does not model needs a new field and
  default before an adapter can use it. This is deliberate: it keeps the
  departure visible and tested.
- Behavior change: a lone duration passed to NGWMN or to ``get_ratings``
  raises ``ValueError`` before sending, instead of returning HTTP 400 from
  NGWMN; ``get_ratings`` already raised, with its own message.

Alternatives rejected:

- *Conditionals on the service name in shared code.* This is how the
  differences listed above were handled before, and reviews missed the cases
  where a change for one service broke another.
- *A client class per service, with methods that send requests.* Behavior
  behind a method can be overridden for one service without a field, and the
  module that defines the dialect would have to import the OGC engine.
- *One flat dialect.* STAC reads the date fields without the rest.
- *Probing a service at runtime for what it accepts.* That adds a request per
  call to learn facts that change rarely, and some come from a firewall the API
  does not describe.

Compliance
----------

- ``.importlinter`` places ``_dates`` with the dependency-free leaves and
  ``_intervals`` below ``ogc``.
- Each ``DateDialect`` field an adapter sets has a ``@pytest.mark.live`` test
  in that adapter's test file.
- From the second step, ``tests/architecture_test.py`` fails if a string a
  shipped dialect lists appears as a literal in ``ogc`` or ``_intervals``;
  from the third, if a service name appears as a literal outside its
  declaration and the configuration roster. Docstrings and comments are
  excluded.

Departures still written into the OGC code, each removed when another service
needs a different value or the code holding it next changes:

- ``monitoring_location_id`` is validated against the ``AGENCY-ID`` form both
  OGC services use.
- ``skipGeometry`` is pygeoapi's name; the standard has no such parameter.
- ``limit`` is capped at 50000, Water Data's largest page.
- An unquoted number in a CQL text filter raises, because every Water Data
  queryable is typed as a string.

Notes
-----

Prompted by the reviews of #442 and #443. Replaces the draft in #445, which
implemented every step in one branch, and the narrower #444.

Steps, one pull request each:

1. The date split: ``_dates``, ``_intervals``, and ``DateDialect``, with the
   duration refusal for NGWMN and STAC.
2. The OGC date fields on ``OgcDialect`` (which parameters carry dates, which
   collections are date-only, which parameters stay timed), reading dates when
   the getter is called, and the dialect-literal test.
3. ``Service`` and ``Endpoint``, and the service-name test.
