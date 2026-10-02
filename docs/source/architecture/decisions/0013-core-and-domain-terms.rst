ADR 0013: Distinguish the terms we own from the terms the services own
======================================================================

Status
------

Accepted

Amended after acceptance under :doc:`0000-documenting-decisions`; the
``Notes`` section records the clause added.

Context
-------

``CONTEXT.md`` is one flat glossary. Every term in it reads as equally binding,
and every place the code disagrees is filed under *Known legacy names* -- a
list whose framing is that the disagreement is a defect, tolerated until
someone corrects it.

For most of the glossary that framing is right. But it is wrong for a small set
of terms, and being wrong about those has produced the same review argument
repeatedly: whether a docstring may say *site*, whether ``service=`` may name a
collection, whether prose about NWIS is bound by a word chosen from the Water
Data API.

The two sets are treated differently because their authority differs.

Terms like *chunk*, *page*, *fan-out*, *plan*, *interruption*, *dialect* and
*leaf* appear nowhere in any USGS API's vocabulary. They were invented here to
describe mechanisms this package owns. Nothing external constrains them, so when
the package spells one of them two ways -- the resolution chain's code said
*tier* for what its founding records, ADRs 0009 and 0010, call a *source* --
that is an inconsistency, and one that can be removed by deciding.

Terms like *monitoring location* and *collection* are different. The services
name those things, and they differ:

.. list-table::
   :header-rows: 1

   * - Concept
     - NWIS
     - WQP
     - Water Data
     - NGWMN
   * - a place where measurements are recorded
     - ``site_no``, ``sites=``
     - ``Station``, ``siteid``
     - ``monitoring_location_id``
     - the ``sites`` collection
   * - a named set of records
     - ``service=``
     - ``Result``, ``Station``
     - ``collection``
     - ``collection``

No decision here makes those match. A caller who has read the WQP
documentation looks for ``Station``; one reading Water Data's looks for
``monitoring_location_id``. An adapter that renamed either would be harder to
use, not easier, and the parameter names are public surface besides.

Treating both sets under one rule forces a choice between two bad options:
abandon the glossary, and the shared modules lose the vocabulary that lets them
be shared; or enforce it everywhere, and every adapter's public surface drifts
from the API it wraps.

Decision
--------

The glossary holds two kinds of term, and they impose different obligations.

**Core terms are ours.** The package invented them and no service defines
them: everything under *Retrieval*, *Failure and resumption*, *Configuration*
and *Boundaries*, plus *Collection family*, *Metadata* and *Unified argument*. One spelling,
enforced everywhere it appears -- prose, identifiers, tests. A second spelling
of a core term is a defect, not a variation, and is fixed rather than recorded.
This is what makes the lower-level modules shareable: transport, configuration
and the OGC engine can be written once because the words they are written in
are constrained by nothing outside this package.

**Domain terms belong to the services.** *Monitoring location* and *collection*
name things the services define and spell differently. For these the glossary
chooses one term for **prose**, so that documents about the package are
internally consistent. It does not choose the names used in requests, and it
does not choose for an adapter's public surface: each adapter keeps its own
service's spelling in its parameters, and reproduces that service's vocabulary
where it appears in returned data.

An adapter uses both kinds. Its public surface uses its service's
terms; what it passes to the shared modules uses the core terms. The
translation is the adapter's responsibility, and a divergence at that boundary
is the design working as intended rather than a defect.

Two rules follow:

- **A term the glossary does not define is not used in the glossary.** A word
  used in ``CONTEXT.md``'s prose needs an entry. Naming a term
  only to say what an ADR calls it is a cross-reference, not a definition, and
  does not permit using the word elsewhere.
- **Only core misnamings are legacy.** *Known legacy names* records a core term
  the code spells wrongly and cannot be renamed. A domain term at an adapter's
  surface is not a legacy name; it is that adapter using its service's
  terms, and belongs with the term's own entry.

**A unified argument may stand beside a service's spelling.** Where one domain
concept is a filter in several adapters and each spells it differently, a getter
may also take an argument the package names -- ``state``, ``county`` -- that
accepts every common spelling of the concept and sends the service its own.
It is added to the adapter's surface and removes nothing from it, so a caller
who has read a service's documentation still finds the parameters described
there. It is allowed on four conditions:

- **The service's parameters stay.** They are not deprecated, and they still
  send the service's raw value, including values the conversion does not cover
  (a non-US FIPS code, a county the service files under a retired name).
- **The conversion is exact.** It is a table in ``dataretrieval.codes``, checked
  against the service's own reference collection by a live test. A value the
  table does not hold raises ``ValueError`` naming the native parameters,
  rather than being guessed at or sent unchecked. A service that cannot express
  a converted value exactly raises too: NGWMN still files Connecticut sites
  under the counties the state replaced with planning regions, so
  ``ngwmn.get_sites(county="09110")`` raises rather than returning no rows.
- **It cannot be combined with what it replaces.** Passing it together with the
  native parameter it is sent as raises ``ValueError``.
- **Its docstring names what it is sent as.** It says the argument is
  dataretrieval's rather than the API's, and names the field or fields the
  service receives, so the two documentations can be read against each other.

Consequences
------------

- The recurring question -- may this docstring say *site*? -- has an answer that
  does not depend on who is reviewing. In ``nwis`` it may, because that is the
  spelling its service and its parameters use. In ``transport`` it may not,
  because nothing there is about NWIS.
- Enforcement splits. A core term can be checked mechanically, since one
  spelling is correct everywhere. A domain term cannot: the correct spelling
  depends on which adapter the prose is about, so it stays a review judgement.
- *Known legacy names* becomes shorter and means something narrower. The entries
  it loses are not resolved; they move to the term they belong to, as part of
  its definition rather than a list of exceptions.
- A glossary entry now has an obligation to say which kind it is. That is a
  small cost per term and the reason the distinction is usable at all.
- A caller filtering by place across adapters learns one spelling rather than
  one per service, at the cost of a table per concept to keep current. The
  live tests that compare each table with its reference collection are what
  keep that cost visible.
- The package's own inconsistencies in core vocabulary become defects to fix
  rather than variations to tolerate. The resolution chain's
  ``tier``-for-*source* identifiers are the current example.

Compliance
----------

``CONTEXT.md`` marks each domain term as such and names the per-service
spellings in the entry itself, so a reader who needs to know whether a word is
allowed to vary can see it without asking.

The mechanical part is that the glossary must define what it uses:
``tests/architecture_test.py`` asserts every ``ADR NNNN`` citation resolves, and
the same file is where a check that ``CONTEXT.md`` defines its own vocabulary
would go. None is proposed yet -- a word-list check over prose has a poor
precision record in this repository, and the failure it would catch is one a
reader notices immediately.

Whether a given adapter docstring should say *site* or *monitoring location*
remains a review judgement, and is meant to.

For unified arguments, ``tests/utils_test.py`` and ``tests/counties_test.py``
compare the state and county tables with the Water Data ``states`` and
``counties`` collections, and ``tests/ngwmn_test.py`` checks that NGWMN files
its sites under the table's county names, live. Each getter's tests assert the
mutual exclusion with its native parameters.

Notes
-----

Prompted by review of :doc:`0000-documenting-decisions`, where a reviewer found
``CONTEXT.md`` using *tier* in its own prose without defining it. The word was
already there before that record was written; recording each explanation once
made the gap visible rather than creating it.

The per-service spellings in the table above were read from the adapters on
2026-09-01.

The unified-argument clause was added on 2026-10-01, with ``county``. ``state``
had been a unified argument since NGWMN joined the shared OGC code, with no
record saying why it did not contradict the rule that each adapter keeps its
service's spelling, and review read it as hiding the APIs' own parameters. The
clause states the conditions both arguments already met, so the next one is
judged against them rather than against that precedent.
