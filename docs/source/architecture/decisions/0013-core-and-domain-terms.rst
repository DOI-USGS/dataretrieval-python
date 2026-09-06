ADR 0013: Distinguish the terms we own from the terms the services own
======================================================================

Status
------

Accepted

Context
-------

``CONTEXT.md`` is one flat glossary. Every term in it reads as equally binding,
and every place the code disagrees is filed under *Known legacy names* -- a
list whose framing is that the disagreement is debt, tolerated until someone
gets to it.

For most of the glossary that framing is right. But it is wrong for a small set
of terms, and being wrong about those has produced the same review argument
repeatedly: whether a docstring may say *site*, whether ``service=`` may name a
collection, whether prose about NWIS is bound by a word chosen from the Water
Data API.

The two sets behave differently because their authority differs.

Terms like *chunk*, *page*, *fan-out*, *plan*, *interruption*, *dialect* and
*leaf* appear nowhere in any USGS API's vocabulary. They were invented here to
describe machinery this package owns. Nothing external constrains them, so when
the package spells one of them two ways -- the resolution chain's code said
*tier* for what its founding records, ADRs 0009 and 0010, call a *source* --
that is simply an inconsistency, and one that can be removed by deciding.

Terms like *monitoring location* and *collection* are different. The services
name those things, and they do not agree with each other:

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

No decision here makes those agree. A caller who has read the WQP
documentation looks for ``Station``; one reading Water Data's looks for
``monitoring_location_id``. An adapter that renamed either would be harder to
use, not easier, and the parameter names are public surface besides.

Treating both sets under one rule forces a choice between two bad options:
abandon the glossary, and the shared modules lose the vocabulary that lets them
be shared; or enforce it everywhere, and every adapter's public surface drifts
from the API it wraps.

Decision
--------

The glossary holds two kinds of term, and they carry different obligations.

**Core terms are ours.** The package invented them and no service has a claim
on them: everything under *Retrieval*, *Failure and resumption*, *Configuration*
and *Boundaries*, plus *Collection family* and *Metadata*. One spelling,
enforced everywhere it appears -- prose, identifiers, tests. A second spelling
of a core term is a defect, not a variation, and is fixed rather than recorded.
This is what makes the lower-level modules shareable: transport, configuration
and the OGC engine can be written once because the words they are written in
answer to nothing outside this package.

**Domain terms belong to the services.** *Monitoring location* and *collection*
name things the services define and spell differently. For these the glossary
chooses one term for **prose**, so that documents about the package are
internally consistent. It does not choose for the wire, and it does not choose
for an adapter's public surface: each adapter keeps its own service's spelling
in its parameters, and reproduces that service's vocabulary faithfully where it
appears in returned data.

An adapter is where the two meet. Its public surface speaks its service's
language; what it hands to the shared modules speaks the core's. The
translation is the adapter's job, and a divergence at that boundary is the
design working rather than debt.

Two rules follow:

- **A term the glossary does not define is not used in the glossary.** A word
  that earns a place in ``CONTEXT.md``'s prose earns an entry. Naming a term
  only to say what an ADR calls it is a cross-reference, not a definition, and
  does not license using the word elsewhere.
- **Only core misnamings are legacy.** *Known legacy names* records a core term
  the code spells wrongly and cannot be renamed. A domain term at an adapter's
  surface is not a legacy name; it is that adapter speaking its service's
  language, and belongs with the term's own entry.

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
- A glossary entry now carries an obligation to say which kind it is. That is a
  small cost per term and the reason the distinction is usable at all.
- The package's own inconsistencies in core vocabulary become defects with a
  deadline rather than curiosities. The resolution chain's ``tier``-for-*source*
  identifiers are the standing example.

Compliance
----------

``CONTEXT.md`` marks each domain term as such and names the per-service
spellings in the entry itself, so a reader who needs to know whether a word is
negotiable can see it without asking.

The mechanical part is that the glossary must define what it uses:
``tests/architecture_test.py`` asserts every ``ADR NNNN`` citation resolves, and
the same file is where a check that ``CONTEXT.md`` defines its own vocabulary
would go. None is proposed yet -- a word-list check over prose has a poor
precision record in this repository, and the failure it would catch is one a
reader notices immediately.

Whether a given adapter docstring should say *site* or *monitoring location*
remains a review judgement, and is meant to.

Notes
-----

Prompted by review of :doc:`0000-documenting-decisions`, where a reviewer found
``CONTEXT.md`` using *tier* in its own prose without defining it. The word was
already there before that record was written; recording each explanation once
made the gap visible rather than creating it.

The per-service spellings in the table above were read from the adapters on
2026-09-01.
