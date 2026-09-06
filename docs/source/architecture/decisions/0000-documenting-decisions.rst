ADR 0000: Record each explanation once, in the place that owns it
=================================================================

Status
------

Accepted

Amended after acceptance under the reader's-position rule below; the ``Notes``
section records the clause added.

Context
-------

This package documents itself heavily and deliberately. Its public getters are
thin wrappers whose numpydoc parameter tables *are* the deliverable: 55% of all
docstring lines in ``dataretrieval/`` sit in the service adapters, at a ratio of
2.5 prose lines per line of code. CONTRIBUTING already requires those tables.

The problem is in the internal modules behind them. Rationale -- the argument
for why a rule holds -- accumulated in module and function docstrings alongside
the ADRs that already owned it, because a paragraph can be written where the
reader is standing while a citation sends them to a record they have to open.
Those modules hold 82% of the package's comment lines, and an audit of that
prose found roughly 500 lines restating decisions already recorded in ADRs 0003
through 0011: ``configuration.py`` re-derives the layered-resolution design in
65 docstring lines while citing ADRs 0009, 0010, and 0011 in the course of it,
the no-progress budget is argued from first principles in five places across
``transport/``, and which failures may be retried is enumerated in three lists
that can drift apart.

Duplication is not a tidiness problem here; it is a correctness problem. Every
copy is a place the rule can be updated while the others are not, and the audit
found copies that had already gone stale -- an overview paragraph describing
concurrency caps that a later ADR had removed, and an ADR clause describing a
credential rejection the code deliberately no longer performs. A reader has no
way to tell which copy is current.

Decision
--------

Each explanation is recorded once, in the venue that owns that kind of
knowledge, and referenced from anywhere else that needs it.

**Docstrings own the contract.** What a caller must know to use the documented
object: the numpydoc ``Parameters``, ``Returns``, ``Raises``, and ``Examples``
sections, what the function does, and what it guarantees. Public getters keep
their full parameter tables, however long. A private helper's docstring says
what it does and what its callers may rely on.

**Inline comments own the local constraint.** Why *these* lines are written this
way, when a name cannot carry it -- an ordering that matters, an upstream quirk,
a bail-out that looks removable. One or two lines, adjacent to the code they
explain. A comment that outgrows that is describing something wider than the
lines beneath it, and belongs in one of the venues below.

**Commit messages own the history.** Benchmark numbers, the symptom that
prompted a change, what the code used to do, what was tried and rejected. This
is the venue with a date and a diff attached. It is the one place where "was
once optional" or "measured 1.6x slower" stays true forever without maintenance.
Source files carry the current state, not the route to it.

**ADRs own the cross-cutting decision.** A choice that constrains code outside
the file stating it, or that a future contributor could plausibly undo from
somewhere else. The code cites the record by number rather than restating its
argument. Adding a clause to an existing ADR is preferred over a new record;
number a new one sequentially and follow :doc:`template`.

**The glossary owns the vocabulary.** ``CONTEXT.md`` defines terms with
package-wide meaning. Documents use those terms rather than redefining them, and
where a term and the code disagree, the term wins.

Three rules follow:

- **Cite, do not restate.** Prose that argues for a rule an ADR already owns is
  replaced by a reference to that ADR's number. A pointer that does not resolve
  is visible; a paraphrase that has drifted is not.
- **Write from the reader's position.** A citation replaces an argument only if
  the sentence left behind stands on its own. Prose that assumes the reader has
  the cited record already open, or leans on a term the glossary does not
  define, has moved the cost of the duplication rather than removed it.
- **An accepted ADR is not edited to reverse its meaning.** A later decision
  supersedes it and links back, as the decisions index already requires.
  Additive clauses and corrections are recorded in the amended record's
  ``Notes``, and its ``Status`` says the record was amended, so a reader meets
  that fact before the Decision text rather than after it.

Consequences
------------

- A rule has one current statement, so updating it cannot leave stale copies
  behind in modules nobody thought to grep.
- Reading a module gets slower in one respect: some rationale now requires
  opening an ADR. That cost is accepted -- the reader who needs the argument is
  rarer than the reader who needs the contract, and the ADR is the version that
  is maintained.
- Rationale is not deleted when it moves. Prose that leaves a docstring lands in
  an ADR clause or in the commit message that removes it. The commit message is
  where a reviewer looks for what a documentation change discarded.
- Docstring volume in the service adapters is expected to stay high and is not a
  metric to optimize. A ratio measured over a public adapter says nothing about
  whether it is over-documented.
- The policy applies going forward. Existing prose is migrated when a module is
  being changed for another reason, rather than in a sweep that would touch
  every file at once.

Compliance
----------

Reviewers apply two questions to added prose. First: *does this explain the
lines beneath it, or does it argue for a rule that binds another file?* The
second belongs in an ADR, cited by number. Then: *could a reader who has not
opened the cited record follow this sentence?* If not, the citation has hidden
the explanation rather than relocated it. The repair is to restore the reader's
footing -- name the term, resolve the pronoun, say which venue owns the rest --
not to restate the argument the citation replaced.

The mechanical part is checkable, and it is the part that goes stale: a
docstring or comment that names an ADR must name one that exists.
``tests/architecture_test.py`` asserts that every ``ADR NNNN`` reference in
``dataretrieval/`` resolves to a record in
``docs/source/architecture/decisions/``, so a renumbered or deleted record fails
the suite rather than leaving a dangling pointer. Whether a given paragraph
should have been a citation remains a review judgement. No test is proposed: a
proxy metric here would push contributors to delete parameter documentation to
move a number.

Notes
-----

The reader's-position rule and the second review question were added after
acceptance. Review of the pull request that introduced this record found prose
this record had put in the right venue and left unreadable from outside the
author's head: undefined jargon, a pronoun with no antecedent, and a mapping
between two numbering schemes that needed a second document open. One instance
broke this record's own history rule. The venue rules say where an explanation
goes; none of them asked who it reads for.

``Context`` and the measurements below are this package's. ``Decision``,
``Consequences``, and the review questions in ``Compliance`` are written to hold
for any project; the paragraph naming ``tests/architecture_test.py`` is not. A
project adopting this record writes its own ``Context`` from its own audit and
keeps the rest.

This record was written after ADRs 0001 through 0011. It is numbered 0000
because it governs how every record is written, not because it came first. Its
``Context`` describes the package as the audit found it, and the migration it
authorizes is incremental, so some of the prose described there is still in
place.

Prose measurements were taken over ``dataretrieval/`` on 2026-08-26: 9,283
docstring lines and 1,157 comment lines against 6,127 lines of code; public
service adapters at 2.46 prose lines per code line, internal modules at 1.29.
The ~500-line restatement estimate comes from the subsystem audit recorded in
the pull request that introduced this ADR.
