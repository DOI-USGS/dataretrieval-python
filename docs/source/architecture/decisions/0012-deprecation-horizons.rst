ADR 0012: Announce every removal through one advisory with a published horizon
==============================================================================

Status
------

Accepted

Context
-------

This package's value is that established calls keep working. Public API
compatibility is its first architecture characteristic after artifact integrity.
Names therefore leave slowly: a renamed argument, a retired module, a getter
whose service no longer exists.

Four spellings of "tell the caller something is going away" grew up
independently, and only one carried a date. A caller could not tell how long
they had, a maintainer could not audit what was due, and the warning category
was a per-author choice -- which matters, because downstream projects run their
suites under ``-W error::DeprecationWarning``.

ADR 0005 sets a removal date for legacy NWIS, but it is scoped to that adapter.
Nothing recorded the general rule, and the deprecations kept accumulating.

Decision
--------

Every deprecation is announced through the shared mechanism in
``dataretrieval._deprecation``, and every one has a published removal horizon
recorded in ``REMOVALS``.

A deprecation advisory names three things: what is going away, what to use
instead, and the date on or after which it may be removed. The mechanism
tolerates an advisory with no date -- it then promises nothing specific rather
than implying a schedule it does not have. A deprecation of a public name is
expected to carry one, and an advisory naming a replacement the caller cannot
yet use is not finished.

``REMOVALS`` is the single table of horizons. One table is auditable -- what is
due can be listed, and a horizon can be extended in one place -- whereas four
hand-rolled shims could only be found by grep. A renamed public argument keeps
working under its old name through one shared decorator rather than a shim
written for each getter.

The *warning category* an advisory carries is not the author's choice, but the
rule setting it is not this record's. :doc:`0004-error-retry-resume` decides
when an advisory is a ``DeprecationWarning`` (a name in this package is going
away) and when it is a ``DataCurrencyWarning`` (an upstream dataset has stopped
being updated). This record governs the mechanism and the horizon.

A horizon is a floor, not a schedule. Passing it permits removal; it does not
require one, and removal remains a deliberate change with its own release note.

Consequences
------------

- A caller can see, from the warning alone, how long they have and what to move
  to.
- Horizons can be audited and extended centrally, so a removal date cannot
  arrive unnoticed in a module nobody is reading.
- Deprecating something costs more than adding a ``warnings.warn`` call: the
  replacement must exist and a date must be chosen. That is the intended cost.
- The package accumulates long-lived compatibility shims. This is accepted --
  it is what the compatibility characteristic buys, and the table makes the
  accumulation visible rather than hidden.
- Nothing is removed on the horizon alone. A removal still needs a release that
  says so.

Compliance
----------

``tests/deprecation_test.py`` covers the shared mechanism: that an advisory
names its replacement, and that the horizon it prints is the one in
``REMOVALS``. Per-surface tests assert the individual advisories, including that
a renamed argument still works under its old name. The *warning category*
assertions -- that ``DataCurrencyWarning`` is not a ``DeprecationWarning``
subclass -- belong to :doc:`0004-error-retry-resume`.

Notes
-----

ADR 0005 remains the record for legacy NWIS specifically, including its
2027-05-06 date. This record generalizes the mechanism without changing that
decision.
