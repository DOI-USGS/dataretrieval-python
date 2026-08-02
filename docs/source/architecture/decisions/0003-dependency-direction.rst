ADR 0003: Direct dependencies toward stable policy
==================================================

Status
------

Accepted

Context
-------

Service modules change when upstream APIs change. Error policy, generic data
structures, and transport contracts should be more stable. If shared
infrastructure imports service details, unrelated upstream changes propagate
inward and reuse becomes unsafe. Several current private imports also act as
unintended cross-package contracts.

Decision
--------

Dependencies point from public facades to service/protocol adapters and then to
stable shared policy and third-party infrastructure. In particular:

- ``dataretrieval.exceptions`` is a runtime-dependency-light leaf.
- ``dataretrieval.ogc`` must not import Water Data, NGWMN, Water Use, or NWIS.
- Modern modules must not import deprecated NWIS.
- New non-OGC services must not obtain generic transport behavior by importing
  private OGC implementation symbols.

Underscore-prefixed symbols remain implementation details even when existing
internal modules currently use them.

Consequences
------------

- Stable policy can be reused without pulling in service schemas.
- Static import checks can detect architectural erosion early.
- Moving existing private seams requires compatibility-aware sequencing.
- Some duplication is preferable to a premature abstraction that couples
  unlike upstream protocols.

Compliance
----------

``tests/architecture_test.py`` parses runtime imports and enforces the rules
that hold today. Its allowlist is the authoritative inventory of exact temporary
cross-boundary imports; this ADR owns the direction and rationale rather than a
second copy of that mutable inventory.

The allowlist should shrink as private seams move. Any growth requires explicit
architecture review, and a change to the dependency policy requires this ADR to
be superseded.
