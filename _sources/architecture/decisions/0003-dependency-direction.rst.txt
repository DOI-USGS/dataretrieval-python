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

Dependencies point from public facades to service/protocol adapters, then to
service-neutral transport and stable policy, and finally to third-party
infrastructure. In particular:

- ``dataretrieval.exceptions`` is a runtime-dependency-light leaf.
- ``dataretrieval.ogc`` must not import Water Data, NGWMN, Water Use, or NWIS.
- Modern modules must not import deprecated NWIS.
- Service-neutral transport must not import OGC modules or service adapters.
- Non-OGC services must obtain generic execution behavior from transport, not
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

``.importlinter`` is the single authority for dependency direction. It declares
the layer stack, the allowlist of OGC consumers, NGWMN's facade-only seam, the
NWIS quarantine, collection-family independence, and package-wide acyclicity;
``lint-imports`` checks all of it against the transitive import graph in
pre-commit and CI. A boundary that legitimately moves is one edit, in that file,
alongside the ADR it cites.

These rules were previously asserted a second time in
``tests/architecture_test.py``, by hand-parsing the AST. That duplication is
gone. The tests now cover only what an import graph cannot express — which
symbols cross a seam, what a module's declared exports are, the AST shape of a
facade, and the one boundary that has to be asserted positively rather than
forbidden. A new rule that is purely about module-to-module direction belongs in
``.importlinter``.

Named contracts verify the current boundaries: NGWMN's only OGC dependency is
the facade, ``ogc.shaping`` does not depend on ``ogc.engine``, Water Use and the
other non-OGC adapters cannot reach the OGC subsystem at all, and the runtime
graph is acyclic package-wide rather than only within ``ogc`` and ``transport``.
``waterdata.utils`` not bulk re-exporting private OGC helpers stays in the
fitness functions, because that claim is about the module's ``__all__``.

The OGC consumer list is an allowlist, so a new service module is refused until
someone places it deliberately. It should shrink as private seams move. Any
growth requires explicit architecture review, and a change to the dependency
policy requires this ADR to be superseded.
