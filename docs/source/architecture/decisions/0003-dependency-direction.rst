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
- ``dataretrieval.ogc`` must not depend on the mixed legacy ``utils`` module;
  shared scoped state lives in a dependency-free leaf instead.
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
the layer stack, the allowlist of OGC consumers, the facade-only seam, OGC's
independence from legacy utilities, the NWIS quarantine, and collection-family
independence; ``lint-imports`` checks all of it against the transitive import
graph in pre-commit and CI. A boundary that legitimately moves is one edit, in
that file, alongside the ADR it cites.

These rules were previously asserted a second time in
``tests/architecture_test.py``, by hand-parsing the AST. That duplication is
gone. The tests now cover only what the configured contracts cannot express —
which symbols cross a seam, what a module's declared exports are, the AST shape
of a facade, boundaries that have to be asserted positively rather than
forbidden, and full-graph cycle detection. Import Linter's
``acyclic_siblings`` contract does not detect a cycle between a package facade
and one of its descendants. A new rule that is purely about module-to-module
direction belongs in ``.importlinter``.

Named contracts verify the current boundaries: the only OGC dependency of
NGWMN and ``waterdata.cql`` is the facade, ``ogc.shaping`` does not depend on
``ogc.engine``, Water Use and the other non-OGC adapters cannot reach the OGC
subsystem at all. The fitness functions verify that the runtime graph is
acyclic package-wide rather than only within ``ogc`` and ``transport``.
``waterdata.utils`` not bulk re-exporting private OGC helpers stays there too,
because that claim is about the module's
``__all__``.

The OGC consumer list is an allowlist, so a new service module is refused until
someone places it deliberately. It should shrink as private seams move. Any
growth requires explicit architecture review, and a change to the dependency
policy requires this ADR to be superseded.
