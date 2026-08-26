ADR 0001: Retain a modular monolith with service adapters
=========================================================

Status
------

Accepted

Context
-------

``dataretrieval`` is one Python library serving users who often combine several
hydrologic data sources in one process or notebook. The upstream services have
different protocols and schemas, but the package has one release lifecycle and
maintainer group. Distributed deployment or separately versioned service
packages would add operational and compatibility cost without improving the
library's primary use cases.

Decision
--------

Maintain one installable distribution organized as a modular monolith. Expose
functions grouped by data portal. Keep service- and protocol-specific adapters
independent behind those facades, and share infrastructure only where its
contract is genuinely API-neutral.

Treat the OGC subsystem as a protocol component used by Water Data and NGWMN,
not as a universal service framework. Do not force NLDI, StreamStats, WQP, or
Water Use into OGC-shaped return values or paging semantics.

Consequences
------------

- Users install and version one coherent package.
- Cross-service API compatibility can be tested in one pipeline.
- Component boundaries are source-level rather than deployment boundaries and
  therefore require import checks and review discipline.
- Shared infrastructure must remain small enough that it does not become a god
  module.
- A new service should begin as its own adapter and earn shared abstractions
  through demonstrated duplication rather than up-front generalization.

Compliance
----------

``.importlinter`` prevents shared OGC infrastructure from importing service
adapters and prevents modern modules from depending on legacy NWIS;
``lint-imports`` checks it in pre-commit and CI. A package-wide fitness function
requires the runtime import graph, including package facades, to stay acyclic.
The installed-wheel CI job verifies that the whole monolith ships as one usable
artifact.
