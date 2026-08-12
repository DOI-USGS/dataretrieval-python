Architecture Decision Records
=============================

Architecture Decision Records (ADRs) capture why a significant structural or
cross-cutting choice was made, the trade-offs it accepts, and how compliance is
checked. They complement code and API documentation rather than repeating
implementation details.

Statuses are ``Proposed``, ``Accepted``, ``Superseded``, or ``Rejected``. Do not
edit an accepted decision to reverse its meaning; a later ADR supersedes it and
links back to the old record. Keep records concise and commit them with the
change that makes the decision effective.

Use :doc:`template` when proposing a decision. Number accepted and proposed
records sequentially.

.. toctree::
   :maxdepth: 1

   0001-modular-monolith
   0002-sync-api-async-internals
   0003-dependency-direction
   0004-error-retry-resume
   0005-legacy-nwis
   0006-service-neutral-transport
   0007-adapter-facades
   0008-fan-out-execution
   0009-layered-configuration
   0010-adapter-scoped-settings
   0011-configuration-profiles
   0012-pydantic-settings
   template
