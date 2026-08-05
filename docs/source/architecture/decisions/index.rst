Architecture Decision Records
=============================

Architecture Decision Records (ADRs) capture why a significant structural or
cross-cutting choice was made, the trade-offs it accepts, and how compliance is
checked. They complement code and API documentation rather than repeating
implementation details.

Statuses are ``Proposed``, ``Accepted``, ``Superseded``, or ``Rejected``. An
accepted decision is not edited to reverse its meaning; a later ADR supersedes
it and links back to the old record. Keep records concise and commit them with
the change that makes the decision effective.

Use :doc:`template` when proposing a decision. Number accepted and proposed
records sequentially.

.. toctree::
   :maxdepth: 1

   0001-modular-monolith
   0002-sync-api-async-internals
   0003-dependency-direction
   0004-error-retry-resume
   0005-legacy-nwis
   0006-api-neutral-transport
   template
