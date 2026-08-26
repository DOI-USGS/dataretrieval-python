ADR 0005: Quarantine and retire legacy NWIS
===========================================

Status
------

Accepted

Context
-------

The legacy ``dataretrieval.nwis`` facade overlaps modern Water Data APIs and
contains functions backed by retired services. Existing users still require a
migration window, while new code should not acquire dependencies on behavior
scheduled for removal.

Decision
--------

Keep ``dataretrieval.nwis`` importable during its published deprecation window
and remove it on or after 2027-05-06 through the project's compatibility and
release process.

Add no new NWIS capabilities. Permit compatibility, security, and correctness
fixes only. Every active deprecated function should warn once per user call and
name a supported replacement where one exists. Modern modules must never import
NWIS.

Do not treat WQP's legacy result profiles as part of NWIS retirement; they have
separate upstream constraints and migration behavior.

Consequences
------------

- Existing users have a predictable migration period.
- Maintainers avoid investing in a second implementation of modern retrieval
  behavior.
- Legacy integration tests may require special handling as upstream endpoints
  disappear.
- Removal still requires release notes, replacement checks, and an intentional
  compatibility boundary.

Compliance
----------

Deprecation tests verify one warning per public call and validate named Water
Data replacements. The ``nwis-quarantine`` contract in ``.importlinter``
prevents modern package modules from importing ``dataretrieval.nwis``, directly
or through an intermediary.
