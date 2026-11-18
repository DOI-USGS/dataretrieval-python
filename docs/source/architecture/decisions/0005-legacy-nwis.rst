ADR 0005: Quarantine and retire legacy NWIS
===========================================

Status
------

Accepted. Carried out: ``dataretrieval.nwis`` has been removed.

Amended after acceptance under :doc:`0000-documenting-decisions`; the
``Notes`` section records every clause added or corrected.

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
  are retired.
- Removal still requires release notes, replacement checks, and an intentional
  compatibility boundary.

Compliance
----------

``dataretrieval/nwis.py`` is a stub: importing it raises ``ImportError`` naming
the Water Data replacements and the last release that includes the module
(1.4.0). Attribute access after ``import dataretrieval``, which never loads the
stub, raises ``AttributeError`` with the same message from the package
``__getattr__``. ``tests/contracts/removed_modules_test.py`` covers both.

Notes
-----

The module was removed before the 2027-05-06 horizon, because USGS is
decommissioning the waterservices endpoints it called. The Compliance section
was rewritten at removal: it described the deprecation tests that verified one
warning per public call and named Water Data replacements, and the
``nwis-quarantine`` contract in ``.importlinter``. Both were removed with the
module.

The Compliance sentence on attribute access was added after removal:
``import dataretrieval`` followed by ``dataretrieval.nwis`` raised a bare
``AttributeError`` that named no replacement.
