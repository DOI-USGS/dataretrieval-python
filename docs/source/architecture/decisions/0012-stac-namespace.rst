ADR 0012: Namespace raw STAC catalog operations
================================================

Status
------

Accepted

Context
-------

The Water Data STAC API exposes protocol-shaped catalog discovery, item lookup,
queryables, and search operations. Prefixing every operation with the protocol
name would make the main Water Data facade wider, repeat that qualifier on every
operation, and still leave generic concepts such as queryables competing with
the facade's existing tabular OGC helpers. These STAC names were developed on an
unreleased feature branch, so they have no published compatibility contract.

The analysis-ready ratings workflow has a different role: callers request a
monitoring location and receive parsed rating tables rather than navigating raw
STAC documents.

Decision
--------

Expose raw catalog operations only through the public ``waterdata.stac``
namespace: ``get_catalog``, ``get_conformance``, ``get_collections``,
``get_collection``, ``get_items``, ``get_item``, ``get_queryables``, and
``search``. Do not export flat prefixed aliases from ``waterdata`` because those
draft names were never released.

Keep ``waterdata.get_ratings`` on the main facade as the analysis-ready ratings
API. Keep the existing tabular ``waterdata.get_queryables`` distinct from raw
``waterdata.stac.get_queryables``.

Consequences
------------

- Related raw STAC operations are discoverable together without widening the
  main facade by eight prefixed names.
- Call sites make the raw STAC boundary explicit.
- ``waterdata`` publicly exports one namespace object in place of eight
  functions.
- Code written against the unreleased flat draft names must adopt the nested
  namespace; no deprecation period applies.
- Documentation and executable public-surface snapshots must be updated when
  the nested STAC surface changes.

Compliance
----------

``tests/contracts/public_api_test.py`` freezes the ``waterdata`` export surface.
``tests/waterdata_stac_test.py`` freezes ``waterdata.stac.__all__``, verifies all
eight nested operations, and rejects every former flat draft name. The full
Sphinx build exercises the documented namespace and example notebook.
