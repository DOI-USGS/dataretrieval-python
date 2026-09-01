ADR 0007: Organize service adapters behind stable facades
=========================================================

Status
------

Accepted

Amended after acceptance under :doc:`0000-documenting-decisions`; the
``Notes`` section records every clause added or corrected.

Context
-------

A service facade can remain stable while its implementation grows for unrelated
upstream collections. Keeping every Water Data getter in one module coupled
together changes to time series, monitoring metadata, field measurements,
reference catalogs, Samples, statistics, and generalized CQL queries. Active
service modules also relied on Python's implicit wildcard-export behavior,
making their intended public surfaces difficult to distinguish from imported
helpers.

Decision
--------

``dataretrieval.waterdata.api`` is a compatibility facade with no collection
logic. Implementation functions are grouped by collection family in
``time_series``, ``metadata``, ``measurements``, ``reference``, ``samples``, and
``cql``. Existing focused modules continue to own ratings, nearest-value
selection, Statistics API execution, shared Water Data policy, and type
vocabularies.

The facade re-exports the established functions and preserves their signatures,
their identity at ``dataretrieval.waterdata``, and the private Samples
constants that compatibility tests rely on. It does not rewrite their
``__module__``: each function reports the family module that defines it, so a
traceback names a file that contains code. Collection-family modules do not
import one another; shared behavior belongs in Water Data policy, OGC, or
transport modules.

Active service and focused implementation modules declare explicit ``__all__``
exports. Deprecated NWIS remains outside this modernization. Service adapters do
not import another adapter's implementation to obtain transport behavior.

The ``__module__`` rule above is scoped to this facade, where the family module
is a real file a traceback can name. It is not a package-wide prohibition: the
legacy ``dataretrieval.utils`` names are split across private modules by
dependency and *do* report the documented path, because there the alternative is
a public, documented import location pointing at a private module.

**Typed getters are the surface; exactly one generic escape hatch sits beside
them.** ``cql`` is the only untyped member of the collection families, and
deliberately so. The alternative in one direction -- a single generic query
function replacing the typed getters -- gives up the parameter documentation
and validation that are most of these getters' value. The alternative in the
other -- a ``cql=`` passthrough on every family -- multiplies the escape hatch
by the number of collections while making each family's surface partly untyped.
One hatch, named as such, keeps both properties.

**Identifier columns are parsed as text.** This one clause applies
package-wide, legacy NWIS included: it is about what an adapter hands back, not
how it is organized. HUCs, parameter codes, FIPS codes, and monitoring-location
identifiers (``site_no`` in NWIS) carry significant leading zeros, and a bare
``read_csv`` infers them as integers and
drops those zeros -- ``"00060"`` becomes ``60``, so the value is silently wrong
rather than missing. Every adapter reading a USGS tabular response names its
identifier columns as ``str`` before parsing, which is why a two-pass header
read is not a redundancy to be optimized away.

Return contracts remain service-specific. Tabular services generally return a
``(DataFrame, metadata)`` pair, while NLDI returns geospatial values directly,
StreamStats exposes response/domain objects, and ratings return parsed tables or
raw catalog features. Uniformity is not a reason to break these established
contracts.

Consequences
------------

- Collection changes touch fewer implementation and test files.
- Existing package and ``waterdata.api`` import paths remain stable.
- Explicit exports make accidental public-surface growth reviewable.
- More modules mean a facade to maintain, plus executable signature and export
  snapshots.
- Tests are described as public-contract, adapter-contract, component, or
  cross-component layers without moving established files.

Compliance
----------

``tests/contracts/public_api_test.py`` freezes Water Data imports, signatures,
facade identity, and compatibility names. ``tests/architecture_test.py``
requires a logic-free facade, exact active-service exports, and separate OGC
request construction and schema execution. ``.importlinter`` keeps the
collection families independent of each other, holds the facade-only consumers
(NGWMN and ``waterdata.cql``) to the OGC facade, and prevents one adapter from
importing another. The identifier-column rule is covered by
``tests/nwdc_test.py::test_huc12_id_kept_as_string_with_leading_zero`` and the equivalent
leading-zero assertions in the WQP and NWIS adapter tests.

Notes
-----

The ``__module__`` scoping note and the escape-hatch and identifier-column
clauses were added after the original decision; the rest of the record is
unchanged. They consolidate under ADR 0000 the rules the code was carrying in
prose. The scoping note in particular records why ``_querying.py`` reassigning
``__module__`` is not a violation of this record, a question an audit raised.
