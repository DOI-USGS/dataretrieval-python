ADR 0007: Organize service adapters behind stable facades
=========================================================

Status
------

Accepted

Context
-------

A service facade can remain stable while its implementation grows for unrelated
upstream collections. Keeping every Water Data getter in one module coupled
changes to time series, monitoring metadata, field measurements, reference
catalogs, Samples, statistics, and generalized CQL queries. Active service
modules also relied on Python's implicit wildcard-export behavior, making their
intended public surfaces difficult to distinguish from imported helpers.

Decision
--------

``dataretrieval.waterdata.api`` is a compatibility facade with no collection
logic. Implementation functions are grouped by collection family in
``time_series``, ``metadata``, ``measurements``, ``reference``, ``samples``, and
``cql``. Existing focused modules continue to own ratings, nearest-value
selection, Statistics API execution, shared Water Data policy, and type
vocabularies.

The facade re-exports the established functions and preserves their signatures,
their identity at ``dataretrieval.waterdata``, and the private Samples constants
compatibility tests rely on. It does not rewrite their ``__module__``: each
function reports the family module that defines it, so a traceback names a file
that contains code. Collection-family modules do not
import one another; shared behavior belongs in Water Data policy, OGC, or
transport modules.

Active service and focused implementation modules declare explicit ``__all__``
exports. Deprecated NWIS remains outside this modernization. Service adapters do
not import another adapter's implementation to obtain transport behavior.

Return contracts remain service-specific. Tabular services generally return a
``(DataFrame, metadata)`` pair, while NLDI returns geospatial values directly,
StreamStats exposes response/domain objects, and ratings return parsed tables or
raw catalog features. Uniformity is not a reason to break these established
contracts.

Consequences
------------

- Collection changes have a smaller implementation and test blast radius.
- Existing package and ``waterdata.api`` import paths remain stable.
- Explicit exports make accidental public-surface growth reviewable.
- More modules require a maintained facade and executable signature/export
  snapshots.
- Tests are described as public-contract, adapter-contract, component, or
  cross-component layers without forcing a disruptive move of established
  files.

Compliance
----------

``tests/contracts/public_api_test.py`` freezes Water Data imports, signatures,
facade identity, and compatibility names. ``tests/architecture_test.py``
requires a logic-free facade, exact active-service exports, and separate OGC
request construction and schema execution. ``.importlinter`` keeps the
collection families independent of each other, holds NGWMN to the OGC facade,
and prevents lateral adapter reach-through.
