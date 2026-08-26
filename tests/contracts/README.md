# Test layers

The suite uses four dependency-oriented layers without moving established tests:

- **Public contract** (`tests/contracts/`): imports, exports, signatures, return
  annotations, metadata/error promises, and compatibility paths. These tests use
  public modules and no live services.
- **Adapter contract** (`waterdata_test.py`, `ngwmn_test.py`, `nwdc_test.py`,
  `wqp_test.py`, `nldi_test.py`, `streamstats_test.py`): service request wiring,
  response parsing, and documented protocol behavior.
- **Component** (`transport_test.py`, `waterdata_chunking_test.py`,
  `waterdata_queryables_test.py`, `rdb_test.py`): one internal responsibility in
  isolation.
- **Cross-component** (`architecture_test.py`, `headers_host_scoping_test.py`,
  `waterdata_progress_test.py`): dependency fitness functions and behavior that
  spans adapters, OGC, transport, or security boundaries.

Live API cases remain in their existing adapter files and are not part of the
public-contract layer.
