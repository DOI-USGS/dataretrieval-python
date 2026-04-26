# DuckDB connector — design notes

Notes captured before implementation. The goal is a prototype that lets users
query USGS waterdata endpoints via DuckDB SQL.

## Package conventions to follow

- **Style**: ruff-managed, py38 target, double quotes, docstring code
  formatted at width 72.
- **Type hints**: full hints, use `from __future__ import annotations`,
  `str | list[str] | None` style.
- **Docstrings**: numpy-style (Parameters / Returns sections with dashes).
  Module top has a short one-paragraph summary.
- **Naming**: snake_case functions, leading underscore for private helpers,
  UPPER_SNAKE_CASE module constants.
- **Logging**: `logger = logging.getLogger(__name__)` at module top.
- **Errors**: raise `ImportError` with a pip install hint when an optional
  dep is missing, `ValueError` for bad arguments, `RuntimeError` for
  unexpected response shapes.

## Optional-dependency pattern (mirror `geopandas`)

`dataretrieval/waterdata/utils.py:24`:
```python
try:
    import geopandas as gpd
    GEOPANDAS = True
except ImportError:
    GEOPANDAS = False
```

For the connector we will:
- attempt `import duckdb` at the top of the module;
- on `ImportError` set a sentinel and raise a clear `ImportError` from
  the public entry point telling users to `pip install dataretrieval[duckdb]`.

`pyproject.toml` extras (current state):
```toml
[project.optional-dependencies]
test = [...]
doc  = [...]
nldi = ['geopandas>=0.10']
```
Add a new `duckdb = ["duckdb>=1.0.0"]` extra.

## Endpoint shape

All `dataretrieval.waterdata.api.get_*` functions return
`tuple[pandas.DataFrame | geopandas.GeoDataFrame, BaseMetadata]`. Pagination
is fully handled inside `_walk_pages`, so a single call is the whole
result set.

Endpoints we will expose first (highest user value, all OGC):
- `get_monitoring_locations` — site discovery (returns GeoDataFrame when
  geopandas installed)
- `get_daily` — daily values
- `get_continuous` — instantaneous values (≤3y per call by API contract)
- `get_time_series_metadata` — what's available at each site
- `get_latest_continuous`, `get_latest_daily` — most recent obs

Each accepts `monitoring_location_id`, `parameter_code`, etc. as scalar or
list, plus the new `filter` / `filter_lang` CQL passthrough (#238).

## Architecture (after wqp + per-source split)

After surveying `wqp.py` we moved to a per-source connector package:

```
dataretrieval/duckdb_connectors/
├── _base.py        # _require_duckdb, _flatten_geometry, _BaseConnection
├── waterdata.py    # WaterdataConnection + connect()
└── wqp.py          # WQPConnection + connect()  (handles legacy / WQX3 flag)
```

`dataretrieval/duckdb_connector.py` stays as a thin alias re-exporting
the waterdata connector so the older import path keeps working.

WQP differences vs waterdata that the connector has to absorb:

* WQP getters take `**kwargs` (CamelCase URL params) rather than fully
  enumerated signatures, so the connector can't validate kwargs — it
  just forwards them.
* Two parallel schemas (legacy WQX vs WQX 3.0) controlled by `legacy=`
  per call. The connection holds a default that callers can override
  per call.
* `ssl_check` is also a connection-level default.
* WQP returns a custom `WQP_Metadata` instead of `BaseMetadata`, but
  since the connector only consumes the DataFrame this doesn't matter.

Joining across the two sources: each connector owns its own duckdb
connection, so to join you either materialise to a DataFrame and
`.con.register(name, df)` it onto the other connection, or open a
single `duckdb.connect()` directly and pass it into both
`WaterdataConnection(con)` and `WQPConnection(con)` manually.

Other modules surveyed but not given connectors:

* `nwis` — deprecated; users are being pushed to waterdata.
* `nldi` — returns GeoDataFrames / dicts; spatial-only, different
  contract; possible later.
* `streamstats`, `nadp` — return non-tabular data (Watershed objects,
  zip files / TIFs); not connector candidates.
* `ngwmn` — does return DataFrames but very narrow scope; could add
  later if needed.
* `samples` — already covered by the waterdata connector via
  `wd.samples(...)` (the `samples.py` module is a deprecated shim
  that forwards to `waterdata.get_samples`).

## DuckDB integration choices

DuckDB ≥0.8 supports registering Python objects via `con.register(name, df)`
which makes a pandas DataFrame queryable as a view. That's the simplest
path and works with any DuckDB build — no compiled extension needed for a
prototype.

DuckDB also supports `create_function` for **scalar** UDFs but **table**
UDFs (table-valued functions callable as `FROM tvf(...)`) require either
the in-progress python table-function API or a workaround. For a
prototype the simpler API is preferable — register helper *methods* on a
connection that take kwargs, fetch a DataFrame, register it under a
caller-chosen name, and return a `duckdb.DuckDBPyRelation`. The user
writes:

```python
con = waterdata_duckdb.connect()
sites = con.monitoring_locations(state_name="Illinois")  # relation
con.sql("SELECT * FROM sites WHERE site_type = 'Stream'")
```

This keeps it pythonic and lets users compose with arbitrary SQL,
including joins across two registered relations.

A second affordance: a `con.sql_table(name, fn, **kwargs)` that registers
a one-shot DataFrame view by name, so:

```python
con.sql_table("daily", waterdata.get_daily,
              monitoring_location_id="USGS-05586100",
              parameter_code="00060", time="2023/2024")
con.sql("SELECT date_trunc('month', time) AS m, avg(value) "
        "FROM daily GROUP BY 1 ORDER BY 1")
```

## Geometry handling

When geopandas is available, `get_monitoring_locations` returns a
GeoDataFrame with a `geometry` column. DuckDB has a `spatial` extension
that understands WKB/WKT but it isn't loaded by default. Safe path for
the prototype: convert geometry to WKT string and add `longitude` /
`latitude` columns. That keeps the relation queryable from plain DuckDB
without extension setup.

## Tests

Existing tests (`tests/waterdata_test.py`) use `requests-mock` against
real URLs. For our connector we don't need to re-test the HTTP layer —
we should mock the waterdata `get_*` functions directly with
`unittest.mock.patch` (this is the pattern in
`tests/waterdata_nearest_test.py`) and assert that:

1. `connect()` raises a clean `ImportError` if duckdb isn't installed.
2. Helper methods invoke the underlying `get_*` with the kwargs we passed.
3. The returned object is a queryable DuckDB relation.
4. `sql_table` registers a view that returns the same row count as the
   source DataFrame.
5. Geometry conversion produces WKT + lon/lat columns and drops the
   GeoDataFrame `geometry` column (or keeps it as WKT) without needing
   the spatial extension.

## Notebook

Goes in `demos/`. Should:
- show a real query against `api.waterdata.usgs.gov`
- demonstrate something easier in SQL than pandas (window function over
  daily flow, monthly aggregation, join of monitoring-location metadata
  to daily values)
- gracefully note that this needs `pip install dataretrieval[duckdb]`

Demos are excluded from ruff (`extend-exclude = ["demos"]` in
pyproject.toml) so we don't have to fight formatting there.
