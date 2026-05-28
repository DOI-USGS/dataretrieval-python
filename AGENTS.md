# AGENTS.md

## Scope
- Python code is in `dataretrieval/`; `dataretrieval/waterdata/` is the modern USGS Water Data API, `dataretrieval/nwis.py` is legacy/deprecated.
- `R/dataRetrieval/` is the R project copy; leave it alone unless the task asks for R work.
- Exclude `.claude/worktrees/` from searches and edits; it contains stale worktrees that pollute results.

## Example Notebooks
- `demos/*.ipynb` — top-level Water Data tour: `USGS_WaterData_Introduction_Examples.ipynb` is the entry point; `_ContinuousData_`, `_DailyStatistics_`, `_DiscreteSamples_`, `_ReferenceLists_` cover individual collections; `WaterData_demo.ipynb`, `peak_streamflow_trends.ipynb`, and `R Python Vignette equivalents.ipynb` are standalone walkthroughs.
- `demos/hydroshare/*.ipynb` — per-service HydroShare examples (NLDI, NWIS WaterUse, and Water Data DailyValues / GroundwaterLevels / Measurements / ParameterCodes / Peaks / Ratings / Samples / SiteInfo / SiteInventory / Statistics / UnitValues). Mirror these when adding examples for a new collection.
- `demos/nwqn_data_pull/` — non-notebook example: a lithops/Docker batch pipeline (`retrieve_nwqn_samples.py`, `retrieve_nwqn_streamflow.py`) with its own `README.md`.
- Any `Untitled*.ipynb`, `*_test.ipynb`, or notebooks not listed here are untracked local scratch; ignore them.

## Environment
- Use `pip install .[test,nldi]` (CI uses pip, not uv despite `uv.lock`). Docs: `pip install .[doc,nldi]`.

## Commands
- Lint: `ruff check .` and `ruff format --check .`.
- Tests: `coverage run -m pytest tests/ && coverage report -m`, or focused like `pytest tests/waterdata_test.py::test_mock_get_samples`.
- Docs: install docs deps, `ipython kernel install --name "python3" --user`, then `make html` from `docs/`. `make docs` adds doctest+linkcheck (network-dependent).

## Testing Gotchas
- Tests mock HTTP with `pytest-httpx`'s `httpx_mock` fixture and fixtures under `tests/data/`; keep new API tests offline. `tests/conftest.py` relaxes the fixture's strict-mode defaults (unused mocks and unmocked requests are tolerated) so rerun-on-failure works.
- `tests/nwis_test.py::test_nwis_service_live` hits live NWIS.
- `tests/nadp_test.py` is module-skipped (NADP deprecated).
- `tests/waterdata_test.py` and `tests/waterdata_ratings_test.py` skip on Python <3.10, so a 3.9 run does not cover them.

## Implementation Notes
- HTTP client is `httpx` (migrated from `requests` in #289); new code should use `httpx` and tests should mock with `httpx_mock`.
- Public download helpers return `(DataFrame, metadata)`.
- `dataretrieval/__init__.py` star-imports service modules; `dataretrieval/waterdata/__init__.py` controls Water Data exports via `__all__`.
- `dataretrieval.waterdata.utils._default_headers()` adds `X-Api-Key` from `API_USGS_PAT`; never hard-code tokens in examples or tests.
- Water Data request builders translate Python kwargs to API spellings (`skip_geometry` -> `skipGeometry`, `filter_lang` -> `filter-lang`); tests assert exact URLs/query params.
- Multi-value OGC params are comma-joined GETs, except `monitoring-locations` which POSTs CQL2 JSON. The OGC edge WAF caps total request bytes (URL + body) at ~8200, so `dataretrieval/waterdata/chunking.py` auto-splits oversized queries across sub-requests (both GET and POST paths); preserve this when adding new list-shaped kwargs.
- NLDI requires `geopandas` at import time (`pip install .[nldi]`); other modules fall back to pandas when geopandas is absent.
