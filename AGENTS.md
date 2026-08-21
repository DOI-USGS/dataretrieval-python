# AGENTS.md

## Start here
- **`CONTEXT.md` is the shared vocabulary** — getter, query, chunk, plan, fan-out,
  page, adapter, facade, leaf, transport, collection, profile, effective
  configuration, and the legacy names that are deliberately not renamed. Read it
  before writing code, docstrings, or commit messages; when a term there
  conflicts with a name in the code, the term wins.
- Architectural decisions and their rationale: `docs/source/architecture/decisions/`
  (ADRs, referenced by number throughout the code and by `.importlinter`).
- Contributor workflow, style, and the quality gates in detail: `CONTRIBUTING.md`.

## How the tree is organized
Use `ls`/`grep` for the file list; what follows is the placement logic, so you
can predict where a thing lives.

- `dataretrieval/` — the public surface is one *adapter* module per service,
  named for the service (`nldi`, `nwdc`, `ngwmn`, `streamstats`, `wqp`, and
  legacy `nwis`); each owns that service's URLs, parameters, and response
  quirks. `waterdata/` is the one adapter large enough to be a package, split by
  collection family; its `api.py` is a compatibility facade holding no logic.
  Everything else in the package is shared machinery the adapters sit on top of
  — configuration, credentials, progress, exceptions, code tables, response
  formats. Shared machinery below the adapter layer must not know about any
  particular service.
- `dataretrieval/ogc/` — the OGC API protocol machinery (chunk planning,
  filters, request building, response shaping). Shared by the two OGC services
  only; `.importlinter` refuses any other importer.
- `dataretrieval/transport/` — service-neutral request machinery (HTTP, retry,
  pagination, fan-out). It names no service and no protocol, and is not public API.
- Leading-underscore top-level modules are private; the dependency-free *leaves*
  sit at the floor of the stack so anything may use them without pulling in the
  rest of the package. Check for an existing leaf before writing a small helper.
- **`.importlinter` is the map.** Its `layers` contract lists every top-level
  module in dependency order and is `exhaustive = True`, so it is both the
  authoritative statement of where a module sits and the thing that fails when a
  new module has no home. Read it before adding a module or an import.
- `tests/` — flat, one `*_test.py` per module or concern, organized into four
  dependency-oriented layers (public contract, adapter contract, component,
  cross-component) that `tests/contracts/README.md` defines and assigns files to.
  `architecture_test.py` holds the fitness functions a boundary checker cannot
  express (symbols, `__all__`, AST shape, cycles); pure module-to-module
  direction belongs in `.importlinter` instead.
- `docs/source/` — `reference/` (one page per public module), `userguide/`
  (prose topics), `architecture/` (overview + ADRs), `meta/` (project docs),
  `examples/` (`.nblink` files pointing at `demos/`; the docs build executes them).
- `demos/` — one notebook per Water Data collection or topic, plus
  `hydroshare/` mirroring them per service for HydroShare, and
  `nwqn_data_pull/` as a non-notebook batch-pipeline example. When adding a
  collection, add a demo alongside the existing ones and an `.nblink` in
  `docs/source/examples/`.

## Not part of the repo
- `R/`, `experiments/`, `build/`, `dist/`, `.kiro/`, and any `Untitled*.ipynb` or
  `*_test.ipynb` at the top level are untracked local scratch — don't edit,
  commit, or cite them.
- Exclude `.claude/worktrees/` from searches and edits; stale worktrees there
  pollute results.

## Environment
- `pip install .[test,nldi]` (CI uses pip, not uv, despite `uv.lock`).
  Docs: `pip install .[doc,nldi]`. Gates: `pip install -e .[metrics]`.
- Python >= 3.10; the CI test matrix is 3.10, 3.13, 3.14.

## Commands
- Lint: `ruff check .` and `ruff format --check .` (pinned to the version in
  `.pre-commit-config.yaml` and the CI lint job — keep them aligned).
- Tests: `coverage run -m pytest tests/ && coverage report`, or focused like
  `pytest tests/waterdata_test.py::test_mock_get_samples`. `coverage report` is
  a merge gate: branch coverage with a `fail_under` ratchet in
  `[tool.coverage.report]`. Chase the uncovered *branch*, not the number -- a
  test written to colour a line green catches nothing and costs a maintenance
  slot. If a path is genuinely unreachable, add it to `exclude_also` with a
  reason, or leave the ratchet alone.
- Types: `mypy` (`strict = true` in `pyproject.toml`; CI runs it over the
  PR-merged-into-main, so bare `dict`/`list` annotations fail there even if they
  pass on your branch).
- Structure: `lint-imports`, `xenon --max-absolute C --max-modules B --max-average A dataretrieval`,
  and `complexipy dataretrieval` (max complexity 10). All three gate merges.
- Docs: install docs deps, `ipython kernel install --name "python3" --user`, then
  `make html` from `docs/`. `make docs` adds doctest+linkcheck (network-dependent).

## Testing gotchas
- The suite is offline by default: `addopts = "-m 'not live'"`. Tests marked
  `@pytest.mark.live` hit real USGS services and run on a schedule
  (`.github/workflows/live-api.yml`); run them locally with `pytest tests/ -m live`.
- HTTP is mocked with `pytest-httpx`'s `httpx_mock` fixture plus fixtures under
  `tests/data/`; keep new API tests offline.
- `tests/conftest.py` relaxes the fixture's strict-mode defaults and pins the
  fan-out env (`API_USGS_CONCURRENT=1`, `API_USGS_RETRIES=0`,
  `API_USGS_STALL_TIMEOUT=0`) plus a nonexistent `DATARETRIEVAL_CONFIG`, so tests
  are deterministic and never read a developer's real config. Concurrency and
  retry tests opt back in via `monkeypatch.setenv` inside the test body.

## Error messages
Most callers here are programs — a script, a pipeline stage, an agent — so a
message is the only channel through which a caller can correct itself. Every
raise states the problem and then the move that fixes it, in that order.

- Name the remedy, not just the fault. `"Service not recognized"` gives a caller
  nothing to try next; listing the services it does accept does. For a transport
  failure the remedy is whether to retry, and `transport.pagination.
  paginated_failure_message()` is the model: cause, then `To recover: …`.
- Don't invent a phrasing for a check that recurs. `dataretrieval/_validation.py`
  owns the wording for the shared shapes — bad value in a closed vocabulary
  (`require_one_of`), missing argument (`require_argument`), incomplete group
  (`require_together`), no filter at all (`require_any_of`), and conflicting
  arguments (`require_exactly_one`, `reject_together`). Reach for one before
  hand-writing a message. Neither a service-specific pointer nor an exception
  class is a reason to hand-write: every check takes a `remedy=` for the move it
  cannot derive, and an `error=` for the class to raise (`nwis` passes
  `TypeError`, which its query entry points raised long before this module).
- `require_argument` returns the narrowed value, so use its result rather than
  re-testing for `None` to satisfy mypy — a second, unreachable message beside
  the first is how the two drift apart.
- **Paste the remedy back before trusting it.** Whatever a message names must be
  a real parameter of the function the *caller* called — not a private helper's
  local, not a prose label — and following it literally must produce a working
  call. Messages that read well have failed all three: `datetime_input` was a
  private local no getter accepts, `configure(Configuration(...))` was a silent
  no-op because `configure` is a context manager, `pip install
  dataretrieval[nldi]` globs in zsh, and a navigation missing its `data_source`
  spelled `None` into the URL and returned an empty frame. Run the corrected
  call against the real service; wording review does not catch these.
- Shared checks take the caller's spelling. `_validate_data_source`,
  `_format_api_dates`, and `require_one_of` all accept a `name=` so the subject
  of the message is the argument that was actually passed. A helper that hard-codes
  one noun reports the wrong parameter the moment a second call site reuses it.
- Prefer raising over returning something empty when the library cannot tell
  "no data" from "the service misbehaved": a caller that gets an empty frame has
  no signal to act on. `nldi._query_nldi` is the deliberate exception — a 200
  with a non-JSON body becomes an empty GeoDataFrame by design.

## Implementation notes
- HTTP client is `httpx` (migrated from `requests` in #289); new code uses
  `httpx` and tests mock with `httpx_mock`.
- Public getters return `(DataFrame, metadata)`.
- `dataretrieval/__init__.py` imports the service modules by name and lists them
  in `__all__`; it does not star-import them, so a getter is reached through its
  module (`dataretrieval.nwis.get_record`), never from the top level. `nldi` is
  deliberately absent — it needs `geopandas` at import time, so it is imported on
  demand. `dataretrieval/waterdata/__init__.py` controls Water Data exports via
  `__all__`.
- The `API_USGS_PAT` credential is owned by the `credentials` leaf and applied as
  the `X-Api-Key` header by `transport.http.default_headers()`, which sends it
  only to the host it belongs to. Never hard-code tokens in examples or tests.
- Water Data request builders translate Python kwargs to API spellings
  (`skip_geometry` -> `skipGeometry`, `filter_lang` -> `filter-lang`); tests
  assert exact URLs and query params.
- Multi-value OGC params are comma-joined GETs, except `monitoring-locations`
  which POSTs CQL2 JSON. The OGC edge WAF caps total request bytes (URL + body)
  at ~8200, so `dataretrieval/ogc/chunking.py` auto-splits oversized queries
  across chunks (both GET and POST paths); preserve this when adding new
  list-shaped kwargs.
- NLDI requires `geopandas` at import time (`pip install .[nldi]`); other modules
  fall back to pandas when geopandas is absent.
