"""Executable fitness functions that complement the dependency contracts.

Plain dependency direction -- who may import whom, and in which direction -- is
declared in ``.importlinter`` and checked by ``lint-imports`` in pre-commit and
CI. Those rules used to be asserted here too, and are not any more: one rule
enforced in two places is one rule that gets updated in one place.

What remains is everything a boundary checker cannot see, because an import
graph has no opinion about it:

* which *symbols* cross a boundary, not just which modules (``ogc.engine``'s
  compatibility surface, ``ogc.requests`` borrowing header policy but not the
  executing calls);
* the declared public surface -- ``__all__`` presence, ownership, and the
  facade union;
* the AST shape of a module, such as a facade proving it contains no logic, or
  a call proving it passes a destination URL;
* an import that must *exist*: ``lint-imports`` can forbid an edge, never
  require one;
* package-wide acyclicity, including package facades (ADR 0003).

Adding a rule here that is purely about module-to-module direction is a
regression -- put it in ``.importlinter`` instead.
"""

from __future__ import annotations

import ast
import configparser
import functools
import sys
from graphlib import CycleError, TopologicalSorter
from importlib.util import resolve_name
from pathlib import Path

PACKAGE_ROOT = Path(__file__).parents[1] / "dataretrieval"

#: How many request-building names ``ogc.engine`` currently needs. A ceiling
#: rather than an exact list allows renames and deletions without weakening the
#: rule that orchestration must not absorb request construction again.
_MAX_ENGINE_REQUEST_IMPORTS = 5


def _module_name(path: Path) -> str:
    """Return the import name for one Python file below ``PACKAGE_ROOT``."""
    parts = list(path.relative_to(PACKAGE_ROOT.parent).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


@functools.cache
def _package_modules() -> frozenset[str]:
    """Return every importable module implemented by this package."""
    return frozenset(_module_name(path) for path in PACKAGE_ROOT.rglob("*.py"))


def _is_type_checking(test: ast.expr) -> bool:
    """Whether an ``if`` condition is ``TYPE_CHECKING``."""
    return (
        isinstance(test, ast.Name)
        and test.id == "TYPE_CHECKING"
        or (
            isinstance(test, ast.Attribute)
            and isinstance(test.value, ast.Name)
            and test.value.id == "typing"
            and test.attr == "TYPE_CHECKING"
        )
    )


def _resolve_from(current_module: str, path: Path, node: ast.ImportFrom) -> list[str]:
    """Resolve one absolute or relative ``from`` import to module names."""
    if node.level == 0:
        base_module = node.module or ""
    else:
        package = (
            current_module
            if path.name == "__init__.py"
            else current_module.rpartition(".")[0]
        )
        base_module = resolve_name("." * node.level + (node.module or ""), package)

    dependencies: set[str] = set()
    for alias in node.names:
        candidate = ".".join(part for part in (base_module, alias.name) if part)
        if candidate in _package_modules():
            dependencies.add(candidate)
        elif base_module:
            dependencies.add(base_module)
    return sorted(dependencies)


class _RuntimeImportVisitor(ast.NodeVisitor):
    """Collect imports that can execute, excluding ``TYPE_CHECKING`` blocks."""

    def __init__(self, current_module: str, path: Path) -> None:
        self.current_module = current_module
        self.path = path
        self.modules: set[str] = set()

    def visit_If(self, node: ast.If) -> None:
        if _is_type_checking(node.test):
            for statement in node.orelse:
                self.visit(statement)
            return
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        self.modules.update(alias.name for alias in node.names)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        self.modules.update(_resolve_from(self.current_module, self.path, node))


# Pure over an unchanging tree and called from several tests; without caching
# the suite re-parses the same package files on each call.
@functools.cache
def _runtime_imports(path: Path) -> set[str]:
    module = _module_name(path)
    visitor = _RuntimeImportVisitor(module, path)
    visitor.visit(ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))
    return visitor.modules


def _package_import_graph() -> dict[str, set[str]]:
    """Return runtime edges between modules that belong to this package."""
    paths = sorted(PACKAGE_ROOT.rglob("*.py"))
    return {
        _module_name(path): _runtime_imports(path) & _package_modules()
        for path in paths
    }


def _literal_exports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__all__"
            for target in node.targets
        ):
            return set(ast.literal_eval(node.value))
    raise AssertionError(f"{path.relative_to(PACKAGE_ROOT.parent)} has no __all__")


def test_exceptions_has_no_runtime_third_party_dependency() -> None:
    """The shared error-policy leaf must remain cheap and cycle-safe."""
    imports = _runtime_imports(PACKAGE_ROOT / "exceptions.py")
    roots = {module.partition(".")[0] for module in imports}
    third_party = roots - sys.stdlib_module_names - {"dataretrieval"}
    assert not third_party, (
        "dataretrieval.exceptions gained runtime third-party dependencies: "
        f"{sorted(third_party)}"
    )


def test_runtime_import_graph_is_acyclic() -> None:
    """The complete runtime module graph, including facades, must be a DAG."""
    try:
        tuple(TopologicalSorter(_package_import_graph()).static_order())
    except CycleError as exc:
        cycle = " -> ".join(exc.args[1])
        raise AssertionError(f"Runtime import cycle: {cycle}") from exc


def test_settings_is_a_first_party_leaf() -> None:
    """Settings resolution must stay importable from anywhere.

    ``dataretrieval.settings`` is read by ``utils`` (headers), ``ogc.chunking``
    (concurrency), ``ogc.retry``, and ``ogc.progress``. If it imported any of
    them it would create a cycle, so ``dataretrieval.exceptions`` is its one
    allowed first-party import: the taxonomy leaf, itself free of first-party
    and runtime third-party imports (asserted above), so depending on it adds no
    weight and cannot cycle. ``ConfigurationError`` lives there because settings
    resolve on the request path, so a broken settings file must be catchable as
    ``except DataRetrievalError`` like any other failure of that call.

    The *third-party* half of this contract was withdrawn by ADR 0012: the
    module is built on ``pydantic-settings``, which is a runtime dependency of
    the package. What the roster below still buys is that the list stays
    deliberate -- a leaf that quietly grew ``httpx`` or ``pandas`` would make the
    cheapest module in the package expensive, and every adapter imports this one.
    """
    imports = _runtime_imports(PACKAGE_ROOT / "settings.py")
    first_party = {
        name
        for name in imports
        if name.startswith("dataretrieval") and name != "dataretrieval.exceptions"
    }
    assert not first_party, (
        "dataretrieval.settings may only import dataretrieval.exceptions: "
        f"{sorted(first_party)}"
    )
    roots = {module.partition(".")[0] for module in imports}
    # Static analysis sees both sides of the version guard. Python 3.10's stdlib
    # inventory does not yet include the unreachable ``tomllib`` branch.
    allowed = {"dataretrieval", "pydantic", "pydantic_settings", "tomli", "tomllib"}
    third_party = roots - sys.stdlib_module_names - allowed
    assert not third_party, (
        "dataretrieval.settings gained third-party dependencies beyond the "
        f"settings stack: {sorted(third_party)}"
    )


def test_engine_request_import_surface_does_not_grow() -> None:
    """Engine imports only request names it uses and may not grow a new hub.

    An import that nothing uses is dead weight, and a used name past the cap
    means request construction is migrating back into engine.
    """
    path = PACKAGE_ROOT / "ogc" / "engine.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "dataretrieval.ogc.requests"
        for alias in node.names
    }
    assert len(imported) <= _MAX_ENGINE_REQUEST_IMPORTS, (
        "ogc.engine imports more request names than before; use the canonical "
        "requests module instead of expanding engine's request surface.\n"
        f"limit={_MAX_ENGINE_REQUEST_IMPORTS}\nobserved={sorted(imported)}"
    )
    referenced = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }
    unused = sorted(imported - referenced)
    assert not unused, f"ogc.engine has unused request imports: {unused}"

    # Every imported name must resolve in ``requests``; a stale import of a name
    # that moved or was deleted fails as soon as engine loads.
    from dataretrieval.ogc import requests as ogc_requests

    missing = sorted(name for name in imported if not hasattr(ogc_requests, name))
    assert not missing, (
        f"ogc.engine re-exports names ogc.requests no longer has: {missing}"
    )


# --- Seams that are about symbols and call sites, not module direction ---


def test_ngwmn_uses_ogc_facade() -> None:
    """NGWMN must retain its positive dependency on the public OGC facade."""
    ngwmn_imports = _runtime_imports(PACKAGE_ROOT / "ngwmn.py")
    assert "dataretrieval.ogc" in ngwmn_imports, (
        "NGWMN must retain its dependency on the OGC facade; internal OGC "
        "imports are forbidden separately by .importlinter. "
        f"Found: {sorted(ngwmn_imports)}"
    )


def test_waterdata_utils_is_not_an_ogc_reexport_hub() -> None:
    """Water Data policy wrappers may not bulk re-export OGC internals."""
    path = PACKAGE_ROOT / "waterdata" / "utils.py"
    ogc_deps = {
        dependency
        for dependency in _runtime_imports(path)
        if dependency == "dataretrieval.ogc"
        or dependency.startswith("dataretrieval.ogc.")
    }
    assert ogc_deps == {"dataretrieval.ogc"}, (
        f"Water Data utils crossed its intended OGC seam: {ogc_deps}"
    )

    exports = _literal_exports(path)

    old_reexports = {
        "GEOPANDAS",
        "_as_str_list",
        "_check_monitoring_location_id",
        "_construct_api_requests",
        "_construct_cql_request",
        "_default_headers",
        "_format_api_dates",
        "_paginate",
        "_raise_for_non_200",
        "_run_sync",
        "_switch_properties_id",
        "_walk_pages",
        "fetch_ogc_request",
    }
    assert exports.isdisjoint(old_reexports), (
        "waterdata.utils regained private OGC re-exports: "
        f"{sorted(exports & old_reexports)}"
    )


def test_default_header_calls_are_target_scoped() -> None:
    """Every production header construction must name the destination URL."""
    violations: list[str] = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            function_name = (
                node.func.id
                if isinstance(node.func, ast.Name)
                else node.func.attr
                if isinstance(node.func, ast.Attribute)
                else None
            )
            if function_name not in {"_default_headers", "default_headers"}:
                continue
            has_target = bool(node.args) or any(
                keyword.arg == "target_url" for keyword in node.keywords
            )
            if not has_target:
                violations.append(
                    f"{path.relative_to(PACKAGE_ROOT.parent)}:{node.lineno}"
                )

    assert not violations, (
        "_default_headers calls without destination URL context:\n"
        + "\n".join(violations)
    )


# --- Where code and constants are allowed to live ---


def test_transport_is_execution_policy_only() -> None:
    """Transport owns HTTP execution, not presentation or result assembly.

    Terminal rendering (``progress``) and pandas result assembly (``combining``)
    are top-level leaves that transport reports *into* and returns *through*.
    They lived here only because they had to leave ``ogc`` and this was the
    nearest home; keeping them out is what makes "transport is HTTP execution
    policy" a checkable claim rather than a description of a grab bag.
    """
    misplaced = {
        "dataretrieval/transport/progress.py",
        "dataretrieval/transport/combining.py",
        # An exception taxonomy is not HTTP execution policy either. ``fanout``
        # raises ``FanOutInterrupted`` and belongs here; defining it here would
        # not, since adapters catch it whether or not they went through
        # transport.
        "dataretrieval/transport/interruptions.py",
    }
    present = {
        path
        for path in misplaced
        if (PACKAGE_ROOT.parent / path).exists()  # repo-root-relative
    }
    assert not present, (
        "Presentation or frame-assembly code reappeared inside transport: "
        f"{sorted(present)}"
    )


def test_credential_policy_has_one_definition() -> None:
    """Only ``dataretrieval.credentials`` may name the API-key host.

    Attaching the key and stripping it back off have to agree about which host
    is authorized; the way they stop agreeing is a second copy of the host
    string. ``transport.http`` re-exports the predicate, it does not restate it.
    """
    host = "api.waterdata.usgs.gov"
    # Walked as AST string *values*, not as source text. A line-substring match
    # is wrong in both directions: it missed the ``"https://…"`` form three
    # modules use to spell the same authority, and it flagged docstring prose
    # that merely names the service. Docstrings are excluded here (they are
    # documentation, not a second source of truth) while every other literal --
    # bare host or full base URL -- counts.
    offenders: list[str] = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        if path.name == "credentials.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docstrings = {
            text
            for node in ast.walk(tree)
            if isinstance(
                node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
            )
            for text in [ast.get_docstring(node, clean=False)]
            if text is not None
        }
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            if host in node.value and node.value not in docstrings:
                offenders.append(f"{_module_name(path)}:{node.lineno}")
    assert not offenders, (
        "The API-key host must come from dataretrieval.credentials, "
        f"not be restated at: {offenders}"
    )


# --- Adapter structure and public export boundaries ---


def _waterdata_family_paths() -> tuple[str, ...]:
    """Read the collection-family inventory from its dependency contract."""
    parser = configparser.ConfigParser()
    parser.read(PACKAGE_ROOT.parent / ".importlinter")
    return tuple(
        module.removeprefix("dataretrieval.").replace(".", "/") + ".py"
        for module in parser["importlinter:contract:waterdata-families"][
            "modules"
        ].split()
    )


# The collection-family modules the ``waterdata.api`` facade re-exports.
_WATERDATA_FAMILIES = _waterdata_family_paths()

#: The modules whose public surface must be declared, not inferred. This is a
#: list of *files*, not of names: naming the expected exports too would restate
#: every getter a third time and fail on renames, which break no boundary.
_EXPLICIT_EXPORT_MODULES = (
    "ngwmn.py",
    "nldi.py",
    "streamstats.py",
    "nwdc.py",
    "wqp.py",
    "waterdata/nearest.py",
    "waterdata/ratings.py",
    "waterdata/stats.py",
    "waterdata/types.py",
    *_WATERDATA_FAMILIES,
)


def _top_level_definitions(path: Path) -> set[str]:
    """Names bound at module scope by a def, class, or assignment."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    defined: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            defined.add(node.name)
        elif isinstance(node, ast.Assign):
            defined.update(
                target.id for target in node.targets if isinstance(target, ast.Name)
            )
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            defined.add(node.target.id)
    return defined


def test_active_service_exports_are_explicit() -> None:
    """Every service module declares ``__all__`` and exports only its own names.

    ``_literal_exports`` raises when ``__all__`` is missing, so the call is the
    first assertion. The second is what keeps these modules from becoming
    re-export hubs: a name in ``__all__`` that the module does not define came
    from somewhere else, and now has two public homes that can drift apart.
    """
    for relative in _EXPLICIT_EXPORT_MODULES:
        path = PACKAGE_ROOT / relative
        exports = _literal_exports(path)
        assert exports, f"{relative} declares an empty __all__"
        borrowed = sorted(exports - _top_level_definitions(path))
        assert not borrowed, (
            f"{relative} re-exports names it does not define: {borrowed}"
        )


def test_each_family_getter_has_exactly_one_home() -> None:
    """A getter exported by two family modules would give callers two import
    paths that can diverge, and makes the facade's union ambiguous."""
    seen: dict[str, str] = {}
    for relative in _WATERDATA_FAMILIES:
        for name in _literal_exports(PACKAGE_ROOT / relative):
            assert name not in seen, (
                f"{name} is exported by both {seen[name]} and {relative}"
            )
            seen[name] = relative


def test_api_facade_exports_exactly_the_family_union() -> None:
    """The facade re-exports every family getter and invents none of its own.

    Derived from the families' own ``__all__`` rather than a frozen copy: a
    hardcoded union is 19 more strings to edit per new getter, and it would still
    pass if a family gained an export the facade forgot to re-export -- the one
    thing worth catching here.
    """
    families = set().union(
        *(_literal_exports(PACKAGE_ROOT / f) for f in _WATERDATA_FAMILIES)
    )
    assert _literal_exports(PACKAGE_ROOT / "waterdata/api.py") == families


def test_waterdata_api_is_a_logic_free_compatibility_facade() -> None:
    """The facade re-exports; it does not run anything.

    Statement *kinds* are checked, not just ``def``/``class``. Scanning for
    definitions alone let a module-level ``for`` loop live here that rewrote
    every re-exported getter's ``__module__`` -- code owned by the family
    modules, mutated from a file certified "logic-free". A docstring, imports,
    and plain assignments are the whole legitimate vocabulary of a facade.
    """
    path = PACKAGE_ROOT / "waterdata" / "api.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    allowed = (ast.Import, ast.ImportFrom, ast.Assign, ast.AnnAssign)
    offenders = [
        f"line {node.lineno}: {type(node).__name__}"
        for node in tree.body
        if not isinstance(node, allowed)
        and not (
            isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
        )  # the module docstring
    ]
    assert not offenders, f"waterdata.api contains implementation: {offenders}"


def test_ogc_request_construction_does_not_execute_http() -> None:
    """Building a request may borrow header policy, never the executing calls.

    Two assertions, because the first alone was once true while the rule was
    broken: ``requests.py`` imported ``ogc.schema`` purely to forward a name,
    and ``ogc.schema`` calls ``transport.http.get`` -- so constructing a request
    dragged in the executing path with this test still green.

    The edge is named explicitly rather than checked over the transitive graph.
    A closure from ``ogc.requests`` reaches the whole package, because
    ``transport.retry`` imports ``dataretrieval`` for the progress reporter and
    the package ``__init__`` imports every service; a rule stated that way would
    be either vacuous or a list of exceptions.
    """
    path = PACKAGE_ROOT / "ogc" / "requests.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    transport_names = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "dataretrieval.transport.http"
        for alias in node.names
    }
    assert transport_names == {"default_headers"}, (
        f"ogc.requests imports executing transport helpers: {sorted(transport_names)}"
    )
    assert "dataretrieval.ogc.schema" not in _runtime_imports(path), (
        "ogc.requests imports ogc.schema, which executes HTTP; import the schema "
        "helper from ogc.schema at its point of use instead of forwarding it here"
    )


def test_empty_result_shaping_consults_the_schema_endpoint() -> None:
    """``_deal_with_empty`` names columns from the collection schema.

    That is a real network call on an empty result, so the dependency is worth
    pinning deliberately rather than leaving it to be removed as dead weight.
    """
    assert "dataretrieval.ogc.schema" in _runtime_imports(
        PACKAGE_ROOT / "ogc" / "shaping.py"
    )


def test_nwdc_does_not_reimplement_fan_out_orchestration() -> None:
    """Water Use must drive its locations through the shared fan-out executor.

    It previously ran its own ``asyncio.gather`` with a private semaphore and a
    hand-copied failure-precedence rule, kept in sync with ``FanOut`` by a
    comment. Two copies of that rule is how they drift, and the duplicate lost
    resume, progress, and the shared concurrency setting. Assert the duplication
    cannot quietly return.
    """
    source = (PACKAGE_ROOT / "nwdc.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    offenders = {
        f"{node.value.id}.{node.attr}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "asyncio"
        and node.attr in {"gather", "Semaphore", "wait", "TaskGroup"}
    }
    assert not offenders, (
        "Water Use re-implemented fan-out orchestration instead of using "
        f"transport.fanout.FanOut: {sorted(offenders)}"
    )


def test_fan_out_plans_are_sized_and_repeatably_iterable() -> None:
    """What ``FanOut`` needs of a plan, checked on both real plan types.

    ``FanOutPlan`` is the two standard protocols, so a ``list`` conforms with
    no adapter class and ``isinstance`` against a ``runtime_checkable``
    protocol would prove only that the methods exist. What is actually
    load-bearing and *not* guaranteed by the type is repeatability: resume
    keys completed work by position, so a plan whose second pass differed --
    a generator mistaken for a collection, say -- would re-issue the wrong
    chunks.
    """
    import httpx

    from dataretrieval.ogc.planning import ChunkPlan

    def _build(**args: object) -> httpx.Request:
        return httpx.Request("GET", "https://example.invalid/items", params=args)

    plans = [
        ChunkPlan({"sites": ["a", "b"]}, _build, url_limit=8000),
        # Water Use hands its request list straight to ``FanOut``.
        [httpx.Request("GET", "https://example.invalid/data")],
    ]
    for plan in plans:
        name = type(plan).__name__
        first = list(plan)
        assert len(first) == len(plan), (
            f"{name} yielded {len(first)} items but reports len {len(plan)}"
        )
        assert list(plan) == first, f"{name} is not repeatably iterable"
