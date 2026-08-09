"""Executable fitness functions for architecture rules the import graph cannot
express.

Plain dependency direction -- who may import whom, in which direction, without
cycles -- is declared in ``.importlinter`` and checked by ``lint-imports`` in
pre-commit and CI. Those rules used to be asserted here too, and are not any
more: one rule enforced in two places is one rule that gets updated in one
place.

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
  require one.

Adding a rule here that is purely about module-to-module direction is a
regression -- put it in ``.importlinter`` instead.
"""

from __future__ import annotations

import ast
import functools
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).parents[1] / "dataretrieval"

#: How many names ``ogc.engine`` may import from ``ogc.requests``. A ceiling
#: rather than an exact name list: the claim being enforced is "the legacy
#: compatibility surface does not grow", and a name list also fails on every
#: rename and every deletion -- neither of which grows anything.
_MAX_ENGINE_REQUEST_IMPORTS = 14


def _module_name(path: Path) -> str:
    """Return the import name for one Python file below ``PACKAGE_ROOT``."""
    parts = list(path.relative_to(PACKAGE_ROOT.parent).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


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
        return [node.module] if node.module else [alias.name for alias in node.names]

    current_parts = current_module.split(".")
    package_parts = current_parts if path.name == "__init__.py" else current_parts[:-1]
    ascend = node.level - 1
    base = package_parts[: len(package_parts) - ascend]
    if node.module:
        return [".".join([*base, node.module])]
    return [".".join([*base, alias.name]) for alias in node.names]


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


def test_engine_request_import_surface_does_not_grow() -> None:
    """Engine may preserve legacy request names but may not grow a new hub.

    Each name here is either used by engine's own code or re-exported purely so
    an old import path keeps working. Both are capped: a re-export that nothing
    imports is dead weight, and a used name past the cap means request
    construction is migrating back into engine.
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
        "requests module instead of expanding compatibility exports.\n"
        f"limit={_MAX_ENGINE_REQUEST_IMPORTS}\nobserved={sorted(imported)}"
    )
    # Every imported name must resolve in ``requests``; a stale re-export of a
    # name that moved or was deleted is an ImportError waiting for the first
    # caller of the compatibility path.
    from dataretrieval.ogc import requests as ogc_requests

    missing = sorted(name for name in imported if not hasattr(ogc_requests, name))
    assert not missing, (
        f"ogc.engine re-exports names ogc.requests no longer has: {missing}"
    )


# --- Seams that are about symbols and call sites, not module direction ---


def test_waterdata_utils_is_not_an_ogc_reexport_hub() -> None:
    """Water Data policy wrappers may not bulk re-export OGC internals."""
    path = PACKAGE_ROOT / "waterdata" / "utils.py"
    ogc_deps = {
        dependency
        for dependency in _runtime_imports(path)
        if dependency == "dataretrieval.ogc"
        or dependency.startswith("dataretrieval.ogc.")
    }
    assert ogc_deps == {
        "dataretrieval.ogc",
        "dataretrieval.ogc.dates",
        "dataretrieval.ogc.shaping",
    }, f"Water Data utils crossed its intended OGC seam: {ogc_deps}"

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

#: The modules whose public surface must be declared, not inferred. This is a
#: list of *files*, not of names: naming the expected exports too would restate
#: every getter a third time and fail on renames, which break no boundary.
_EXPLICIT_EXPORT_MODULES = (
    "ngwmn.py",
    "nldi.py",
    "streamstats.py",
    "wateruse.py",
    "wqp.py",
    "waterdata/cql.py",
    "waterdata/measurements.py",
    "waterdata/metadata.py",
    "waterdata/nearest.py",
    "waterdata/ratings.py",
    "waterdata/reference.py",
    "waterdata/samples.py",
    "waterdata/stats.py",
    "waterdata/time_series.py",
    "waterdata/types.py",
)

# The collection-family modules the ``waterdata.api`` facade re-exports.
_WATERDATA_FAMILIES = (
    "waterdata/time_series.py",
    "waterdata/metadata.py",
    "waterdata/measurements.py",
    "waterdata/reference.py",
    "waterdata/samples.py",
    "waterdata/cql.py",
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
