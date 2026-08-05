"""Executable fitness functions for package dependency direction."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).parents[1] / "dataretrieval"

_SERVICE_PREFIXES = (
    "dataretrieval.ngwmn",
    "dataretrieval.nldi",
    "dataretrieval.nwis",
    "dataretrieval.streamstats",
    "dataretrieval.waterdata",
    "dataretrieval.wateruse",
    "dataretrieval.wqp",
)

# NGWMN is the only top-level OGC consumer and uses the small facade
# (``dataretrieval.ogc``) exclusively. Exact equality makes growth or removal
# an intentional architecture change.
_ALLOWED_TOP_LEVEL_OGC_IMPORTS = {
    "dataretrieval.ngwmn": {"dataretrieval.ogc"},
}

_ENGINE_REQUEST_IMPORTS = {
    "_NO_NORMALIZE_PARAMS",
    "_as_str_list",
    "_check_monitoring_location_id",
    "_check_ogc_requests",
    "_construct_api_requests",
    "_construct_cql_request",
    "_cql2_param",
    "_dialect",
    "_get_args",
    "_normalize_str_iterable",
    "_ogc_base_url",
    "_ogc_query_params",
    "_row_cap",
    "_switch_arg_id",
    "_switch_properties_id",
    "prepare_request_args",
}


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


def _runtime_imports(path: Path) -> set[str]:
    module = _module_name(path)
    visitor = _RuntimeImportVisitor(module, path)
    visitor.visit(ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))
    return visitor.modules


def _package_import_graph() -> dict[str, set[str]]:
    return {
        _module_name(path): _runtime_imports(path)
        for path in sorted(PACKAGE_ROOT.rglob("*.py"))
    }


def test_exceptions_has_no_runtime_third_party_dependency() -> None:
    """The shared error-policy leaf must remain cheap and cycle-safe."""
    imports = _runtime_imports(PACKAGE_ROOT / "exceptions.py")
    roots = {module.partition(".")[0] for module in imports}
    third_party = roots - sys.stdlib_module_names - {"dataretrieval"}
    assert not third_party, (
        "dataretrieval.exceptions gained runtime third-party dependencies: "
        f"{sorted(third_party)}"
    )


def test_ogc_does_not_depend_on_service_adapters() -> None:
    """The reusable protocol subsystem must not point back to its callers."""
    violations: list[str] = []
    for module, imports in _package_import_graph().items():
        if not (
            module == "dataretrieval.ogc" or module.startswith("dataretrieval.ogc.")
        ):
            continue
        for dependency in imports:
            if dependency.startswith(_SERVICE_PREFIXES):
                violations.append(f"{module} -> {dependency}")
    assert not violations, "Forbidden OGC-to-service imports:\n" + "\n".join(violations)


def test_modern_modules_do_not_depend_on_legacy_nwis() -> None:
    """NWIS stays quarantined during its deprecation window."""
    violations: list[str] = []
    for module, imports in _package_import_graph().items():
        if module in {"dataretrieval", "dataretrieval.nwis"}:
            continue
        for dependency in imports:
            if dependency == "dataretrieval.nwis" or dependency.startswith(
                "dataretrieval.nwis."
            ):
                violations.append(f"{module} -> {dependency}")
    assert not violations, "Modern modules import legacy NWIS:\n" + "\n".join(
        violations
    )


def test_top_level_ogc_consumers_match_documented_variances() -> None:
    """No new top-level service may acquire an accidental OGC dependency."""
    observed: dict[str, set[str]] = {}
    for path in sorted(PACKAGE_ROOT.glob("*.py")):
        module = _module_name(path)
        if module in {"dataretrieval", "dataretrieval.nwis"}:
            continue
        dependencies = {
            dependency
            for dependency in _runtime_imports(path)
            if dependency == "dataretrieval.ogc"
            or dependency.startswith("dataretrieval.ogc.")
        }
        if dependencies:
            observed[module] = dependencies

    assert observed == _ALLOWED_TOP_LEVEL_OGC_IMPORTS, (
        "Top-level OGC dependencies differ from the architecture allowlist. "
        "Update the code and allowlist; supersede ADR 0003 if the dependency "
        "policy changes.\n"
        f"expected={_ALLOWED_TOP_LEVEL_OGC_IMPORTS!r}\nobserved={observed!r}"
    )


def test_engine_request_import_surface_is_frozen() -> None:
    """Engine may preserve legacy request names but may not grow a new hub."""
    path = PACKAGE_ROOT / "ogc" / "engine.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "dataretrieval.ogc.requests"
        for alias in node.names
    }
    assert imported == _ENGINE_REQUEST_IMPORTS, (
        "ogc.engine request imports changed; use the canonical requests module "
        "instead of expanding compatibility exports.\n"
        f"expected={sorted(_ENGINE_REQUEST_IMPORTS)}\nobserved={sorted(imported)}"
    )


# --- Strengthened OGC boundary tests ---


def test_ogc_runtime_graph_is_acyclic() -> None:
    """The OGC runtime import graph (including the facade) has no cycles.

    Now that no implementation module imports ``dataretrieval.ogc`` (the facade
    ``__init__.py``), the full OGC graph — facade included — forms a DAG.
    This is enforced without any documented exclusion.
    """
    ogc_modules: dict[str, set[str]] = {}
    for module, imports in _package_import_graph().items():
        if module == "dataretrieval.ogc" or module.startswith("dataretrieval.ogc."):
            # Filter to intra-OGC dependencies
            ogc_deps = {
                dep
                for dep in imports
                if dep == "dataretrieval.ogc" or dep.startswith("dataretrieval.ogc.")
            }
            ogc_modules[module] = ogc_deps

    # DFS cycle detection
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {m: WHITE for m in ogc_modules}
    path: list[str] = []

    def dfs(node: str) -> list[str] | None:
        color[node] = GRAY
        path.append(node)
        for dep in ogc_modules.get(node, set()):
            if dep not in color:
                continue
            if color[dep] == GRAY:
                cycle_start = path.index(dep)
                return path[cycle_start:] + [dep]
            if color[dep] == WHITE:
                result = dfs(dep)
                if result:
                    return result
        path.pop()
        color[node] = BLACK
        return None

    for module in ogc_modules:
        if color[module] == WHITE:
            cycle = dfs(module)
            if cycle:
                raise AssertionError(
                    f"Cycle in OGC runtime graph: {' -> '.join(cycle)}"
                )


def test_shaping_has_no_engine_dependency() -> None:
    """ogc.shaping must not import ogc.engine, even lazily."""
    shaping_imports = _runtime_imports(PACKAGE_ROOT / "ogc" / "shaping.py")
    engine_deps = {dep for dep in shaping_imports if dep == "dataretrieval.ogc.engine"}
    assert not engine_deps, (
        f"ogc.shaping must not depend on ogc.engine. Found: {engine_deps}"
    )


def test_ngwmn_uses_ogc_facade() -> None:
    """NGWMN must use ONLY the ogc facade, not engine or other internals."""
    ngwmn_imports = _runtime_imports(PACKAGE_ROOT / "ngwmn.py")
    ogc_deps = {
        dep
        for dep in ngwmn_imports
        if dep == "dataretrieval.ogc" or dep.startswith("dataretrieval.ogc.")
    }
    # Exact equality: the ONLY OGC dependency is the facade package itself.
    assert ogc_deps == {"dataretrieval.ogc"}, (
        "NGWMN must use ONLY the OGC facade (dataretrieval.ogc), not internals. "
        f"Found: {ogc_deps}"
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
    assert ogc_deps == {
        "dataretrieval.ogc",
        "dataretrieval.ogc.dates",
        "dataretrieval.ogc.shaping",
    }, f"Water Data utils crossed its intended OGC seam: {ogc_deps}"

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    exports: set[str] | None = None
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(
            isinstance(target, ast.Name) and target.id == "__all__"
            for target in node.targets
        ):
            exports = set(ast.literal_eval(node.value))
            break
    assert exports is not None, "waterdata.utils must declare its intentional exports"

    old_reexports = {
        "GEOPANDAS",
        "_as_str_list",
        "_check_monitoring_location_id",
        "_check_ogc_requests",
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


# --- API-neutral transport boundaries ---


def test_transport_does_not_depend_on_ogc_or_services() -> None:
    """Transport policy must point inward, never back to protocol adapters."""
    violations: list[str] = []
    transport_root = PACKAGE_ROOT / "transport"
    for path in sorted(transport_root.rglob("*.py")):
        module = _module_name(path)
        for dependency in _runtime_imports(path):
            if (
                dependency == "dataretrieval.ogc"
                or dependency.startswith("dataretrieval.ogc.")
                or dependency.startswith(_SERVICE_PREFIXES)
            ):
                violations.append(f"{module} -> {dependency}")
    assert not violations, "Transport crossed an adapter boundary:\n" + "\n".join(
        violations
    )


def test_wateruse_has_no_ogc_dependency() -> None:
    """The non-OGC Water Use adapter must consume transport directly."""
    imports = _runtime_imports(PACKAGE_ROOT / "wateruse.py")
    ogc_dependencies = {
        dependency
        for dependency in imports
        if dependency == "dataretrieval.ogc"
        or dependency.startswith("dataretrieval.ogc.")
    }
    assert not ogc_dependencies, (
        f"Water Use imported OGC implementation modules: {sorted(ogc_dependencies)}"
    )


def test_transport_runtime_graph_is_acyclic() -> None:
    """The API-neutral transport package must remain a directed acyclic graph."""
    graph = {
        module: {
            dependency
            for dependency in imports
            if dependency == "dataretrieval.transport"
            or dependency.startswith("dataretrieval.transport.")
        }
        for module, imports in _package_import_graph().items()
        if module == "dataretrieval.transport"
        or module.startswith("dataretrieval.transport.")
    }
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(module: str, path: tuple[str, ...]) -> None:
        if module in visiting:
            start = path.index(module)
            cycle = (*path[start:], module)
            raise AssertionError(
                f"Cycle in transport runtime graph: {' -> '.join(cycle)}"
            )
        if module in visited:
            return
        visiting.add(module)
        for dependency in graph.get(module, set()):
            if dependency in graph:
                visit(dependency, (*path, module))
        visiting.remove(module)
        visited.add(module)

    for module in graph:
        visit(module, ())
