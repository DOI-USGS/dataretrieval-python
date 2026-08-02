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

# These top-level modules currently reach into OGC. NGWMN is an OGC adapter;
# Water Use's imports are an accepted temporary variance recorded by ADR 0003.
# Exact equality makes either growth or removal an intentional architecture
# change that updates this fitness function and the ADR together.
_ALLOWED_TOP_LEVEL_OGC_IMPORTS = {
    "dataretrieval.ngwmn": {"dataretrieval.ogc.engine"},
    "dataretrieval.wateruse": {
        "dataretrieval.ogc.combining",
        "dataretrieval.ogc.engine",
    },
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
        "Top-level OGC dependencies differ from ADR 0003. Update the code, ADR, "
        "and allowlist together.\n"
        f"expected={_ALLOWED_TOP_LEVEL_OGC_IMPORTS!r}\nobserved={observed!r}"
    )
