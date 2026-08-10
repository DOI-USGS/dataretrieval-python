"""Report the health signals that mean something for this package.

pyscn grades six sub-scores; four of them do not apply here. Complexity counts
synthetic per-file rows, CBO and LCOM stay pinned at 100 in a function-oriented
package, and Architecture penalises a leaf for being depended upon -- extracting
BaseMetadata to a dependency-free leaf lowered it. The composite averages all
six, so it moves for reasons that are not about this codebase.

What is left is genuinely useful, and is what this prints: dead code, a NEW
clone group appearing, and the dependency depth. Plus the interface cost of the
public getters, which nothing else measures and which is the closest automatable
proxy for whether a module is deep.
"""

import ast
import glob
import json
import os
import sys
from pathlib import Path

BASELINE = Path(".pyscn-known-clones.json")
PACKAGE = Path("dataretrieval")


_TREES: dict[str, ast.Module] = {}
_SPANS: dict[str, dict[int, str]] = {}


def tree_for(path):
    """Parse each file once; both walks below want the same trees."""
    if path not in _TREES:
        _TREES[path] = ast.parse(Path(path).read_text(encoding="utf-8"))
    return _TREES[path]


def enclosing(path, start):
    if path not in _SPANS:
        _SPANS[path] = {
            n.lineno: n.name
            for n in ast.walk(tree_for(path))
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        }
    for line in range(start, start + 3):
        if line in _SPANS[path]:
            return _SPANS[path][line]
    return f"<line {start}>"


def clone_groups(report):
    groups = []
    for g in report["clone"]["clone_groups"]:
        groups.append(
            frozenset(
                f"{c['location']['file_path']}::"
                f"{enclosing(c['location']['file_path'], c['location']['start_line'])}"
                for c in g["clones"]
            )
        )
    return groups


def required_arguments():
    """Public functions carrying required arguments, worst module first."""
    out = []
    for path in sorted(PACKAGE.rglob("*.py")):
        tree = tree_for(str(path))
        declared = None
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
            ):
                declared = set(ast.literal_eval(node.value))
        required = 0
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            public = (
                node.name in declared
                if declared is not None
                else not node.name.startswith("_")
            )
            if not public:
                continue
            positional = node.args.posonlyargs + node.args.args
            required += max(0, len(positional) - len(node.args.defaults))
        if required:
            out.append((str(path), required))
    return sorted(out, key=lambda r: -r[1])


reports = sorted(glob.glob(".pyscn/reports/*.json"))
if not reports:
    print("no pyscn JSON report found")
    sys.exit(0)
report = json.loads(Path(reports[-1]).read_text())

# pyscn infers a project root, and when it guesses wrong it resolves only a
# fraction of the imports -- which *raises* its scores, because most of what it
# grades is dependency-derived. A degraded run therefore looks like an improved
# one. Report the root so that is visible rather than flattering.
summary = report["system"]["Summary"]
root = summary["ProjectRoot"]
if os.path.realpath(root) != os.path.realpath(os.getcwd()):
    print(
        f"WARNING: pyscn resolved against {root!r}, not this checkout -- import\n"
        f"resolution is probably degraded and these numbers are not comparable."
    )
print(f"modules              {summary['TotalModules']}")

dead = report["dead_code"]["summary"]["total_findings"]
deps = report["system"]["DependencyAnalysis"]
print(f"dead code            {dead} findings")
print(
    f"dependency depth     {deps['MaxDepth']} (max path), "
    f"{deps['TotalDependencies']} edges"
)

current = clone_groups(report)
if BASELINE.exists():
    baseline = json.loads(BASELINE.read_text())["groups"]
    known = {frozenset(g["members"]) for g in baseline}
    new = [g for g in current if g not in known]
    gone = [g for g in known if g not in current]
    print(
        f"clone groups         {len(current)} total, "
        f"{len(known) - len(gone)} known, {len(new)} NEW"
    )
    for g in new:
        print("  NEW GROUP -- not in .pyscn-known-clones.json:")
        for member in sorted(g):
            print(f"      {member}")
    for g in gone:
        print(f"  resolved (baseline is stale by one group): {sorted(g)[0]} ...")
else:
    print(f"clone groups         {len(current)} (no baseline file to compare against)")

print("\ninterface cost -- public functions with required arguments:")
rows = required_arguments()
if not rows:
    print("  none: every public function is fully keyword-optional")
for path, count in rows[:6]:
    print(f"  {path:<44} {count} required")
