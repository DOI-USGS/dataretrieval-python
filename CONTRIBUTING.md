# Contributing Guidelines

Contributions to `dataretrieval` are welcome and greatly appreciated, but
please read this document for information on *how* to contribute.

`dataretrieval` broadly follows a ["forking" workflow](https://docs.github.com/en/get-started/quickstart/contributing-to-projects),
however writing code is not the only way to contribute.

---

## Table of Contents

- [Contributing Guidelines](#contributing-guidelines)
  - [Table of Contents](#table-of-contents)
  - [Bugs](#bugs)
    - [Reporting Bugs](#reporting-bugs)
    - [Fixing Bugs](#fixing-bugs)
  - [Code Contributions](#code-contributions)
    - [Implementing Features](#implementing-features)
    - [Pull Request Guidelines](#pull-request-guidelines)
    - [Coding Standards and Style](#coding-standards-and-style)
      - [Style](#style)
      - [Docstrings](#docstrings)
      - [Quotes](#quotes)
    - [Updating Package Version](#updating-package-version)
  - [Documentation](#documentation)
    - [Contributing to the Documentation](#contributing-to-the-documentation)
    - [Adding Examples to the Documentation](#adding-examples-to-the-documentation)
  - [Feedback and Feature Requests](#feedback-and-feature-requests)
    - [Submitting Feedback](#submitting-feedback)
    - [Feature Requests](#feature-requests)
  - [Acknowledgements](#acknowledgements)

---

## Bugs

### Reporting Bugs

Report bugs at https://github.com/DOI-USGS/dataretrieval-python/issues.

When reporting a bug, please include:

* Detailed steps to reproduce the bug
* Your operating system name and version
* The Python version, as well as information about your local Python
  environment, such as the versions of installed packages
* Any additional details about your local setup that might be helpful in
  troubleshooting

### Fixing Bugs

Look through the GitHub [issues](https://github.com/DOI-USGS/dataretrieval-python/issues)
for known and unresolved bugs. Any unassigned issue labeled "bug" is open for
resolution. Comment on the issue to state that you intend to fix it, so that
two contributors do not do the same work.

---

## Code Contributions

Code contributions should be made following a ["forking" workflow](https://docs.github.com/en/get-started/quickstart/contributing-to-projects).
First *fork* the repository, creating a personal mirror of the project. Next,
create a *feature branch* in your fork and push your code changes to it. Once
your modifications and additions are complete, open a pull request from the
*feature branch* in your fork to the original upstream repository.

### Implementing Features

Look through the GitHub [issues](https://github.com/DOI-USGS/dataretrieval-python/issues)
for outstanding feature requests. Anything tagged with "enhancement"
and "please-help" is open to whoever wants to implement it.

Please do not combine multiple feature enhancements into a single pull request.

### Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. If the pull request adds or modifies package functionality, unit tests
   should be written to test the new functionality.
2. If the pull request adds or modifies functionality, update the documentation
   or function docstrings that describe it.
3. The pull request should work for Python 3.10 and later. Refer to the
   [Python package workflow](https://github.com/DOI-USGS/dataretrieval-python/blob/main/.github/workflows/python-package.yml)
   for the versions and operating systems currently tested by CI.
4. Build-related changes should preserve the installed-wheel smoke test; tests
   run from a source checkout are not sufficient to prove package contents.
5. Architecturally significant changes should update the
   [architecture documentation](docs/source/architecture/index.rst), add or
   supersede an ADR, and adjust the corresponding fitness function.

### Running the Tests

`pytest tests/` runs the whole suite offline: every HTTP call is mocked, so a
test run neither depends on USGS uptime nor spends anyone's rate limit.

The exception is a small set of tests marked `live`, which query the real
services to notice when an upstream API changes shape -- something a mock cannot
tell us, because the mock is what would need updating. They are deselected by
default and run on a nightly schedule
([live-api.yml](https://github.com/DOI-USGS/dataretrieval-python/blob/main/.github/workflows/live-api.yml)).
Run them locally with:

```bash
pytest tests/ -m live
```

New tests should be offline. Reach for `live` only when the assertion is a claim
about the upstream service rather than about this package.

### Coding Standards and Style

**Before adding a small helper, check whether a leaf already generalizes it.**
This package keeps its general mechanisms in dependency-free leaves --
`_ambient.Ambient` for scoped context values, `config` for every setting
(`API_USGS_*`, the config file, and `configure()` blocks all resolve through
it, and it is the only module that reads the environment for a setting),
`transport.links.resolve_next_url` for pagination cursors. Each of those has
been re-implemented at least once by someone who did not know it was there, and
the copies drift: the same question gets a different cycle guard, a different
error message, a different edge case. None of the automated checks catch it,
because two eight-line helpers are below the clone detector's floor and neither
one couples nor complicates anything. A grep for the mechanism you are about to
write is the only thing that does.

The continuous integration and pre-commit configurations enforce formatting,
linting, and strict type checking. Run the relevant checks before opening a PR:

```bash
ruff check .
ruff format --check .
mypy
coverage run -m pytest tests/
coverage report
xenon --max-absolute C --max-modules B --max-average A dataretrieval
complexipy dataretrieval
lint-imports
```

The last three come from `pip install -e '.[metrics]'`, and each has a pre-commit
hook running the identical check, so a clean pre-commit run means CI agrees.

`coverage report` is a ratchet too. The threshold lives in
`[tool.coverage.report]` in `pyproject.toml` and sits at the measured value, so
it fails on regression rather than demanding new tests of a change that added
none. Raise it when coverage rises; lower it only deliberately, and say why in
the commit.

Coverage is measured with branches on, because most of what this package gets
wrong is a branch rather than a line -- a dispatch arm routing to the wrong
getter, an error path that never fires, a fallback that quietly becomes the
norm. Chase the *uncovered branch*, not the percentage: a test written only to
turn a line green adds maintenance and catches nothing. If a path cannot be
reached without contorting the code, exclude it in
`[tool.coverage.report] exclude_also` with a reason, or leave the ratchet where
it is. Either costs less than a test that adds maintenance and catches nothing.

The blocking run is a single Linux job. The OS/Python matrix reports its own
number with `--fail-under=0`, because several tests are POSIX-only and a
Windows run genuinely measures a smaller suite.

For the same reason, the threshold assumes the whole suite: on Windows, or
without the `nldi` extra installed, some tests skip and the local number comes
in under the gate through no fault of your change. Run
`coverage report --fail-under=0` in that situation and let CI grade the
ratchet.

`xenon` and `complexipy` are complexity ratchets: the thresholds are the
tightest the package passes today, so they fail only when a change pushes a
score above today's. They disagree because they count different things. `xenon` counts
branches (cyclomatic complexity), so a wide flat dispatch scores high;
`complexipy` counts how hard the control flow is to follow (cognitive
complexity), so it scores that dispatch lower and nesting higher. Both name the
offending block, so the fix is local -- usually extracting a branch rather than
restructuring.

`lint-imports` checks the dependency contracts declared in
[`.importlinter`](.importlinter) against the *transitive* import graph: the layer
stack, which modules may consume OGC, NGWMN's facade-only seam, the NWIS
quarantine, and collection-family independence.

**That file is the only place dependency direction is enforced.** These rules
were once asserted a second time in `tests/architecture_test.py` by hand-parsing
the AST; that duplication is gone, and re-adding it would mean one rule with two
homes that drift apart. What the tests still own is everything an import graph
cannot see -- which *symbols* cross a seam, declared `__all__` surfaces, the AST
shape of a facade, boundaries that must be asserted positively (`lint-imports`
can forbid an edge, never require one), and package-wide cycle detection (see
ADR 0003). If you are adding a rule and it is purely "module A must not import
module B", it belongs in `.importlinter`. A boundary that legitimately moves is
one edit there, plus the ADR it cites.

To see the *trend* rather than a pass/fail, that extra also installs
[`wily`](https://github.com/tonybaloney/wily), which indexes metrics across git
history:

```bash
wily build dataretrieval --max-revisions 50   # index recent commits (slow, once)
wily report dataretrieval                     # how the package moved over time
wily diff dataretrieval --revision main       # what your branch changed
wily rank dataretrieval maintainability.mi    # worst-maintained files today
```

`wily` is advisory and is never a merge gate -- rising complexity in a file that
gained a complex feature is information, not a failure.

#### The periodic deep sweep

Duplication, coupling, cohesion, dependency depth, and dead code are tracked by
[`pyscn`](https://github.com/ludo-technologies/pyscn) on a weekly schedule
([code-health.yml](https://github.com/DOI-USGS/dataretrieval-python/blob/main/.github/workflows/code-health.yml)),
which attaches an HTML and a JSON report to each run. Nothing gates on it. These
measures move over months rather than commits.

You do not need it to contribute. It answers "what should we clean up next?" --
including for an agent working on this repo, which gets a whole-package
structural picture from one command:

```bash
pip install -e '.[health]'   # wheels: macOS ARM64, Linux x86-64, Windows x86-64
                             # (no source distribution or other platform wheels)
pyscn analyze dataretrieval  # HTML report, or --json for the numbers
```

Read its findings as leads, not verdicts. Its clone detector flags this
package's per-collection getters -- thin, heavily documented wrappers whose
bodies are necessarily similar -- and collapsing them into one parameterized
function would trade the documented public surface for a metric. Its
dependency-injection heuristics expect a class-oriented design this package
deliberately does not have.

The same extra installs `pyscn-mcp`, a stdio MCP server exposing those analyses
as tools (`analyze_code`, `detect_clones`, `find_dead_code`,
`get_health_score`, and others). Registering it with an MCP-capable assistant is
a personal workflow choice, so this repository does not configure one.

`pylint` was evaluated as a periodic second opinion and deliberately not
adopted. Over `dataretrieval/`, pylint 4.0.8 with its default checks reports
635 findings; all but fourteen are checks ruff also implements, and 419 are a
single false positive -- its `unused-argument` cannot see through the
`locals()` forwarding the collection getters use, so every documented keyword
argument they accept looks unused. The fourteen ruff has no rule for are size
metrics, duplication, and one unused wildcard re-export: duplication is
`pyscn`'s job, the size metrics grade a class-oriented design this package does
not have, and the wildcard is a deprecated alias module re-exporting its
replacement on purpose. The capability ruff lacks is import resolution, and a
missing dependency is already caught by the wheel smoke test in
`python-package.yml`, which imports every module the wheel ships from an
installed wheel outside the checkout. One check has no other home -- an
`except` tuple whose members shadow one another, which ruff's B014 does not
catch because it knows alias pairs rather than subclass relationships. If you
want that second opinion, it costs one command and no configuration:

```bash
uvx --with pandas --with httpx --with anyio pylint -j0 --disable=all \
  --enable=bad-except-order,overlapping-except \
  --load-plugins=pylint.extensions.overlapping_exceptions dataretrieval tests
```

For documentation changes, install `.[doc,nldi]` and run `make html` from
`docs/`. The broader `make docs` target also runs doctests and network-dependent
link checking.

#### Style

* Follow the [PEP8 style guidelines](https://peps.python.org/pep-0008/).
* The public interface should emphasize functions over classes; classes can and
  should be used internally and in tests.
* Group public download functions by data portal. For example, modern Water
  Data functions belong in `dataretrieval.waterdata`; legacy NWIS functions
  remain quarantined in `dataretrieval.nwis` during deprecation.
* Treat a change to a service's documented return shape or metadata type as a
  public compatibility change; update contract tests and architecture
  documentation and follow the deprecation process where required.
* Preserve the dependency direction documented in
  [`docs/source/architecture`](docs/source/architecture/index.rst): public
  facades depend on service/protocol adapters, which depend on stable shared
  policy and infrastructure. Shared OGC code must not import service adapters,
  and modern modules must not depend on legacy NWIS.
* Treat underscore-prefixed helpers as implementation details. Existing
  cross-package uses are documented variances, not extension points for new
  code.

#### Docstrings
* A docstring documents the *contract*. Rationale that argues for a rule binding
  other files belongs in an ADR, cited by number; measurements, symptoms, and
  what the code used to do belong in the commit message. See
  [ADR 0000](docs/source/architecture/decisions/0000-documenting-decisions.rst).
* Docstrings should follow the [numpy standard](https://numpydoc.readthedocs.io/en/v1.5.0/format.html):
  * Example:
    ``` python
    def foo(param1, param2):
    """Example function with types documented in the docstring.

    A more detailed description of the function and its implementation.

    Parameters
    ----------
    param1 : int
        The first parameter.
    param2 : str
        The second parameter.

    Returns
    -------
    bool
        True if successful, False otherwise.

    Examples
    --------
    Examples should be written in doctest format and should demonstrate basic usage.

    >>> foo(1,'bar')
    True

    """
    ```
  * For more details see https://github.com/sphinx-doc/sphinx/blob/master/doc/ext/example_numpy.py

#### Quotes

* Quote conventions follow http://stackoverflow.com/a/56190/5549:

  * Use double quotes around strings that are used for interpolation or that
    are natural language messages
  * Use single quotes for small symbol-like strings (but break the rules if
    the strings contain quotes)
  * Use triple double quotes for docstrings and raw string literals for
    regular expressions even if they aren't needed
  * Example:

    ``` python
    LIGHT_MESSAGES = {
        "English": "There are %(number_of_lights)s lights.",
        "Pirate": "Arr! Thar be %(number_of_lights)s lights.",
    }


    def lights_message(language, number_of_lights):
        """Return a language-appropriate string reporting the light count."""
        return LIGHT_MESSAGES[language] % locals()


    def is_pirate(message):
        """Return True if the given message sounds piratical."""
        return re.search(r"(?i)(arr|avast|yohoho)!", message) is not None
    ```

### Updating Package Version

The package version is derived automatically from Git tags by
`setuptools_scm` (see `[tool.setuptools_scm]` in `pyproject.toml`), so there is
no version string to edit by hand. To cut a release, tag the commit (for
example, `git tag v1.2.3`) and push the tag; both the installed package version
and the documentation's `version` and `release` values follow from it.

---

## Documentation

### Contributing to the Documentation

Documentation is built using [sphinx](https://www.sphinx-doc.org/en/master/),
and is located within the `docs/source/` subdirectory in the repository.
Documentation is written using [reStructuredText](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html).

Contributions to the documentation follow the same forking workflow as code
contributions. Before opening a pull request, test your documentation changes
locally, and describe what they add or fix.

### Adding Examples to the Documentation

The documentation includes examples as Jupyter notebooks, all of which live in
the `demos/` subdirectory. To add one that the documentation runs and renders,
do the following in a separate branch of the repository:

1. Add your notebook to the `demos/` subdirectory after clearing all outputs
2. Add a corresponding `.nblink` file to the `docs/source/examples/`
   subdirectory; see the existing examples for reference, or the [nbsphinx-link](https://nbsphinx-link.readthedocs.io/en/latest/) documentation.
3. Add the example and some text describing it to one of the `.rst` files in
   the examples subdirectory.
4. Run the documentation locally to ensure it renders as you expect, and then
   open a pull request describing the addition.

---

## Feedback and Feature Requests

### Submitting Feedback

Send feedback by opening an issue at
https://github.com/DOI-USGS/dataretrieval-python/issues.

Please be as clear as possible. If you are reporting a bug, refer to
[Reporting Bugs](#reporting-bugs).


### Feature Requests

To request or propose a new feature, open an issue at
https://github.com/DOI-USGS/dataretrieval-python/issues.

Please be sure to:
* Explain in detail how it would work, possibly with pseudo-code or an example
  use-case
* Keep the scope of the proposed feature as narrow as possible
* Make clear whether you would like to implement this feature, you need help
  devising the implementation, or you are unable to implement the feature
  yourself but would like it as a user

---

## Acknowledgements
This document was adapted from the cookiecutter project's [CONTRIBUTING file](https://github.com/audreyr/cookiecutter/blob/master/CONTRIBUTING.rst).
