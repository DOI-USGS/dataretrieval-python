Architecture health iteration log
=================================

Purpose
-------

This log records small architecture experiments evaluated with ``pyscn
1.29.0``. It keeps rejected ideas available for future work without leaving
non-improving code in the branch. PySCN is treated as an architectural fitness
function, not as a substitute for design review: public compatibility,
dependency direction, and the package's function-oriented API remain higher
priority than optimizing a composite metric.

Method
------

Each iteration starts from the latest accepted commit, runs ``pyscn analyze
--json --no-open dataretrieval``, and compares the rounded health score with the
accepted score. A code change is retained only when the target health score
increases and the test, type, lint, and dependency-contract checks pass.
Experiments that improve an unrounded or subsidiary signal but leave the target
score unchanged are reverted and recorded below. Iteration stops at 95 or after
three consecutive experiments leave the accepted score unchanged.

Results
-------

.. list-table:: PySCN architecture iterations
   :header-rows: 1
   :widths: 8 20 12 12 48

   * - Iteration
     - Experiment
     - Health score
     - Decision
     - Evidence and rationale
   * - Baseline
     - Existing ``ci/health-report-signals`` branch
     - 82
     - Starting point
     - Complexity 95, Dead Code 100, Duplication 70, Coupling 100,
       Cohesion 100, Dependencies 80, Architecture 87; maximum dependency
       depth 8 with 139 internal edges.
   * - 1
     - Remove ``ogc.engine -> ogc.chunking`` from the execution path
     - 83
     - **Accepted** (``0172b0af``)
     - The chunk planner/executor composition moved to the OGC engine while
       ``ogc.chunking`` retained the public ``parallel_chunks`` dial and
       compatibility imports. Maximum depth fell from 8 to 7, dependency score
       rose from 80 to 85, and edges fell from 139 to 138. The full test suite,
       Ruff, mypy, and all import-linter contracts passed.
   * - 2
     - Inline private ``transport.env`` into ``transport.retry``
     - 83
     - Rejected
     - Modules fell from 58 to 57 and edges from 138 to 135, but every rounded
       PySCN sub-score and the composite were unchanged. This remains a
       reasonable cleanup if module count or edge count becomes a separately
       adopted fitness function.
   * - 3
     - Extract ``FanOut._run`` failure precedence
     - 83
     - Rejected
     - The high-risk-function count fell from 20 to 19 and 175 focused tests
       passed, but Complexity remained 95 and the composite remained 83. Revisit
       if the project adopts the raw high-risk count as a ratchet.
   * - 4
     - Remove historical transport aliases from ``ogc.chunking``
     - 83
     - Rejected
     - Architecture rose from 87 to 88 and edges fell from 138 to 137, but the
       target composite did not move. The experiment also removed deliberately
       retained compatibility names for a marginal metric gain, conflicting
       with the project's higher-priority compatibility characteristic.

Convergence
-----------

The accepted score is **83**. Iterations 2, 3, and 4 each returned 83, so the
three-straight-iterations convergence condition was reached. Further attempts
were stopped rather than collapsing typed public getter families or widely
used policy leaves merely to satisfy tool heuristics.

The remaining 70 Duplication score is dominated by intentional symmetry:
collection-specific Water Data and NGWMN getters preserve explicit typed
signatures and documentation, WQP wrappers preserve discoverable service
entry points, and deprecated NWIS signatures are frozen. The Architecture
score also treats dependency-free leaves and service-neutral orchestrators as
single-responsibility violations because they are used across several module
communities. Those findings remain useful review prompts, but changing those
boundaries solely for the score would weaken the documented architecture.

Future experiments
------------------

- Reconsider the ``transport.env`` consolidation if dependency edge count is
  promoted from an informational signal to an explicit fitness function.
- Reconsider the ``FanOut._run`` extraction if raw high-risk findings, rather
  than the rounded Complexity score, become a ratchet.
- Remove ``ogc.chunking`` compatibility aliases only through an intentional
  compatibility decision, not as an incidental score optimization.
- Prefer changes that reduce maximum dependency depth, introduce no new clone
  groups, preserve typed public APIs, and keep all import-linter contracts.
