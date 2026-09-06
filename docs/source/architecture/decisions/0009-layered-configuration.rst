ADR 0009: Layered configuration resolution
==========================================

Status
------

Accepted, with clauses superseded twice.

:doc:`0010-adapter-scoped-settings` supersedes "One flat set of setting names"
and "Per-service overrides are deferred" below: the premise of the first -- that
every service accepts the same settings -- is false.

:doc:`0011-configuration-profiles` supersedes three more:

- **The** ``[profiles.<name>]`` **table** in step 3 of the chain, and the
  recommendation in "``parallel_chunks`` at the top level of the file warns" to
  put the setting in one. A profile is now named under the adapter it
  configures (``[<adapter>.<name>]``); the global table and
  ``DATARETRIEVAL_PROFILE`` are retired, since a table that switched every
  service at once could not carry per-service detail.
- **"The environment ranks above the file"**, inverted for -- and only for -- a
  profile selected in code. Everything the caller did not name in code still
  follows the rule as written here.
- **The refusal of a configuration object**, stated in the leaf clause ("a
  scoped action, not a ``Configuration`` dataclass") and in "A configuration
  object would have no way to reach the call". ``configure()`` now takes
  exactly such objects. The grounds were that an instance had no way to reach a
  free function; the ``ContextVar`` this ADR established is one, and ADR 0010
  had already narrowed the objection to a payload-shape preference.

The chain itself, the ``ContextVar`` delivery, host-scoped credentials, and the
leaf constraint stand.

Amended after acceptance under :doc:`0000-documenting-decisions`; the
``Notes`` section records every clause added or corrected.

Context
-------

Settings reached the library through one mechanism: process-global environment
variables (``API_USGS_PAT``, ``API_USGS_CONCURRENT``, ``API_USGS_RETRIES``,
``API_USGS_PROGRESS``), each with its own parser at its point of use. Nothing
could report the effective configuration, and what those parsers accepted was
free to drift apart.

That mechanism cannot express a per-call credential. An application holding
keys in a secret store, a notebook pulling for two accounts, or a server
handling concurrent users must assign to ``os.environ`` -- which is
process-global, so it races across threads and tasks (issue #352).

An ``api_key=`` parameter on the public getters is where a per-call value would
normally go, and it is unsafe here. Every Water Data getter ends in
``_get_args(locals())`` with a ``**queryables`` catch-all that forwards
unrecognized keywords to the API as query parameters. A credential parameter
missed in one of ~20 signatures would be serialized into a URL. The maintainers
object to an ``api_key=`` parameter on a second ground: it invites keys pasted
into shared scripts.

Decision
--------

Every setting resolves through one ordered chain, owned by a new
``dataretrieval.configuration`` module:

1. An active ``dataretrieval.configure(...)`` block (a ``ContextVar``).
2. The setting's environment variable.
3. The configuration file: ``~/.dataretrieval/config.toml``, or the path in
   ``DATARETRIEVAL_CONFIG``. Top-level keys are the defaults; a
   ``[profiles.<name>]`` table layers over them per setting when selected.
4. The built-in default.

Supporting decisions:

- **Precedence is per setting, not per source.** An environment that sets only
  ``API_USGS_PAT`` leaves a file-provided ``concurrency`` in effect. A
  *blank* environment variable does not count as set, so it cannot shadow the
  file: container and CI tooling routinely creates one. The exception is
  ``progress``, where a blank ``API_USGS_PROGRESS`` has always meant "off" --
  so "does blank count as a value?" is a property of the setting
  (``configuration._BLANK_MEANS_SET``) rather than an extra tier in the chain.
- **The environment ranks above the file.** This follows the precedence used by
  `pip
  <https://pip.pypa.io/en/stable/topics/configuration/#precedence-override-order>`_
  and `AWS
  <https://docs.aws.amazon.com/sdkref/latest/guide/settings-reference.html#precedenceOfSettings>`_,
  supports deployment-time overrides without editing mounted files, and keeps
  the pre-existing ``API_USGS_*`` interface authoritative.
- **Omitted and explicitly cleared values differ.** An omitted
  ``configure()`` argument inherits from lower sources. Explicit ``None`` is a
  scoped reset to built-in behavior, so a server can guarantee an anonymous
  call rather than accidentally falling through to its process credential.
- **No public getter grows a credential parameter.** ``configure`` is the only
  programmatic path, and a fitness function asserts no getter accepts
  ``api_key`` / ``session`` / ``token``. The generic ``**queryables`` path also
  refuses credential-shaped names before request construction so they cannot
  enter a URL. That refusal covers names carrying a secret; ``session`` is
  deliberately not among them (see Notes).
- **The module owns each setting's parser.** ``unbounded``, bounds, and
  rejection messages live in one place. ``tomllib`` returns typed scalars, so
  the file and Python API validate source-level types before normalized values
  pass through the shared parsers. Legacy environment-only forms, including a
  blank numeric value and an arbitrary non-empty progress value, remain
  compatible without making the new surfaces equally permissive.
- **Each setting's policy is a row in a named table, never a branch in shared
  code.** Type, bounds, and parser are declared as data, guarded at import time
  for completeness, so adding a setting cannot silently inherit whatever the
  fallback branch happened to do. The rejected alternative -- an ``if``/``elif``
  chain with an implicit integer default -- fails by omission, and fails
  quietly.
- **The file format is forward compatible; the table layout is not.** A key the
  running version does not recognize warns and is ignored, so a file written for
  a newer release still loads rather than breaking a caller who downgraded. A
  key the version *does* recognize, placed in a table that cannot use it, raises:
  that is a mistake the caller can fix, and ignoring it silently would leave the
  user believing a setting is in effect when it is not.
- **Credential-shaped keyword refusal is a usability guardrail, not a security
  control.** Names are matched as substrings after separators are stripped, and
  the check errs toward rejecting. It never inspects values, so it stops a
  caller who mistyped a credential into a query filter -- it does not stop
  anyone determined to send one. Naming it a security control would invite
  reliance it cannot carry.
- **The key travels only over https, to the one authorized host.** The scheme is
  matched as well as the host, because redirects and server-supplied next-page
  links are attacker-influenced data and a downgrade to http would put the
  credential on the wire in clear text. Userinfo on a handed-in URL is stripped
  before the request is built, so ``httpx`` cannot build an ``Authorization``
  header nobody configured. This states the predicate ADR 0006 defers to the
  credentials leaf.
- **TOML, read with** ``tomllib``. Stdlib from Python 3.11; the ``tomli``
  backport is declared under an environment marker and disappears when
  ``requires-python`` moves to ``>=3.11``. YAML was rejected because PyYAML is
  a dependency at every Python version and the settings are flat.
- **Not every setting gets an environment variable.** ``parallel_chunks``
  spends rate-limit quota and must stay a deliberate choice: the library cannot
  tell in advance whether a query is large -- ten states over a short window
  might fit in one page, where extra chunks would only spend quota -- so a high
  value is a judgement the caller makes per query, not a process default. It
  adds no new process-global variable; the file and ``configure`` block are its
  only sources, with a scoped block as the recommended use, which also keeps the
  setting from leaking into unrelated calls.
- **Names distinguish execution capacity from planning granularity.**
  ``concurrency`` names the maximum chunks in flight and maps to the existing
  ``API_USGS_CONCURRENT`` variable. ``parallel_chunks`` asks the planner for
  optional extra chunks; it does not promise that many execute simultaneously.
  The name is retained because the context manager is already public.
  ``parallelism`` and ``chunk_parallelism`` were rejected because they would
  conflate this planning hint with ``concurrency``.
- **Configuration errors are in the error taxonomy.** ``ConfigurationError`` is a
  ``DataRetrievalError`` *and* a ``ValueError``. Configuration resolves lazily
  on the request path, so an invalid file raises from inside whichever getter
  runs first; ``except DataRetrievalError`` around a call has to catch it like
  any other failure of that call, while the ``ValueError`` base keeps the
  handlers that predate the file layer working.
- **``parallel_chunks`` at the top level of the file warns.** It is the one
  setting that spends rate-limit quota, so a value left there applies to every
  splittable query in every process that reads the file. A
  ``[profiles.<name>]`` table is opt-in per run, which is the scope this setting
  needs; the top-level form still works, but warns.
- **``dataretrieval.configuration`` is a lightweight leaf.** It uses only the
  standard library, the ``tomli`` backport on Python 3.10, and
  ``dataretrieval.exceptions`` -- itself a dependency-free leaf, so this adds no
  weight and cannot create an import cycle. It is read by ``utils`` (headers),
  ``ogc.chunking``, ``ogc.retry``, and ``ogc.progress``, so under ADR 0003 it
  must import none of them. The public callable is named ``configure`` rather
  than ``config`` so it does not shadow the module. It is a scoped action, not a
  ``Configuration`` dataclass: a value object would imply snapshot, equality,
  serialization, and representation contracts while risking disclosure of the
  API key through generated helpers.

- **One flat set of setting names, shared by every service.** ``concurrency``
  means the same thing to every adapter, so the chain resolves one name rather
  than one per service. Services differ in the *value* they want, not the
  vocabulary, and that difference is expressed as a caller-supplied default:
  ``wateruse`` passes its ``DEFAULT_CONCURRENT_REQUESTS`` of 4 to
  ``configuration.concurrency()`` where the OGC getters take the package default
  of 32, and the single-shot adapters pass ``_GATEWAY_STATUSES`` to
  ``RetryPolicy.from_configuration()`` because WQP and StreamStats report a
  rejected query as a 500. A value resolved from the chain always outranks a
  caller default -- a service able to override an explicit setting would make
  ``concurrency=1`` a lie.

- **Per-service overrides are deferred, not refused.** One ``configure()``
  block cannot currently set one value for Water Use and another for Water Data.
  Every known service difference is a default, which the caller already
  supplies, so nothing needs it yet. If something does, the shape is a namespace
  inside this chain -- a ``[wateruse]`` table beside the top-level keys, read as
  ``configuration.concurrency(default, service=...)``. It costs a second
  dimension in resolution, which ``show_configuration()`` must then render as a
  matrix rather than a list, and that cost should buy a requirement before it is
  paid.

- **A configuration object would have no way to reach the call.** The public
  surface is free functions -- ``waterdata.get_daily(...)``, not a client with
  methods. An instance would therefore arrive either as a parameter on every
  getter, which is the per-call passing the ``ContextVar`` exists to remove and
  which the ``**queryables`` catch-all makes unsafe, or through a module-level
  global, which restores the cross-thread and cross-task leakage this ADR
  exists to end. A library entered through a constructed client can hold
  settings on that client; one entered through free functions cannot, and the
  scoped block follows from that.

Consequences
------------

- A credential can be supplied per thread or per task without touching
  ``os.environ``, which is what issue #352 asked for.
- Host scoping is unchanged and unconditional: a key from any source is sent
  only to ``api.waterdata.usgs.gov`` and is stripped on cross-host redirects.
- ``show_configuration()`` reports the effective value and source of each
  setting without ever printing the key.
- Behavior is unchanged when no file exists and no block is active, so
  existing environment-variable users are unaffected.
- A configuration file becomes a supported artifact whose format is now a
  compatibility surface.
- The minimum Python version and the file format are coupled: raising
  ``requires-python`` to ``>=3.11`` drops the ``tomli`` dependency with no
  other change.

Compliance
----------

``tests/architecture_test.py::test_config_is_a_standard_library_only_leaf``
asserts the module imports nothing from ``dataretrieval`` other than the
``exceptions`` taxonomy leaf, and no third-party package other than the
``tomli`` backport.
``tests/configuration_test.py`` covers the precedence chain, per-setting merging,
thread and asyncio isolation, host scoping for file-sourced keys, redaction in
``show_configuration``, and rejection of credential parameters on public getters.

Notes
-----

The setting-table, forward-compatibility, guardrail-scoping, and credential-
egress clauses were added after the original decision, consolidating under ADR
0000 rules that the code was carrying in prose. None changes behavior.

The "Not every setting gets an environment variable" bullet was also extended in
place: it deferred its argument to a ``parallel_chunks`` docstring, and that
argument now sits in the bullet itself, because the docstring it pointed at was
the prose being consolidated.

The ``**queryables`` clause above originally named ``session`` among the
rejected spellings. It was corrected after the fact: ``session`` carries no
secret, so refusing it with a credentials message told callers the wrong thing,
and as a substring it claimed part of a namespace the *server* owns -- any
future query parameter containing it would have been unreachable behind that
message. ``dataretrieval/credentials.py`` records the exclusion at the
predicate. The decision the clause makes -- credential-shaped names never reach
a URL -- is unchanged.
