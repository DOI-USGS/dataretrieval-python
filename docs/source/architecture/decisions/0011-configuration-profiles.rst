ADR 0011: Settings profiles, scoped to one adapter
========================================================

Status
------

Accepted, and re-spelled by :doc:`0012-pydantic-settings`. Every decision below
stands; the classes named in them are pydantic-settings models rather than
frozen dataclasses, and carry the library's names --
``<Adapter>Settings`` for ``<Adapter>Configuration``, ``AdapterSettings`` for
``BaseConfiguration``, ``validate_settings()`` for ``validate()``. Two details
follow from the move: the "annotations are the schema" argument in decision 5
is now enforced rather than aspirational, and the ``_UNSET`` sentinel that
distinguished an omitted setting from an explicit ``None`` is replaced by
``model_fields_set``.

Supersedes two clauses of :doc:`0010-adapter-scoped-settings` --
decision 5 (adapter schemas held centrally as ``TypedDict``) and decision 8
(each adapter a named keyword on ``configure``) -- and three of
:doc:`0009-layered-configuration`: the global ``[profiles.<name>]`` table; the
environment-above-file rule, inverted for a profile selected in code; and the
refusal of a configuration object, which ADR 0010 had already narrowed to a
preference about the payload's shape. The chain, the ``ContextVar`` delivery,
host-scoped credentials and the leaf constraint stand.

Context
-------

ADR 0010 gave each adapter its own slice of the chain, so ``[ngwmn]`` narrows a
setting to NGWMN. That covers "tune one service" but not the case a
multi-service caller actually has:

- **Several named configurations per adapter.** A caller with an overnight
  bulk shape and a polite daytime shape for Water Data cannot store both. The
  only named construct is ``[profiles.<name>]``, which switches *every*
  service at once.
- **Composing them.** The two mechanisms do not compose:
  ``[profiles.bulk.ngwmn]`` raises, so a profile cannot carry per-service
  detail. That refusal was recorded in ADR 0010 on the grounds that layering
  them needed a fourth precedence rule nobody had asked for. Someone has now
  asked for it, and it is the primary use case.

Two further problems ADR 0010 left open feed into the same decision. The
adapter roster is spelled in four places, only one of which is derived --
adding an adapter needs coordinated edits, and forgetting one leaves a schema
no call site can reach, which happened to three adapters and shipped
undetected until a fitness test was written. And a setting's definition lives
in ``config`` rather than in the module that reads it, so adding a Water Data
setting edits a file that knows nothing about Water Data.

Decision
--------

**A configuration profile is a named set of settings for one adapter.** The
file gains named profiles beside each adapter's default profile::

    concurrency = 16              # package-wide defaults

    [waterdata]                   # waterdata's DEFAULT profile: always active
    concurrency = 32

    [waterdata.bulk]              # a NAMED profile: only when selected
    parallel_chunks = 8

    [ngwmn.gentle]
    concurrency = 4

A named profile never enters the chain unless a caller selects it. The global
``[profiles.<name>]`` table and ``DATARETRIEVAL_PROFILE`` are retired; nothing
has shipped, so nothing is deprecated.

**``configure()`` takes configuration objects.** Positionally, one per
adapter, and nothing else::

    with dataretrieval.configure(
            Settings(api_key=vault.read("usgs/pat")),
            WaterdataSettings.load("bulk"),
            NgwmnSettings(concurrency=4),
    ):
        ...

The adapter an instance targets is a property of its class, so the caller
never restates it -- which is what removes the roster duplication. Naming two
configurations for one adapter raises: they would be the one pairing with no
defined order.

Keyword settings are removed, so ``configure(api_key=...)`` no longer works.
This is the most-typed line the feature exists to enable, and making it wordier
is a real cost, accepted deliberately for one shape everywhere.

**Schemas live with their adapter; names live centrally.** ``configuration``
is a standard-library-only leaf every adapter may import, so it cannot import
adapters. It holds the tuple of adapter *names*, which is what parsing a file
needs (is ``[ngwmn]`` a table or a typo?). Each adapter package owns its
subclass, which is what a setting's definition needs to be local to the
service that reads it.

Registration at import alone would not do: ``dataretrieval`` imports six of
seven adapters eagerly, but NLDI is deliberately on demand for the geopandas
extra, so a registry built from imports would reject a valid ``[nldi]`` table
until something imported it, and the report would vary by what a caller had
touched.

**Precedence**, highest first:

1. A configuration instance passed to ``configure()``
2. A profile selected in code, ``WaterdataSettings.load("bulk")``
3. The setting's environment variable (package-wide settings only)
4. The adapter's default profile in the file
5. Package-wide defaults in the file
6. The adapter's built-in preference in code
7. The package built-in default

Each level overrides the one below **per key**, so a named profile still
inherits its adapter's default profile and the package-wide keys. Positions 1
and 2 are both code and both target one adapter, so the same-adapter rule
means they cannot tie.

Position 2 above 3 inverts ADR 0009's environment-above-file rule for this one
case. A profile named in code is a more deliberate act than a variable
inherited from a shell, and losing to that variable is the behaviour a caller
would file a bug about. Everything the caller did *not* name in code still
follows the original rule.

**Validation is lazy.** A file's structure is checked when it is parsed; a
table's keys are checked when that adapter first resolves a setting. This
keeps the blast-radius rule ADR 0010 established -- a malformed ``[nldi]``
table must not fail a Water Data call -- and it is what allows the schema to
live in a module the parser cannot import.

**Base URLs may be configured, from code only.** An adapter's configuration
may carry its base URL, settable in a ``configure()`` block and rejected from
the file and the environment. A file that silently redirects a data-retrieval
library to another host is a supply-chain-shaped hazard; an in-code block
keeps the redirect where a reader sees it.

**The module is renamed** ``dataretrieval.config`` to
``dataretrieval.settings``,
and ADR 0009's rule reserving ``config`` as an abbreviation for the module and
the file is withdrawn. The path has never been released, so no alias is
needed.

**Credentials are unchanged, and measurement settled why.** The API key stays
one package-wide setting scoped to the single host that honours it. Probing
the live services:

.. list-table::
   :header-rows: 1

   * - Host
     - No key
     - With key
     - Bad key
   * - ``api.waterdata.usgs.gov`` (waterdata, ngwmn)
     - no limit header
     - ``x-ratelimit-limit: 4000``
     - 403
   * - ``api.water.usgs.gov`` (nwdc)
     - ``1000``
     - ``1000``
     - 403
   * - ``api.water.usgs.gov`` (nldi)
     - ``3600``
     - ``3600``
     - 403

NWDC and NLDI meter by address and report the *same* limit with or without a
key; the gateway validates one only if present. Sending the key there would
gain nothing and would turn a stale key into 403s on calls that work
anonymously today. The three hosts also keep independent counters, so ADR
0010's "one key, one quota pool" is true of waterdata and ngwmn only.

Consequences
------------

- **The multi-service case gets a spelling**, which is the point. One block,
  several adapters, at most one configuration each, any of them from the file
  or from code.
- **The roster stops being duplicated.** An adapter declares itself once. The
  failure mode where a schema exists that nothing passes becomes impossible by
  construction rather than caught by a fitness test.
- **A setting's definition moves next to the code that reads it.** Adding a
  Water Data setting no longer edits a service-neutral module.
- **``configure(api_key=...)`` breaks.** The README, the settings guide,
  the PR description and ADR 0009's examples all use it and all must change in
  the same commit.
- **``show_settings()`` can only resolve the settings an adapter accepts
  once that adapter has been imported.** It names the adapters it could not
  check rather than omitting them silently, which is the honest cost of lazy
  validation. The *profile list* is not import-limited: what a profile is
  called is a fact about the file, so every ``[<adapter>.<name>]`` table it
  defines is listed, imported or not -- withholding one would make the
  section's answer depend on which optional extras happened to be installed.
- **Two names differ only by case** -- the ``configuration`` module and the
  ``Settings`` class. The module stays out of the package's public
  exports so the confusing import line cannot arise.
- **Separate quota pools are still not modelled.** Three exist. Nothing in the
  library needs to know yet.
- **``ssl_check`` is unaffected** and remains a per-call argument, for the
  reasons in ADR 0010.

Compliance
----------

Satisfied. In ``tests/settings_test.py``:

- ``test_several_named_profiles_are_selected_independently`` -- one block,
  a different profile per adapter.
- ``test_a_named_profile_layers_per_key_over_the_tiers_below`` -- a profile
  inherits its adapter's default profile and the package-wide keys per key.
- ``test_adding_a_named_profile_changes_nothing_until_it_is_selected`` -- a
  named profile is inert until something selects it.
- ``test_two_configurations_for_one_adapter_raise``.
- ``test_a_code_selected_profile_outranks_the_environment``, plus a case per
  rung of the seven-rung ladder above, each written against one file that
  populates every rung with a distinct value.
- ``test_inner_block_can_lower_a_setting_an_outer_block_scoped`` -- the
  innermost block wins, including over an adapter-scoped outer one.
- ``test_a_table_for_an_unimported_adapter_stays_valid`` and
  ``test_a_malformed_table_does_not_fail_another_adapters_call`` -- the
  blast-radius rule under lazy validation.
- ``test_base_url_applies_from_code_and_is_refused_from_the_file``, with
  ``test_base_url_is_refused_from_the_environment`` for the other source, and
  ``test_every_water_data_endpoint_use_goes_through_redirected`` -- an AST scan
  over ``dataretrieval/waterdata`` for the one adapter that cannot resolve its
  base at a single choke point, so a family module cannot quietly keep sending
  traffic to the host a caller redirected away from.
- ``test_adapter_roster_names_real_modules_that_register_themselves`` and
  ``test_every_adapter_is_actually_wired_to_a_read_site`` -- the roster
  resolves, and no configuration exists that nothing reads. An adapter name
  the code does not recognize now raises out of ``_resolve`` rather than
  falling through to the package-wide value, so the grep is a backstop rather
  than the only guard.

``tests/architecture_test.py::test_config_is_a_standard_library_only_leaf``
asserts the module imports no adapter -- ``dataretrieval.exceptions`` is its
only first-party import -- and ``lint-imports`` keeps ``configuration`` below
``credentials``.

Notes
-----

- Live measurements taken 2026-08-11 against ``api.waterdata.usgs.gov`` and
  ``api.water.usgs.gov``.
- Open, not decided here: whether ``parallel_chunks`` is renamed. ``fan_out``
  was suggested and conflicts with the glossary, where fan-out is *executing*
  chunks concurrently -- which ``concurrency`` already governs -- while
  ``parallel_chunks`` asks the planner to *divide* more finely. ADR 0009
  rejected ``parallelism`` and ``chunk_parallelism`` for the same conflation.
  ``chunk_count`` or ``target_chunks`` would stay on the correct side of it.
