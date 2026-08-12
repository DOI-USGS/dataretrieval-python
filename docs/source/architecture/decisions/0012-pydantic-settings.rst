ADR 0012: Settings resolution is built on pydantic-settings
============================================================

Status
------

Accepted. Supersedes the standard-library-only clause of
:doc:`0009-layered-configuration` ("``dataretrieval.configuration`` is a
lightweight leaf") in its *third-party* half only, and renames the vocabulary
that ADRs 0009 through 0011 established. The chain, the ``ContextVar``
delivery, host-scoped credentials, adapter scoping and per-adapter profiles all
stand unchanged; what changes is who implements them.

Context
-------

ADR 0009 built the resolution chain from scratch: a hand-written per-setting
type check (``_coerce_typed``), a hand-written grammar per setting
(``_VALIDATORS`` and the ``_parse_*`` family), a hand-written merge across
tiers, and a frozen dataclass per adapter whose field annotations were, as ADR
0010 admitted, "decorative" -- an adapter that drifted to ``retries: str | None``
would type-check clean under ``mypy --strict`` and fail only when a value
reached the chain.

That is a settings library. One already exists, is widely deployed, and is
maintained by people whose job this is.

Three candidates were considered.

**dynaconf** was rejected on its two central abstractions. It is schemaless --
settings are a dynamic object and ``Validator`` objects are runtime assertions,
not annotations -- so the per-adapter vocabularies ADR 0011 is built around
would go back to being a hand-maintained table, which is the failure mode that
shipped undetected for three adapters. And its *environments* switch every
service at once, which is exactly the global ``[profiles.<name>]`` table ADR
0011 retired for being unable to carry per-service detail.

**typed-settings** was the near miss. Its loader chain is arguably a cleaner
statement of a tiered resolution than ``settings_customise_sources``, and its
attrs/dataclass backend would have left the adapter classes as the frozen
dataclasses they already were. It was rejected because the custom work does not
shrink -- adapter tables, named profiles, the ``ContextVar`` tier, the
``base_url`` refusal and the environment's legacy grammars are custom loaders
either way -- while it is a substantially smaller project, has no
``extra="forbid"`` equivalent for rejecting unknown keys in an arbitrary TOML
table, and exposes no stable per-value provenance. Provenance is load-bearing
here: ``show_settings()`` exists to report it.

Decision
--------

**Settings are pydantic-settings models.** Each adapter's class is a
``BaseSettings`` subclass; the shared setting groups are model mixins. The
annotations are now enforced rather than decorative, which is what ADR 0011
wanted from them.

**Each tier of the chain is a** ``PydanticBaseSettingsSource``. The four
sources -- the ``configure()`` block, the ``API_USGS_*`` variables, the
``[<adapter>]`` table, the file's top-level keys -- are listed in ``_CHAIN``,
highest first, and resolution keeps the first value it sees for each key. ADR
0009's "precedence is per setting, not per source" is therefore an ordering
rather than a stack of hand-written fallbacks.

**The vocabulary follows the library's.** ``Configuration`` is ``Settings``,
``BaseConfiguration`` is ``AdapterSettings``, ``<Adapter>Configuration`` is
``<Adapter>Settings``, ``show_configuration()`` is ``show_settings()``, and the
module ``dataretrieval.configuration`` is ``dataretrieval.settings``. The file
keeps the name ``config.toml`` and the variable keeps the name
``DATARETRIEVAL_CONFIG``: both are compatibility surfaces, and ``config`` is
the conventional name for a file on disk.

The glossary follows: a *configuration profile* is a **settings profile**, and
*effective configuration* is the **effective settings**. ``configure()`` keeps
its name -- it is the verb, and pydantic-settings has no competing spelling.

**It is a required dependency, not an extra.** Settings resolve on the request
path -- every call reads the API key -- so an optional dependency would mean
shipping a second, standard-library implementation of the same chain and
testing both. That is more hand-rolled settings code than this ADR exists to
delete.

**Resolution does not go through** ``BaseSettings.__init__``. This is the one
place the library's shape is set aside, and it is a cost decision rather than a
design one. ``BaseSettings.__init__`` builds the four stock sources on every
instantiation -- two of which snapshot and case-fold the whole of ``os.environ``
-- before ``settings_customise_sources`` can discard them. That is the right
trade for a settings object built once at start-up and the wrong one for a
package that resolves lazily per read: profiling put 74% of a single read
inside ``_settings_init_sources``. So ``_resolved`` walks ``_CHAIN`` itself and
validates the merged mapping, and ``__init__`` validates the caller's keywords
directly. The sources, their order, the field schema, ``extra="forbid"`` and
``model_post_init`` are all still the library's.

**Two behaviors change**, both narrowing an inconsistency rather than adding
one:

- A setting an adapter does not read, or a misspelled one, raises
  ``ConfigurationError`` from ``extra="forbid"`` rather than the dataclass's
  bare ``TypeError``. The same mistake written into the file has always raised
  ``ConfigurationError``, so the two surfaces now agree, and the message lists
  the settings the adapter *does* accept.
- The "unset" sentinel is gone. ``_UNSET`` existed to distinguish an omitted
  setting from an explicit ``None``; ``model_fields_set`` is pydantic's record
  of exactly that, so every field simply defaults to ``None``.

Consequences
------------

- **Roughly half the module is deleted.** ``_coerce_typed``, ``_validated_raw``,
  the ``_UNSET`` sentinel, the memoized ``_settings_of``, and the hand-written
  merge in ``_resolve`` all go. What remains is what pydantic-settings has no
  opinion about: the TOML grammar of adapter tables and named profiles, the file
  cache, the provenance labels, and the ``ContextVar``.

- **A read costs about twice as much.** Measured on one Windows machine, per
  read with no settings file: 26--37 us before, 68--82 us after. With a settings
  file both are dominated by the file open, which each re-reads on Windows for
  the reason ADR 0009 gives, and which on-access virus scanning inflates to
  0.8--1.1 ms and 1.3--2.5 ms respectively. At the eight reads a one-chunk query
  performs, that is ~0.3 ms against ~0.6 ms, on a 100--500 ms round trip: a
  fraction of a percent either way.

- **pydantic enters the dependency tree**, at about 9 MB installed -- 5.6 MB of
  it the compiled ``pydantic_core`` -- against the ~100 MB of pandas and numpy
  the package already requires. It is also the most widely installed of the
  three candidates, so for many users it is already present.

- **The settings module is no longer standard-library-only.** It is still a
  leaf: it imports no adapter and nothing first-party but
  ``dataretrieval.exceptions``, so it cannot cycle, and the fitness function
  still asserts that. What it no longer asserts is the absence of third-party
  imports; it pins an allowed roster instead, so a leaf that quietly grew
  ``httpx`` or ``pandas`` still fails.

- **Two names still differ only by case** -- the ``settings`` module and the
  ``Settings`` class -- as they did for ``configuration``/``Configuration``. The
  module stays out of the package's public exports for the same reason, so the
  confusing import line cannot arise.

- **A validation hook had to be renamed.** ``BaseConfiguration.validate()``
  is ``AdapterSettings.validate_settings()``: pydantic's ``BaseModel`` already
  owns ``validate``.

Compliance
----------

``tests/architecture_test.py::test_settings_is_a_first_party_leaf`` asserts the
module imports nothing first-party but the taxonomy leaf, and no third-party
package outside the settings stack.

``tests/settings_test.py`` is ADR 0009--0011's suite, carried over intact: the
144 cases covering the precedence ladder, per-setting merging, thread and
asyncio isolation, named profiles, host scoping, redaction, lazy validation and
the ``base_url`` refusal all pass unchanged against the new implementation,
which is what "the behavior is the same, the implementation is not" means here.

Notes
-----

- Benchmarks taken 2026-08-12 on Windows 11 / CPython 3.12, comparing this
  branch against PR #353 in isolated environments. Absolute figures are
  machine-specific; the ratio is the point.
- Open, not decided here: raising ``requires-python`` to ``>=3.11`` would drop
  the ``tomli`` backport and let ``typing.Self`` replace the ``_S`` TypeVar.
  Both are consequences of the floor, not of this ADR, so they belong to
  whichever change moves it.
