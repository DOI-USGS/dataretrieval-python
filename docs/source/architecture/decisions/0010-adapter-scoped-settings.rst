ADR 0010: Adapter-scoped settings
=================================

Status
------

Accepted, except for two clauses, and re-spelled by
:doc:`0012-pydantic-settings` -- which also settles decision 5's worry directly:
the schemas are pydantic models, so their annotations are checked rather than
decorative, and an adapter that drifted to ``retries: str | None`` now rejects
an integer at construction instead of type-checking clean and failing when a
value reaches the chain.

Supersedes the "One flat set of setting names" and "Per-service overrides are
deferred" clauses of
:doc:`0009-layered-configuration`; the rest of ADR 0009 stands, subject to what
ADR 0011 supersedes there.

:doc:`0011-configuration-profiles` supersedes decisions 5 and 8 below --
adapter schemas held centrally as ``TypedDict``, and each adapter a named
keyword on ``configure()``. Each adapter now declares a ``AdapterSettings``
subclass in the module that *reads* those settings, and ``configure()`` takes
instances of them positionally, which is what removes the adapter roster from
the call site. The spelling shown in decision 1 goes with decision 8. Decisions
2, 3, 4, 6 and 7 stand: the tiers, source-major precedence, package-wide
environment variables, the host-scoped key, and the adapter names.

Context
-------

ADR 0009 resolved every setting through one flat namespace, on the premise that
"a setting means the same thing to every service; services differ in the value
they want, not the vocabulary." Surveying the seven APIs this package retrieves
from shows the premise is false. The settings themselves differ:

.. list-table::
   :header-rows: 1

   * - Adapter
     - Host / path
     - Fan-out
     - Retryable statuses
     - ``ssl_check``
   * - ``waterdata``
     - ``api.waterdata.usgs.gov/ogcapi``
     - yes (OGC chunking)
     - all 5xx + 429
     - yes, on 3 of its getters
   * - ``ngwmn``
     - ``api.waterdata.usgs.gov/ngwmn/ogcapi``
     - yes (OGC chunking)
     - all 5xx + 429
     - --
   * - ``nwdc``
     - ``api.water.usgs.gov/nwaa-data``
     - yes (fan-out)
     - all 5xx + 429
     - yes
   * - ``nldi``
     - ``api.water.usgs.gov/nldi/linked-data``
     - no
     - gateway only
     - no
   * - ``wqp``
     - ``www.waterqualitydata.us``
     - no
     - gateway only
     - yes
   * - ``streamstats``
     - ``streamstats.usgs.gov``
     - no
     - gateway only
     - no
   * - ``nwis`` (deprecated, not an adapter key)
     - ``waterservices.usgs.gov``
     - no
     - fixed, no retry
     - yes

``concurrency`` and ``parallel_chunks`` are meaningless for the four
single-shot adapters -- there is nothing to fan out. ``ssl_check`` applies to
four adapters (``waterdata``, ``nwdc``, ``nwis``, ``wqp``) and is currently a
per-call keyword outside the chain entirely; it reaches ``httpx``'s ``verify``,
verified by spying on the client. A flat namespace accepts
``configure(streamstats={"parallel_chunks": 8})`` without complaint, which is
the typo class ADR 0009 exists to catch.

The credential is a separate axis, and measurement settled it. Probing the live
APIs with and without a key:

* NGWMN and Water Data are served from the *same host*
  (``ngwmn.py`` derives its base URL from ``credentials.WATERDATA_BASE_URL``).
* Both return ``200`` with no key, and both return ``x-ratelimit-limit: 1000``
  with one.
* Alternating authenticated calls decrement a *single* counter
  (997, 996, 996, 994, 993, 992), so the two adapters share one quota pool.
* Water Data's OpenAPI declares ``ApiKeyHeader``/``ApiKeyQuery``; NGWMN's
  declares no security scheme at all across 34 paths -- yet the gateway meters
  it regardless. Every response carries ``via: ... api-umbrella``.

The key is therefore a credential of the **gateway fronting the host**, not of
either adapter. It cannot meaningfully vary per adapter: two keys against one
quota pool is not a state the gateway can be in.

Decision
--------

Settings are scoped to the **adapter**, not the service, and not the host.

1. **The configuration file gains one table per adapter**, beside the existing
   top-level keys::

       concurrency = 16          # every adapter

       [ngwmn]
       concurrency = 4           # this adapter only

   ``configure()`` takes the same shape, so one block configures several
   adapters at once::

       with dataretrieval.configure(ngwmn={"concurrency": 4},
                                    wqp={"retries": 2}):
           ...

   .. note::

      The file table stands; the ``configure()`` spelling above is superseded
      by :doc:`0011-configuration-profiles` along with decision 8. One block
      still configures several adapters at once, now as
      ``configure(NgwmnSettings(concurrency=4),
      WqpSettings(retries=2))``.

2. **The top-level tier survives.** An adapter table *overrides* it per key; it
   does not replace it. Every setting still has a package-wide spelling, and
   the shipped ``API_USGS_*`` variables are package-wide by construction.
   ``retries`` and ``stall_timeout`` are additionally adapter-scopable, because
   a service that answers slowly or refuses often warrants its own budget
   without changing anyone else's. ``progress`` is not: it describes the
   caller's terminal, and there is one progress line per call, so scoping it
   per adapter could only produce a contradiction.

3. **Precedence stays source-major.** Resolution walks block, then environment,
   then file, as ADR 0009 defines; *within* each source an adapter-scoped value
   outranks a top-level one. The environment therefore still outranks the file,
   so a stale adapter table cannot quietly beat a variable exported for one run.

4. **Adapter-scoped settings get no environment variables.** Every entry in
   ``ENV_VARS`` stays package-wide, for the reason ``parallel_chunks`` already
   has none: an exported variable is inherited by every subprocess and
   invisible at the call site. Six adapters times four settings would be a
   namespace nobody could hold in mind.

5. **Each adapter's schema is a** ``TypedDict``. Its ``__annotations__`` *are*
   the schema -- there is no second table to maintain, ``mypy --strict`` checks
   literal dicts at call sites, and the file path validates against the same
   annotations. A key an adapter does not accept raises ``ConfigurationError``
   at block entry, the way an unknown profile already does.

   *Superseded by* :doc:`0011-configuration-profiles`. The schema is now a
   frozen dataclass owned by the adapter, for the same "the annotations are the
   schema" reason -- what changed is where it lives. A ``TypedDict`` had to be
   declared centrally to annotate a central keyword, which put a Water Data
   setting's definition in a module that knows nothing about Water Data.

6. **The API key stays host-scoped and is not an adapter setting.**
   ``credentials`` keeps sole ownership of which host honors the key. There is
   no ``[ngwmn] api_key``.

7. **Adapters are keyed by their service's name**, matching the module:
   ``waterdata``, ``ngwmn``, ``nwdc``, ``wqp``, ``nldi``, ``streamstats``.
   The deprecated ``nwis`` is deliberately absent: its calls pin
   ``max_retries=0``, so a ``[nwis]`` table could only be reported as live and
   then ignored -- the failure this decision exists to prevent.

8. **Each adapter is a named, typed parameter on** ``configure()``, annotated
   with its own ``TypedDict``, so a type checker rejects a setting the adapter
   does not read before the code runs. A ``**unknown`` catch-all remains, and
   exists to turn a misspelled *setting* into a message naming the settings --
   ``configure(concurrancy=8)`` would otherwise be a bare ``TypeError``.

   *Superseded by* :doc:`0011-configuration-profiles`. ``configure()`` takes
   configuration objects positionally instead, so the adapter is named by the
   class rather than by a keyword. The type checking survives -- a setting an
   adapter does not read is not a field of its class -- and the catch-all is
   no longer needed for a misspelling, because
   ``WaterdataSettings(concurrancy=8)`` is already a ``TypeError`` naming
   the keyword that does not exist. What the change buys is that ``configure()``
   no longer enumerates the adapters at all: that enumeration was the roster
   this ADR left spelled in four places.

Consequences
------------

- **A caller can be gentle with one adapter without throttling the rest** --
  the requirement ADR 0009 deferred. Because NGWMN and Water Data share a quota
  pool, throttling NGWMN now measurably preserves quota for Water Data.

- **The schema stops being a separate mechanism.** Choosing ``TypedDict`` over
  a hand-maintained table removes the failure mode where a new adapter setting
  is added and the validation table is not, and over a dataclass per adapter it
  keeps the payload a plain mapping, so the file and block paths share one
  validator and ``configuration`` grows no runtime classes. *Superseded with
  decision 5*: the classes exist, and live with their adapters rather than in
  the leaf.

- **A configuration object is still refused, but on narrower grounds than ADR
  0009 stated.** That ADR rejected an object because it had no way to *reach*
  the call. A per-adapter payload type does not have that problem -- the
  ``ContextVar`` remains the delivery mechanism and the type is only the
  payload's shape. ``TypedDict`` is chosen over a dataclass for the reason
  above, not because an object could not be delivered.

  *Withdrawn by* :doc:`0011-configuration-profiles`, which took the remaining
  step. Narrowing the objection to a payload-shape preference is what left it
  open, and a dataclass turned out to buy the thing a mapping could not: an
  instance knows which adapter it targets, so the caller stops naming one and
  the roster stops being duplicated.

- **``show_settings()`` grows a second section, not a matrix.** It prints
  the top-level tier as today, then only those adapter overrides actually set.
  A seven-by-eight grid of mostly-inherited values would bury the answer to
  "what will this call use".

- **The shared quota pool is not modelled.** ``[waterdata]`` and ``[ngwmn]``
  read as independent dials but draw on one 1000/hour allowance. A host or
  gateway tier would express it; that is deferred until someone is confused by
  it, since the pool is a property of the credential, which is already
  host-scoped.

- **``stall_timeout`` joins the chain.** ``API_USGS_STALL_TIMEOUT`` was read
  directly from ``os.environ``, so it could not be set by a block or the file
  and never appeared in ``show_settings()`` -- a gap in ADR 0009's own
  claim that every setting resolves through one chain. It is package-wide by
  default and adapter-scopable. ``dataretrieval/transport/env.py`` existed only
  to parse it and is deleted, so ``configuration`` is now the only module in
  the package that reads ``os.environ`` for a setting.

- **``ssl_check`` stays a per-call argument and does not become a setting.**
  It is a defaulted keyword on 23 shipped getters across four adapters --
  ``wqp`` (9), ``nwis`` (10), ``waterdata`` (3) and ``nwdc`` (1) -- and it does
  reach ``httpx``'s ``verify``. It was added in 2023 to what were then the only
  modules; the OGC getters arrived later and never adopted it, so its
  distribution records the package's history rather than a boundary.

  Three reasons not to promote it. It disables certificate verification, so as
  a per-call keyword it is a visible, scoped decision, while a config-file key
  or environment variable would make a security downgrade process-wide and
  invisible at the call site -- the opposite of the direction this chain
  narrows everything else. It does not respect adapter boundaries: within
  ``waterdata`` it applies only to the getters that bypass the OGC engine, so
  ``[waterdata] ssl_check`` would be honored by three getters and silently
  ignored by the rest, exactly the shape this ADR refuses elsewhere. And the
  need it serves is already met better: the legitimate case is a
  TLS-intercepting corporate proxy, and ``httpx`` natively honors
  ``SSL_CERT_FILE`` and ``SSL_CERT_DIR`` on both its sync and async clients --
  so that mechanism already covers *every* getter, including the OGC ones that
  have no ``ssl_check``, and it trusts the corporate CA rather than trusting
  nothing. The ``bool`` type cannot even carry a CA bundle path, which is the
  value a caller actually wants.

  The settings guide documents ``SSL_CERT_FILE`` for that case. Whether
  ``ssl_check`` should be deprecated outright is a public-API question left to
  its own change.

- ``tests/settings_test.py`` covers adapter-table resolution, top-level
  inheritance per setting, source-major precedence (the environment still
  outranks an adapter table), an adapter block outranking a package-wide one,
  and rejection of a setting an adapter does not read -- from both the file and
  ``configure()``.
- ``test_api_key_is_never_adapter_scoped`` asserts no adapter configuration
  accepts ``api_key``.
- ``test_adapter_roster_names_real_modules_that_register_themselves`` imports
  every name in the roster, so a renamed adapter cannot leave a configuration
  pointing at nothing.
- ``lint-imports`` continues to place ``configuration`` between ``credentials``
  and ``exceptions``.

The two entries covering decision 8's ``**adapters`` catch-all
(``test_a_misspelled_setting_is_not_taken_for_an_adapter``) and the central
``TypedDict`` registry (``test_adapter_schema_names_a_real_module``) went with
the clauses ADR 0011 superseded; the checks they stood for are named above in
their current form.

Notes
-----

- Supersedes two clauses of :doc:`0009-layered-configuration`; the chain, the
  ``ContextVar`` delivery, and the leaf constraint are unchanged.
- Live-API measurements behind the credential decision were taken 2026-08-11
  against ``api.waterdata.usgs.gov`` and ``api.water.usgs.gov``.
- The ``wateruse`` module is renamed ``nwdc`` under separate cover; the service
  names itself "National Water Availability Assessment Data Companion" and
  serves ten models, only five of which are water use.
