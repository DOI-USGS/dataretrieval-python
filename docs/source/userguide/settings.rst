.. _settings:

=============
Settings
=============

``dataretrieval`` retrieves from several services, and most of what you would
want to adjust — a concurrency cap, a retry budget, where requests go — belongs
to *one* of them. So a **settings profile** is a named set of settings for
one adapter, written in code or stored in your settings file, and a
``configure`` block puts one profile per adapter into effect for the calls
inside it. The Water Data API key is the exception that proves the rule: it
authenticates to a gateway rather than to an adapter, so it stays package-wide.

.. contents::
   :local:
   :depth: 1


.. _configuration-one-block:

One block, several services
---------------------------

This is the case the mechanism exists for. Say the file holds what you would
write once and keep — the key, a retry budget, and Water Data's everyday
concurrency — plus two named profiles for the shapes you only sometimes want:

.. code-block:: toml

   api_key = "your_api_key_here"   # package-wide: every adapter that reads it
   retries = 6

   [waterdata]
   concurrency = 16                # waterdata's default profile: always active

   [waterdata.overnight]           # a named profile: only when selected
   concurrency = "unbounded"
   parallel_chunks = 8

   [ngwmn.gentle]
   concurrency = 2

Then one block configures three services, taking two of them from the file by
name and building the third on the spot:

.. code-block:: python

   import dataretrieval
   from dataretrieval import ngwmn, waterdata, wqp
   from dataretrieval.ngwmn import NgwmnSettings
   from dataretrieval.waterdata import WaterdataSettings
   from dataretrieval.wqp import WqpSettings

   with dataretrieval.configure(
       WaterdataSettings.load("overnight"),  # from the file, by name
       NgwmnSettings.load("gentle"),         # from the file, by name
       WqpSettings(retries=2),               # built here
   ):
       flow, _ = waterdata.get_daily(monitoring_location_id=sites, time="P30D")
       levels, _ = ngwmn.get_water_level(monitoring_location_id=wells)
       samples, _ = wqp.get_results(siteid=sites)

Inside the block Water Data runs unbounded and asks the planner for eight
chunks, NGWMN runs two requests at a time, and WQP retries twice. Everything a
configuration does *not* name still comes from below it, per setting: Water
Data and NGWMN both retry six times and both send the ``api_key``, written once
at the top of the file, because a configuration contributes what it names and
inherits the rest. Only WQP named ``retries``, so only WQP departs from the
file's six.

Outside the block nothing has changed, and putting those two profiles in the
file changed nothing on its own — a named profile is inert until a caller
selects it, which is what makes one safe to add to a file other people's jobs
also read.

Two rules keep a block like that unambiguous. A configuration knows which
adapter it targets — that is a property of its class — so you never restate it,
and ``Settings`` targets none of them, which is what makes it
package-wide. And there is at most one configuration per adapter: naming two
raises rather than picking one, because there would be no defined order between
them — combine them into one instead.


Settings
--------

.. list-table::
   :header-rows: 1
   :widths: 18 12 26 44

   * - Setting
     - Default
     - Environment variable
     - What it does
   * - ``api_key``
     - none
     - ``API_USGS_PAT``
     - Water Data API key. Raises your hourly request quota substantially;
       `register for one <https://api.waterdata.usgs.gov/signup/>`_.
   * - ``concurrency``
     - ``32``
     - ``API_USGS_CONCURRENT``
     - Cap on sub-requests in flight at once for a chunked query. A positive
       integer, ``1`` to run them one at a time, or ``"unbounded"`` to remove
       the cap. Does not change how many requests are made, only how many run
       simultaneously.
   * - ``retries``
     - ``4``
     - ``API_USGS_RETRIES``
     - Retries after a transient failure (429, 5xx, timeout). ``0`` disables.
   * - ``progress``
     - auto
     - ``API_USGS_PROGRESS``
     - Whether to draw the status line. Auto means on for a terminal or
       Jupyter kernel, off for redirected output and CI.
   * - ``parallel_chunks``
     - ``1``
     - *(none — see below)*
     - Default fan-out for multi-value queries. ``1`` means split only as far
       as the URL byte limit forces.
   * - ``stall_timeout``
     - ``60``
     - ``API_USGS_STALL_TIMEOUT``
     - Seconds a call may go without receiving *any* data before retrying
       stops and the failure surfaces. Bounds the wall-clock cost of a dead
       connection, which ``retries`` alone does not — it counts attempts, not
       seconds. Progress resets the clock; ``0`` disables the bound.
   * - ``base_url``
     - the service's own
     - *(none — code only)*
     - Where to send one service's requests. Per adapter, and settable only in
       a ``configure`` block: a file that silently redirected the library to
       another host would be a supply-chain hazard. See
       :ref:`configuration-redirect`.


Where settings come from
------------------------

Highest precedence first:

1. A configuration passed to an active ``dataretrieval.configure(...)`` block.
2. A named profile you selected in that block —
   ``WaterdataSettings.load("bulk")``.
3. The environment variable for that setting.
4. The adapter's default profile in the settings file: the
   ``[<adapter>]`` table.
5. The package-wide keys at the top of the settings file —
   ``~/.dataretrieval/config.toml``, or the path in ``DATARETRIEVAL_CONFIG``.
6. The adapter's own built-in preference, where it has one — NWDC asks for a
   ``concurrency`` of 4, because that is as far as the service is
   stress-tested. It is a default, not a cap: anything you set above outranks
   it.
7. The package built-in default, which for ``concurrency`` is 32.

The top two rungs both name a single adapter, and naming two configurations
for one adapter raises, so they cannot disagree inside one block. Between
nested blocks the innermost decides, as it does for everything else.

Precedence applies **per setting**. An environment that sets only
``API_USGS_PAT`` leaves a file-provided ``concurrency`` fully in effect —
sources are merged, not replaced.

A variable that is *set but empty* (``export API_USGS_PAT=``, or a CI secret
that resolves to nothing) does not count as configured, so an empty variable
your tooling happened to create cannot silently discard the key in your config
file. The one exception is ``API_USGS_PROGRESS``, where blank has always meant
"off" and so is treated as a real value.

.. note::

   The environment ranks above the file, matching common deployment tools and
   preserving the existing ``API_USGS_*`` variables as authoritative runtime
   overrides. The reasoning is in :doc:`ADR 0009
   </architecture/decisions/0009-layered-configuration>`.

   The one exception is rung 2 above rung 3 — a profile you name in code. That
   is a more deliberate act than a variable inherited from whatever started
   your process, and having it lose to that variable is the kind of thing you
   would file a bug about. The inversion covers what the profile names and
   nothing else: every setting you did *not* name still follows the
   environment-above-file rule, in the same block. See :doc:`ADR 0011
   </architecture/decisions/0011-configuration-profiles>`.


An environment variable
-----------------------

Still fully supported, and the simplest option for a single key on one
machine:

.. code-block:: bash

   export API_USGS_PAT="your_api_key_here"

This is also the mechanism the `R dataRetrieval package
<https://github.com/DOI-USGS/dataRetrieval>`_ uses, under the same variable
name, so one export serves both.


A configuration file
--------------------

Better when you would rather not have a credential in your shell environment,
where it is inherited by every process you start. Create
``~/.dataretrieval/config.toml``:

.. code-block:: toml

   api_key = "your_api_key_here"

Restrict it so other users on the machine cannot read it — ``dataretrieval``
warns once if a file containing a key is group- or world-readable:

.. code-block:: bash

   chmod 600 ~/.dataretrieval/config.toml

Any setting can go in the file:

.. code-block:: toml

   api_key = "your_api_key_here"
   concurrency = 16
   retries = 8

Point ``DATARETRIEVAL_CONFIG`` at a different path to override the location —
useful for a container or a job scheduler that mounts secrets elsewhere.


Per-adapter settings
~~~~~~~~~~~~~~~~~~~~

To tune one service and leave the rest alone, name the adapter — the same name
you import:

.. code-block:: toml

   concurrency = 16          # every adapter

   [ngwmn]
   concurrency = 4           # NGWMN only

   [wqp]
   retries = 2

.. code-block:: python

   from dataretrieval.ngwmn import NgwmnSettings
   from dataretrieval.wqp import WqpSettings

   with dataretrieval.configure(
       NgwmnSettings(concurrency=4), WqpSettings(retries=2)
   ):
       ...

An adapter table *overrides* the top-level one per setting, so ``[ngwmn]``
above still inherits ``retries`` and the ``api_key``. Precedence is unchanged
otherwise: an adapter-scoped value outranks a package-wide one only within the
same source, so ``API_USGS_CONCURRENT`` exported for one run still beats a
``[ngwmn] concurrency`` in the file.

Between ``configure`` blocks that tie-break applies per block: an adapter
configuration beats a package-wide value set by the *same* block, while
anything set by a block nested inside it wins over both. So a
``configure(Settings(concurrency=1))`` can still throttle a call an
enclosing block had scoped to one adapter, and the innermost block decides.

Each adapter accepts only the settings it reads, and they are the fields of its
configuration class — ``concurrency`` and ``parallel_chunks`` are meaningless to
an adapter that issues a single request, so ``StreamstatsSettings`` has no
such field and ``[streamstats] parallel_chunks = 8`` is an error rather than a
line that quietly does nothing:

====================================  ======================================  ========================================
Adapter                               Settings profile                        Accepts
====================================  ======================================  ========================================
``waterdata``                         ``waterdata.WaterdataSettings``         ``concurrency``, ``parallel_chunks``,
                                                                              ``retries``, ``stall_timeout``,
                                                                              ``base_url``
``ngwmn``                             ``ngwmn.NgwmnSettings``                 the same five
``nwdc``                              ``nwdc.NwdcSettings``                   ``concurrency``, ``retries``,
                                                                              ``stall_timeout``, ``base_url``
``wqp``, ``nldi``, ``streamstats``    ``wqp.WqpSettings`` and so on           ``retries``, ``stall_timeout``,
                                                                              ``base_url``
====================================  ======================================  ========================================

Each class lives in the module whose code reads those settings, so a setting's
definition sits next to its use rather than in a service-neutral file.

``api_key`` is deliberately not per-adapter. It authenticates to the *gateway*
in front of a host, and Water Data and NGWMN are served from the same host —
one key, one hourly quota shared between them — so a per-adapter key would
describe a distinction the service does not have. ``progress`` is likewise
package-wide: there is one progress line per call.


Named profiles
~~~~~~~~~~~~~~

An adapter can hold more than one shape at a time. The ``[<adapter>]`` table is
that adapter's **default profile** — always in effect, as above — while a
``[<adapter>.<name>]`` table is a **named profile**, inert until you select it:

.. code-block:: toml

   [waterdata]
   concurrency = 16          # the default profile: always in effect

   [waterdata.bulk-pull]
   concurrency = "unbounded" # only when selected
   parallel_chunks = 8

So one file can hold an overnight bulk shape beside a polite daytime one, and
name as many of each as an adapter has uses for.

A named profile states only what differs: everything it does not name still
comes from the adapter's default profile, the package-wide keys, and the tiers
below — per setting.

``load`` reads the table and hands you a configuration object, so a name the
file does not define raises there and then, listing the names it does define —
a profile you just typed is more likely a typo than a request to fall through
to settings you did not ask for. What comes back is inert until you pass it to
``configure``; that is what puts a selected profile above the environment,
since selecting one is something your code did.

A profile holds settings and nothing else: ``[waterdata.bulk-pull.ngwmn]`` is
not a Water Data profile carrying NGWMN detail, and selecting it says so rather
than quietly ignoring the nested table. Two adapters means two profiles,
selected in the same block, as in :ref:`the example above
<configuration-one-block>`.


A ``configure`` block
---------------------

The highest-precedence source, and the one to use when a setting must apply to
*this* call and no other:

.. code-block:: python

   import dataretrieval
   from dataretrieval import Settings, waterdata

   with dataretrieval.configure(Settings(api_key=secrets["usgs"])):
       df, md = waterdata.get_daily(
           monitoring_location_id="USGS-05114000",
           parameter_code="00060",
           time="P7D",
       )

``configure`` takes configuration objects positionally, and nothing else. The
adapter a configuration targets is a property of its class, so you never
restate it — and ``Settings`` targets none of them in particular, which is
what makes it package-wide.

.. note::

   Settings are not keywords on ``configure``. ``configure(api_key=...)`` and
   the per-adapter mappings ``configure(ngwmn={"concurrency": 4})`` were an
   earlier spelling and are gone; write ``Settings(api_key=...)`` and
   ``NgwmnSettings(concurrency=4)`` instead. Passing anything that is not
   a configuration raises and names the replacement, so an old script says what
   to write rather than failing obscurely.

Because it is backed by a :class:`~contextvars.ContextVar`, the value applies
to the current thread and to asyncio tasks started inside the block, and
cannot leak into another thread or task. That is what makes it usable from a
web service or a notebook working with more than one account:

.. code-block:: python

   # each thread keeps its own key; no os.environ mutation, no race
   def fetch_for(user):
       with dataretrieval.configure(Settings(api_key=vault.read(user.key_path))):
           return waterdata.get_daily(monitoring_location_id=user.sites)

Blocks nest and merge per setting, so an inner block that tunes one thing
keeps the rest:

.. code-block:: python

   with dataretrieval.configure(Settings(api_key=key, concurrency=8)):
       ...
       # api_key still applies
       with dataretrieval.configure(Settings(concurrency=1)):
           ...

Values are validated when the configuration is *constructed*, so a typo raises
on the line you wrote it on rather than deep inside a later request.

Omitted settings inherit from an outer block or a lower-precedence source.
Passing ``None`` explicitly suppresses those sources and restores built-in
behavior for that block. ``Settings(api_key=None)``, for example, makes an
anonymous call even if ``API_USGS_PAT`` is set.

.. tip::

   Prefer reading the key from a secret store, environment, or config file
   over writing a literal into a script — a literal is what ends up committed
   or pasted into a shared notebook.


Checking what is in effect
--------------------------

``show_settings()`` reports each setting's effective value and where it came
from. It never prints the key itself. The report below is what a file holding a
key, a package-wide ``concurrency``, an ``[ngwmn]`` table and a
``[waterdata.bulk]`` profile produces, with ``API_USGS_RETRIES`` exported and
the ``bulk`` profile selected for the block:

.. code-block:: python

   >>> with dataretrieval.configure(WaterdataSettings.load("bulk")):
   ...     dataretrieval.show_settings()
   settings file  /home/u/.dataretrieval/config.toml (found)
   api_key          <set>  /home/u/.dataretrieval/config.toml
   concurrency      16     /home/u/.dataretrieval/config.toml
   retries          8      $API_USGS_RETRIES
   progress         auto   built-in default
   parallel_chunks  1      built-in default
   stall_timeout    60s    built-in default

   A built-in default is package-wide. An adapter may prefer its own for
   its own calls; a value from any source above overrides both.

   adapter overrides
     waterdata  parallel_chunks  8  configure() block [waterdata.bulk]
     ngwmn      concurrency      4  /home/u/.dataretrieval/config.toml [ngwmn]

   profiles in the file: [waterdata.bulk]
     A profile applies only where a row above names it; select one in
     code with <Adapter>Settings.load("<name>").

   not reported: nldi (not imported, so the settings each accepts are unknown here)

Each line names the exact source, including which table inside the file, which
is usually enough to answer "why is it still using my old key?". A value that
came from a profile names the profile — ``configure() block
[waterdata.bulk]``, not merely "a block" — so a report taken from inside a
``with`` block says which selection produced it. Only settings actually
overridden for an adapter get a row in the second section; everything else is
inherited from the rows above it.

The profile section lists what the *file* defines, whether or not this run
selected any of it. A named profile does nothing until a caller selects it, so
seeing ``[waterdata.bulk]`` there while no row above mentions it is the answer
to "I added a profile and nothing changed".

The last line is the honest cost of validating an adapter's settings lazily:
``dataretrieval`` cannot say what ``nldi`` accepts until something imports it,
so it says that rather than quietly omitting the service. It is named rather
than left out, because an omitted service would read as "nothing is configured
for it", which is a different claim.

It never raises. A malformed file or a value that fails its grammar is reported
in place — on the ``settings file`` line for a whole-file problem, or in that
setting's own row — because a broken configuration is exactly when you reach
for this.


Why ``parallel_chunks`` has no environment variable
---------------------------------------------------

Every other setting can be set from the environment. ``parallel_chunks``
cannot, on purpose.

Raising it splits a query into more sub-requests, and *each sub-request spends
rate-limit quota*. Whether that is a good trade depends on the size of the
query — which the library cannot know in advance. The setting therefore does
not add another process-global environment knob that could be exported once
and inherited by every subprocess.

Set it per call, which is almost always what you want:

.. code-block:: python

   with waterdata.parallel_chunks(8):
       df, md = waterdata.get_daily(monitoring_location_id=many_sites)

or as a baseline in the config file — deliberately written, and visible in
``show_settings()``. Put it in a ``[<adapter>.<name>]`` table rather than
at the top level: a named profile applies only to runs that select it, while a
top-level value applies to every query in every process that reads the file,
which is how a setting added for one bulk pull quietly exhausts an hourly quota
months later. ``dataretrieval`` warns if it finds one at the top level.

The value limits optional refinement only. URL-byte safety can require more
sub-requests than the configured value, and an input with nothing to split
stays a single request.

``parallel_chunks(n)`` is sugar for
``configure(Settings(parallel_chunks=n))``: one scoping mechanism, so the
innermost block wins whichever spelling set it, and ``show_settings()``
always reports the value the chunker will actually use.


.. _configuration-redirect:

Pointing an adapter at another host
-----------------------------------

``base_url`` sends one adapter's requests somewhere else — a staging instance,
a mirror, or a recording proxy — for the duration of a block:

.. code-block:: python

   import dataretrieval
   from dataretrieval import waterdata
   from dataretrieval.waterdata import WaterdataSettings

   with dataretrieval.configure(
       WaterdataSettings(base_url="https://staging.example/waterdata")
   ):
       df, md = waterdata.get_daily(monitoring_location_id="USGS-05114000")

It names one adapter, so nothing else moves: NGWMN is served from the same host
as Water Data, and a ``WaterdataSettings`` still leaves it alone. What the
value replaces is that adapter's own base, and the package appends its usual
paths to it — for Water Data that is the root all four of its APIs hang off, so
one value moves the OGC collections, the Samples database, the statistics
service and the STAC catalog together.

**Code only.** The configuration file and the environment both refuse it. A
``base_url`` key anywhere in the file, and an exported ``API_USGS_BASE_URL``,
each raise a ``ConfigurationError`` saying the setting *may only be set in
code, in a configure() block* and naming the configuration to pass it on
instead.

A file or a shell export that silently redirected a data-retrieval library to
another host would be a supply-chain hazard: nothing at the call site would
show it, and a script that reads correctly would be talking to someone else's
service. A ``with`` block keeps the redirect where a reader of the script sees
it. The refusal is loud rather than silent for the same reason — a variable
that was quietly ignored would leave you believing you had redirected
something.

**The API key does not follow.** It is scoped to the one host that honors it
(:ref:`below <configuration-secret-store>`), so a redirected call goes out
without it. That is deliberate: the host you redirected to is not the host you
gave a credential to. If the mirror needs its own credential, it needs its own
mechanism.


.. _configuration-secret-store:

Keeping a key out of your environment entirely
----------------------------------------------

If your credentials live in a secret manager, nothing needs to touch
``os.environ``:

.. code-block:: python

   import dataretrieval
   import boto3
   from dataretrieval import Settings, waterdata

   secrets = boto3.client("secretsmanager")
   key = secrets.get_secret_value(SecretId="usgs-pat")["SecretString"]

   with dataretrieval.configure(Settings(api_key=key)):
       df, md = waterdata.get_continuous(monitoring_location_id="USGS-05114000")

Wherever the key comes from, it is sent only to ``api.waterdata.usgs.gov`` and
is stripped from any cross-host redirect, so it cannot leak to another host.


Behind a TLS-intercepting proxy
-------------------------------

On a corporate network that re-signs HTTPS traffic, requests fail with a
certificate-verification error. Point the standard OpenSSL variables at your
organization's CA bundle:

.. code-block:: bash

   export SSL_CERT_FILE=/path/to/corporate-ca.pem
   # or, for a directory of hashed certificates:
   export SSL_CERT_DIR=/etc/ssl/certs

``httpx`` honors these natively, so they apply to **every** getter in the
package — including the OGC collection getters (``get_daily``,
``get_continuous``, and the rest), which take no SSL parameter of their own.

Prefer this to ``ssl_check=False``. That argument exists on some of the older
getters and switches certificate verification *off* rather than trusting your
CA, so it accepts any certificate a network path offers — and it is not
available on the OGC getters at all. A CA bundle keeps verification on and
works everywhere.

.. note::

   ``SSL_CERT_FILE`` is read by OpenSSL, not by ``dataretrieval``, so it does
   not appear in :func:`~dataretrieval.show_settings`.
