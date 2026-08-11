.. _configuration:

=============
Configuration
=============

``dataretrieval`` has a handful of settings — most importantly your Water Data
API key. Each one resolves through the same ordered chain, so you can pick the
mechanism that suits how your code runs.

.. contents::
   :local:
   :depth: 1


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
       another host would be a supply-chain hazard.


Where settings come from
------------------------

Highest precedence first:

1. A configuration passed to an active ``dataretrieval.configure(...)`` block.
2. A named profile you selected in that block —
   ``WaterdataConfiguration.load("bulk")``.
3. The environment variable for that setting.
4. The adapter's default profile in the configuration file: the
   ``[<adapter>]`` table.
5. The package-wide keys at the top of the configuration file —
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

   from dataretrieval.ngwmn import NgwmnConfiguration
   from dataretrieval.wqp import WqpConfiguration

   with dataretrieval.configure(
       NgwmnConfiguration(concurrency=4), WqpConfiguration(retries=2)
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
``configure(Configuration(concurrency=1))`` can still throttle a call an
enclosing block had scoped to one adapter, and the innermost block decides.

Each adapter accepts only the settings it reads, and they are the fields of its
configuration class — ``concurrency`` and ``parallel_chunks`` are meaningless to
an adapter that issues a single request, so ``StreamstatsConfiguration`` has no
such field and ``[streamstats] parallel_chunks = 8`` is an error rather than a
line that quietly does nothing:

====================================  ======================================  ========================================
Adapter                               Configuration                           Accepts
====================================  ======================================  ========================================
``waterdata``                         ``waterdata.WaterdataConfiguration``    ``concurrency``, ``parallel_chunks``,
                                                                              ``retries``, ``stall_timeout``,
                                                                              ``base_url``
``ngwmn``                             ``ngwmn.NgwmnConfiguration``            the same five
``nwdc``                              ``nwdc.NwdcConfiguration``              ``concurrency``, ``retries``,
                                                                              ``stall_timeout``, ``base_url``
``wqp``, ``nldi``, ``streamstats``    ``wqp.WqpConfiguration`` and so on      ``retries``, ``stall_timeout``,
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

A profile is a named set of settings **for one adapter**. The ``[<adapter>]``
table is that adapter's default profile and is always in effect; a
``[<adapter>.<name>]`` table is a named one, inert until you select it, so
adding one never changes an existing script:

.. code-block:: toml

   api_key = "your_api_key_here"

   [waterdata]
   concurrency = 16          # the default profile: always in effect

   [waterdata.bulk-pull]
   concurrency = "unbounded" # only when selected
   parallel_chunks = 8

   [ngwmn.gentle]
   concurrency = 2

Select one in code, and compose as many as you have adapters:

.. code-block:: python

   from dataretrieval.ngwmn import NgwmnConfiguration
   from dataretrieval.waterdata import WaterdataConfiguration

   with dataretrieval.configure(
       WaterdataConfiguration.load("bulk-pull"),
       NgwmnConfiguration.load("gentle"),
   ):
       df, md = waterdata.get_daily(monitoring_location_id=many_sites)

A named profile states only what differs: everything it does not name still
comes from the adapter's default profile, the package-wide keys, and the tiers
below — per setting. The ``api_key`` above is written once and both profiles
use it.

``load`` reads the table and hands you a configuration object, so a name the
file does not define raises there and then, listing the names it does define —
a profile you just typed is more likely a typo than a request to fall through
to settings you did not ask for. What comes back is inert until you pass it to
``configure``; that is what puts a selected profile above the environment,
since selecting one is something your code did.

A profile holds settings and nothing else: ``[waterdata.bulk-pull.ngwmn]`` is
not a Water Data profile carrying NGWMN detail, and selecting it says so rather
than quietly ignoring the nested table. Two adapters means two profiles,
selected in the same block, as above.

Naming two configurations for the same adapter raises, because there would be
no defined order between them. Combine them into one instead.


A ``configure`` block
---------------------

The highest-precedence source, and the one to use when a setting must apply to
*this* call and no other:

.. code-block:: python

   import dataretrieval
   from dataretrieval import Configuration, waterdata

   with dataretrieval.configure(Configuration(api_key=secrets["usgs"])):
       df, md = waterdata.get_daily(
           monitoring_location_id="USGS-05114000",
           parameter_code="00060",
           time="P7D",
       )

``configure`` takes configuration objects positionally, and nothing else. The
adapter a configuration targets is a property of its class, so you never
restate it — and ``Configuration`` targets none of them in particular, which is
what makes it package-wide.

Because it is backed by a :class:`~contextvars.ContextVar`, the value applies
to the current thread and to asyncio tasks started inside the block, and
cannot leak into another thread or task. That is what makes it usable from a
web service or a notebook working with more than one account:

.. code-block:: python

   # each thread keeps its own key; no os.environ mutation, no race
   def fetch_for(user):
       with dataretrieval.configure(Configuration(api_key=vault.read(user.key_path))):
           return waterdata.get_daily(monitoring_location_id=user.sites)

Blocks nest and merge per setting, so an inner block that tunes one thing
keeps the rest:

.. code-block:: python

   with dataretrieval.configure(Configuration(api_key=key, concurrency=8)):
       ...
       # api_key still applies
       with dataretrieval.configure(Configuration(concurrency=1)):
           ...

Values are validated when the configuration is *constructed*, so a typo raises
on the line you wrote it on rather than deep inside a later request.

Omitted settings inherit from an outer block or a lower-precedence source.
Passing ``None`` explicitly suppresses those sources and restores built-in
behavior for that block. ``Configuration(api_key=None)``, for example, makes an
anonymous call even if ``API_USGS_PAT`` is set.

.. tip::

   Prefer reading the key from a secret store, environment, or config file
   over writing a literal into a script — a literal is what ends up committed
   or pasted into a shared notebook.


Checking what is in effect
--------------------------

``show_configuration()`` reports each setting's effective value and where it came
from. It never prints the key itself:

.. code-block:: python

   >>> dataretrieval.show_configuration()
   config file  /home/u/.dataretrieval/config.toml (found)
   api_key          <set>       /home/u/.dataretrieval/config.toml
   concurrency      32          built-in default
   retries          8           $API_USGS_RETRIES
   progress         auto        built-in default
   parallel_chunks  1           built-in default
   stall_timeout    60s         built-in default

   adapter overrides
     ngwmn  concurrency  4  /home/u/.dataretrieval/config.toml [ngwmn]

   not reported: nldi (not imported, so the settings each accepts are
   unknown here)

Each line names the exact source, including which table inside the file, which
is usually enough to answer "why is it still using my old key?". Only settings
actually overridden for an adapter get a row in the second section; everything
else is inherited from the rows above it.

The last line is the honest cost of validating an adapter's settings lazily:
``dataretrieval`` cannot say what ``nldi`` accepts until something imports it,
so it says that rather than quietly omitting the service.

It never raises. A malformed file or a value that fails its grammar is reported
in place — on the ``config file`` line for a whole-file problem, or in that
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
``show_configuration()``. Put it in a ``[<adapter>.<name>]`` table rather than
at the top level: a named profile applies only to runs that select it, while a
top-level value applies to every query in every process that reads the file,
which is how a setting added for one bulk pull quietly exhausts an hourly quota
months later. ``dataretrieval`` warns if it finds one at the top level.

The value limits optional refinement only. URL-byte safety can require more
sub-requests than the configured value, and an input with nothing to split
stays a single request.

``parallel_chunks(n)`` is sugar for
``configure(Configuration(parallel_chunks=n))``: one scoping mechanism, so the
innermost block wins whichever spelling set it, and ``show_configuration()``
always reports the value the chunker will actually use.


Keeping a key out of your environment entirely
----------------------------------------------

If your credentials live in a secret manager, nothing needs to touch
``os.environ``:

.. code-block:: python

   import dataretrieval
   import boto3
   from dataretrieval import Configuration, waterdata

   secrets = boto3.client("secretsmanager")
   key = secrets.get_secret_value(SecretId="usgs-pat")["SecretString"]

   with dataretrieval.configure(Configuration(api_key=key)):
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
   not appear in :func:`~dataretrieval.show_configuration`.
