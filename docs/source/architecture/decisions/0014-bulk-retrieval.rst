ADR 0014: Read history from the nightly files and changes from the API
=======================================================================

Status
------

Proposed

Context
-------

Since October 2026, USGS publishes the ``daily`` and ``continuous`` OGC
collections a second way: as Parquet files, one set per monitoring location,
written by a nightly job and indexed by the Water Data STAC catalog
(https://waterdata.usgs.gov/blog/api-faster-access/). ``continuous`` has two
files per location, ``historical`` and ``current``. ``historical`` ends 150
days before the night it was last written, and ``current`` starts at the
next value, so ``current`` holds 150 days or more. On 2026-11-23, every
``current`` file sampled had been written that night, and ``historical``
files had been written on dates from 2026-10-26 to that night.

The two sources trade speed for freshness. Paging the OGC API for the full
daily discharge record of 40 Ohio gages took 39.8 s; downloading their files
took a few seconds. But a file is a snapshot. It lags the API by up to a day,
so a caller reading only files misses the newest values and every revision
since the snapshot.

The files carry what is needed to close that gap. Every record has the same
``last_modified`` as in the API, and the overlapping records were identical in
value and approval status when compared. A record changed after the file was
written therefore has a larger ``last_modified`` than any record in the file,
and an OGC query can select exactly those changes.

Two facts constrain the result's shape. ``daily_id`` and ``continuous_id`` are
random version-4 UUIDs (checked against hashes of ``time_series_id`` and
``time``). They are stable, and each resolves as the record's item in the OGC
API, but they hold no data and the files omit them, so a row read from a file
cannot carry one. And a ``last_modified`` query sees changes but not removals.

Decision
--------

**Keep one getter per collection, and choose the path with a setting.**
``get_daily`` and ``get_continuous`` keep their signatures. The Water Data
adapter gains a ``bulk`` setting (ADR 0011), ``"off"``, ``"auto"``, or
``"always"``, set with ``WaterdataConfiguration(bulk=...)`` or in the
``[waterdata]`` table of the configuration file. Under ``"always"`` the
getters return the same columns and types from a different retrieval path,
less the record id (below). The default is ``"auto"``, which lands in a
major release because it removes the record id from most ``get_daily`` and
``get_continuous`` results. It changes no signature. ``"off"`` keeps the
API path and the record id.

The choice is a setting because it changes how a query runs, not what it
asks for, like ``parallel_chunks`` and ``api_version``. A keyword would add
a parameter to two signatures that most callers should never set: with
``"auto"`` as the default, the router decides. Forcing a path remains
possible for benchmarks, tests, and bug reports, with a ``configure`` block.
Like ``api_version``, it has no environment variable (ADR 0011 refuses one
for adapter-only settings).

**The retrieval path is one private module,** ``waterdata/_bulk.py``. The
getters build their arguments as before and hand them to its one entry point,
``get_data``, which reads the setting and answers from the files or the API.
The getters do not know which. Nothing in the module is public, so its plan,
merge, and file handling can change without a deprecation. What differs
between ``daily`` and ``continuous`` (the id column, the accepted filters, a
date or a timestamp ``time``, the ``historical`` file, geometry, the row
estimate) is one table in that module.

**With the setting on, the result has no record id.** For a query the files
can answer, ``daily_id`` / ``continuous_id`` is left out whether the files or
the API answered it. A column filled only for the rows changed since last
night is of no use to a caller, and under ``"auto"`` the columns would
otherwise depend on how many locations the query named.
``(time_series_id, time)`` identifies a record in both. A query only the API
accepts is returned as the API gives it, and ``"off"`` changes nothing.

**A snapshot plus its changes.** For the requested monitoring locations:

1. Search the catalog for their files. ``waterdata.catalog.search`` splits
   the identifiers across searches that each fit the edge firewall's URL
   limit, and runs them as chunks of one fan-out.
2. Download every file as one fan-out chunk (ADR 0008), and read only the rows
   matching the query's stable filters and ``time``. ``historical`` is skipped
   when ``time`` starts within the last 120 days.
3. Take each location's snapshot time ``T`` as the largest ``last_modified``
   in its files. Read it from the row-group statistics, and floor it to the
   day as a margin. For ``continuous``, take the later of the two files.
   ``historical`` holds only old records, so its newest change can be months
   older than the night it was written. This rule depends on ``historical``
   being rewritten the night after one of its records changes: on
   2026-11-23, no record in the range of five older ``historical`` files had
   changed in the API since the file was written.
4. Send **one** OGC query whose CQL-text filter has a top-level ``OR`` clause
   per cutoff day, ``monitoring_location_id IN (...) AND last_modified >= T``,
   plus one clause asking for the whole query for any location without a
   usable file. Each clause names at most 100 locations, because the OGC
   chunker splits a filter only at top-level ``OR``. It does that split when
   the filter outgrows a URL, and fans the pieces out under the concurrency
   cap. So the change clauses run concurrently without a second fan-out
   mechanism.
5. Concatenate, and keep the API's version of every ``(time_series_id,
   time)`` present in both.

Steps 4 and 5 are the downloads' ``finalize`` hook, so resuming an
interruption (``exc.call.resume()``) returns the whole result. A file that
answers 404 is treated as missing and its location gets the whole query; any
other error on a file is raised.

**Filters are classified by whether a record can change them.**
``parameter_code``, ``statistic_id``, ``time_series_id``, and
``unit_of_measure`` are fixed when a record is written, so they are applied
while reading the files and sent with the change query.
``approval_status`` and ``method_category`` can change after a record is
written, so the change query does not send them, and the merge applies them.
Otherwise a file row that was Provisional at the snapshot and is Approved now
would be returned for ``approval_status="Provisional"``. The change query
would never fetch the Approved version, so nothing would replace it.

**Under ``"always"``, reject what the path cannot honor.**
``monitoring_location_id`` is required,
because the files are published per location. Every argument outside the
filters above, ``time``, and ``skip_geometry`` raises ``ValueError`` before
any request is sent: ``filter``, ``properties``, ``max_rows``,
``convert_type=False``, a duration in ``time``, and ``method_category`` for
``daily``.

**``"auto"`` chooses per query, without a request.** A query the bulk
path cannot honor goes to the API. Otherwise the router estimates the rows
the API would page through, as locations x series x values per day x days,
and reads the files when that exceeds 10,000 rows plus a per-location
download cost: 250 rows' worth for a daily or ``current`` file, 10,000 for a
``historical`` one. The series count comes from ``parameter_code`` and
``statistic_id`` (2 for daily and 3 for continuous when neither is given),
and an open-ended ``time`` counts as a typical record (40 years daily, 20
continuous). The estimate can be wrong. The rule is tuned so that a wrong
choice costs at most a second or two, while the right one saves minutes on
large pulls.

The constants come from a benchmark run on 2026-11-22 against the live
services. It drew fresh monitoring locations for every run, so that no run
was served from a cache warmed by another: 31 query shapes, two runs each,
for both paths, over 1-100 locations and 7 days to the whole record. Paging
cost about 1 s per 8,000-12,000 rows. The files cost about 2 s, plus a little
per file. The rule picked the faster path in 29 of the 31 shapes. The two
misses cost 0.1 s and 0.4 s. In a holdout of 7 new shapes over other states,
it picked the faster path every time. Measured extremes:

=============================================  ========  =======
Query                                          API       Files
=============================================  ========  =======
1 location, 1 year daily                       0.8 s     1.9 s
100 locations, whole daily record              304.5 s   9.7 s
25 locations, 60 days continuous               23.0 s    5.7 s
1 location, 7 days continuous                  1.0 s     2.1 s
=============================================  ========  =======

``"auto"`` is the default.

**pyarrow is an optional extra,** ``dataretrieval[bulk]``, imported only on
the bulk path. Without it, a query the setting sends to the files pages the
API instead and emits a ``UserWarning`` that names the install command. A
setting can come from a shared configuration file, and a missing optional
dependency should not turn a working call into a failure.

Consequences
------------

- For the 40-gage pull above, the bulk path returned the same 421,785 rows,
  values, columns, and types in 10.6 s instead of 39.8 s. One site's
  continuous discharge since 2025 took 3.1 s instead of 5.9 s. A short query
  for one location gains nothing, which is why ``"auto"`` routes it to the
  API.
- With ``"auto"`` or ``"always"``, a result has no ``daily_id`` /
  ``continuous_id`` column. Under the default, that is every
  ``get_daily`` and ``get_continuous`` call the files can answer, including
  the small ones the router sends to the API. A caller that needs the id
  sets ``"off"``.
- A record removed from the API since the files were written is still
  returned, until the next night's files. Every case seen so far is the
  trailing edge of a rolling window: Provisional daily values drop out after
  three years, and some series keep only 120 days. Files written before the
  window moved still hold the day or two that dropped off. Against freshly
  written files, the results were identical.
- The path depends on behavior USGS has not documented as a contract: the
  file URL layout, the ``current`` / ``historical`` split and its 150-day
  boundary, ``last_modified`` in the files matching the API, and
  ``historical`` being rewritten the night after one of its records changes.
- Each file and each catalog page counts as one request against the API
  key's hourly count. The OGC API, the catalog, and the files share one
  count, with different ceilings: 1,000, 4,000, and 10,000 requests an hour
  on 2026-11-23. The change query runs after every download, so a pull of
  about 1,000 files leaves it no OGC requests for the rest of the hour.
- ``get_ratings``, ``get_parquet_files``, and this path share one catalog
  search, ``waterdata.catalog.search``, which alone knows the edge's URL
  limit.
- The configuration guide and ADR 0011 use ``bulk`` as an example profile
  name (``[waterdata.bulk]``). Now that ``bulk`` names a retrieval path, the
  guide's example should be renamed in a separate change. ADR 0011 is
  accepted and stays as written.

Alternatives considered:

- **Separate getters,** ``get_bulk_daily`` and ``get_bulk_continuous``. Each
  signature would list only what works, but there would be two more public
  functions per collection that resemble their originals, and callers would
  need to know they exist. A setting makes the faster path available to
  existing calls, and a default flip later changes no import.
- **A ``bulk`` keyword on both getters** (``bool | "auto"``), as first
  drafted. It puts a choice most callers should leave to the router into
  both signatures, and it makes a script opt in call by call instead of
  once in the configuration file.
- **A ``source=`` keyword.** *Source* is a core term (ADR 0013), naming where
  a setting's value came from, and one spelling binds identifiers too.
- **An Iceberg or DuckLake catalog over the files, built outside USGS.** The
  files are rewritten in place each night, so a catalog's per-file statistics
  go stale. In a test, a filtered Iceberg scan of a rewritten file returned
  no rows instead of 300. Such a catalog is correct only if the job that
  writes the files commits it.
- **Trimming file rows to each series' ``begin`` from
  ``get_time_series_metadata``.** The metadata lagged the API by a day too,
  and it cannot express a Provisional span that drops out in the middle of a
  series.

Follow-up work, not part of this decision:

- Ask WDFN to confirm the retention rules (which records expire, when, and
  whether the files and the API cut over at the same moment). With those
  rules, a short full query around each window edge would remove the extra
  rows.
- Ask WDFN to confirm the URL layout and the file-generation behavior as a
  stable contract.

Compliance
----------

- ``tests/waterdata_bulk_test.py`` (offline) pins the routing rule's eligibility
  and size decisions, and the merge rule (the API
  wins), the classification of filters, the single OR-clause change query,
  the whole-query clause for locations without a file, the cutoff from the
  later file, the ``historical`` skip, column types when no record changed,
  chunked catalog searches and change clauses, the 404-only fallback, resume
  returning the whole result, and every rejection.
- ``test_live_bulk_matches_the_api`` (``live`` marker, scheduled run) compares
  ``bulk="always"`` with ``bulk="off"`` for both getters. It checks rows, values,
  columns, and types, less the id column. A failure there means
  one of the undocumented behaviors above has changed.
- ``test_live_file_layout_is_unchanged`` (``live``) reads the footers of
  three files with range requests and checks the layout a range-request
  reader would rely on: rows sorted by ``time`` in row groups of 50,000, and
  UTC ``last_modified``. Nothing in the current read path depends on it.
- ``tests/configuration_test.py`` pins the setting: ``"auto"`` by default,
  read from code and the ``[waterdata]`` table, validated, and refused from
  the environment. ``tests/waterdata_bulk_test.py`` pins the pyarrow
  fallback and its warning.

Notes
-----

- #476 implements this decision, with ``waterdata.get_parquet_files`` and
  the shared catalog search.
