# Context

The shared vocabulary for `dataretrieval`. This is a glossary, not a
specification: it fixes what words mean so that code, docstrings, ADRs, and
conversation use them the same way. Architectural decisions live in
`docs/source/architecture/decisions/`.

When a term here conflicts with a name in the code, the term wins and the name
is legacy. Legacy names are listed below.

## Retrieval

**Getter** — A public function that retrieves data and returns
`(DataFrame, metadata)`. The package's unit of public API. `waterdata.get_daily`
and `wqp.get_results` are getters; the helpers they call are not.

**Query** — One logical request a caller makes by calling a getter. A query may
reach the service as several requests; it is still one query.

**Chunk** — One of the requests a query was split into. Following Dask, where
chunks describe how an array is split into sub-arrays, a chunk is a piece of the
whole, named for *being a piece* rather than for why it was made one.

A query needing no split has exactly one chunk, not zero.

**Chunking** — How a query is split into chunks. A query may be chunked because
the service forces it — a URL over the byte limit, or an API accepting one
location per request — or because the caller asked for it. Both produce chunks;
the reason is not part of the term.

**Plan** — An enumeration of a query's chunks: how many there are, and what each
one is. A plan says how a query divides; it does not execute. Computing a plan
is protocol-specific — a byte budget, a per-location rule — while executing one
is not, which is why the two live apart.

**Fan-out** — Executing a query's chunks concurrently. Chunking is how the work
divides; fan-out is how it is distributed. The two are independent, and only
chunking depends on the service's protocol.

**Page** — One response in a cursor-followed sequence from a single chunk. A
page is *not* a chunk: chunks divide a query, pages divide a chunk's response.
A chunk of a large query commonly spans many pages.

## Failure and resumption

**Transient failure** — A failure a later attempt could survive: a rate limit, a
service error, a timeout. Distinguished from a **deterministic failure**, which
would fail identically every time — an unresolvable hostname, an unsupported
scheme, a malformed request. Only transient failures are retried, and only
transient failures produce a resumable interruption. Both answers follow from
one judgement about what a failure means, and must agree.

**Stall timeout** — How long a call may receive nothing at all before retrying
stops, measured from when data last arrived rather than from the call's start.
ADR 0006 sets the policy and calls it the *no-progress budget*.

**Interruption** — A transient failure that stopped a fan-out partway, raised
with the completed chunks preserved. The caller may wait for the condition to
clear and resume.

**Resume** — Continuing an interrupted query by re-issuing only the chunks that
did not complete. Completed chunks are never re-fetched. A chunk that failed
partway through its pages is not complete, so resuming re-walks its pages from
the start.

## Services

Each is an external system this package retrieves from. They are separate
services with separate conventions, not one API with modes.

**Water Data** — The modern USGS API at `api.waterdata.usgs.gov`, covering
monitoring locations, time series, field measurements, samples, ratings, and
statistics. The package's primary target.

**NGWMN** — The National Ground-Water Monitoring Network, a distinct OGC API
covering sites, water levels, lithology, well construction, and providers.

**NWDC** — The National Water Availability Assessment Data Companion. Serves
ten modeled national-scale datasets, of which the water-use models are five;
the rest are hydrologic, atmospheric-forcing, and assessment outputs. The
package reaches it through the `nwdc` adapter, named for the service like every
other adapter. Legacy: that module was `wateruse`, which named one subset of
what the service offers.

**WQP** — The Water Quality Portal, a multi-agency water-quality clearinghouse.

**NLDI** — The Network Linked Data Index, which navigates the hydrologic network
from an origin to connected features, flowlines, or basins.

**NWIS** — The legacy USGS waterservices interface. Deprecated: it is retained
for compatibility and is not where new work goes.

**StreamStats** — Basin characteristics and delineation for a point on a stream.

## Data

**Collection** — One named set of records a service offers — `daily`,
`monitoring-locations`, `time-series-metadata`. The unit a getter targets.

A collection is not a service. Water Data is a service; `daily` is one of its
collections. The distinction matters because the OGC machinery is shared: the
same code path retrieves a Water Data collection and an NGWMN one, and only the
service differs.

**Collection family** — A group of collections sharing a shape and therefore a
getter signature. Their getters deliberately resemble one another; the
resemblance is the public contract, not duplication to be removed.

**Monitoring location** — A place where measurements are recorded. The canonical
term. Legacy: the deprecated NWIS getters and the WQP profiles call this a
*site*, and their parameters keep that spelling.

**Metadata** — The second half of every getter's return: the request URL, the
elapsed time, and the response headers. Describes the *retrieval*, not the data.

## Configuration

**Configuration profile** — A named set of settings for one adapter, stored in
the configuration file or built in code. A profile is an *input* to resolution,
never its result. In prose, **configuration** shortens this; in code,
`Configuration` is the package-wide settings class, and an adapter's profile is
its own `*Configuration` subclass. The two differ only by case, so prefer the
full term wherever a reader could take it either way.

**Default profile** — The profile an adapter uses when no other is selected:
the `[<adapter>]` table's own keys. Always in effect. A **named profile**
(`[<adapter>.bulk]`) is in effect only when a caller selects it, so adding one
to a file never changes an existing script.

**Effective configuration** — The resolved set of settings a call will use:
what the chain produces after every profile, variable and default has been
applied. Distinct from a configuration profile, which is one contribution to
it. **Configure** is the verb for applying one.

**Setting** — One named tunable the caller may adjust: the API key, the
concurrency cap, the retry count, the progress line, the fan-out baseline. A
setting means the same thing wherever it applies, but it does not apply
everywhere: `concurrency` is meaningless to an adapter that issues one request
at a time, and `parallel_chunks` applies only to the adapters whose chunking the
planner can refine. Which settings an adapter accepts is part of that adapter's
vocabulary.

A public keyword is not automatically a setting. `ssl_check` is a getter
argument on four adapters and resolves through no chain at all; the settings are
the roster the configuration system knows.

**Package-wide setting** — A setting that applies to every adapter: the retry
count, the progress line, the stall timeout. Set once, honored everywhere.

**Adapter-scoped setting** — A setting named under one adapter, applying to
that adapter and no other. It overrides the package-wide value for that adapter
alone; it does not replace the package-wide tier. An adapter rejects a setting
it has no use for, rather than accepting and ignoring it.

The scope is the *adapter*, not the service and not the host, because the
adapter is what owns the conventions being tuned. The API key shows where the
boundary falls: it belongs to the gateway fronting a host, so Water Data and
NGWMN — two adapters, one host — necessarily share one key and one quota pool.
Credentials are host-scoped; tunables are adapter-scoped.

**Source** — Where a setting's value came from, as one of the ordered
categories: a `configure()` block, the environment, the file, the built-in
default. The order is resolved per setting rather than per source: a value
supplied for one setting does not displace another setting's value from a lower
source. ADR 0010 calls a source a *tier* and ADR 0011 a *rung*; both are this
term, and the accepted records keep their own wording.

**Origin label** — The exact thing a value came from, at finer grain than its
source: `$API_USGS_RETRIES`, a path to the config file, the profile a caller
selected. What `show_configuration()` prints beside each value, and what a
parser names when it rejects one. A source is the category; an origin label is
the instance within it.

The code carries both, and spells them the other way around: `_resolve` returns
its origin label under the name `source` and its source under the name `tier`.
Prose uses the terms above.

**Selection** — Naming which profile an adapter should use. Done in code; a
profile is never selected by the environment or implied by the file, so the
set of profiles in a file is inert until something asks for one.

**Built-in default** — The value a setting takes when no source supplies one.
Package-wide.

**Adapter default** — The value a *particular adapter* prefers when no source
supplies one, because that adapter warrants a different figure — NWDC asks for
4 concurrent requests where the OGC getters take 32. Supplied by the adapter in
code, not by the user. It replaces the built-in default for calls through that
adapter and nothing else. A value from any source outranks it: otherwise an
adapter could discard a value the caller set explicitly.

Distinct from an **adapter-scoped setting**, which is the *user* naming a value
for one adapter. Both narrow to a single adapter; only one of them is something
the caller wrote.

All three are called "the default" in casual use, and they are not the same
value. Where the distinction matters — reporting what a call will actually use
— say which one is meant.

## Boundaries

**Adapter** — A module owning one service's conventions: its URLs, parameters,
error shapes, and response quirks. Adapters may use shared machinery; shared
machinery may not know about adapters.

**Dialect** — The per-API quirks the shared OGC machinery needs in order to
serve two services from one code path: which collections must be POSTed as
CQL2, which render dates date-only, which columns to coerce and sort by. An
adapter supplies one and the machinery reads it, which is how protocol code
stays free of service names.

**Single-shot adapter** — An adapter whose query is always exactly one request:
WQP, NLDI, StreamStats, and deprecated NWIS. Nothing divides and nothing
distributes, so `concurrency` and `parallel_chunks` are not part of its
vocabulary. NWDC is not one: its query fans out per location even though it
never chunks by bytes.

**Fitness function** — An executable check that an architectural rule still
holds, living in `tests/architecture_test.py`. ADR 0003 divides the work between
these and `.importlinter`.

**Facade** — A module that re-exports a subsystem's public surface and contains
no logic of its own, so callers depend on a stable name rather than on internal
layout.

**Leaf** — A module with no dependencies inside the package beyond other leaves,
holding one general mechanism so that anything may use it without acquiring the
rest of the package. Before writing a small helper, check whether a leaf already
generalizes it.

**Transport** — The service-neutral machinery for issuing requests: timeouts,
retry, pagination, fan-out, aggregation. It names no service and no protocol,
and is not public API.

## Known legacy names

Recorded so they are not mistaken for the canonical term, and not re-litigated:

- `completed_chunks` / `total_chunks` on interruptions, and `set_chunks()` /
  `start_chunk()` on the progress reporter, count chunks as defined above and
  are consistent with this glossary. They predate it; the agreement is real
  rather than coincidental.
- `ChunkInterrupted` is a permanent alias of `FanOutInterrupted` — the same
  class object under the name it was first published as. Both spellings are
  correct; neither is scheduled for removal.
- *No-progress budget* is ADR 0006's name for the **stall timeout**. Both
  spellings are current; the setting is `stall_timeout`.
- `ChunkedCall` is a permanent alias of `FanOut`, published on the OGC
  compatibility path. Like `ChunkInterrupted`, both spellings are correct.
- `utils.query` is one *request*, not a query as defined above. It is a frozen
  public path (`dataretrieval.utils.query`) and predates this glossary.
- `site` appears in deprecated NWIS and WQP parameter names where *monitoring
  location* is meant. These are frozen public surfaces and will not be renamed.
  Where the Water Data API itself names a thing `site-types` or
  `site_type_code`, that is the service's vocabulary and is reproduced
  faithfully rather than translated.
- `service` named a collection throughout the OGC machinery. Resolved: the
  OGC internals, the Water Data wrappers, and all eleven typed getters now say
  `collection`; `waterdata.get_cql` takes `collection`; and the type alias is
  `WATERDATA_COLLECTIONS`. `service=` on `get_cql` and the `WATERDATA_SERVICES`
  alias remain — a deprecated keyword and a permanent alias respectively. Two
  `OgcDialect` fields also still say it: `cql2_services` and
  `date_only_services` are keyed by collection.

  `service` still means the external system in `transport` and `progress`,
  where it labels a progress line. That usage is correct.
- `waterdata.get_samples(service=)` names a *resource*, not a service. The
  Samples OpenAPI document declares `results`, `locations`, `activities`,
  `projects` and `organizations` as tags and titles itself the "Resource
  Center"; it never calls them services. They are also not collections --
  they share 22 of 23 query parameters, so they are five projections of one
  query rather than five sets of data, and the OGC definition of *collection*
  is scoped to "access mechanisms defined by OGC API standard(s)", which
  Samples does not implement. Kept as-is by decision: renaming a public
  keyword costs a deprecation cycle, and no better-evidenced replacement is in
  reach.
- `waterdata.get_codes(code_service=)` is correct and stays. The Samples
  documentation calls it a "code service" in prose and serves it from
  `/codeservice/`, so this reproduces the service's own vocabulary, like
  `site-types`.
