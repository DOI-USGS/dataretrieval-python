# Context

The shared vocabulary for `dataretrieval`. This is a glossary, not a
specification: it fixes what words mean so that code, docstrings, ADRs, and
conversation use them the same way. Architectural decisions live in
`docs/source/architecture/decisions/`.

When a term here conflicts with a name in the code, the term wins and the name
is legacy. Legacy names are called out below rather than quietly tolerated.

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
transient failures produce a resumable interruption. The two answers are one
judgement about what a failure means, and must agree.

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

## Settings

**Settings profile** — A named set of settings for one adapter, stored in the
settings file or built in code. **Profile** is the short form; the class that
carries one is `<Adapter>Settings`. A profile is an *input* to resolution,
never its result.

**Default profile** — The profile an adapter uses when no other is selected:
the `[<adapter>]` table's own keys. Always in effect. A **named profile**
(`[<adapter>.bulk]`) is in effect only when a caller selects it, so adding one
to a file never changes an existing script.

**Effective settings** — The resolved set of settings a call will use: what the
chain produces after every profile, variable and default has been applied.
Distinct from a settings profile, which is one contribution to it.
**Configure** is the verb for applying one, and keeps that spelling: it is what
the caller writes, and the settings library has no competing name for it.

**Settings file** — `~/.dataretrieval/config.toml`, or the path in
`DATARETRIEVAL_CONFIG`. The file keeps the name `config.toml` while the
vocabulary around it says *settings*: the path is a compatibility surface, and
`config` is the conventional name for a file on disk.

**Setting** — One named tunable the caller may adjust: the API key, the
concurrency cap, the retry count, the progress line, the fan-out baseline. A
setting means the same thing wherever it applies, but it does not apply
everywhere: `concurrency` and `parallel_chunks` are meaningless to an adapter
that issues one request, and `ssl_check` is meaningful to only three. Which
settings an adapter accepts is part of that adapter's vocabulary.

**Package-wide setting** — A setting that applies to every adapter: the retry
count, the progress line, the stall timeout. Set once, honored everywhere.

**Adapter-scoped setting** — A setting named under one adapter, applying to
that adapter and no other. It overrides the package-wide value for that adapter
alone; it does not replace the package-wide tier. An adapter rejects a setting
it has no use for, rather than accepting and ignoring it.

The scope is the *adapter*, not the service and not the host, because the
adapter is what owns the conventions being tuned. The API key is the
counter-example that fixes the distinction: it belongs to the gateway fronting
a host, so Water Data and NGWMN — two adapters, one host — necessarily share
one key and one quota pool. Credentials are host-scoped; tunables are
adapter-scoped.

**Source** — Where a setting's value came from. Sources are ordered, and the
order is resolved per setting rather than per source: a value supplied for one
setting does not displace another setting's value from a lower source.

**Selection** — Naming which profile an adapter should use. Done in code; a
profile is never selected by the environment or implied by the file, so the
set of profiles in a file is inert until something asks for one.

**Built-in default** — The value a setting takes when no source supplies one.
Package-wide.

**Adapter default** — The value a *particular adapter* prefers when no source
supplies one, because that adapter warrants a different figure — NWDC asks for
4 concurrent requests where the OGC getters take 32. Supplied by the adapter in
code, not by the user. It replaces the built-in default for calls through that
adapter and nothing else. A value from any source outranks it: an adapter able
to override an explicit setting would make that setting a lie.

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
- `site` appears in deprecated NWIS and WQP parameter names where *monitoring
  location* is meant. These are frozen public surfaces and will not be renamed.
  Where the Water Data API itself names a thing `site-types` or
  `site_type_code`, that is the service's vocabulary and is reproduced
  faithfully rather than translated.
- `service` named a collection throughout the OGC machinery. Resolved: the
  OGC internals, the Water Data wrappers, and all eleven typed getters now say
  `collection`; `waterdata.get_cql` takes `collection`; and the type alias is
  `WATERDATA_COLLECTIONS`. `service=` on `get_cql` and the `WATERDATA_SERVICES`
  alias remain — a deprecated keyword and a permanent alias respectively.

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
  keyword costs a deprecation cycle for a term with no better-evidenced
  replacement in reach.
- `waterdata.get_codes(code_service=)` is correct and stays. The Samples
  documentation calls it a "code service" in prose and serves it from
  `/codeservice/`, so this reproduces the service's own vocabulary, like
  `site-types`.
