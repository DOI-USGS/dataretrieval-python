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

**NWDC** — The National Water Availability Assessment Data Companion, providing
modeled national-scale water-use data.

**WQP** — The Water Quality Portal, a multi-agency water-quality clearinghouse.

**NLDI** — The Network Linked Data Index, which navigates the hydrologic network
from an origin to connected features, flowlines, or basins.

**NWIS** — The legacy USGS waterservices interface. Deprecated: it is retained
for compatibility and is not where new work goes.

**StreamStats** — Basin characteristics and delineation for a point on a stream.

## Data

**Collection** — One named set of records a service offers — `daily`,
`monitoring-locations`, `time-series-metadata`. The unit a getter targets.

**Collection family** — A group of collections sharing a shape and therefore a
getter signature. Their getters deliberately resemble one another; the
resemblance is the public contract, not duplication to be removed.

**Monitoring location** — A place where measurements are recorded. The canonical
term. Legacy: the deprecated NWIS getters and the WQP profiles call this a
*site*, and their parameters keep that spelling.

**Metadata** — The second half of every getter's return: the request URL, the
elapsed time, and the response headers. Describes the *retrieval*, not the data.

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
