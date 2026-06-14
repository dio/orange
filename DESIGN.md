# Orange: Cherry Mapped-Split Producer

**Status**: Active design.
**Date**: 2026-06-14

## Purpose

Orange is a Go library for producer-side delivery of Cherry mapped-split
configuration. It turns already-normalized `cherry.Input` component layers into
Cherry bundles, wraps those bundles in Orange snapshot payloads, publishes a
typed mapped-split map, and exposes the mapped-split `SnapshotService` for data
planes such as Plum.

Orange is not a request runtime. It does not own Envoy module state, request
matching, upstream picking, provider auth injection, tenant databases, user key
storage, secret resolution, HTTP process lifecycle, business routes, rebuild
triggers, or operational endpoints. Those responsibilities belong to Plum or to
the embedding control plane.

The supported delivery shape is mapped split:

1. Low-churn `llm-generic` and `mcp-servers` component bundles.
2. Partitioned high-churn `llm-user-key-*` and `mcp-user-profile-*` component
   bundles.
3. One typed SoTW mapped-split map per authenticated lane.
4. Component bundle fetches by resource name from the typed map.

## Boundary

```text
embedding control plane
  owns tenants, users, keys, catalogs, policy, auth, storage, ops routes
  -> normalized Cherry component inputs with SecretRef strings only
  -> Orange config.Server or lower-level mappedsplit/snapshot/server packages
       build Cherry component bundles
       publish component SnapshotEnvelope resources
       publish typed MappedSplitSnapshot
       serve SnapshotService from an embedder-owned mux
  -> data-plane source
       fetch typed map
       fetch changed component bundles
       open with Cherry
       publish active view
  -> data path
       match, pick, adapt, resolve selected SecretRef at request time
```

Orange starts after embedding-owned code has selected and normalized data.
Orange packages must not perform tenancy joins, ownership checks, key or JWT
verification, project/workspace reachability, product rule merge, source
database interpretation, or secret material resolution.

Secret handling is reference-only. Provider and MCP credentials remain refs
such as `env://OPENAI_KEY`, `file:///etc/keys/openai`, `orange://...`,
`vault://...`, or `sm://...`. Orange must not turn those refs into secret bytes
while building, caching, logging, or serving snapshots.

## API Contract

Orange exposes mapped-split delivery through `api/orange/config/v1`.

```proto
service SnapshotService {
  rpc FetchMappedSplitMap(FetchMappedSplitMapRequest) returns (FetchMappedSplitMapResponse);
  rpc FetchMappedSplitBundle(FetchMappedSplitBundleRequest) returns (FetchMappedSplitBundleResponse);
}
```

`FetchMappedSplitMap` returns a typed SoTW `MappedSplitSnapshot` for the
authenticated lane. `FetchMappedSplitBundle` returns one component bundle
resource referenced by that map. Both methods carry `last_version` and
`last_checksum`; when both match the published state, Orange returns
`Unchanged`.

Lane selection is never a request field. Orange authenticates the caller,
resolves the authenticated principal to a mapped-split lane, and serves the map
or resource for that lane. Local examples may use a development header to
simulate identity-to-lane mapping.

For component bundles, `SnapshotEnvelope.payload` is a marshaled
`ConfigPayload`, and `ConfigPayload.payload` is the Cherry zstd bundle bytes.
The component payload media type is
`application/vnd.dio.orange.cherry-bundle`.

The typed map contains:

- scope kind, selected scope ID, concrete scopes, generation, and map revision
- low-churn bundle refs keyed by lane
- partitioned bundle refs keyed by lane and partition
- partitioning metadata for producer/consumer alignment
- pack checksum and size for every component ref

`partition_bundles` is state-of-the-world, not a patch. If a previously present
partition is omitted from the latest map, consumers must stop serving that
partition's old reader.

## Mapped-Split Production

Embedding code owns source loading and slicing. It passes normalized component
inputs to Orange:

```go
spec := config.MappedSplitSpec{
    LLMUserKeyPartitions:     64,
    MCPUserProfilePartitions: 64,
}

server := config.NewServer(config.ServerOptions{
    Producer:      "control-plane/1.2.3",
    Authenticator: auth,
    LaneResolver:  lanes,
})

_, err := server.PublishMappedSplit(ctx, config.MappedSplitRequest{
    Selection:               config.Selection{ScopeKind: "workspace", ScopeID: "prod"},
    Lane:                    lane,
    Scopes:                  []string{"prod"},
    SourceRevision:          sourceRevision,
    GenerationID:            generationID,
    MapRevision:             mapRevision,
    LLMDefaultPrincipalSlug: "slug:default",
    Spec:                    spec,
    Components:              components,
})
```

Each `config.ComponentInput` is one component key plus one normalized
`config.Input` layer. The facade aliases the Cherry and mapped-split types that
embedders need, so simple producer integrations can import only
`github.com/dio/orange/config`.

Component responsibilities:

- `llm-generic`: providers, models, default/platform LLM routes, and platform
  LLM secret refs.
- `mcp-servers`: MCP server catalog, direct `s/<server>` paths, and platform
  MCP secret refs.
- `llm-user-key-*`: principal/key-specific routes, BYOK refs, and rate policy
  for one partition.
- `mcp-user-profile-*`: profile paths, selected tools, and user/profile secret
  refs for one partition.

Low-churn components are not permanent static bundles. Rebuild them whenever
catalogs, defaults, direct paths, or platform secret refs change.

For an N+1 update, build changed component payloads first, keep unchanged
component resources unchanged, publish a new map revision that references the
changed refs and omits removed partitions, and make the new map visible only
after every referenced component resource is readable.

## `config.Server`

`config.Server` is the high-level server facade. It is intentionally not an
HTTP listener helper and does not start goroutines. It is an attachable handler
plus a `config.Store`.

Responsibilities:

- Build mapped-split components with `mappedsplit.Builder`.
- Publish built components and maps through the configured `Store`.
- Return `Unchanged` for matching version/checksum requests.
- Mount the generated Connect `SnapshotService` handler on an embedder-owned
  mux.

Non-responsibilities:

- No source/database reads.
- No tenancy or policy decisions.
- No credential verification beyond caller-supplied `Authenticator`.
- No lane selection from request messages.
- No secret material resolution.
- No HTTP listener/process ownership.

Typical server attachment:

```go
snapshotServer := config.NewServer(config.ServerOptions{
    Producer:      "control-plane",
    Authenticator: auth,
    LaneResolver:  lanes,
})

mux := http.NewServeMux()
snapshotServer.Mount(mux)
mux.HandleFunc("/healthz", healthz)
mux.HandleFunc("POST /internal/rebuild", rebuild)
```

`config.Store` is the multi-replica boundary:

```go
type Store interface {
    config.BundleResourceProvider
    config.MappedSplitMapProvider
    PublishMappedSplit(ctx context.Context, publication config.MappedSplitPublication) (config.PublishResult, error)
}
```

`config.NewMemoryStore` is the default for examples, tests, and single-replica
development. Multi-replica deployments should provide a durable store whose
`PublishMappedSplit` implementation makes the new typed map visible only after
every referenced component resource is readable. Durable stores should allocate
monotonic map/resource versions and keep version/checksum state resource-local
so polling clients can use `Unchanged` correctly.

Advanced embedders can either inject a store into `config.ServerOptions`, or
mount `config.NewSnapshotServiceWithProviders` directly when publication is
owned by another transaction path.

### Durable Store And Scheduling

Async rebuild scheduling is layered on top of durable Postgres store
publication and must be correct with multiple Orange server replicas. The store
owns correctness; schedulers only signal that work may be needed.

**Correctness boundary.** The store owns:

- last known good typed map per lane
- immutable component bundle snapshots and map revisions
- build request and coalescing state
- per-lane build lease (used by both cold-start fetch and background workers)

Queue systems (PgQue, cron, webhooks) must not own current pointers, map
revisions, component resources, or dirty flags. No correctness decision may
depend on process-local memory; in-memory caches are allowed only as optional
read-through optimizations with short TTLs or explicit invalidation.

**`BuildCoordinator` interface.** A separate optional interface extends the
existing `Store` so in-memory examples stay small:

```go
type BuildCoordinator interface {
    MarkMappedSplitDirty(ctx context.Context, req BuildRequest) error
    GetMappedSplitBuildRequest(ctx context.Context, lane string) (*BuildRequest, error)
    ClearMappedSplitDirty(ctx context.Context, lease BuildLease, mapVersion uint64) error
    WithMappedSplitBuildLease(ctx context.Context, lane string, fn func(context.Context, BuildLease) error) error
}
```

`MarkMappedSplitDirty` must be idempotent across replicas. Multiple replicas
may mark the same lane dirty concurrently; the result is one dirty row with
last-writer attribution. `BuildRequest` carries lane plus embedder-owned
diagnostic metadata such as source revision and change hint; Orange must not
interpret tenancy, ownership, policy precedence, or source database schema from
that metadata.

**Fetch path.** On a cold-start miss (on-demand build enabled), the caller
acquires the store lease, re-reads the current map inside the lease, and builds
if still missing. Once a good map exists, fetch keeps serving stale-good state
until a worker successfully publishes a new map. Every replica executes this
path identically against Postgres.

**Worker path.** Workers run under the same store lease, giving single-writer
behavior per lane regardless of duplicate queue delivery, multiple worker
processes, or crash-then-redeliver scenarios. Different lanes build
concurrently. The store contract must be correct with N replicas and N worker
loops from day one.

**Lease implementation.** Short critical sections may use `pg_advisory_xact_lock`.
For production multi-replica targets, a lease table (`lane`, `holder_id`,
`lease_version`, `locked_until`, `heartbeat_at`, `generation_started_at`)
provides observability and crash recovery without holding a database transaction
during expensive build steps. `lease_version` is the fencing token; publish and
dirty-clear operations assert the current `(lane, holder_id, lease_version)` so
an expired holder cannot publish after a new replica acquired the lease.

**Publish transaction.** `PublishMappedSplit` atomically publishes a new
current map:

```text
assert build lease fencing token
allocate next resource versions for changed resources
insert immutable resource payload rows
allocate next map version
insert immutable map payload row
update current pointer to map version
```

The current pointer is never updated before all referenced component resources
are readable. If any write fails, the previous current pointer remains valid.
Scheduled build paths clear dirty state after publishing while asserting the
same lease fencing token and the published map version; stale or expired holders
must not clear dirty state for another builder's publication.

Old map and resource rows are not deleted by publish. Retention and garbage
collection are separate operational concerns. Retry paths should be safe after
ambiguous failures: either the store detects that equivalent checksums already
exist, or it publishes a new valid revision without corrupting current state.

**Store tables.** Five logical tables under the Orange migration namespace:
`orange_mapped_split_current`, `orange_mapped_split_maps`,
`orange_mapped_split_resources`, `orange_mapped_split_build_requests`,
`orange_mapped_split_build_leases`. Payloads are inline `bytea`. Versions are
monotonic per lane (maps) or per `(lane, resource)` (resources) and are
store-allocated, never process-local.

**Migration split.** Store migrations (`config/postgres/migration`) and PgQue
schema setup (`config/pgque`) are independently runnable DDL tracks. The store
package must be usable without PgQue installed. Do not mix PgQue SQL into the
Orange store migration namespace. Queue creation and subscription happen after
both schemas exist and are idempotent at multi-replica startup.

**Implemented durable path:**

- `config/postgres/migration.Migrate` applies explicit, idempotent store
  migrations (embedded SQL in `config/postgres/migration/sql/postgres/`).
- `config.NewPgStore` constructs a `pgxpool`-backed `Store` + `BuildCoordinator`.
  Construction does not run DDL; callers apply migrations first.
- `config/pgque.Setup` installs the optional PgQue signal layer separately.
- `config.NewPgQueScheduler` marks lanes dirty, sends PgQue events, and runs
  worker builds under the same store lease used by cold-start fetches. PgQue
  payloads carry lane and diagnostic metadata only, never source records or
  secrets. Workers tick only their configured queue, not the global PgQue
  scheduler, so unrelated queues in a shared database cannot affect
  mapped-split builds. Events are acked only after a successful store operation
  or proven no-op; transient failures are nacked for retry.

**Integration tests** use `internal/embeddedpg/testpg.Pool(t)` with a random
free port per test binary. Store-only tests run without any PgQue schema
installed; PgQue tests run without any Orange store tables installed.

Lower-level packages remain available:

- `mappedsplit`: build typed maps, open mapped-split views, and work with
  component refs directly.
- `snapshot`: immutable envelope assembly and snapshot primitives.
- `producer`: single component bundle wrapping primitives.

Use `config.Store` for durable/distributed storage or custom publication
transactions. Use `config.Server` with `config.NewMemoryStore` for the common
in-process mapped-split server shape.

## Auth And Lane Mapping

Orange uses small fetch-boundary concepts:

- `Principal`: the authenticated caller identity returned by the embedder's
  authenticator. It usually represents a Plum instance, workload, service
  account, or agent.
- `Lane`: the Orange-local snapshot stream key for a published mapped-split
  view.

The embedding application must perform:

```text
request credential
  -> verify issuer/signature/expiry/audience or mTLS/API key identity
  -> Principal{ID, Scopes}
  -> authorize data-plane fetch
  -> lane lookup
```

Unknown credentials fail as `unauthenticated`. Known principals without access
fail as `permission_denied` or, if the embedder chooses to hide existence,
`not_found`. Missing published snapshots for an authorized lane return
`not_found`.

## Consumer Flow

The data plane always fetches the typed map first:

```text
FetchMappedSplitMap(last_version, last_checksum)
  -> validate typed map checksum
  -> decode MappedSplitMap
  -> diff refs against active view
  -> FetchMappedSplitBundle(resource, last_version, last_checksum) for missing/stale refs
  -> validate ConfigPayload and media type
  -> open Cherry bundle
  -> validate scope/generation/checksum/size against map
  -> publish next immutable active view
```

When the map is `Unchanged`, the consumer does not inspect component resources.
When a map changes, matching refs are reused, missing or stale refs are fetched,
and omitted refs are dropped.

`config.Client` is the high-level consumer facade. It owns typed-map polling,
per-resource version/checksum state, component resource fetching, media-type
validation, `mappedsplit.Open` application, and the current immutable opened
view. Consumers can call `FetchMap` or `FetchBundle` for diagnostics, or `Sync`
for normal map-diff/component-fetch/open behavior.

## Package Shape

Current package roles:

```text
api/orange/config/v1/
  generated SnapshotService and snapshot protobuf types
config/
  Server, Client, Store, and SnapshotService for mapped-split integrations
producer/
  ConfigPayload builder for Cherry bundle bytes
snapshot/
  immutable SnapshotEnvelope assembly and in-memory primitives
mappedsplit/
  mapped-split build/open/proto conversion helpers
examples/mappedsplit/
  runnable embedder-owned server/client demo
```

Deleted legacy admin, generic fetch, YAML server/client, and standalone server
helpers should stay out of the mapped-split design unless a new production
contract is written first.

## Error Model

The public fetch API should use stable Connect codes:

- `invalid_argument`: malformed fetch request.
- `not_found`: no published lane/resource or intentionally hidden access.
- `permission_denied`: authenticated caller cannot fetch the lane.
- `unauthenticated`: caller credentials are missing or invalid.
- `failed_precondition`: provider data cannot become a valid Cherry bundle.
- `unavailable`: producer dependency is temporarily unavailable.
- `internal`: unexpected producer failure.

## Deferred Work

- Remote admin publication API. In-process publish is the supported shape until
  the mutation/auth/storage contract is clear.
- Streaming watch API. Initial consumers poll map and bundle resources with
  version/checksum `Unchanged` semantics.
