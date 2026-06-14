# Mapped-Split Store And Scheduler

## Decision

Design the Postgres mapped-split store before designing the scheduler. Postgres
is the first durable store target, and multi-instance deployment is the default
target. Orange servers may run as several replicas behind a load balancer, and
any replica may serve fetches, schedule builds, or run worker loops.

The store owns correctness:

- last known good typed map per lane
- immutable component bundle snapshots
- immutable map revisions
- build request/coalescing state
- build lease for cold-start and worker execution

PgQue, cron, webhooks, HTTP admin routes, or future control-plane schedulers
only signal that work may be needed. They must not be the source of truth for
whether a mapped-split publication is current, already running, or safe to make
visible.

The local and integration-test story should use `embeddedpg`, following the
pattern in `/Users/dio/src/tetrateio/fraser/orange/internal/embeddedpg`.

No correctness decision may depend on process-local memory. In-memory caches are
allowed only as optional read-through optimizations with short TTLs or explicit
invalidation.

## Existing Boundary

Orange already has the right first boundary:

```go
type Store interface {
	BundleResourceProvider
	MappedSplitMapProvider
	PublishMappedSplit(ctx context.Context, publication MappedSplitPublication) (PublishResult, error)
}
```

`config.Server.PublishMappedSplit` builds a `MappedSplitPublication` and hands
it to the store. Durable stores must preserve the existing publication rule:
make the new typed map visible only after every referenced component resource is
readable.

The scheduler should build on top of this boundary, not replace it.

## Store Extensions

For async or on-demand builds, a durable store needs a companion build
coordination surface. This can be a separate optional interface so the existing
`Store` remains small for in-memory examples:

```go
type BuildCoordinator interface {
	MarkMappedSplitDirty(ctx context.Context, req BuildRequest) error
	GetMappedSplitBuildRequest(ctx context.Context, lane string) (*BuildRequest, error)
	ClearMappedSplitDirty(ctx context.Context, lease BuildLease, mapVersion uint64) error
	WithMappedSplitBuildLease(ctx context.Context, lane string, fn func(context.Context, lease BuildLease) error) error
}
```

The lease wrapper is intentionally store-level. Both cold-start fetch and async
worker builds use the same lease, so correctness does not depend on process
local memory.

`BuildRequest` should identify the lane and the embedder-owned source revision
or change hint. Orange should not interpret tenancy, ownership, policy
precedence, or source database schema from it.

`MarkMappedSplitDirty` must be idempotent across replicas. Multiple replicas may
mark the same lane dirty concurrently; the result should be one dirty row with
last-writer attribution and enough source/change metadata for a later worker to
build the latest requested state.

## Fetch Path

The data-plane fetch path should serve the last known good state:

```text
FetchMappedSplitMap(lane)
  -> store.FetchMappedSplitMap(lane)
  -> if found: return current or Unchanged
  -> if missing and on-demand build is enabled:
       WithMappedSplitBuildLease(lane):
         re-read current map
         if still missing: build and PublishMappedSplit
       return current map
  -> if missing and on-demand build is disabled:
       return no snapshot
```

The second read inside the lease is required. It handles the race where another
caller built the first current map while this caller waited for the lock.

Once a good map exists, fetch should not wait for a scheduled rebuild. It should
keep serving stale-good state until a worker successfully publishes a new map.

Every replica should execute this path identically against Postgres. A replica
that has never seen the lane before must still return the current map because
current state is not process-local.

## Worker Path

Workers also use the store lease. It is safe to run a worker loop in every
server replica:

```text
worker receives build signal
  -> WithMappedSplitBuildLease(lane):
       read build request
       if dirty=false: no-op
       embedder loads source and prepares normalized component inputs
       Orange builds mapped-split publication
       store.PublishMappedSplit(publication)
       ClearMappedSplitDirty(lane, map_version)
  -> ack scheduler message
```

This gives single-writer behavior per lane even if:

- PgQue delivers duplicate events
- multiple worker processes are running
- cold-start fetch races with an async worker
- a worker crashes after receiving a scheduler message but before publishing

The target is one active builder per lane, not one worker process globally.
Different lanes should be able to build concurrently. The store contract must be
correct with N replicas and N worker loops from day one.

## Postgres Store

The first production store should be a `pgxpool`-backed implementation:

```go
type PgStore struct {
	pool   *pgxpool.Pool
	schema string
}
```

It should implement `config.Store` plus the optional build-coordination
interface. Construction should not run DDL:

```go
func NewPgStore(pool *pgxpool.Pool, opts ...PgStoreOption) (*PgStore, error)
```

Callers must apply migrations before constructing the store. This keeps startup
policy explicit for embedders and avoids hidden DDL on request-serving paths.

Use explicit `pgx` queries and transactions. Avoid ORMs. Every operation should
accept `context.Context`, use parameterized SQL, and return domain errors for
not-found/current-conflict cases instead of leaking raw SQL details across the
public config API.

## Lease Implementation

For short critical sections, advisory transaction locks are acceptable:

```sql
SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2));
```

Use stable lock keys:

```text
$1 = "orange:mapped-split-build"
$2 = lane
```

Use `pg_advisory_xact_lock` only when the build/publish critical section is
short enough to keep a transaction open. The callback should re-read current
state after the lock is held.

For the production multi-replica target, prefer a store-owned lease table before
builds become expensive:

```text
lane
holder_id
lease_version
locked_until
heartbeat_at
generation_started_at
```

The lease-table version gives observability and crash recovery without holding a
database transaction while the embedder loads source data and builds Cherry
bundles. The Go API can stay `WithMappedSplitBuildLease`.

Lease table behavior:

- acquire by `INSERT ... ON CONFLICT ... WHERE locked_until < now()` or an
  equivalent transaction
- include `holder_id` so logs can identify which replica is building
- increment `lease_version` on every successful acquisition and treat it as a
  fencing token
- heartbeat while building if the callback can exceed the initial lease
- release or replace the lease after publish/no-op
- allow a later replica to take over after `locked_until`
- re-read dirty/current state after acquiring the lease
- require publish and dirty-clear operations to assert the current
  `(lane, holder_id, lease_version)`, so an expired old holder cannot publish
  after another replica acquired the lease

## Store Tables

The Postgres store schema should be independent from the queue schema. The table
names below are logical names; exact DDL belongs in reviewed migration files.

Suggested store-owned tables:

```text
orange_mapped_split_current
  lane
  map_version
  map_checksum
  updated_at

orange_mapped_split_maps
  lane
  map_version
  map_checksum
  map_payload
  created_at

orange_mapped_split_resources
  lane
  resource
  version
  checksum
  envelope_payload
  created_at

orange_mapped_split_build_requests
  lane
  dirty
  requested_by
  source_revision
  change_hint
  updated_at

orange_mapped_split_build_leases
  lane
  holder_id
  lease_version
  locked_until
  heartbeat_at
  generation_started_at
```

For the first implementation, store typed map payloads and component
`SnapshotEnvelope` payloads inline as `bytea`. This keeps embedded/local and
test deployments simple. A later object-store backed implementation can store
refs instead of bytes while preserving the same current-pointer contract.

Version allocation should be store-owned:

- map versions are monotonic per lane
- component resource versions are monotonic per `(lane, resource)`
- checksums are stored with the payload bytes
- `FetchMappedSplitMap` and `FetchResource` compare both `last_version` and
  `last_checksum` to return `Unchanged`

Use row-level locking or lane-local counter rows for version allocation. Do not
allocate versions in process memory; replicas must not race into duplicate
versions after restart or failover.

## Publish Transaction

`PublishMappedSplit` should be atomic at the current-map level:

```text
MappedSplitPublication
  -> write immutable component resource snapshots
  -> write immutable typed map snapshot
  -> update current pointer to the typed map version
  -> clear dirty build request for the lane
```

Never update current before all referenced component resources are readable. If
any write fails, the old current pointer remains valid.

In Postgres, this should be one transaction:

```text
begin
  assert build lease fencing token for lane
  allocate next resource versions for changed resources
  insert immutable resource payload rows
  allocate next map version
  insert immutable map payload row
  update current pointer to map version
  clear dirty request for lane if publish came from a scheduled build
commit
```

The transaction should not delete old resources immediately. Retention and
garbage collection are separate operational concerns.

`PublishMappedSplit` should be idempotent for retry where possible. If a worker
retries after an ambiguous commit result, the store should either detect that the
same map/resource checksums were already published or publish a new valid
revision without corrupting current state.

## Migration Story

Keep migrations split by ownership:

```text
Orange Postgres store migrations
  current/maps/resources/build_requests/leases

PgQue migrations
  pgque schema and functions only

Embedder/application wiring
  queue creation and consumer subscription after both schemas exist
```

The store package must be usable without PgQue installed. That keeps
synchronous cold-start builds and store-only tests independent from the
scheduler.

The PgQue adapter must not own current pointers, typed map versions, component
resource rows, or dirty flags. It should call a scheduler/coordinator API that
marks a lane dirty and sends a queue event in one transaction.

For multi-instance deployments, queue creation and subscription should be
idempotent at startup. Multiple replicas may execute startup concurrently.

Migration package shape should follow the other Orange checkout:

```text
config/postgres/migration
  migration.go
  sql/postgres/000001_foundation.sql
  sql/postgres/000002_...
```

`migration.go` should embed SQL files, track applied migrations in an
Orange-owned schema migration table, support an optional schema/search path, and
use an advisory migration lock. Store constructors should validate options and
assume migrations already ran.

PgQue installation remains separate:

```text
internal/pgque or config/pgque
  installs/upgrades PgQue schema/functions
  creates queue and subscription
  does not create Orange store tables
```

Do not mix PgQue SQL into the Orange store migration namespace.

The two DDL tracks must be independently runnable:

- Store migrations must succeed and support `PgStore` fetch/publish without any
  `pgque` schema installed.
- PgQue setup must succeed and support queue send/receive without any Orange
  mapped-split store tables installed.
- When both are enabled, startup may run them in either order, then perform
  application wiring such as queue creation/subscription.

## Embedded Postgres

Add an `internal/embeddedpg` package based on the other Orange checkout:

```text
internal/embeddedpg
  embeddedpg.go
  testpg/testpg.go
```

The helper should:

- wrap `github.com/fergusstrange/embedded-postgres`
- default to user `orange`, password `orange`, database `orange`
- default to port `5433` for local manual runs
- support `ORANGE_EMBEDDED_PG_DIR`
- expose `Instance.DSN()` for `pgxpool.New`
- provide `internal/embeddedpg/testpg.Pool(t)` with a random free port and
  temp root per test binary
- provide `testpg.Cleanup()` for `TestMain`

Use embeddedpg for integration tests of the Postgres store and the future PgQue
adapter. Unit tests that do not need Postgres should stay in-memory.

## PgQue Shape

PgQue should carry build signals:

```text
ScheduleBuild
  -> transaction:
       MarkMappedSplitDirty(lane, req)
       pgque.send(queue, "orange.mapped_split.build", lane payload)

Worker
  -> pgque.receive
  -> run worker path under WithMappedSplitBuildLease
  -> ack on success or no-op
  -> nack/retry on transient build or publish failure
```

PgQue can deliver duplicate events. That is acceptable because the store dirty
flag is the source of whether work remains.

For replicas, use one logical consumer group for the mapped-split builder. If
PgQue cooperative consumers are used, each replica should register a distinct
subconsumer under the same logical consumer. If the initial adapter uses plain
receive with the same consumer name from multiple replicas, the store lease and
dirty flag still make duplicate delivery safe, but cooperative mode gives
clearer worker identity and stale-worker takeover.

Ack only after the store operation has completed successfully or proven the
event is a no-op. Nack transient failures so another attempt can occur. If a
replica dies after publishing but before ack, redelivery should no-op because
the dirty row was cleared or the current map already advanced.

## Test Plan

Store-only tests:

- cold-start fetch builds once under concurrent callers
- worker path is no-op when dirty is false
- publish failure leaves current unchanged
- current pointer advances only after map and component resources exist
- dirty requests coalesce and last requester wins
- `FetchMappedSplitMap` and `FetchResource` return `Unchanged` for matching
  version/checksum
- store survives process restart because current state is in Postgres
- concurrent `MarkMappedSplitDirty` calls from many goroutines coalesce to one
  dirty request
- concurrent publish attempts for one lane serialize through the store lease
- different lanes can publish concurrently

PgQue integration tests:

- duplicate queue events produce one published map revision
- worker crash before ack redelivers but does not double-publish
- multiple workers race on one lane and only one builds
- different lanes can build independently
- multiple replicas can run scheduler startup idempotently
- one replica can take over after another replica's build lease expires

The next implementation step should be:

1. Copy/adapt `internal/embeddedpg` and `internal/embeddedpg/testpg`.
2. Add Postgres store migrations.
3. Implement `config.PgStore`.
4. Add embeddedpg-backed store tests.
5. Add PgQue migration/worker wiring only after the Postgres store contract is
   proven.
