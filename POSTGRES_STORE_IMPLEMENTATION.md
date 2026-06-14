# Orange Postgres Store Implementation Plan

**Companion to:** `DESIGN.md` §Durable Store And Scheduling and
`docs/design/mapped-split-store-scheduler.md`.
**Format:** Each task below is a self-contained prompt. Hand one to an engineer
or agent without additional context and it should be executable.
**Status convention:** mark a task `[done]` when its PR merges and its
acceptance gate passes.

---

## How To Use This Document

- Tasks are grouped into 6 slices. Slices run sequentially. Tasks inside a slice
  may run in parallel only when the prompt says so.
- Every task must end with code, tests, and documentation updates when the
  prompt calls for them. No implementation-only PRs.
- Durable architecture lives in `DESIGN.md` and
  `docs/design/mapped-split-store-scheduler.md`. If a task discovers a durable
  decision, update those docs in the same PR.
- Keep Orange's boundary intact. Orange builds and serves mapped-split
  snapshots. Embedders own tenancy, source DB reads, auth, source-change
  classification, and secret material resolution.
- Optimize for multi-instance deployment. Every server replica may fetch,
  schedule, and run workers. Correctness must come from Postgres, not process
  memory.

### Cross-Cutting Rules

- Use `pgx`/`pgxpool`; do not introduce an ORM.
- Store constructors must not run DDL. Migrations are explicit.
- Store DDL and queue/PgQue DDL are independent migration tracks. Do not put
  PgQue setup in store migrations, and do not put Orange store tables in PgQue
  setup.
- All SQL must be parameterized and context-aware.
- Always close rows and check row iteration errors.
- Store secret refs only. Never store or log secret material.
- Published maps/resources are immutable. Failed publishes leave current state
  unchanged.
- Per-lane build leases are controller-runtime-like leases: holder identity,
  expiry, heartbeat, and a fencing token.

---

## Slice 1 — Embedded Postgres And Migration Foundation

**Slice goal:** Orange has a Postgres test/runtime foundation matching the
other Orange checkout: embeddedpg for local integration tests and explicit
embedded SQL migrations for store schema evolution. No mapped-split store logic
exists yet.

### Task 1.1 — Add `internal/embeddedpg`

> Add `internal/embeddedpg` and `internal/embeddedpg/testpg`, adapted from
> `/Users/dio/src/tetrateio/fraser/orange/internal/embeddedpg`.
>
> Requirements:
> - Add the required dependencies to `go.mod`: `github.com/fergusstrange/embedded-postgres`
>   and `github.com/jackc/pgx/v5`.
> - `embeddedpg.Config` supports `Root`, `Username`, `Password`, `Database`,
>   `Port`, `StartTimeout`, and `ResetData`.
> - Defaults: user `orange`, password `orange`, DB `orange`, port `5433`.
> - Root resolution order: explicit config, `ORANGE_EMBEDDED_PG_DIR`,
>   OS user cache dir.
> - `Instance.DSN()` returns a libpq URL for `pgxpool.New`.
> - `testpg.Pool(t)` starts one embedded Postgres per test binary on a random
>   free port and temp root.
> - `testpg.Cleanup()` closes the pool, stops Postgres, and removes temp data.
>
> Tests:
> - Add a minimal package test that starts embeddedpg through `testpg.Pool`,
>   runs `SELECT 1`, and cleans up from `TestMain`.
>
> Acceptance:
> - `go test ./internal/embeddedpg/...` passes.

### Task 1.2 — Add Postgres Migration Package

> Add `config/postgres/migration` with embedded SQL migrations.
>
> Requirements:
> - Follow the package shape from the other Orange checkout:
>   `migration.go`, `sql/postgres/*.sql`.
> - Expose `Migrate(ctx, pool, opts...)`, `Status(ctx, pool, opts...)`, and
>   `Plan(ctx, pool, opts...)`.
> - Support `WithSchema(schema string)` and `WithSchemaTable(table string)`.
> - Use an Orange-owned migration metadata table.
> - Use an advisory transaction lock during migration.
> - Validate schema/table identifiers before using them.
> - Migrations should be explicit SQL files, not generated at runtime.
>
> Initial SQL:
> - Create the mapped-split store tables from
>   `docs/design/mapped-split-store-scheduler.md`.
> - Include `orange_mapped_split_build_leases` with `lease_version`.
> - Include enough constraints/indexes for per-lane current lookup,
>   per-resource fetch, dirty-row upsert, and lease acquisition.
> - Do not install PgQue, create queues, create consumers, or reference the
>   `pgque` schema from these migrations.
>
> Tests:
> - With embeddedpg, verify `Plan` sees pending migrations, `Migrate` applies
>   them, `Status` shows no pending migrations, and rerunning `Migrate` is
>   idempotent.
> - Test `WithSchema` by applying migrations in a non-public schema.
>
> Acceptance:
> - `go test ./config/postgres/migration ./internal/embeddedpg/...` passes.

---

## Slice 2 — Postgres Store Read/Publish Contract

**Slice goal:** `config.PgStore` implements the existing `config.Store`:
`FetchMappedSplitMap`, `FetchResource`, and `PublishMappedSplit`. This slice
does not add scheduler or dirty/lease APIs yet.

### Task 2.1 — Add `config.PgStore`

> Add a `pgxpool`-backed store in `config`.
>
> Public shape:
>
> ```go
> type PgStore struct { ... }
> type PgStoreOption func(*PgStore)
>
> func NewPgStore(pool *pgxpool.Pool, opts ...PgStoreOption) (*PgStore, error)
> func WithPgStoreSchema(schema string) PgStoreOption
> ```
>
> Requirements:
> - Constructor validates nil pool and schema identifiers.
> - Constructor does not run migrations.
> - Implement `config.Store`.
> - Use `set_config('search_path', ..., true)` inside transactions when schema
>   is configured.
> - Return existing Orange not-found behavior consistently with `MemoryStore`
>   and `snapshot.ErrNoSnapshot`.
> - Clone or rehydrate protobuf values so callers cannot mutate stored state.
>
> Tests:
> - Embeddedpg-backed tests that apply migrations, construct `PgStore`, and
>   verify empty fetch returns no snapshot.
>
> Acceptance:
> - `go test ./config ./config/postgres/migration` passes.

### Task 2.2 — Implement Durable `PublishMappedSplit`

> Implement `PgStore.PublishMappedSplit`.
>
> Requirements:
> - Store component `SnapshotEnvelope` payloads inline as immutable rows.
> - Store typed `MappedSplitSnapshot` payloads inline as immutable rows.
> - Update the per-lane current pointer only after all referenced component
>   resources and the typed map row exist.
> - Allocate map versions monotonically per lane in Postgres.
> - Allocate resource versions monotonically per `(lane, resource)` in
>   Postgres.
> - If a component payload checksum is unchanged from the current resource,
>   reuse the current resource version instead of writing a duplicate row.
> - Failed publish attempts leave previous current state visible.
>
> Tests:
> - Publish one mapped split and fetch the map and at least one resource.
> - Republish unchanged components and verify unchanged resources keep their
>   version while map version advances.
> - Force a publish error before current update and verify old current remains.
> - Verify `FetchMappedSplitMap` and `FetchResource` return `Unchanged` when
>   version/checksum match.
>
> Acceptance:
> - `go test ./config ./mappedsplit ./producer ./snapshot` passes.

### Task 2.3 — Prove Multi-Replica Store Reads

> Add tests that use two `PgStore` instances backed by the same `pgxpool` or two
> pools pointing at the same embeddedpg database.
>
> Requirements:
> - Store A publishes.
> - Store B fetches the current map and resources without any process-local
>   state from Store A.
> - Store B publishes a newer map.
> - Store A fetches the newer map.
>
> Tests:
> - Include both map and bundle resource fetches.
>
> Acceptance:
> - `go test ./config -run PgStore` passes.

---

## Slice 3 — Build Coordination And Per-Lane Lease

**Slice goal:** Postgres owns dirty build requests and per-lane build leases.
Every replica can safely try to schedule and build.

### Task 3.1 — Add Build Coordination Types

> Add build coordination types to `config` without wiring workers yet.
>
> Proposed shape:
>
> ```go
> type BuildRequest struct {
>     Lane           string
>     RequestedBy    string
>     SourceRevision string
>     ChangeHint     string
> }
>
> type BuildLease struct {
>     Lane         string
>     HolderID     string
>     LeaseVersion int64
>     LockedUntil  time.Time
> }
>
> type BuildCoordinator interface {
>     MarkMappedSplitDirty(ctx context.Context, req BuildRequest) error
>     GetMappedSplitBuildRequest(ctx context.Context, lane string) (*BuildRequest, error)
>     ClearMappedSplitDirty(ctx context.Context, lease BuildLease, mapVersion uint64) error
>     WithMappedSplitBuildLease(ctx context.Context, lane string, fn func(context.Context, BuildLease) error) error
> }
> ```
>
> Requirements:
> - Keep this separate from `Store` so `MemoryStore` stays simple.
> - Add options for holder ID, lease duration, and heartbeat interval to
>   `PgStore`.
> - Do not add source DB reading or component generation here.
>
> Tests:
> - Compile-time assertion that `PgStore` implements `BuildCoordinator`.
>
> Acceptance:
> - `go test ./config` passes.

### Task 3.2 — Implement Dirty Request Coalescing

> Implement `MarkMappedSplitDirty` and `GetMappedSplitBuildRequest`.
>
> Requirements:
> - `MarkMappedSplitDirty` is an upsert by lane.
> - Concurrent callers coalesce to one dirty row.
> - Last writer wins for `RequestedBy`, `SourceRevision`, and `ChangeHint`.
> - Empty lane is rejected.
>
> Tests:
> - Many goroutines call `MarkMappedSplitDirty` for one lane; exactly one dirty
>   row remains and latest metadata is visible.
> - Different lanes produce independent dirty rows.
>
> Acceptance:
> - `go test ./config -run BuildRequest` passes with `-race`.

### Task 3.3 — Implement Controller-Style Build Lease

> Implement `WithMappedSplitBuildLease` using the Postgres lease table.
>
> Requirements:
> - Lease is scoped by lane.
> - Acquisition sets `holder_id`, increments `lease_version`, sets
>   `locked_until`, and records `generation_started_at`.
> - If a non-expired lease exists for another holder, return a typed
>   `ErrBuildLeaseHeld` or equivalent no-work signal.
> - If the callback may exceed the lease duration, heartbeat/renew while the
>   callback runs.
> - Release is best effort. Correctness comes from expiry and fencing.
> - Callback receives the acquired `BuildLease`.
>
> Tests:
> - Two stores with different holder IDs race for one lane; only one callback
>   runs.
> - Different lanes can acquire leases concurrently.
> - A second holder can acquire after `locked_until` expires.
>
> Acceptance:
> - `go test ./config -run BuildLease -race` passes.

### Task 3.4 — Fence Publish And Dirty Clear

> Make scheduled-build publish paths assert the lease fencing token.
>
> Requirements:
> - `ClearMappedSplitDirty(ctx, lease, mapVersion)` succeeds only when the
>   current lease row matches `(lane, holder_id, lease_version)`.
> - Add a lease-aware publish helper if needed, but do not break the existing
>   `Store.PublishMappedSplit` interface used by examples.
> - A stale holder whose lease was taken over must not be able to clear dirty
>   state or publish as the active scheduled build.
>
> Tests:
> - Holder A acquires lease, lease expires, holder B acquires lease. Holder A's
>   dirty-clear/publish fencing attempt fails. Holder B succeeds.
>
> Acceptance:
> - `go test ./config -run BuildLease -race` passes.

---

## Slice 4 — On-Demand Build Boundary

**Slice goal:** Add the server-side boundary for cold-start on-demand builds
without adding Orange-owned source reads or a standalone server process.

### Task 4.1 — Add Optional Build Callback

> Add optional build callback support around `config.Server`, not inside the
> public SnapshotService request messages.
>
> Requirements:
> - Define an embedder callback that turns a `BuildRequest` or lane into a
>   `MappedSplitRequest`.
> - The callback belongs to server options or a small coordinator type.
> - Orange must not read tenant/source DBs itself.
> - If no callback is configured, missing snapshots still return not found.
>
> Tests:
> - Existing server tests remain unchanged when no callback is configured.
>
> Acceptance:
> - `go test ./config` passes.

### Task 4.2 — Cold-Start Fetch Uses Store Lease

> Add optional cold-start behavior: when `FetchMappedSplitMap` would return no
> snapshot and on-demand build is enabled, acquire the per-lane build lease,
> re-read current map, and build/publish only if still missing.
>
> Requirements:
> - Serve stale-good state immediately when a current map exists.
> - Do not block normal `Unchanged` behavior.
> - Multiple replicas racing on first fetch should produce one build.
> - No process-local singleflight is sufficient for correctness.
>
> Tests:
> - Many concurrent fetches against a missing lane trigger one callback and all
>   callers see the resulting map or a consistent not-found/error.
> - If current appears while a caller waits for lease, callback is not invoked.
>
> Acceptance:
> - `go test ./config -run ColdStart -race` passes.

---

## Slice 5 — PgQue Scheduler Adapter

**Slice goal:** Add a PgQue-backed scheduler as an optional adapter. PgQue
signals work; Postgres store state remains authoritative.

### Task 5.1 — Add PgQue Migration/Install Helper

> Add a PgQue helper package separate from the Orange store migrations.
>
> Requirements:
> - Install/upgrade PgQue schema/functions from the local PgQue SQL source or
>   embedded vendored SQL according to the agreed dependency policy.
> - Create the Orange mapped-split build queue idempotently.
> - Register/subscribe the logical consumer idempotently.
> - Multiple replicas may call setup concurrently.
> - Do not create or modify Orange store tables.
> - Do not depend on the Orange store migration package. PgQue setup should be
>   runnable against a database that has no Orange store tables, and store
>   migrations should be runnable against a database that has no PgQue schema.
>
> Tests:
> - Embeddedpg integration test: two setup calls from separate clients both
>   succeed and produce one usable queue/subscription.
>
> Acceptance:
> - `go test ./... -run PgQue` passes for the new package.

### Task 5.1a — Prove Migration Independence

> Add tests that prove the store and queue migration tracks are independent.
>
> Requirements:
> - Fresh embeddedpg DB A: run only store migrations. Construct `PgStore`, use
>   store-only fetch/publish tests. Verify no PgQue schema is required.
> - Fresh embeddedpg DB B: run only PgQue setup. Create queue/subscription and
>   send/receive a simple event. Verify no Orange store tables are required.
> - Fresh embeddedpg DB C: run both in either order. Verify both store and
>   queue operations work.
>
> Acceptance:
> - Independence tests pass with embeddedpg.

### Task 5.2 — Add `ScheduleBuild`

> Add a scheduler API that marks the lane dirty and sends a PgQue event in one
> transaction.
>
> Requirements:
> - `ScheduleBuild(ctx, BuildRequest)` calls `MarkMappedSplitDirty` and
>   `pgque.send` atomically.
> - Duplicate schedule calls are allowed.
> - PgQue payload contains lane and diagnostic metadata, not source records or
>   secrets.
>
> Tests:
> - Schedule the same lane many times; one dirty row remains.
> - Force tick/receive and verify at least one queue event is visible.
>
> Acceptance:
> - Embeddedpg PgQue tests pass.

### Task 5.3 — Add Worker Loop

> Add a worker loop that can run in every replica.
>
> Requirements:
> - Each replica has a distinct holder/subconsumer identity.
> - Worker receives PgQue events, acquires `WithMappedSplitBuildLease`, reads
>   dirty request, invokes the embedder build callback, publishes, clears dirty,
>   and then acks.
> - If dirty is false, ack as no-op.
> - Transient build/publish errors nack for retry.
> - If a worker dies after publish but before ack, redelivery no-ops because
>   dirty/current state already advanced.
>
> Tests:
> - Duplicate events produce one published map revision.
> - Two workers racing one lane produce one build.
> - Different lanes can build independently.
> - Simulated post-publish/pre-ack failure redelivers and no-ops.
>
> Acceptance:
> - `go test ./... -run PgQue -race` passes.

---

## Slice 6 — Multi-Replica E2E And Documentation

**Slice goal:** Prove the complete Postgres store + optional PgQue path behaves
correctly under multi-replica conditions, then document embedder integration.

### Task 6.1 — Store Multi-Replica E2E

Status: complete in `config/multi_replica_e2e_test.go`.

> Add an embeddedpg-backed e2e test with multiple `config.Server` instances
> sharing one `PgStore` database.
>
> Requirements:
> - Server A publishes or cold-starts a lane.
> - Server B serves map and bundle fetches.
> - Server C publishes a newer map.
> - Server A/B observe the newer current state.
> - Fetches return `Unchanged` on matching version/checksum.
>
> Acceptance:
> - `go test ./... -run MultiReplica` passes.

### Task 6.2 — Scheduler Multi-Replica E2E

Status: complete in `config/multi_replica_e2e_test.go`.

> Add an embeddedpg + PgQue e2e test with at least three worker identities.
>
> Requirements:
> - Every replica starts scheduler setup idempotently.
> - Many duplicate build schedules for one lane produce one dirty row and one
>   published map revision.
> - One worker lease expires and another worker takes over.
> - Different lanes build concurrently.
>
> Acceptance:
> - `go test ./... -run SchedulerMultiReplica -race` passes.

### Task 6.3 — Update User-Facing Docs

Status: complete in `DESIGN.md`, `docs/design/mapped-split-store-scheduler.md`,
and `mappedsplit/README.md`.

> Update durable integration docs after behavior exists.
>
> Requirements:
> - `DESIGN.md` references the implemented packages and keeps architecture
>   concise.
> - `mappedsplit/README.md` or `config` docs show how an embedder wires
>   `PgStore`, migrations, and optional PgQue scheduling.
> - Do not advertise a standalone Orange server process. Embedders still mount
>   handlers on their own mux.
>
> Acceptance:
> - Docs match implemented API names.
> - `go test ./...` passes.

---

## Completion Gate

The implementation track is complete when:

- `config.PgStore` is production-ready for multi-replica fetch/publish.
- Store migrations are explicit, idempotent, and tested with embeddedpg.
- Per-lane build leases include holder identity, expiry, heartbeat, and fencing
  token.
- Cold-start and worker paths share the same store lease.
- PgQue, if enabled, is only a scheduler signal layer.
- Multi-replica tests prove no process-local state is required for correctness.
- `go test ./...` and relevant `-race` suites pass.
