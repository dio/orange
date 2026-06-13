# Orange: Implementation Tracker

**Companion to:** `DESIGN.md`
**Format:** Each task below is written as a self-contained prompt. Hand one to
an engineer or agent without additional context and it should be executable.
**Status convention:** mark a task `[done]` in this file when its PR merges and
its acceptance gate passes.

---

## How To Use This Document

- Tasks are grouped into 5 slices. Slices run sequentially; tasks inside a slice
  may run in parallel only when the prompt says so.
- Each prompt states the goal, files in play, contract, tests, and acceptance
  gate. Read the whole prompt before starting.
- Every implementation task must include code and tests. Documentation-only
  follow-ups are allowed only when the prompt explicitly says so.
- Cross-cutting rules from `DESIGN.md` apply to every task:
  - Orange is producer/control-plane side only.
  - Plum consumes Cherry bundle bytes through `SnapshotService.Fetch`.
  - Secret handling is reference-only; no secret bytes in snapshots, logs,
    metadata, tests, or fixtures.
  - Lane selection for fetch is derived from authenticated client identity
    through embedder-owned principal-to-partition mapping.
  - Initial delivery uses polling, not server streaming.

### Document Scope

- This file is an execution prompt pack, not architecture. If a task discovers a
  lasting design decision, promote that decision into `DESIGN.md` in the same
  PR and shorten the prompt here.
- Prompts in this file target the **Orange repository only**. They must not edit
  Plum, Cherry, or any other repository. If a task needs an upstream change,
  file that separately and note the dependency here.

### Current Baseline

- `api/orange/config/v1/service.proto` defines `SnapshotService.Fetch`.
- `api/orange/config/v1/snapshot.proto` defines `SnapshotEnvelope`.
- `api/orange/config/admin/v1/admin.proto` defines a stub
  `ConfigAdminService.PublishSnapshot`.
- Generated Go, Connect, and vtprotobuf files exist for the current protos.
- `server/server.go` and `main.go` are stubs.

---

## Slice 1 — Snapshot API Shape

**Slice goal:** The checked-in protobuf API can carry a Cherry bundle wrapped in
diagnostic metadata, and admin publishing can pass prepared data into the
snapshot manager without inventing a second service.

**Status (2026-06-13):**
- Task 1.1 — pending.
- Task 1.2 — pending.

### Task 1.1 — Add `ConfigPayload` wrapper

> `SnapshotEnvelope.payload` currently says it contains a compressed or raw
> `ConfigPayload`, but no `ConfigPayload` message exists. Add the explicit
> payload wrapper described in `DESIGN.md`.
>
> Files in play:
> - `api/orange/config/v1/snapshot.proto`
> - generated files under `api/orange/config/v1/`
> - `buf.gen.golang.yaml`, `buf.yaml` only if generation requires updates
>
> Implement:
>
> ```proto
> message ConfigPayload {
>   uint32 schema_version = 1;
>   PayloadFormat format = 2;
>   bytes payload = 3;
>   SnapshotMetadata metadata = 4;
> }
>
> message PayloadFormat {
>   string media_type = 1;
>   string encoding = 2;
>   string format_version = 3;
> }
>
> message SnapshotMetadata {
>   string producer = 1;
>   string source_revision = 2;
>   google.protobuf.Timestamp created_at = 3;
>   string lane = 4;
>   string scope_kind = 5;
>   string scope_id = 6;
>   repeated string scopes = 7;
>   uint64 payload_size = 8;
>   bytes payload_sha256 = 9;
> }
> ```
>
> Add validation where it is useful and stable:
> - `schema_version > 0`
> - `format.media_type` non-empty
> - `format.encoding` non-empty
> - `payload` non-empty
> - `payload_sha256` empty or exactly 32 bytes
>
> Do not add secret-bearing fields. Do not add tenant/user schema fields beyond
> opaque lane and scope labels.
> Do not use Cherry-specific names in public proto fields; use neutral bundle or
> payload terminology. Cherry is the internal implementation detail.
>
> Regenerate Go, Connect, and vtprotobuf outputs using the repository's existing
> buf setup.
>
> **Tests:** If generated code compiles only through existing packages, add no
> extra test in this task. Otherwise add a small proto round-trip test under an
> appropriate package.
>
> **Acceptance:** `go test ./...` passes, generated files are updated, and
> `SnapshotEnvelope.payload` has a concrete typed payload documented in proto.

### Task 1.2 — Expand `PublishSnapshot` admin messages

> `api/orange/config/admin/v1/admin.proto` currently has empty
> `PublishSnapshotRequest` and `PublishSnapshotResponse` messages. Expand them
> around the existing `ConfigAdminService.PublishSnapshot` RPC. Do not introduce
> a new admin service.
>
> Files in play:
> - `api/orange/config/admin/v1/admin.proto`
> - generated files under `api/orange/config/admin/v1/`
> - imports from `orange/config/v1/snapshot.proto` if response needs checksum
>   or metadata types
>
> Request fields:
> - `uint64 expected_version`
> - `bytes expected_checksum`
> - `bytes prepared_data`
> - optional opaque lane/scope selectors only if needed for admin publication
>   before the snapshot package exists
>
> Response fields:
> - `uint64 previous_version`
> - `uint64 published_version`
> - `bytes published_checksum`
> - `repeated string scopes`
>
> Validation:
> - `expected_checksum` empty or exactly 32 bytes
> - `prepared_data` may be empty for in-process publish paths, but remote admin
>   clients should normally provide it
>
> Keep the existing auth annotation:
> `AUTH_TYPE_API_KEY` with `scopes: ["admin"]`.
>
> **Tests:** generated code compiles. Add a small protovalidate test only if the
> repository already has validation test helpers by the time this task starts.
>
> **Acceptance:** `go test ./...` passes and `PublishSnapshot` can carry
> optimistic concurrency, prepared data, and publication diagnostics.

**Slice 1 acceptance gate:** Protos and generated code are coherent; no
parallel `BundleService` or `SnapshotAdminService` exists.

---

## Slice 2 — Producer And Snapshot Core

**Slice goal:** Orange can turn normalized `cherry.Input` into an immutable
published snapshot with version/checksum semantics and thread-safe read-side
fetch.

**Status (2026-06-13):**
- Task 2.1 — pending.
- Task 2.2 — pending.
- Task 2.3 — pending.

### Task 2.1 — Producer builder around Cherry

> Add the producer-side builder that converts normalized `cherry.Input` into a
> Cherry bundle and `ConfigPayload`.
>
> Files/packages to create:
> - `producer/`
>
> Define:
>
> ```go
> type Selection struct {
>     ScopeKind string
>     ScopeID   string
> }
>
> type BuildResult struct {
>     SourceRevision string
>     Scopes         []string
>     Input          cherry.Input
> }
>
> type Builder struct { ... }
> ```
>
> Implement a method that:
> 1. Calls `cherry.BuildWithManifest`.
> 2. Calls `cherry.NewBundle`.
> 3. Calls `cherry.EncodeBundleZstd`.
> 4. Builds `configv1.ConfigPayload` with metadata.
> 5. Computes SHA-256 for the Cherry bundle bytes.
>
> Keep the builder deterministic for the same input except for explicitly
> supplied metadata such as `created_at`. Prefer injecting a clock in options so
> tests do not depend on wall time.
>
> **Tests:** unit test successful build with a minimal valid `cherry.Input`;
> invalid input returns an error; secret refs pass through as refs and are not
> resolved.
>
> **Acceptance:** `go test ./producer/...` passes.

### Task 2.2 — Snapshot envelope builder

> Add code that wraps a `ConfigPayload` into `SnapshotEnvelope`.
>
> Files/packages:
> - `snapshot/`
> - or `producer/` if the implementation would otherwise be too small; prefer
>   `snapshot/` if it is also used by the manager in Task 2.3
>
> Define:
>
> ```go
> type Snapshot struct {
>     Lane       string
>     Version    uint64
>     Scopes     []string
>     Payload    *configv1.ConfigPayload
>     Envelope   *configv1.SnapshotEnvelope
>     BundleZstd []byte
>     Checksum   [32]byte
> }
> ```
>
> The envelope checksum is SHA-256 of the decompressed/raw `ConfigPayload`
> bytes, matching `SnapshotEnvelope.checksum` comments. If payload compression
> is added now, make compression explicit and tested; otherwise store raw proto
> bytes and leave compression for a later task.
>
> **Tests:** marshal/unmarshal round trip; checksum mismatch test; version set
> correctly; slices are copied so callers cannot mutate the published bytes.
>
> **Acceptance:** `go test ./snapshot/...` passes.

### Task 2.3 — Thread-safe `SnapshotManager`

> Implement the manager that publishes immutable snapshots and serves read-side
> fetches concurrently.
>
> Files/packages:
> - `snapshot/`
>
> Required behavior:
> - Mutations are serialized.
> - Read-side fetches are lock-free or read-lock minimal.
> - Published snapshots are immutable.
> - Failed mutations keep the previous snapshot active.
> - `Fetch(lastVersion, lastChecksum)` returns either current envelope or
>   unchanged.
> - Optimistic concurrency checks support expected version/checksum.
> - Multiple lanes are supported internally. Lane is an opaque string resolved
>   before calling the manager.
>
> Define a mutation callback shape compatible with `DESIGN.md`:
>
> ```go
> type MutationCallback func(ctx context.Context, req MutationRequest) (producer.BuildResult, error)
> ```
>
> The manager invokes the callback in the serialized publication path and uses
> the producer builder/envelope builder to publish the result.
>
> **Tests:** concurrent fetch during publish under `-race`; failed callback
> leaves old version active; expected-version mismatch returns a
> failed-precondition style error; multiple lanes do not cross-contaminate.
>
> **Acceptance:** `go test ./snapshot/... -race` passes.

**Slice 2 acceptance gate:** A test can publish a snapshot from `cherry.Input`
and fetch the resulting `SnapshotEnvelope` without running an HTTP server.

---

## Slice 3 — Connect Services And Embedding

**Slice goal:** Embedders can attach orange handlers to a mux or run an
orange-owned server; both paths share the same snapshot manager and callback
registration.

**Status (2026-06-13):**
- Task 3.1 — pending.
- Task 3.2 — pending.
- Task 3.3 — pending.

### Task 3.1 — Auth and lane resolution hooks

> Add small interfaces for request authentication and lane resolution. Do not
> build a universal tenancy model.
>
> Files/packages:
> - `server/`
> - possibly `auth/` if the interface deserves its own package
>
> Required concepts:
>
> ```go
> type Principal struct {
>     ID     string
>     Scopes []string
> }
>
> type Authenticator interface {
>     Authenticate(ctx context.Context, req AnyRequestShape) (Principal, error)
> }
>
> type LaneResolver interface {
>     ResolveLane(ctx context.Context, principal Principal) (string, error)
> }
> ```
>
> Shape the actual request parameter around Connect APIs, not this sketch, but
> keep the boundary: fetch lane comes from authenticated principal to partition,
> not from `FetchRequest`.
>
> Provide a development/no-op implementation only if it is impossible to test
> without one. Production defaults should fail closed.
>
> **Tests:** fetch without auth fails when fail-closed auth is configured; a
> fake principal resolves to the expected lane; clients cannot request a
> different lane via `FetchRequest`.
>
> **Acceptance:** `go test ./server/...` passes.

### Task 3.2 — Implement `SnapshotService.Fetch`

> Implement `orange.config.v1.SnapshotService` using the generated Connect
> handler and the snapshot manager.
>
> Files/packages:
> - `server/`
> - generated package imports under `api/orange/config/v1/...`
>
> Behavior:
> - Authenticate caller using the hook from Task 3.1.
> - Resolve lane from principal.
> - Validate `last_checksum` length using generated/protovalidate behavior or
>   explicit checks.
> - Call snapshot manager fetch.
> - Return `FetchResponse{Unchanged}` when version/checksum match.
> - Return `FetchResponse{Snapshot}` otherwise.
> - Map errors to stable Connect codes.
>
> **Tests:** fake manager current snapshot returns snapshot; matching
> last_version/checksum returns unchanged; malformed checksum returns
> `invalid_argument`; missing lane returns `not_found` or `permission_denied`
> according to the resolver error.
>
> **Acceptance:** `go test ./server/...` passes.

### Task 3.3 — Implement `ConfigAdminService.PublishSnapshot`

> Implement the existing admin RPC using mutation callback registration and the
> snapshot manager. Do not add a separate admin service.
>
> Files/packages:
> - `server/`
> - `snapshot/` if manager API needs minor additions
>
> Behavior:
> - Authenticate with admin requirements.
> - Authorize `admin` scope.
> - Convert remote `prepared_data` into `MutationRequest`.
> - Call the registered mutation callback through the snapshot manager.
> - Enforce expected version/checksum when present.
> - Return previous version, published version, published checksum, and scopes.
> - If callback/build fails, leave current snapshot active.
>
> In-process publish APIs may pass typed prepared data without protobuf bytes,
> but the Connect RPC carries bytes.
>
> **Tests:** successful publish makes fetch return the new envelope; expected
> version mismatch fails and keeps old snapshot; callback error keeps old
> snapshot; missing callback fails closed.
>
> **Acceptance:** `go test ./server/... ./snapshot/... -race` passes.

**Slice 3 acceptance gate:** A test HTTP server can publish a snapshot through
admin RPC and fetch it through `SnapshotService.Fetch`.

---

## Slice 4 — Hosting And Local Fixture

**Slice goal:** Orange can be embedded into an existing mux or run as a
standalone development server with fixture-backed publishing.

**Status (2026-06-13):**
- Task 4.1 — pending.
- Task 4.2 — pending.
- Task 4.3 — pending.

### Task 4.1 — Handler attachment API

> Expose the embedding API from `DESIGN.md`.
>
> Files/packages:
> - `server/`
> - root package if a public `orange.NewService` facade is useful
>
> Required API shape:
>
> ```go
> svc := orange.NewService(options)
> path, handler := svc.SnapshotServiceHandler()
> mux.Handle(path, handler)
>
> adminPath, adminHandler := svc.ConfigAdminServiceHandler()
> mux.Handle(adminPath, adminHandler)
> ```
>
> The exact package name may be `server` instead of root `orange` if that fits
> the repo better, but the two handler methods must exist somewhere public.
> Both handlers must share the same snapshot manager and auth/lane hooks.
>
> **Tests:** attach both handlers to `httptest.Server`, publish through admin,
> fetch through snapshot service.
>
> **Acceptance:** `go test ./server/...` passes.

### Task 4.2 — Standalone server helper

> Add the second embedding mode: orange-managed HTTP server that can run in its
> own goroutine from an embedding process.
>
> Files/packages:
> - `server/`
> - `main.go`
>
> Required behavior:
> - Reuse the exact same service object as mux attachment.
> - Accept context cancellation for shutdown.
> - Do not bypass middleware/auth/lane/callback/snapshot manager rules.
> - Choose explicit address/config options.
>
> **Tests:** start server on `127.0.0.1:0`, publish/fetch through Connect
> clients, cancel context, assert shutdown.
>
> **Acceptance:** `go test ./server/...` passes.

### Task 4.3 — Fixture mutation callback and development binary

> Add a fixture-backed mutation callback so `cmd/orange` or `main.go` can run a
> local development producer. This is for tests and local inspection only, not a
> universal production schema.
>
> Files/packages:
> - `source/fixture` or `internal/fixture`
> - `transform/fixture` if needed
> - `main.go`
>
> Behavior:
> - Read a small local fixture format or use an in-memory example.
> - Produce normalized `cherry.Input`.
> - Register the mutation callback.
> - Publish an initial snapshot on startup.
> - Serve `SnapshotService.Fetch`.
>
> Keep fixture packages clearly removable. They must not define production
> tenancy or secret semantics.
>
> **Tests:** fixture callback publishes a bundle Plum/Cherry can open; no
> plaintext secret bytes appear in metadata or logs.
>
> **Acceptance:** `go test ./...` passes and `go run .` starts without panic.

**Slice 4 acceptance gate:** Both embedding modes are usable; local dev server
can publish and serve one valid snapshot.

---

## Slice 5 — Hardening And Compatibility

**Slice goal:** The implementation has the concurrency, validation, and
compatibility checks needed before Plum depends on it.

**Status (2026-06-13):**
- Task 5.1 — pending.
- Task 5.2 — pending.
- Task 5.3 — pending.

### Task 5.1 — Error mapping and validation audit

> Audit all server and snapshot errors and map them to stable Connect codes.
>
> Required mappings:
> - malformed request/checksum: `invalid_argument`
> - auth missing/invalid: `unauthenticated`
> - authenticated but unauthorized lane/admin: `permission_denied`
> - no snapshot for resolved lane: `not_found`
> - expected version/checksum mismatch: `failed_precondition`
> - callback/build dependency unavailable: `unavailable` where distinguishable
> - unexpected bug: `internal`
>
> **Tests:** table-driven server tests for each mapping.
>
> **Acceptance:** no raw internal errors leak to Connect clients; `go test
> ./server/... ./snapshot/...` passes.

### Task 5.2 — Race, immutability, and secret-safety audit

> Add tests and code checks for the safety properties in `DESIGN.md`.
>
> Cover:
> - concurrent publish/fetch under race detector
> - caller mutation of input slices/maps cannot change published snapshots
> - `literal://` or other secret-looking refs are not printed in metadata
> - bundle bytes are not logged
> - failed publish leaves old snapshot active
>
> **Acceptance:** `go test ./... -race` passes. Add focused assertions rather
> than relying only on code review.

### Task 5.3 — Plum compatibility harness

> In Orange only, add a compatibility test that proves the served payload
> contains Cherry bundle bytes that Cherry can open. Do not edit Plum in this
> task.
>
> Test shape:
> 1. Publish a fixture snapshot.
> 2. Fetch through `SnapshotService.Fetch`.
> 3. Decode `ConfigPayload`.
> 4. Extract `payload`.
> 5. Call `cherry.OpenBundleZstd`.
> 6. Assert metadata and checksums line up.
>
> If importing Cherry is not yet in `go.mod`, add it with a local replace only
> if that is already the repo pattern; otherwise use the module version expected
> by the workspace.
>
> **Acceptance:** `go test ./...` passes and the compatibility test fails if the
> payload stops carrying a valid Cherry bundle.

**Slice 5 acceptance gate:** `go test ./... -race` passes; Orange can publish,
fetch, decode, and open a Cherry bundle through its public API.

---

## Deferred

- Server streaming watch RPC. Initial Plum source uses polling with
  `FetchRequest.last_version` and `last_checksum`.
- Snapshot signatures. Initial integrity relies on transport security, RPC
  authentication, `SnapshotEnvelope.checksum`, and Cherry manifest validation.
- Production source/transform adapters for specific control-plane schemas.
  Embedders can implement `MutationCallback` directly until a reusable adapter
  earns its place.
