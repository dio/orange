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
- Task 1.1 — done.
- Task 1.2 — done.

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
- Task 2.1 — done.
- Task 2.2 — done.
- Task 2.3 — done.

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
- Task 3.1 — done.
- Task 3.2 — done.
- Task 3.3 — done.

### Task 3.1 — Auth and lane resolution hooks

> Add small interfaces for request authentication and lane resolution. Do not
> build a universal tenancy model.
>
> Files/packages:
> - `server/`
> - possibly `auth/` if the interface deserves its own package
>
> Required concepts:
> See `DESIGN.md` "Auth, Principal, Partition, And Lane Mapping" for the stable
> definitions. In short, authentication maps request credentials such as JWT,
> mTLS, or API key material to a `Principal`; the embedding application maps that
> principal to its own partition and then to an Orange snapshot lane.
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
- Task 4.1 — done.
- Task 4.2 — done.
- Task 4.3 — done.

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
- Task 5.1 — done.
- Task 5.2 — done.
- Task 5.3 — done.

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

## Slice 6 — Data-Plane Client Library

**Slice goal:** Plum and other data planes can fetch Orange snapshots through a
small Go client without hand-written polling boilerplate.

**Status (2026-06-13):**
- Task 6.1 — done.

### Task 6.1 — Resilient `./client` fetch library

> Add a public Go package under `./client` that wraps
> `orange.config.v1.SnapshotService.Fetch` for data-plane consumers. The
> package must use the generated Connect client internally and must not require
> Plum to import server, snapshot-manager, producer, fixture, or admin code.
>
> Required behavior:
> - Track the last accepted `version` and `checksum` locally.
> - Send `FetchRequest.last_version` and `last_checksum` automatically.
> - Return cached data when Orange replies with `Unchanged`.
> - Decode `SnapshotEnvelope.payload` as `ConfigPayload`.
> - Validate `SnapshotEnvelope.checksum` against the raw payload bytes.
> - Validate `ConfigPayload.metadata.payload_sha256` when present.
> - Expose the embedded bundle bytes directly to callers.
> - Allow auth/header injection without custom request boilerplate.
> - Support per-attempt timeout and context cancellation.
> - Retry transient Connect errors with bounded exponential backoff.
> - Use `singleflight` so concurrent fetch callers share one in-flight RPC.
> - Return defensive copies so callers cannot mutate cached client state.
>
> Keep policy small and configurable. Do not add server streaming, background
> goroutines, global loggers, or Plum-specific generation publication here. The
> client is a polling helper; Plum remains responsible for opening the Cherry
> bundle and publishing its own runtime generation.
>
> **Tests:** success path with decoded payload and bundle bytes; `Unchanged`
> uses cached data; transient errors retry; auth headers are injected;
> concurrent callers are coalesced through `singleflight`; invalid checksums
> fail without updating cached state.
>
> **Acceptance:** `go test ./client/...` passes.

---

## Slice 7 — yamlserver Embedding Example

**Slice goal:** Demonstrate orange as an embeddable library by building a
self-contained example that watches a YAML config file, rebuilds Cherry bundles
on change with debouncing, and serves them via an application-owned mux.
See `DESIGN.md ## Examples` for the full design rationale.

**Status (2026-06-13):**
- Task 7.1 — done.
- Task 7.2 — done.
- Task 7.3 — done.

### Task 7.1 — YAML schema and `cherry.Input` parser

> Add a YAML schema that mirrors the full `cherry.Input` surface and a parser
> that converts it into a `cherry.Input` value with no secret resolution. This is
> a kitchen-sink example, not a minimal LLM-only fixture.
>
> Files to create:
> - `examples/yamlserver/config.go`
> - `examples/yamlserver/testdata/example.yaml`
>
> Define Go structs with `yaml:"..."` tags that unmarshal the YAML schema from
> `DESIGN.md ## Examples / YAML Schema`. Cover every current top-level
> `cherry.Input` field and nested field:
> - providers: `id`, `kind`, `endpoint`, `secret_ref`, `auth_type`,
>   `path_prefix`
> - models: `id`, `provider`, `name`, `mode`, `capabilities`,
>   `metadata_json`
> - MCP servers: `id`, `endpoint`, `secret_ref`, `auth_type`
> - scopes: `id`, `principals`, `mcp_profiles`
> - principals: `slug`, compatibility `route`, explicit `model_routes`, `rate`
> - route plans: `kind`, `provider`, `model`, `secret_ref`, `retry`,
>   `children`, `split`
> - split children: `weight`, `plan`
> - retry policy: `retry_on`, `per_try_timeout_ms`
> - rate policy: `usd_per_day_cents`, `rpm`, `on_exceed`
> - MCP profiles and tool bindings: `path`, `tools`, `exposed_name`, `server`,
>   `tool`, `secret_ref`, `auth_type`
>
> Implement:
>
> ```go
> // ParseYAML parses YAML config data into cherry.Input.
> // The returned string is a SHA-256 hex digest of data for use as SourceRevision.
> func ParseYAML(data []byte) (cherry.Input, string, error)
> ```
>
> Rules:
> - `secret_ref` values copy verbatim into `cherry.Provider.SecretRef`; they
>   must not be resolved.
> - Unknown YAML keys must return an error (use `yaml.KnownFields` or equivalent
>   strict decoding).
> - Empty or syntactically invalid YAML returns a descriptive error.
>
> `testdata/example.yaml` must be a valid kitchen-sink example matching the
> schema described in `DESIGN.md`. It should exercise provider and tool
> `secret_ref` pass-through, model catalog metadata, compatibility `route`,
> explicit `model_routes`, target/chain/split route plans, retry policy, MCP
> servers, MCP profiles, and tool bindings.
>
> **Tests:** `examples/yamlserver/config_test.go`
> - Kitchen-sink valid YAML parses into expected `cherry.Input`, including all
>   top-level arrays and nested route/MCP structures.
> - Secret refs appear verbatim in providers, route overrides, MCP servers, and
>   MCP tool bindings; they are not resolved.
> - Unknown top-level key returns an error.
> - Unknown nested key returns an error.
> - Empty input returns an error.
> - Parsed kitchen-sink input builds with `cherry.BuildWithManifest` and opens
>   with `cherry.NewBundle`/`cherry.EncodeBundleZstd`/`cherry.OpenBundleZstd`.
>
> **Acceptance:** `go test ./examples/yamlserver/...` passes.

### Task 7.2 — File watcher with debounce

> Add a watcher that monitors a single file path and triggers a callback after a
> configurable debounce window.
>
> Files to create:
> - `examples/yamlserver/watcher.go`
>
> Add `github.com/fsnotify/fsnotify` to `go.mod` if it is not already present.
>
> Required API:
>
> ```go
> type Watcher struct { ... }
>
> func NewWatcher(path string, debounce time.Duration) *Watcher
>
> // Run blocks until ctx is cancelled. onChange is called after each debounced
> // change event. Run returns ctx.Err() on normal shutdown.
> func (w *Watcher) Run(ctx context.Context, logger *slog.Logger, onChange func()) error
> ```
>
> Behavior:
> - Multiple `fsnotify` events within the debounce window are collapsed into one
>   `onChange` call.
> - Log a warning but do not stop on transient watch errors (e.g. atomic editor
>   save triggers a rename); re-add the watch if the file reappears.
> - Return cleanly when ctx is cancelled.
> - Default debounce when zero is 200 ms.
>
> **Tests:** `examples/yamlserver/watcher_test.go`
> - Write the file multiple times within the debounce window; confirm `onChange`
>   fires exactly once after the window expires.
> - Cancel the context; confirm `Run` returns promptly.
>
> **Acceptance:** `go test ./examples/yamlserver/...` passes.

### Task 7.3 — Wire yamlserver: mux embedding and watch loop

> Add `examples/server/main.go` that ties the `examples/yamlserver` library,
> watcher, and orange library together using the mux-attachment embedding mode.
> This is the primary demonstration of orange as an embeddable library.
>
> Files to create:
> - `examples/server/main.go`
>
> Startup sequence:
>
> 1. Parse flags: `--config` (YAML file path, required) and `--addr` (listen
>    address, default `"127.0.0.1:8080"`).
> 2. Create `producer.Builder` with `Producer: "yamlserver"`.
> 3. Define the `MutationCallback`: read the YAML file, call
>    `yamlserver.ParseYAML`, return
>    `BuildResult{SourceRevision: contentHash, Scopes: scopeIDs(input), Input: ...}`.
>    The file is read inside the callback so publication serialization governs
>    consistency.
> 4. Create `snapshot.Manager` with the builder and callback.
> 5. Eager initial `mgr.Publish(ctx, MutationRequest{Lane: "default"})`; exit on
>    error so the server never starts with an empty snapshot.
> 6. Start `yamlserver.Watcher.Run` in a goroutine with the explicit `slog`
>    logger; on each `onChange` call trigger
>    `mgr.Publish(ctx, MutationRequest{Lane: "default"})`, log errors but do not
>    exit.
> 7. Build `server.NewService` with inline `devAuthenticator` and
>    `singleLaneResolver` defined in the same file. These must not be exported or
>    promoted outside `examples/server`.
> 8. Attach `svc.SnapshotServiceHandler()` and `svc.ConfigAdminServiceHandler()`
>    to an `http.NewServeMux()`.
> 9. Add a `/healthz` endpoint to the same mux — this shows that the embedder
>    owns the mux and co-locates its own routes.
> 10. Serve with a plain `net/http.Server` pointing at the mux — not
>     `svc.ListenAndServe` — to clearly demonstrate the mux-attachment path.
> 11. Shut down cleanly on `SIGINT`/`SIGTERM` via context cancellation.
>
> Add at the top of the file:
>
> ```go
> // yamlserver is a development-only example. The devAuthenticator and
> // singleLaneResolver defined below bypass all credential checks. Do not use
> // them in production.
> ```
>
> The lane is always `"default"`, but the Cherry bundle scopes must be derived
> from the parsed `cherry.Input.Scopes`; lane and Cherry scope are distinct
> concepts. `ScopeKind` and `ScopeID` may be left empty; they are opaque labels
> owned by the embedder.
>
> **Tests:** no unit tests required for `main.go` itself; the parser and watcher
> are already tested. Add `examples/yamlserver/testdata/example.yaml` if not
> created in Task 7.1.
>
> **Acceptance:**
> ```
> go run ./examples/server --config examples/yamlserver/testdata/example.yaml
> ```
> starts without panic, `curl http://127.0.0.1:8080/healthz` returns 200, and
> editing the YAML file triggers a logged rebuild within the debounce window.

**Slice 7 acceptance gate:** `go run ./examples/server --config
examples/yamlserver/testdata/example.yaml` starts, serves one valid snapshot
from `SnapshotService.Fetch`, the fetched Cherry bundle metadata scopes match
the YAML `scopes[].id` values, and the server rebuilds the bundle when the YAML
file changes.

---

## Deferred

- Server streaming watch RPC. Initial Plum source uses polling with
  `FetchRequest.last_version` and `last_checksum`.
- Snapshot signatures. Initial integrity relies on transport security, RPC
  authentication, `SnapshotEnvelope.checksum`, and Cherry manifest validation.
- Production source/transform adapters for specific control-plane schemas.
  Embedders can implement `MutationCallback` directly until a reusable adapter
  earns its place.
