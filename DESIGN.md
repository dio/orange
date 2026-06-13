# Orange: Cherry Bundle Producer

**Status**: Initial design.
**Date**: 2026-06-13

## Purpose

Orange is a small Connect-based producer server for Cherry bundles.

Plum is the data plane. It receives Cherry bundle bytes, opens them into an
immutable generation, and serves requests from that generation. Orange sits on
the producer/control-plane side of that boundary: it gives control planes a
reusable way to turn domain config into normalized `cherry.Input`, pack it into
Cherry bundles, and serve those bundles to Plum instances.

Orange is not the old `tetrateio/fraser/orange` runtime config distributor. It
does not own Envoy module state, request matching, upstream picking, provider
auth injection, tenant databases, user key storage, or secret resolution. Those
concerns belong to Plum or to the embedding control plane.

The goal is to provide boring producer primitives:

1. Accept a bundle selection request.
2. Ask an embedding application to read source-of-truth domain config.
3. Transform and normalize that data into `cherry.Input` with `SecretRef`
   strings only.
4. Validate and pack `cherry.Input`.
5. Wrap it in Cherry bundle metadata.
6. Serve the encoded bundle over Connect.
7. Expose enough version and checksum metadata for Plum sources to poll or fetch
   safely.

## System Boundary

```text
embedding control plane
  owns tenants, users, keys, catalogs, policies, secret storage
  -> orange producer primitives
       select scope
       read source records through embedding adapters
       transform/normalize to cherry.Input
       build Cherry bundle
       serve over Connect
  -> plum source
       fetch bundle bytes
       open with Cherry
       publish ConfigGeneration
  -> plum data path
       match, pick, adapt, resolve secrets at request time
```

Orange is a library plus a minimal server. A production control plane embeds it
and supplies domain-specific providers. The standalone binary, when added, is a
reference host for local development and tests, not the owner of production
tenancy.

Orange supports two embedding modes:

1. Attach the Connect handlers to an existing `http.ServeMux` or compatible
   router owned by the embedding server.
2. Start an orange-managed standalone HTTP server from the embedding process,
   usually in its own goroutine.

Both modes use the same producer service, snapshot manager, admin API, and
thread-safety rules. The only difference is listener ownership.

## Non-Goals

- No built-in tenant model.
- No user, team, organization, project, or workspace database.
- No provider credential storage.
- No request-time secret resolution.
- No secret bytes in Cherry bundles, logs, metrics, traces, or cache entries.
- No Plum generation publication.
- No Envoy dynamic-module bootstrap.
- No replacement for control-plane authorization policy.
- No compatibility layer for legacy Orange `AppState` distribution.

Orange may carry opaque IDs and metadata from embedding applications, but it
must not interpret those IDs as a universal tenancy scheme.

## Producer, Source, Transform, And Secret Boundaries

Orange follows Plum's producer/source/transform/secret split. The system has
three separate flows, and orange belongs only to the producer/control-plane
flow.

Producer/control-plane flow:

```text
domain config
  -> source package/adapter
       orgs, projects, workspaces, users, keys, tags
       model catalogs, provider catalogs, MCP catalogs
  -> transformer
       scope selection
       principal reachability
       tag and rule precedence
       route tree validation/materialization
       MCP profile and auth selection
  -> cherry.Input with SecretRef strings only
  -> cherry.BuildWithManifest
  -> cherry.NewBundle
  -> cherry.EncodeBundleZstd
  -> publish/distribute bundle bytes
```

Plum config-channel flow:

```text
plum source.Source
  -> Cherry bundle bytes
  -> plum.Open
  -> ConfigGeneration
  -> Loader.PublishGeneration
```

Plum data path:

```text
request
  -> match captures ConfigGeneration
  -> match chooses provider/model
  -> adapt/provider auth resolves the selected SecretRef for this request
  -> adapt injects authorization and provider-specific request changes
```

Orange answers the producer question: how does domain config become normalized
Cherry input and a packed bundle? It can host reusable interfaces and helper
implementations for that work, but source-of-truth business rules still belong
to the embedding control plane.

`source` has two meanings that must stay separate:

- Orange-side source adapters read domain records before Cherry packing.
- Plum-side `source.Source` implementations fetch already-built bundle bytes.

Orange does not implement the Plum source contract, except indirectly by
serving bytes that a Plum source client can fetch.

`transform` in orange is producer-side code. It may include local fixture
helpers, examples, and reusable normalizers, but production transforms are
supplied by the embedding application because they depend on that application's
tenant model, catalogs, user reachability, policy precedence, and key
management.

`secret` handling is reference-only in orange. Provider credentials remain refs
such as `env://OPENAI_KEY`, `file:///etc/keys/openai`, `literal://...`,
`orange://...`, `vault://...`, or `sm://...`. Orange must not turn those refs
into secret bytes during source loading, transform, packing, caching, logging,
or serving. Plum resolves the selected ref at request time when provider auth is
injected.

## Core Concepts

### Bundle Selection

A bundle selection describes what the caller wants built. It is deliberately
small:

```go
type Selection struct {
    ScopeKind string
    ScopeID   string
    Revision  string
}
```

`ScopeKind` and `ScopeID` are labels for the producer and for Cherry bundle
metadata. They might represent an organization, project, workspace, cell, shard,
or any other embedding-control-plane scope. Orange stores and returns them as
opaque strings.

`Revision` is optional on fetch requests. When provided, it lets the server
return "not changed" semantics instead of rebuilding or retransmitting the same
bundle.

### Input Provider

`InputProvider` is the lowest-level embedding interface. It represents the
result of the producer-side source and transform stages:

```go
type InputProvider interface {
    CurrentRevision(ctx context.Context, selection Selection) (Revision, error)
    BuildInput(ctx context.Context, selection Selection) (BuildResult, error)
}
```

`BuildInput` returns already-normalized Cherry input:

```go
type BuildResult struct {
    Revision string
    Scopes   []string
    Input    cherry.Input
}
```

The provider is where domain records have already been read and normalized. It
owns catalog joins, principal reachability, rule precedence, route tree
materialization, MCP profile selection, and any authorization checks for the
caller. Orange only validates through Cherry by building the pack.

Higher-level helper packages may split this interface into explicit
source-adapter and transformer stages, but the stable server boundary remains
`BuildResult`: selected scopes plus normalized `cherry.Input` containing secret
refs, not secret bytes.

### Bundle Builder

The builder is the narrow bridge to Cherry:

```text
BuildResult.Input
  -> cherry.BuildWithManifest
  -> cherry.NewBundle(scope_kind, scope_id, scopes, blob, manifest)
  -> cherry.EncodeBundleZstd
```

The output is the exact byte artifact Plum sources consume.

Bundle building must be deterministic for the same normalized input. The
initial implementation should avoid hidden timestamps in the bundle bytes.
Revision, creation time, and producer metadata belong in the RPC response
metadata unless Cherry later adds explicit bundle metadata fields for them.

### Bundle Store

A store is optional. Orange should allow published snapshots to be kept only in
memory or backed by an embedding-provided durable store:

```go
type BundleStore interface {
    Get(ctx context.Context, selection Selection) (StoredBundle, bool, error)
    Put(ctx context.Context, bundle StoredBundle) error
}
```

The first implementation can be in-memory. Production deployments can provide
their own durable or distributed store when snapshots must survive process
restart or be shared across orange instances.

Cache keys must include the opaque selection and version. A cache hit must not
cross tenant, project, workspace, or other embedding-defined boundaries.

### Snapshot Manager

Orange serves bundles from immutable snapshots. A snapshot is the prepared
producer result for one selection and version:

```go
type Snapshot struct {
    Selection  Selection
    Version    uint64
    Scopes     []string
    Input      cherry.Input
    BundleZstd []byte
    Checksum   []byte
    Envelope   *configv1.SnapshotEnvelope
}
```

The snapshot manager owns publication and concurrency:

- Mutations are serialized per manager.
- Published snapshots are immutable.
- Bundle fetches read the current snapshot through an atomic pointer or
  equivalent read-safe structure.
- Bundle fetches must not observe a partially built snapshot.
- Failed mutations leave the previous snapshot active.
- The manager copies or otherwise freezes mutable slices, maps, and bundle
  bytes before publication.

This gives both embedding modes the same behavior: admin/mutation calls can
prepare a new snapshot while Plum sources continue to read the previous
published snapshot, and a successful mutation becomes visible atomically.

### Mutation Callback

Embedding applications can register a mutation callback with the snapshot/admin
API. The callback is the hook that turns prepared embedding data into the next
snapshot.

```go
type MutationCallback func(ctx context.Context, req MutationRequest) (BuildResult, error)
```

`MutationRequest` carries the opaque selection, caller/admin metadata, and
prepared data supplied by the embedding application or its admin endpoint. The
prepared data is intentionally not part of orange's universal schema; it may be
a typed Go value in in-process use, bytes from an admin RPC, or a reference to
data already staged by the embedding control plane.

The callback contract:

- It may read embedding-owned databases, caches, or staged prepared data.
- It returns normalized `cherry.Input` with secret refs only.
- It must not return secret bytes.
- It must not mutate previously published snapshots.
- It should treat request prepared data as immutable for the duration of the
  call.
- It can be slow, but orange serializes publication so only complete snapshots
  become visible.

Orange is responsible for invoking callbacks in a thread-safe publication path.
For a single snapshot manager, mutation callbacks are not run concurrently with
other mutations unless a future implementation explicitly documents sharded
mutation lanes. Read-side `Fetch` calls may continue concurrently and see either
the old snapshot or the new snapshot, never an in-between state.

## Connect API

The repository already has the first snapshot-service concept in
`api/orange/config/v1` and `api/orange/config/admin/v1`. Orange should build on
that API instead of adding a parallel bundle service.

The Plum-facing service is:

```proto
service SnapshotService {
  rpc Fetch(FetchRequest) returns (FetchResponse);
}
```

Current request/response shape:

```proto
message FetchRequest {
  uint64 last_version = 2;
  bytes last_checksum = 3;
}

message FetchResponse {
  oneof result {
    SnapshotEnvelope snapshot = 1;
    Unchanged unchanged = 2;
  }
}
```

`Fetch` is the required cold-start and reconnect path. When `last_version` and
`last_checksum` still match the current published snapshot, the server returns
`Unchanged`. Otherwise it returns the current `SnapshotEnvelope`.

The current envelope is:

```proto
message SnapshotEnvelope {
  uint64 version = 1;
  bytes payload = 4;
  bytes checksum = 5;
}
```

`version` is monotonically increasing per snapshot manager. `checksum` is the
SHA-256 of the decompressed payload bytes. `payload` should be a compressed or
raw `ConfigPayload` wrapper, not the bundle bytes directly. The public API
should use neutral bundle/payload names and avoid leaking Cherry as a wire API
concept. Cherry remains Orange's internal bundle implementation.

Recommended payload shape:

```proto
message ConfigPayload {
  uint32 schema_version = 1;
  PayloadFormat format = 2;
  bytes payload = 3;
  SnapshotMetadata metadata = 4;
}

message PayloadFormat {
  string media_type = 1;
  string encoding = 2;
  string format_version = 3;
}

message SnapshotMetadata {
  string producer = 1;
  string source_revision = 2;
  google.protobuf.Timestamp created_at = 3;
  string lane = 4;
  string scope_kind = 5;
  string scope_id = 6;
  repeated string scopes = 7;
  uint64 payload_size = 8;
  bytes payload_sha256 = 9;
}
```

`format.media_type` should identify the payload body format, for example
`application/vnd.dio.orange.config-bundle`. `format.encoding` should describe
the byte encoding, for example `zstd` or `identity`.
`format.format_version` versions the payload body format. `schema_version`
versions the `ConfigPayload` wrapper.

The metadata should stay operational and diagnostic:

- `producer`: name/version of the orange embedder or standalone server.
- `source_revision`: opaque upstream config revision, database transaction ID,
  git SHA, or catalog version used to produce this snapshot.
- `created_at`: publication time for diagnostics and stale-snapshot alerts.
- `lane`: optional opaque snapshot lane when one orange instance serves more
  than one stream of snapshots.
- `scope_kind`, `scope_id`, `scopes`: echo the Cherry selection and concrete
  scopes so Plum and operators can verify they received the expected snapshot.
- `payload_size` and `payload_sha256`: diagnostics and integrity for the
  embedded payload bytes, separate from the envelope checksum.

The metadata must not contain secret bytes, resolved credentials, provider auth
headers, user key material, full tenant records, or normalized config dumps.
Secret refs may remain inside the Cherry bundle because Cherry already carries
refs as config, but logs and metrics should avoid printing them.

The initial integrity model should rely on transport security, RPC
authentication, the envelope checksum, and Cherry manifest validation. Orange
should not add snapshot signatures in the first implementation. Signatures can
be added later if snapshots need offline verification, untrusted storage, or
distribution through intermediaries that are not part of the trusted transport
path.

`SnapshotService.Fetch` is currently annotated with
`AUTH_TYPE_CLIENT_ASSERTION`. That matches the data-plane fetch path: Plum
instances authenticate as clients fetching the latest snapshot.

When one orange process serves multiple snapshot lanes, `FetchRequest` should
not carry lane selection. Orange should derive the lane from the authenticated
client identity by resolving principal to partition. The embedding application
owns that principal-to-partition mapping and the authorization decision. This
keeps data-plane clients from asking for arbitrary lanes and keeps tenancy or
partition structure out of the public fetch request.

The public fetch API should use Connect error codes with stable meanings:

- `invalid_argument`: malformed fetch request or snapshot lane.
- `not_found`: the embedding provider has no such selection.
- `permission_denied`: caller is authenticated but cannot fetch this selection.
- `unauthenticated`: caller credentials are missing or invalid.
- `failed_precondition`: provider returned data that cannot become a valid
  Cherry bundle.
- `unavailable`: producer dependency is temporarily unavailable.
- `internal`: unexpected producer failure.

The admin API already exists as a stub:

```proto
service ConfigAdminService {
  rpc PublishSnapshot(PublishSnapshotRequest) returns (PublishSnapshotResponse);
}
```

`ConfigAdminService.PublishSnapshot` is annotated with `AUTH_TYPE_API_KEY` and
the `admin` scope. The request and response messages are currently empty. They
should be expanded around the existing service instead of adding a separate
admin service.

The first useful `PublishSnapshotRequest` should carry:

- Optional optimistic concurrency, such as `expected_version` and/or
  `expected_checksum`.
- Optional selection fields if a single orange instance serves more than one
  snapshot lane.
- Prepared data for the registered mutation callback, represented as bytes for
  remote admin clients.

The first useful `PublishSnapshotResponse` should return:

- Previous version.
- Published version.
- Published checksum.
- Selected scopes or other non-secret diagnostics needed by the embedder.

For in-process embedding, `prepared_data` does not require serialization through
protobuf. The Go API may pass an opaque typed value to the registered mutation
callback while the Connect admin API carries bytes for remote admin clients.
Both paths must enter the same snapshot manager and publication lock.

Optimistic concurrency is optional. When set and it does not match the current
published snapshot for the lane, `PublishSnapshot` should return
`failed_precondition` and leave the current snapshot unchanged.

## Embedding API

The library should expose handler attachment for existing servers:

```go
svc := orange.NewService(options)
path, handler := svc.SnapshotServiceHandler()
mux.Handle(path, handler)

adminPath, adminHandler := svc.ConfigAdminServiceHandler()
mux.Handle(adminPath, adminHandler)
```

The same service can be run as an orange-owned standalone server:

```go
svc := orange.NewService(options)
srv := orange.NewServer(":8080", svc)
go func() {
    _ = srv.Run(ctx)
}()
```

Both forms must allow the embedder to register mutation callbacks before
serving:

```go
svc.RegisterMutationCallback(callback)
```

The standalone server is only a hosting convenience. It must not bypass the
same middleware hooks, callback registration, snapshot manager, cache, or
publication rules used by mux attachment.

## Security Model

Orange authenticates callers and delegates authorization to the embedding
application. The reusable server should expose middleware hooks rather than
inventing a universal access model.

Secret handling is reference-only. Provider and MCP credentials inside
`cherry.Input` must stay as refs such as:

```text
env://OPENAI_API_KEY
file:///etc/plum/provider.key
orange://tenant/key/openai
vault://...
sm://...
```

Orange may validate that a ref is syntactically acceptable if the embedding
application asks it to, but it must not resolve the ref to bytes while building
or serving a bundle. Plum resolves the selected ref on the request path when it
injects upstream auth.

Operational logs and metrics must not include bundle bytes, secret refs by
default, or full normalized input. Diagnostics can include selection labels,
version, checksum, encoded size, pack table counts, and build latency.

## Runtime Flow

### Fetch

```text
Plum source
  -> SnapshotService.Fetch(last_version, last_checksum)
  -> authenticate request
  -> resolve principal to partition/snapshot lane
  -> authorize the snapshot lane through embedding hook
  -> load current published Snapshot for that lane
  -> return Unchanged if version and checksum match
  -> return SnapshotEnvelope with payload, version, and checksum
```

### Admin Mutation

```text
embedding admin caller
  -> ConfigAdminService.PublishSnapshot(expected_version, prepared_data)
  -> authenticate and authorize through embedding hooks
  -> acquire snapshot mutation lock
  -> verify expected_version/checksum if present
  -> call registered MutationCallback with prepared data
  -> cherry.BuildWithManifest
  -> cherry.NewBundle
  -> cherry.EncodeBundleZstd
  -> build SnapshotEnvelope
  -> freeze and publish immutable Snapshot atomically
  -> release mutation lock
  -> return previous and new version/checksum
```

If the callback or Cherry build fails, orange releases the lock and keeps the
previous snapshot active.

### Watch

```text
Plum source
  -> future SnapshotService.Watch(after_version)
  -> server waits for provider/store notification
  -> stream SnapshotEnvelope
```

There is no watch RPC in `api/orange/config/v1/service.proto` today. The watch
path is deferred. The first Plum source should poll `SnapshotService.Fetch`
with `FetchRequest.last_version` and `last_checksum`; `Unchanged` is the normal
no-update response.

## Package Shape

Initial repository layout:

```text
api/orange/config/v1/
  service.proto
  snapshot.proto
api/orange/config/admin/v1/
  admin.proto
internal/server/
  connect handler wiring
producer/
  Selection
  InputProvider
  Builder
  BundleStore
  Service
snapshot/
  Snapshot
  SnapshotManager
  MutationCallback
source/
  producer-side adapter interfaces and fixture adapters
transform/
  producer-side normalizer helpers
cmd/orange/
  minimal development server
```

The `producer` package is the stable embedding API. `internal/server` can hold
transport details. Generated proto packages should stay separate from producer
interfaces so tests and embedding applications can exercise bundle building
without an HTTP server.

The `source` and `transform` packages, if added, are producer-side helpers only.
They must not be confused with Plum's config-channel `source.Source`, and they
must not define a universal control-plane domain schema.

The `snapshot` package owns thread-safe publication. Both mux-attached handlers
and standalone servers must use this package instead of keeping their own
current-bundle state.

## Compatibility With Plum

Orange serves the artifact Plum already expects:

```text
[]byte from cherry.EncodeBundleZstd
  -> plum source snapshot
  -> cherry.OpenBundleZstd or plum.Open
  -> ConfigGeneration
```

Orange must not require Plum to understand producer domain models. The only
shared contract is the Cherry bundle format plus small delivery metadata:
selection, version, checksum, and contained scopes.

## Implementation Plan

### 1. Producer Library

- Define `Selection`, `Revision`, `BuildResult`, `InputProvider`, and
  `BundleStore`.
- Implement `Builder` around Cherry's bundle APIs.
- Implement `Snapshot`, `SnapshotManager`, and mutation callback registration
  with serialized mutation and atomic read-side publication.
- Keep source and transform helpers producer-side and optional; embedding
  applications can bypass them by implementing `InputProvider` directly.
- Add focused unit tests for deterministic output, validation failures, and
  secret-ref pass-through.

### 2. Connect API

- Use the existing `api/orange/config/v1/SnapshotService.Fetch` as the
  Plum-facing fetch API.
- Expand `api/orange/config/admin/v1/PublishSnapshotRequest` and
  `PublishSnapshotResponse`.
- Implement `ConfigAdminService.PublishSnapshot`.
- Map builder/provider failures to stable Connect error codes.

### 3. Embedding And Local Server

- Expose handler attachment for existing mux/router setups.
- Expose a standalone server helper that runs from the embedding process.
- Add `cmd/orange` as a development server.
- Provide a fixture-backed `InputProvider` for local Plum integration tests.
- Support HTTP/2 cleartext for local Connect clients and ordinary HTTP/1.1 for
  curl-compatible inspection where practical.

### 4. Caching And Revision Checks

- Add an in-memory `BundleStore`.
- Return `Unchanged` from `Fetch` when `last_version` and `last_checksum` match
  the published snapshot.
- Store successful snapshot publications by selection and version.

### 5. Plum Integration

- Add or update a Plum source client that calls `SnapshotService.Fetch`.
- Use polling for the initial source; do not add server streaming until
  production change notifications exist.
- Confirm Plum can fetch, open, publish, and hot reload bundles produced by
  orange.
