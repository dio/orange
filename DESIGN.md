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
7. Expose enough revision metadata for Plum sources to poll or fetch safely.

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

A store is optional. Orange should allow both direct build-on-read and cached
bundle serving:

```go
type BundleStore interface {
    Get(ctx context.Context, selection Selection) (StoredBundle, bool, error)
    Put(ctx context.Context, bundle StoredBundle) error
}
```

The first implementation can be in-memory. Production deployments can provide
their own durable or distributed store when bundle builds are expensive.

Cache keys must include the opaque selection and revision. A cache hit must not
cross tenant, project, workspace, or other embedding-defined boundaries.

## Connect API

The initial API should be a single service with two RPCs:

```proto
service BundleService {
  rpc GetBundle(GetBundleRequest) returns (GetBundleResponse);
  rpc WatchBundles(WatchBundlesRequest) returns (stream BundleEvent);
}
```

`GetBundle` is the required path. It returns either the current encoded Cherry
bundle or a not-modified response when the caller already has the active
revision.

`WatchBundles` is optional in the first server implementation. It gives Plum
sources a lower-latency path later, but polling `GetBundle` is sufficient for
the initial producer.

Provisional message shape:

```proto
message BundleSelection {
  string scope_kind = 1;
  string scope_id = 2;
}

message GetBundleRequest {
  BundleSelection selection = 1;
  string if_revision = 2;
}

message GetBundleResponse {
  BundleSelection selection = 1;
  string revision = 2;
  repeated string scopes = 3;
  bytes bundle_zstd = 4;
  bool not_modified = 5;
}

message WatchBundlesRequest {
  BundleSelection selection = 1;
  string after_revision = 2;
}

message BundleEvent {
  BundleSelection selection = 1;
  string revision = 2;
  repeated string scopes = 3;
  bytes bundle_zstd = 4;
}
```

The API should use Connect error codes with stable meanings:

- `invalid_argument`: malformed selection.
- `not_found`: the embedding provider has no such selection.
- `permission_denied`: caller is authenticated but cannot fetch this selection.
- `unauthenticated`: caller credentials are missing or invalid.
- `failed_precondition`: provider returned data that cannot become a valid
  Cherry bundle.
- `unavailable`: producer dependency is temporarily unavailable.
- `internal`: unexpected producer failure.

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
revision, encoded size, pack table counts, and build latency.

## Runtime Flow

### Fetch

```text
Plum source
  -> BundleService.GetBundle(selection, if_revision)
  -> authenticate request
  -> authorize selection through embedding hook
  -> ask InputProvider.CurrentRevision
  -> return not_modified if revision matches
  -> load cached bundle or call InputProvider.BuildInput
  -> cherry.BuildWithManifest
  -> cherry.NewBundle
  -> cherry.EncodeBundleZstd
  -> return bundle bytes and revision
```

### Watch

```text
Plum source
  -> BundleService.WatchBundles(selection, after_revision)
  -> server waits for provider/store notification
  -> build or load bundle for new revision
  -> stream BundleEvent
```

The watch path must not be required for correctness. Plum should be able to use
polling with revision checks.

## Package Shape

Initial repository layout:

```text
api/orange/v1/
  bundle.proto
internal/server/
  connect handler wiring
producer/
  Selection
  InputProvider
  Builder
  BundleStore
  Service
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
selection, revision, and contained scopes.

## Implementation Plan

### 1. Producer Library

- Define `Selection`, `Revision`, `BuildResult`, `InputProvider`, and
  `BundleStore`.
- Implement `Builder` around Cherry's bundle APIs.
- Keep source and transform helpers producer-side and optional; embedding
  applications can bypass them by implementing `InputProvider` directly.
- Add focused unit tests for deterministic output, validation failures, and
  secret-ref pass-through.

### 2. Connect API

- Add `api/orange/v1/bundle.proto`.
- Generate Go and Connect handlers.
- Implement `BundleService.GetBundle`.
- Map builder/provider failures to stable Connect error codes.

### 3. Local Server

- Add `cmd/orange` as a development server.
- Provide a fixture-backed `InputProvider` for local Plum integration tests.
- Support HTTP/2 cleartext for local Connect clients and ordinary HTTP/1.1 for
  curl-compatible inspection where practical.

### 4. Caching And Revision Checks

- Add an in-memory `BundleStore`.
- Short-circuit `GetBundle` when `if_revision` matches the provider revision.
- Cache successful builds by selection and revision.

### 5. Plum Integration

- Add or update a Plum source client that calls `BundleService.GetBundle`.
- Confirm Plum can fetch, open, publish, and hot reload bundles produced by
  orange.

## Open Questions

- Should `not_modified` be represented as a normal response field, a Connect
  error code, or an HTTP cache status exposed through headers?
- Does the first Plum source need server streaming, or is polling enough until
  production change notifications exist?
- Which metadata should be standardized in the RPC response versus left to
  embedding-specific headers?
- Should orange define optional signature/checksum fields before the first
  implementation, or rely on transport security and Cherry manifest validation
  initially?
