---
name: integrate-orange
description: Integrate Orange into producer/control-plane servers or data-plane consumers. Use when serving SnapshotService.FetchMappedSplitMap or FetchMappedSplitBundle, implementing authenticated lane resolution, polling Orange mapped-split snapshots, or producing/consuming Orange mapped-split Cherry bundles.
---

# Integrate Orange

Use this skill when connecting Orange to an embedding control plane, snapshot
server, or data-plane consumer. Orange is the producer/control-plane side for
Cherry mapped-split bundles; Plum or another enforcement point is the data
plane.

## Core Boundary

Orange starts after embedding-owned code has selected and normalized data. Keep
these responsibilities outside Orange packages:

- tenancy joins and ownership checks
- user/key/JWT verification
- project/workspace reachability
- source database schema assumptions
- product policy authoring and rule precedence
- provider or MCP secret material resolution
- request-time routing, upstream picking, or provider auth injection

The producer boundary is:

```text
source records -> embedder transform -> cherry.Input components -> Orange mapped split
```

The consumer boundary is:

```text
FetchMappedSplitMap -> typed SoTW map
FetchMappedSplitBundle(resource) -> ConfigPayload -> Cherry bundle bytes -> Cherry Reader
```

Use `log/slog`, keep secret refs as refs, and never log or store secret bytes.

## Supported Delivery Shape

Orange supports mapped split as the data-plane delivery shape.

- `SnapshotService.FetchMappedSplitMap` returns a typed
  `MappedSplitSnapshot` for the authenticated lane.
- `SnapshotService.FetchMappedSplitBundle` returns one component bundle
  resource referenced by that map.

Do not add lane selection to request messages. Lane selection comes from
authenticated client identity through the server `Authenticator` and
`LaneResolver` hooks. Local examples may use a header only to simulate
identity-to-lane resolution.

Do not add or recommend an Orange-owned standalone server process, server
goroutine, or listener helper. Production embedders attach `config.Server` or
`config.SnapshotService` to their own mux/router and keep their own business
process, storage, rebuild triggers, auth integration, and operational routes.

## Producer Workflow

Use mapped split when user-key LLM routes or MCP profile paths churn more often
than catalogs/defaults.

Build component `cherry.Input` layers outside the `mappedsplit` package, then
wrap them with `mappedsplit.Builder`:

```go
spec := cherry.MappedSplitSpec{
    LLMUserKeyPartitions:     64,
    MCPUserProfilePartitions: 64,
}

out, err := mappedsplit.NewBuilder(mappedsplit.BuildOptions{
    Producer: "orange-prod",
}).Build(ctx, mappedsplit.BuildRequest{
    Selection:               producer.Selection{ScopeKind: "workspace", ScopeID: "prod"},
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

Publish every component bundle resource first:

```go
for _, name := range out.ComponentSeq {
    component := out.Components[name]
    publishResource(lane, component.Ref.Resource, component.Payload, component.BundleZstd)
}
```

Then publish the typed map for the lane:

```go
typedMap, err := mappedsplit.NewMapSnapshot(nextMapVersion, out.Map)
if err != nil {
    return err
}
publishTypedMap(lane, typedMap)
```

The typed map should become visible only after every referenced component
resource is readable.

## Partitioning

The map's `partitioning` block tells consumers how to compute lookup
partitions:

- `llm-user-key`: `fnv1a64(principal_slug) % partitions`
- `mcp-user-profile`: `fnv1a64(path_suffix) % partitions`

Use `cherry.MappedSplitSpec` instead of copying hash math.

Component layers:

- `llm-generic`: providers, models, default/platform LLM route principal, and
  platform secret refs.
- `mcp-servers`: MCP server catalog and direct `s/<server>` paths.
- `llm-user-key-*`: principal-specific routes, BYOK refs, and rate policies for
  one `MappedSplitSpec` partition.
- `mcp-user-profile-*`: profile paths, selected tools, and user/profile secret
  refs for one `MappedSplitSpec` partition.

`llm-generic` and `mcp-servers` are low-churn catalog/default bundles, not
permanent static bundles. Rebuild them whenever their catalogs, defaults, direct
paths, or platform secret refs change.

For N+1 updates:

1. Build changed or new component payloads first.
2. Keep unchanged component resources/snapshots unchanged.
3. Publish a new typed map revision that references changed refs and omits
   removed partitions.
4. Publish the new map only after every referenced component resource is
   readable.

## Consumer Workflow

Use `config.Client` for the standard consumer path. It keeps one typed-map
polling state per authenticated lane and one bundle polling state per component
resource so version/checksum state remains resource-local.

1. Fetch and apply the current mapped split with `config.Client.Sync`.
2. For diagnostics, fetch the typed map with `config.Client.FetchMap`.
3. For diagnostics, fetch one component bundle with `config.Client.FetchBundle`.
4. Publish the returned `*config.Opened` as the next immutable active view.

Example:

```go
result, err := client.Sync(ctx)
if err != nil {
    return err
}
active := result.Opened
```

`config.Client.Sync` uses `mappedsplit.Open` to diff refs against the active view, reuses matching opened
readers, fetches missing or stale component resources, validates fetched
bundles, and drops omitted partition refs.

## Server Integration Checklist

- Use existing protobuf services:
  - `orange.config.v1.SnapshotService.FetchMappedSplitMap`
  - `orange.config.v1.SnapshotService.FetchMappedSplitBundle`
- Prefer embedder-owned process and mux attachment. Mount Orange's
  `config.Server` or `config.SnapshotService` beside the embedder's own health, rebuild, debug,
  and business routes.
- Use `config.NewSnapshotServiceWithProviders` when the embedder owns the map
  and component resource store.
- Use `config.Store` for multi-replica/durable publication. A durable store
  must make the new map visible only after every referenced component resource
  is readable.
- Do not use a standalone Orange server helper; the reusable
  boundary is the SnapshotService handler.
- Authenticate every fetch and derive lane from the verified principal.
- Do not add lane fields to map or bundle requests.
- Return typed maps from `FetchMappedSplitMap`.
- Return component bundles as `SnapshotEnvelope.payload` containing marshaled
  `ConfigPayload`.
- Set `ConfigPayload.format.media_type`, `encoding`, and `format_version`.
- Set diagnostic metadata: producer, source revision, lane, scope kind, scope
  ID, concrete scopes, payload size, payload SHA-256.
- Keep snapshots immutable and swap only complete snapshots.
- Leave failed publish attempts on the previous active map/view.

## Inspection Integration

Use Orange's inspection API only for read-only diagnostics against a running
data plane's already-opened active configuration. It is not a control-plane
fetch path, mutation API, raw bundle export, or source-of-truth query surface.

### Plum / EP Server Side

Plum should expose attach-mode diagnostics by implementing
`config.InspectionService` over its active `*plum.ConfigGeneration` pointer and
mounting it through `config.NewInspector`. Runtime code should not depend
directly on generated Connect handler types.

```go
type generationProvider interface {
    CurrentGeneration() *plum.ConfigGeneration
}

type inspectionService struct {
    generations generationProvider
}

func (s *inspectionService) Status(ctx context.Context, req *config.StatusRequest) (*config.StatusResponse, error) {
    gen := s.generations.CurrentGeneration()
    if gen == nil {
        return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("no active generation"))
    }
    return &config.StatusResponse{
        ProtocolVersion: config.InspectionProtocolVersion,
        Capabilities: []string{
            "status",
            "generation",
            "scopes",
            "providers",
            "models",
            "resolve_llm",
            "resolve_mcp",
            "match_view",
            "pick_view",
            "adapt_view",
        },
        HasGeneration: true,
        Generation: generationInfoFrom(gen),
    }, nil
}

var genProvider generationProvider // supplied by the embedder runtime

inspector, err := config.NewInspector(config.InspectorOptions{
    Service: &inspectionService{generations: genProvider},
    HandlerOptions: []connect.HandlerOption{
        // Add the embedder's admin auth/interceptor here.
    },
})
if err != nil {
    return err
}
mux := http.NewServeMux()
inspector.Mount(mux)
```

Plum implementation notes:

- The runtime/provider should load the current generation pointer once per RPC
  method and pass that captured generation into helper functions such as
  `generationInfoFrom`, `providersFrom`, `modelsFrom`, `resolveLLMFrom`, and
  `matchViewFrom`.
- Wire the inspection handler only when the Plum/EP process explicitly enables
  it. Keep it off by default.
- Mount the handler on an embedder-owned admin mux/listener. Do not reuse
  Orange's mapped-split `SnapshotService` auth/lane resolver for attach mode;
  attach mode inspects the local EP process, not an Orange control plane lane.
- Keep CLI, readline, REPL rendering, and command dispatch out of Plum runtime
  packages. Runtime only serves sanitized typed inspection responses.

### `cmd/inspector attach` Client Side

`cmd/inspector attach --admin <url>` should call the generated inspection
Connect client and adapt those responses into the Cherry REPL backend. The
inspector process owns REPL state such as active scope, history, completion,
timeouts, output formatting, and `sync`/`reload`; the server remains stateless
apart from its active generation.

```go
client := inspectv1connect.NewInspectionServiceClient(httpClient, adminURL)

status, err := client.Status(ctx, connect.NewRequest(&config.StatusRequest{
    ClientProtocolVersion: config.InspectionProtocolVersion,
}))
if err != nil {
    return err
}
if status.Msg.GetProtocolVersion() != config.InspectionProtocolVersion {
    return fmt.Errorf("unsupported inspection protocol version %d", status.Msg.GetProtocolVersion())
}

models, err := client.Models(ctx, connect.NewRequest(&config.ModelsRequest{}))
if err != nil {
    return err
}
for _, model := range models.Msg.GetModels() {
    // Convert to the local Cherry REPL backend model view.
    _ = model.GetId()
}
```

Client-side rules:

- Use `Status` first for protocol and capability checks before enabling richer
  REPL commands.
- Map Cherry REPL backend methods to inspection RPCs: `Scopes`, `Providers`,
  `Models`, `ResolveLLM`, `ResolveMCP`, `MatchView`, `PickView`, and
  `AdaptView`.
- Implement `sync`/`reload` as a fresh `Status`/`Generation` refresh against
  the running EP. Attach mode must not call Orange SnapshotService or any
  upstream control plane.
- Treat Connect `failed_precondition` from the server as "no active
  generation" and surface it clearly to the operator.
- Preserve active scope locally across refreshes when the refreshed generation
  still contains that scope.
- Do not print raw `literal://` values. The server must redact before sending,
  and the inspector should avoid introducing alternate unredacted output paths.

Implementation rules:

- Capture the active generation pointer once at each RPC handler entry and
  answer entirely from that immutable generation.
- Return stable Connect errors for no generation, malformed requests, unknown
  routes, and no matches.
- Populate `ProtocolVersion` with `config.InspectionProtocolVersion` and list
  only capabilities the handler actually implements.
- Redact secret refs before assigning `RedactedSecretRef` or
  `RedactedAuthRef`. Never include resolved secret bytes, request-time auth
  headers, raw `literal://` values, or full tenant/provider records.
- Keep admin authentication and listener ownership in the embedding process.
  Orange provides the reusable handler facade only; it still must not start a
  standalone server process.
- Do not call Orange SnapshotService, source stores, remote control planes, or
  latest component pointers from inspection handlers.

## Consumer Validation Checklist

- Verify typed map checksums through `config.Client`.
- Verify envelope and `ConfigPayload` checksums through
  `config.Client`.
- Check component media type before opening payload bytes.
- Open Cherry bundles with `cherry.OpenBundleZstd`.
- Validate scope kind, selected scope ID, concrete scopes, generation, checksum,
  and size against the trusted typed map when available.
- Treat `Unchanged` as a cached result from the same lane/resource client.
- Publish active views atomically.
- Clear wrapper caches on active view swap.
- Fetch only missing/stale refs and stop serving omitted partition refs.

## References

- `AGENTS.md`: repository invariants and testing expectations.
- `DESIGN.md`: durable Orange architecture and mapped-split API contract.
- `producer/`: `ConfigPayload` bundle builder.
- `snapshot/`: immutable component snapshot assembly and lane/resource manager.
- `config/`: Server, Client, Store, Connect handlers, auth, and lane resolution.
- `mappedsplit/`: first-class mapped-split builder/opener.
- `examples/mappedsplit/`: runnable server/client mapped-split demo.
