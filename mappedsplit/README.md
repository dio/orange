# Orange Mapped Split

Package `mappedsplit` is Orange's first-class support for Cherry mapped-split
delivery.

The public data-plane API is mapped-split only:

- `SnapshotService.FetchMappedSplitMap` returns a typed SoTW split map for the
  authenticated lane.
- `SnapshotService.FetchMappedSplitBundle` returns one Cherry component bundle
  resource referenced by that map.

## Producer Shape

The embedding control plane owns source loading, tenancy checks, and
domain-specific slicing. It passes already-normalized `cherry.Input` layers to
`mappedsplit.Builder`:

```go
spec := cherry.MappedSplitSpec{
	LLMUserKeyPartitions:     64,
	MCPUserProfilePartitions: 64,
}

out, err := mappedsplit.NewBuilder(mappedsplit.BuildOptions{
	Producer: "orange-prod/1.2.3",
}).Build(ctx, mappedsplit.BuildRequest{
	Selection:               producer.Selection{ScopeKind: "workspace", ScopeID: "prod"},
	Scopes:                  []string{"prod"},
	SourceRevision:          "db-tx-123",
	GenerationID:            "gen-2026-06-14",
	MapRevision:             7,
	LLMDefaultPrincipalSlug: "slug:default",
	Spec:                    spec,
	Components:              componentInputs,
})
```

Publish every `out.Components[name].Payload` on
`out.Components[name].Ref.Resource`, then publish the typed map produced from
`out.Map` through `FetchMappedSplitMap`. The map should become visible only
after every referenced component bundle is readable.

The map's `partitioning` block tells consumers how producer and consumer agree
on partition assignment:

```json
{
  "llm-user-key": {
    "algorithm": "fnv1a64",
    "key": "principal_slug",
    "partitions": 64
  },
  "mcp-user-profile": {
    "algorithm": "fnv1a64",
    "key": "path_suffix",
    "partitions": 64
  }
}
```

For LLM per-key routes, the partition is
`fnv1a64(principal_slug) % partitions`. For MCP profile routes, the partition
is `fnv1a64(path_suffix) % partitions`. Consumers should use
`cherry.MappedSplitSpec` to compute this instead of copying hash math.

## Durable Postgres Store

Multi-replica embedders should apply Orange's store migrations explicitly,
construct `config.PgStore`, and inject it into `config.Server`. Orange still
does not own the HTTP process; the embedder mounts the generated
`SnapshotService` handler on its own mux.

```go
pool, err := pgxpool.New(ctx, dsn)
if err != nil {
	return err
}
defer pool.Close()

if err := migration.Migrate(ctx, pool); err != nil {
	return err
}

store, err := config.NewPgStore(
	pool,
	config.WithPgStoreBuildLeaseHolderID(replicaID),
)
if err != nil {
	return err
}

snapshotServer := config.NewServer(config.ServerOptions{
	Producer:      "control-plane/1.2.3",
	Authenticator: auth,
	LaneResolver:  lanes,
	Store:         store,
	OnDemandBuild: buildMissingLane,
})

mux := http.NewServeMux()
snapshotServer.Mount(mux)
mux.HandleFunc("/healthz", healthz)
```

Use `migration.WithSchema("orange")` and `config.WithPgStoreSchema("orange")`
when the store tables live outside the default search path. `PgStore` stores
current typed maps, immutable component resources, dirty build requests, and
per-lane build leases in Postgres, so any replica can publish or serve fetches.
Matching `last_version` and `last_checksum` values return `Unchanged` for both
map and bundle fetches.

Optional PgQue scheduling is a signal layer over the same store state:

```go
if err := pgque.Setup(ctx, pool, pgque.WithConsumer("orange_builder")); err != nil {
	return err
}

scheduler, err := config.NewPgQueScheduler(store, buildDirtyLane)
if err != nil {
	return err
}

if err := scheduler.ScheduleBuild(ctx, config.BuildRequest{
	Lane:           lane,
	RequestedBy:    replicaID,
	SourceRevision: sourceRevision,
	ChangeHint:     "catalog update",
}); err != nil {
	return err
}
```

Each worker replica may run `scheduler.Run(ctx)` from embedder-owned process
supervision. PgQue carries duplicate-tolerant wake-up events only; `PgStore`
remains authoritative for dirty rows, leases, current maps, map revisions, and
component resources.

## Consumer Shape

The enforcement point always fetches the authenticated lane's typed map first.
The map is the SoTW discovery document for component resources, stable IDs,
checksums, sizes, generation, and removed partitions. Do not prefetch component
resources from a static component list.

Payload shape:

```text
SnapshotService.FetchMappedSplitMap
  -> FetchMappedSplitMapResponse.snapshot
    -> MappedSplitSnapshot.map
      -> typed MappedSplitMap

SnapshotService.FetchMappedSplitBundle(resource)
  -> FetchMappedSplitBundleResponse.snapshot
    -> SnapshotEnvelope.payload
      -> marshaled ConfigPayload proto
        -> ConfigPayload.format.media_type = application/vnd.dio.orange.cherry-bundle
        -> ConfigPayload.payload           = Cherry zstd bundle bytes
```

```go
splitMap, err := mappedsplit.DecodeMapSnapshot(mapResult.Snapshot)
if err != nil {
	return err
}

next, stats, err := mappedsplit.Open(ctx, current, splitMap,
	func(ctx context.Context, ref mappedsplit.BundleRef) (mappedsplit.ComponentPayload, bool, error) {
		result, err := fetchBundleResource(ctx, ref.Resource)
		if err != nil {
			return mappedsplit.ComponentPayload{}, false, err
		}
		return mappedsplit.ComponentPayload{
			BundleZstd:     result.BundleZstd,
			SourceRevision: result.Payload.GetMetadata().GetSourceRevision(),
		}, result.Unchanged, nil
	})
```

`Open` diffs refs against the current opened view, reuses matching readers,
fetches only missing or stale component resources, and omits partitions that no
longer appear in the map. Matching refs are reused across map generation
changes, so unchanged components are not re-fetched or re-opened solely because
the producer assigned a new map generation label.

## Runtime Queries

Use the opened mapped view for diagnostics or simple consumers:

```go
llm, ok := opened.ResolveLLM("prod", "slug:alice", "gpt-4o-mini")
mcp, ok := opened.ResolveMCP("prod", "profile-dev-tools")
```

High-throughput data planes can use the opened components directly and keep any
hot caches outside Cherry readers, clearing those caches on generation/view
swap.
