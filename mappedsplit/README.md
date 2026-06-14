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
	func(ctx context.Context, ref mappedsplit.BundleRef) ([]byte, bool, error) {
		result, err := fetchBundleResource(ctx, ref.Resource)
		if err != nil {
			return nil, false, err
		}
		return result.BundleZstd, result.Unchanged, nil
	})
```

`Open` diffs refs against the current opened view, reuses matching readers,
fetches only missing or stale component resources, and omits partitions that no
longer appear in the map.

## Runtime Queries

Use the opened mapped view for diagnostics or simple consumers:

```go
llm, ok := opened.ResolveLLM("prod", "slug:alice", "gpt-4o-mini")
mcp, ok := opened.ResolveMCP("prod", "profile-dev-tools")
```

High-throughput data planes can use the opened components directly and keep any
hot caches outside Cherry readers, clearing those caches on generation/view
swap.
