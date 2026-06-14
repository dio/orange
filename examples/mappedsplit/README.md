# mappedsplit

`examples/mappedsplit` is a development-only producer and consumer for Cherry
mapped-split snapshots.

The server exposes only the mapped-split SnapshotService API:

- `FetchMappedSplitMap` returns the typed SoTW map for the authenticated lane.
- `FetchMappedSplitBundle` returns one component bundle resource named by that
  map.

Run the server in one terminal:

```sh
go run ./examples/mappedsplit server --addr 127.0.0.1:8090 --partitions 4
```

Run the client in another:

```sh
go run ./examples/mappedsplit client --server http://127.0.0.1:8090 --interval 2s
```

Walk the timeline one step at a time:

```sh
# T1: fetch only the lane's typed map and write an inspection JSON copy.
go run ./examples/mappedsplit client fetch-map \
  --server http://127.0.0.1:8090 \
  --lane lane-a \
  --out /tmp/lane-a-map.json

# Inspect resources from the map, then fetch one component resource.
go run ./examples/mappedsplit client fetch-bundle \
  --server http://127.0.0.1:8090 \
  --lane lane-a \
  --resource llm-user-key-003 \
  --out /tmp/llm-user-key-003.cherry.zst

# Composed-view path: fetch typed map, fetch all missing/stale component
# resources, open bundles, and print query results once.
go run ./examples/mappedsplit client apply \
  --server http://127.0.0.1:8090 \
  --lane lane-a \
  --once
```

Trigger the n+1 update from a third terminal:

```sh
curl -XPOST http://127.0.0.1:8090/debug/nplus1
```

Or let the client trigger it once:

```sh
go run ./examples/mappedsplit client --server http://127.0.0.1:8090 --trigger-update
```

The server accepts `x-orange-lane` as a local development lane identity.
Production embedders should derive lanes from authenticated principal identity
using Orange's server auth and lane resolver hooks.

The example owns the HTTP server, health route, and `/debug/nplus1` business
route, but it still mounts Orange's reusable `config.Server`. That is the
intended production shape: attach the SnapshotService to your control plane
rather than forcing your control plane into an Orange-owned server process.
Multi-replica deployments should inject a durable `config.Store` instead of the
default in-memory store.

The client always fetches the typed map first. Component resources are
discovered from that SoTW map and are fetched only when missing or stale
compared with the active opened view.

The output shows the map snapshot version and component application stats:

```text
map version=1 checksum=... unchanged=false generation=gen-demo revision=1 fetched=10 reused=0 omitted=0
map version=... checksum=... unchanged=false generation=gen-demo revision=2 fetched=1 reused=8 omitted=1
```

The n+1 update changes Alice's LLM user-key partition and removes the MCP
profile partition containing `profile-dev-tools`. Unchanged component resources
keep their existing snapshot versions, so the client fetches only the stale
component, reuses unchanged opened Cherry readers, and drops the removed
partition from the active view.
