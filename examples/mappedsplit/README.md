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

The server builds from watched YAML files in `examples/mappedsplit/data` by
default. Edit `examples/mappedsplit/data/input.yaml` while the server and client
are running; the server polls the directory, publishes a new mapped-split
snapshot, and the client will pick it up on its next sync. Use `--input-dir` to
point at another directory of `.yaml` or `.yml` files.

For a durable local run, add `--local`. The server starts embedded Postgres,
stores data under `.mappedsplit`, applies Orange store migrations, installs
PgQue, constructs `config.PgStore`, and schedules builds through
`config.PgQueScheduler`:

```sh
go run ./examples/mappedsplit server --local --addr 127.0.0.1:8090 --partitions 4
```

Startup prints the Postgres root, DSN, and a ready-to-copy `psql` command:

```text
local postgres root: .mappedsplit
local postgres dsn: postgres://orange:orange@127.0.0.1:5433/orange?sslmode=disable
psql: psql "postgres://orange:orange@127.0.0.1:5433/orange?sslmode=disable"
```

Run the client in another:

```sh
go run ./examples/mappedsplit client --server http://127.0.0.1:8090 --interval 2s
```

The default client mode is an interactive REPL over the active mapped-split
view. It fetches the typed map, opens changed component bundles, and then lets
you run Cherry diagnostic commands against the composed local view:

```text
orange> summary
orange> llm prod slug:alice gpt-4o-mini
orange> mcp list prod profile-dev-tools
orange> inspect principals
orange> sync
orange> quit
```

Press `Tab` in the REPL to complete commands, scopes, principals, models,
providers, MCP paths, and MCP tool names from the active mapped-split view.

The REPL keeps polling in the background. When the server publishes a newer map,
the client prints a notification such as:

```text
notification: mapped split changed lane=lane-a map_version=2 checksum=... generation=gen-demo revision=2 fetched=2 reused=8 omitted=0
```

Use `client apply` for the older non-interactive polling output:

```sh
go run ./examples/mappedsplit client apply --server http://127.0.0.1:8090 --interval 2s
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

This writes an override YAML file under the watched input directory and then
publishes or schedules the rebuild.

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

Horizontal scaling story:

- Without `--local` or `--postgres-dsn`, the example uses the default in-memory
  store and is single-process only.
- `--local` is for one development process that owns the embedded Postgres data
  directory under `.mappedsplit`.
- Additional local server processes can connect to the first process's printed
  DSN with `--postgres-dsn` and a different listen address:

  ```sh
  go run ./examples/mappedsplit server \
    --postgres-dsn 'postgres://orange:orange@127.0.0.1:5433/orange?sslmode=disable' \
    --addr 127.0.0.1:8091 \
    --worker-id replica-b
  ```

- In production-style multi-replica deployments, every server points at the
  same external Postgres DSN with `--postgres-dsn`. Each process constructs its
  own `PgStore` and PgQue worker, but current maps, component bundles, dirty
  rows, and build leases live in Postgres. Duplicate schedules and duplicate
  queue delivery are safe because PgQue is only a signal layer and PgStore owns
  the per-lane lease and fencing token.

The client always fetches the typed map first. Component resources are
discovered from that SoTW map and are fetched only when missing or stale
compared with the active opened view.

The output shows the map snapshot version and component application stats:

```text
map version=1 checksum=... unchanged=false generation=gen-demo revision=1 fetched=10 reused=0 omitted=0
map version=... checksum=... unchanged=false generation=gen-demo revision=2 fetched=2 reused=8 omitted=0
```

The n+1 update changes Alice's LLM user-key partition and removes the
`profile-dev-tools` profile from its MCP profile partition. Unchanged component
resources keep their existing snapshot versions, so the client fetches only
stale components and reuses unchanged opened Cherry readers.
