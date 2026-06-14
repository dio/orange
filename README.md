# Orange

Orange is a Go library for **producer-side delivery of Cherry mapped-split configuration** to Plum data planes. It packs normalized component inputs into immutable, versioned, checksum-validated snapshot bundles and serves them over a [ConnectRPC](https://connectrpc.com) `SnapshotService` that data planes poll.

## Overview

```
Control Plane (embedder)
  └── config.Server
        ├── mappedsplit.Builder   → Cherry bundles (zstd)
        ├── config.Store          → MemoryStore | PgStore
        └── SnapshotService       → HTTP/gRPC (ConnectRPC)
                                        │
                                        ▼
                                  Plum Data Plane
                                    config.Client
                                      └── mappedsplit.Open → ResolveLLM / ResolveMCP
```

Orange handles:
- Packing `cherry.Input` component layers into Cherry bundles
- Wrapping bundles in protobuf `SnapshotEnvelope` / `ConfigPayload` payloads
- Publishing a typed state-of-the-world mapped-split map and per-component bundles
- Durable coordination via Postgres + PgQue for multi-replica control planes

Orange does **not** own request routing, upstream selection, credential resolution, or tenant/key storage — those belong to the embedding control plane or Plum.

## Packages

| Package | Purpose |
|---|---|
| `config` | High-level facade: `Server`, `Client`, `Store`, `PgStore`, `PgQueScheduler` |
| `mappedsplit` | Core typed map construction (`Builder`) and opened-view management (`Open`, `Opened`) |
| `producer` | Lower-level single-component Cherry bundle builder |
| `snapshot` | Immutable `SnapshotEnvelope` assembly |
| `api/orange/config/v1` | Protobuf service and message definitions |
| `config/pgque` | PgQue schema setup for async build scheduling |
| `config/postgres/migration` | Embedded-SQL DDL migrations for the five Orange store tables |
| `internal/embeddedpg` | Embedded Postgres wrapper for tests and local development |

## Usage

### Producer (control plane)

```go
import "github.com/dio/orange/config"

store := config.NewMemoryStore() // or config.NewPgStore(pool, opts)

server := config.NewServer(config.ServerOptions{
    Producer:      "my-control-plane/1.0.0",
    Authenticator: auth,   // config.Authenticator
    LaneResolver:  lanes,  // config.LaneResolver
    Store:         store,
})

mux := http.NewServeMux()
server.Mount(mux)

_, err := server.PublishMappedSplit(ctx, config.MappedSplitRequest{
    Selection: config.Selection{ScopeKind: "workspace", ScopeID: "prod"},
    Lane:      "lane-a",
    Spec: config.MappedSplitSpec{
        LLMUserKeyPartitions:      64,
        MCPUserProfilePartitions:  64,
    },
    Components: components, // []config.ComponentInput
})
```

### Consumer (data plane / Plum)

```go
import "github.com/dio/orange/config"

client, err := config.NewClient(config.ClientOptions{
    BaseURL: "http://control-plane:8090",
})

result, err := client.Sync(ctx)

// Runtime resolution
entry, err := result.Opened.ResolveLLM("prod", "slug:alice", "gpt-4o-mini")
entry, err := result.Opened.ResolveMCP("prod", "s/my-mcp-server")
```

### Durable store with PgQue scheduling

```go
pool, _ := pgxpool.New(ctx, dsn)

store, _ := config.NewPgStore(ctx, pool, config.PgStoreOptions{
    Producer: "my-control-plane/1.0.0",
})

scheduler, _ := config.NewPgQueScheduler(ctx, pool, store, config.PgQueSchedulerOptions{
    BuildFunc: myBuildFunc,
})
scheduler.Start(ctx)
```

See [`mappedsplit/README.md`](mappedsplit/README.md) for the full producer and consumer API reference.

## Example

A fully runnable server + REPL client demo lives in [`examples/mappedsplit/`](examples/mappedsplit/).

```sh
# In-memory store
go run ./examples/mappedsplit server --addr 127.0.0.1:8090 --partitions 4

# With embedded Postgres + PgQue
go run ./examples/mappedsplit server --local --addr 127.0.0.1:8090

# Client REPL
go run ./examples/mappedsplit client --server http://127.0.0.1:8090 --interval 2s
```

See [`examples/mappedsplit/README.md`](examples/mappedsplit/README.md) for the full walkthrough including n+1 updates and horizontal scaling patterns.

## Development

```sh
make format   # gofmt + goimports
make lint     # golangci-lint
make test     # go test ./...
```

Protobuf code generation uses [Buf](https://buf.build):

```sh
buf generate --template buf.gen.golang.yaml
```

## Design

See [`DESIGN.md`](DESIGN.md) for the authoritative architecture document covering the API contract, mapped-split production flow, durable store design, auth/lane mapping, and consumer flow.

## License

See [LICENSE](LICENSE).
