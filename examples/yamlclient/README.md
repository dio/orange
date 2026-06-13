# yamlclient

A development-only Orange client example. It polls a running yamlserver for
configuration snapshots, opens each received Cherry bundle, and exposes a small
HTTP inspection API so you can observe the currently active config while the
server is running.

## Run

Start yamlserver first (terminal 1):

```sh
go run examples/yamlserver/main.go \
  --config examples/yamlserver/testdata/example.yaml
```

Then start yamlclient (terminal 2):

```sh
go run examples/yamlclient/main.go
```

To run an interactive REPL over the downloaded client-side bundle:

```sh
go run examples/yamlclient/main.go --repl
```

Example REPL commands:

```text
summary
llm slug:alice gpt-4o-mini
mcp call github github__list_repos
reload
quit
```

Optional flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--server` | `http://127.0.0.1:8080` | Orange server base URL |
| `--addr` | `127.0.0.1:8081` | Inspection HTTP server listen address |
| `--interval` | `2s` | Snapshot poll interval |
| `--repl` | `false` | Run an interactive REPL instead of the inspection HTTP server |

## Inspection endpoints

### `GET /healthz`

Returns `200 OK`. Useful for readiness probes.

### `GET /snapshot`

Returns `503` until the first snapshot is received, then `200` with a JSON body
describing the currently active bundle:

```json
{
  "version": 1,
  "checksum": "9c76fca9bc519d922d546dbaefcc4c35ad6d0b75b97303872bb4343aaf9ffe3a",
  "fetched_at": "2026-06-13T10:27:54.716823Z",
  "metadata": {
    "producer": "yamlserver",
    "source_revision": "2130f9e871c8e3647a62493ad13f290424960047c5f9bf73324d34bb9e365671",
    "lane": "default",
    "scopes": ["prod"],
    "created_at": "2026-06-13T10:27:51.539735Z",
    "payload_size": 787
  },
  "bundle": {
    "format_version": "pack-v1",
    "scopes": ["prod"],
    "pack": {
      "size_bytes": 1132
    }
  }
}
```

Field reference:

| Field | Source | Description |
|-------|--------|-------------|
| `version` | `SnapshotEnvelope.version` | Monotonic counter incremented by orange on each publish |
| `checksum` | `SnapshotEnvelope.checksum` | Hex SHA-256 of the raw `ConfigPayload` bytes |
| `fetched_at` | client | When this client last received a *changed* snapshot |
| `metadata.producer` | `SnapshotMetadata.producer` | Value set by the producer (e.g. `"yamlserver"`) |
| `metadata.source_revision` | `SnapshotMetadata.source_revision` | SHA-256 of the YAML file content at build time |
| `metadata.lane` | `SnapshotMetadata.lane` | Orange lane this snapshot was published to |
| `metadata.scopes` | `SnapshotMetadata.scopes` | Cherry scope IDs declared in the YAML file |
| `metadata.created_at` | `SnapshotMetadata.created_at` | When the producer built this snapshot |
| `metadata.payload_size` | `SnapshotMetadata.payload_size` | Bytes in the compressed bundle |
| `bundle.format_version` | `cherry.BundleMetadata.FormatVersion` | Cherry bundle format string |
| `bundle.scopes` | `cherry.BundleMetadata.Scopes` | Scope IDs actually packed into the bundle |
| `bundle.pack.size_bytes` | `cherry.Manifest.SizeBytes` | Uncompressed pack blob size |

### `GET /repl?cmd=...&scope=...`

Runs one Cherry REPL command against the bundle downloaded and opened by
yamlclient. `scope` is the active Cherry enforcement scope inside the current
snapshot. The Orange lane is read from snapshot metadata and echoed in the
response.

```sh
curl 'http://127.0.0.1:8081/repl?cmd=summary'
curl 'http://127.0.0.1:8081/repl?scope=prod&cmd=llm%20slug:alice%20gpt-4o-mini'
curl 'http://127.0.0.1:8081/repl?scope=prod&cmd=mcp%20call%20github%20github__list_repos'
```

`POST /repl` accepts the same request as JSON:

```sh
curl -s http://127.0.0.1:8081/repl \
  -H 'content-type: application/json' \
  -d '{"scope":"prod","line":"llm slug:alice gpt-4o-mini"}'
```

### `GET /server-repl?cmd=...&scope=...`

Proxies the same stateless REPL request to yamlserver's `/debug/repl` endpoint.
This demonstrates the server-side debugging shape where the bundle remains on
the server and the server chooses the Orange lane before opening the current
snapshot.

```sh
curl 'http://127.0.0.1:8081/server-repl?scope=prod&cmd=summary'
```

## What this demonstrates

### Lane and scope in the REPL

The REPL absorbs both terms explicitly:

- Orange **lane** selects the snapshot stream. yamlclient gets it from fetched
  snapshot metadata.
- Cherry **scope** selects the enforcement boundary inside that snapshot. Pass it
  as `scope=prod` to HTTP endpoints or run `use prod` in `--repl` mode. When the
  bundle contains one scope, yamlclient selects it automatically.

In `--repl` mode, `reload` swaps the prompt to the latest snapshot already
downloaded by the background poller.

### Polling with `last_version` / `last_checksum`

Each fetch sends the version and checksum from the previous response. Orange
returns `Unchanged` when they match the current snapshot — no retransmission of
bundle bytes. yamlclient logs `snapshot unchanged` at debug level on each
`Unchanged` response and only updates its state on a real change.

```
yamlclient ──FetchRequest{last_version:1, last_checksum:…}──▶ orange
           ◀──────────────────────── Unchanged ──────────────
```

### Atomic snapshot visibility

Editing `examples/yamlserver/testdata/example.yaml` while both processes are
running triggers a rebuild in yamlserver. Within one poll interval, yamlclient
receives the new version and logs the change:

```
level=INFO msg="new snapshot" version=2 source_revision=… lane=default scopes=[prod]
```

The transition is atomic from yamlclient's perspective: it never observes a
partial build.

### Bundle validation

The client validates the `SnapshotEnvelope.checksum` against the raw
`ConfigPayload` bytes, and the `ConfigPayload.metadata.payload_sha256` against
the bundle bytes, before accepting any snapshot. `cherry.OpenBundleZstd`
additionally validates the Cherry pack manifest before the bundle is considered
open. An invalid snapshot is logged and discarded; the previous valid snapshot
remains active.

### Singleflight coalescing

Concurrent `client.Fetch` calls within one in-flight RPC share a single
singleflight group. In this single-goroutine poller the effect is invisible, but
it matters in production data planes where multiple request-handling goroutines
call `Fetch` concurrently.

## How it fits with yamlserver

```
examples/yamlserver/main.go          examples/yamlclient/main.go
  │                                     │
  │  fsnotify detects YAML change       │  poll every 2s
  │  MutationCallback re-reads file      │  client.Fetch(ctx)
  │  snapshot.Manager.Publish(…)        │    sends last_version + last_checksum
  │  SnapshotEnvelope stored in lane    │    receives Unchanged or new envelope
  │                                     │  cherry.OpenBundleZstd(bundleZstd)
  │  ◀── SnapshotService.Fetch ─────────┤  atomic.Pointer[snapshotView].Store
  │                                     │
  │                               GET /snapshot
  │                                     │
  │                              JSON response
```
