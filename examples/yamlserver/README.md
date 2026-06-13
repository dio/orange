# yamlserver

A development-only orange embedding example. It watches a YAML config file,
rebuilds a Cherry bundle on change, and serves snapshots over Connect HTTP/2.

> **Warning:** `devAuthenticator` and `singleLaneResolver` in `main.go` bypass
> all credential checks. Binds to loopback only (default `127.0.0.1:8080`).
> Do not expose to untrusted networks.

## Run

```sh
go run examples/yamlserver/main.go \
  --config examples/yamlserver/testdata/example.yaml
```

Optional flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | *(required)* | Path to YAML config file |
| `--addr` | `127.0.0.1:8080` | Listen address |

---

## Glossary

Every noun used in Orange and this example, in one place.

| Noun | Where it lives | What it is |
|------|---------------|------------|
| **Credential** | request header / transport | Raw auth material arriving with the request: JWT, mTLS cert, API key, session token. Orange hands it to the embedder's `Authenticator` without inspecting it. |
| **Principal** | `server.Principal` | Verified caller identity produced by `Authenticator`. Carries an opaque `ID` and optional `Scopes`. Typically a workload, service account, or Plum instance — not an end user. |
| **Partition** | embedder's `LaneResolver` | Embedder-owned grouping that maps a principal to a lane. May represent a tenant, workspace, project, region, or cell. Orange never sees partition names; they exist only inside `ResolveLane`. |
| **Lane** | `snapshot.Manager` | Orange's internal storage key for one current snapshot stream. `Manager.Fetch(lane, …)` and `Manager.Publish(req.Lane, …)` operate on it. Looks like `"default"` or `"tenant-acme"`. |
| **Snapshot** | `snapshot.Snapshot` | The current published bundle for a lane, plus its version counter and checksum. Immutable once published; a new `Publish` atomically replaces it. |
| **SnapshotEnvelope** | `api/orange/config/v1` proto | Wire representation returned by `SnapshotService.Fetch`. Wraps a serialized `ConfigPayload` with an outer checksum and version. |
| **ConfigPayload** | `api/orange/config/v1` proto | Orange's wrapper around the Cherry bundle bytes, carrying format metadata (`media_type`, `encoding`), SHA-256 checksum, producer name, source revision, lane, and scopes. |
| **Cherry** | `github.com/dio/cherry` | The bundle format library. Orange calls `BuildWithManifest`, `NewBundle`, `EncodeBundleZstd`; it does not reimplement packing. |
| **Cherry bundle** | `cherry.Bundle` | The packed binary artifact that Plum opens at runtime. Contains providers, models, scopes, principals, MCP servers/profiles, and routing trees — all in a compact indexed format. |
| **cherry.Input** | `cherry.Input` | The normalized, in-memory representation of LLM config passed to Cherry for building. Secret values must appear as refs, never as bytes. |
| **BuildResult** | `producer.BuildResult` | What the `MutationCallback` returns: a source revision string, a list of Cherry scope IDs, and a `cherry.Input`. |
| **MutationCallback** | `snapshot.MutationCallback` | The embedder-supplied hook called inside the publication critical section. Reads domain data, returns a `BuildResult`. Failures leave the previous snapshot active. |
| **MutationRequest** | `snapshot.MutationRequest` | Passed to `MutationCallback`. Carries the lane, optional `PreparedData` bytes from admin callers, and optimistic-concurrency guards. |
| **Producer / Builder** | `producer.Builder` | Bridges `BuildResult` to Cherry: calls `BuildWithManifest`, wraps the result in a `ConfigPayload`, computes the SHA-256. |
| **Scope** (Cherry) | `cherry.Scope` | A logical enforcement boundary *inside* a bundle, e.g. `prod` or `dev`. One bundle can contain many scopes. Different from a lane — a lane selects which bundle; a scope selects which part of the bundle Plum enforces for a request. |
| **Provider** | `cherry.Provider` | An upstream LLM provider descriptor: ID, kind, endpoint, `secret_ref`, auth type, path prefix. The `secret_ref` is never resolved by Orange. |
| **Model** | `cherry.Model` | A logical model name mapping to a provider, plus catalog metadata (mode, capabilities, `metadata_json`). |
| **RoutePlan** | `cherry.RoutePlan` | A compiled routing tree node: `target` (single provider/model), `chain` (ordered fallback with retry), or `split` (weighted random). Nested recursively. |
| **Principal** (Cherry) | `cherry.Principal` | A per-scope user entry: a `slug` identifying the end user, `model_routes` or a compatibility `route`, and a `rate` policy. Different from `server.Principal` — this lives *inside* the bundle. |
| **secret\_ref** | string in config | An opaque URI referencing secret material: `env://`, `file://`, `vault://`, `sm://`, `orange://`, etc. Orange copies them verbatim into the bundle; Plum resolves them at request time. |
| **MCP server** | `cherry.MCPServer` | An upstream Model Context Protocol server descriptor: ID, endpoint, `secret_ref`, auth type. |
| **MCP profile** | `cherry.MCPProfile` | A set of tool bindings exposed under a path suffix within a scope. Maps `exposed_name → (server, tool)` with optional per-binding `secret_ref`. |
| **Plum** | data plane (separate repo) | Consumes `SnapshotService.Fetch`, opens the Cherry bundle, and enforces routing and rate limits on LLM requests. Orange has no Plum-specific code. |
| **Orange** | this repo | The control-plane library. Turns domain config into Cherry bundles and serves them over Connect. Ships no universal tenancy model; embedders own auth, partitions, and domain schema. |
| **Embedder** | your control plane | The application that imports Orange as a library, supplies `Authenticator`, `LaneResolver`, and `MutationCallback`, and owns the HTTP server and tenant model. yamlserver is a minimal embedder. |

---

## Core concepts

The glossary above defines each noun. This section walks through the four
fetch-boundary concepts in depth, with code examples from yamlserver.

### Credential

Raw authentication material arriving with the request: a JWT bearer token, an
mTLS client certificate, an API key header, or any other scheme the embedding
application knows about. Orange never inspects credentials directly — it hands
them to the `Authenticator` the embedder provides.

### Principal

The stable, verified caller identity that the `Authenticator` produces after
validating a credential. It carries an opaque `ID` (any string that uniquely
identifies this caller in your system) and a list of authorization `Scopes`.

```go
// orange/server auth.go
type Principal struct {
    ID     string
    Scopes []string
}
```

A principal is usually a workload identity, a service account, or a Plum
instance — not an end user. The end user lives inside the Cherry bundle (as a
`slug` under a scope principal).

**yamlserver example:** `devAuthenticator` returns a hard-coded principal for
every request because there is no real credential to verify:

```go
func (devAuthenticator) Authenticate(_ context.Context, _ http.Header) (server.Principal, error) {
    return server.Principal{ID: "dev", Scopes: []string{"admin", "read"}}, nil
}
```

In production you might verify a JWT and return:

```go
return server.Principal{ID: "iss:my-idp/sub:plum-eu-1", Scopes: []string{"read"}}, nil
```

### Partition

An embedder-owned grouping of published snapshots — think tenant, workspace,
project, environment, or cell. Orange never sees partition names in API calls;
they exist only inside the embedder's `LaneResolver`. The resolver maps a
verified principal to exactly one lane.

Partitions are a design hook, not a type. Your `LaneResolver` can derive the
partition from a database lookup, a principal ID prefix, or a trusted claim in
the token — whatever matches your tenancy model.

### Lane

The key under which `snapshot.Manager` stores one published snapshot stream.
A lane is a string (e.g. `"default"`, `"tenant-acme"`, `"eu-prod"`). Each
lane holds exactly one current snapshot; publishing to a lane atomically
replaces it.

The full request path looks like this:

```
HTTP request with credential
  │
  ▼
Authenticator.Authenticate(header)
  │  returns Principal{ID: "plum-eu-1", Scopes: ["read"]}
  ▼
LaneResolver.ResolveLane(principal)
  │  returns "tenant-acme"   ← your partition lookup happens here
  ▼
Manager.Fetch("tenant-acme", lastVersion, lastChecksum)
  │  returns SnapshotEnvelope or Unchanged
  ▼
SnapshotService.Fetch response
```

**Lane vs Cherry scope:** these are different things.

- A **lane** is an orange storage key. It governs which published snapshot a
  caller receives. There is one snapshot per lane.
- A **Cherry scope** (e.g. `prod`, `dev`, `alice-workspace`) is a logical
  enforcement boundary packed *inside* a bundle. One snapshot can contain
  multiple Cherry scopes for different user groups.

```
lane: "tenant-acme"
  └─ snapshot
       └─ Cherry bundle
            ├─ scope: prod   (principals: alice, bob)
            └─ scope: dev    (principals: dev-bot)
```

`BuildResult.Scopes` lists the Cherry scope IDs in the bundle so metadata is
consistent. yamlserver derives them from the parsed YAML:

```go
scopes := make([]string, len(input.Scopes))
for i, s := range input.Scopes {
    scopes[i] = s.ID   // "prod", "dev", … — not the lane name
}
```

**yamlserver example:** `singleLaneResolver` always returns `"default"` because
there is only one config file and no real tenant lookup:

```go
func (r singleLaneResolver) ResolveLane(_ context.Context, _ server.Principal) (string, error) {
    return r.lane, nil   // always "default"
}
```

In a multi-tenant production embedder:

```go
func (r *tenantLaneResolver) ResolveLane(ctx context.Context, p server.Principal) (string, error) {
    tenant, err := r.db.TenantForPrincipal(ctx, p.ID)
    if err != nil {
        return "", server.ErrPermissionDenied
    }
    return "tenant-" + tenant.ID, nil
}
```

### MutationCallback

The hook that turns your domain data into a Cherry bundle. Orange calls it
inside the publication critical section, so the snapshot transitions atomically:
the previous snapshot stays active if the callback or builder fails.

```go
type MutationCallback func(ctx context.Context, req MutationRequest) (BuildResult, error)
```

yamlserver's callback reads the YAML file inside the callback (not in the
watcher goroutine) so the manager's serialization ensures consistency:

```go
callback := func(ctx context.Context, _ snapshot.MutationRequest) (producer.BuildResult, error) {
    data, _ := os.ReadFile(*configPath)          // re-read on every publish
    input, hash, _ := yamlserver.ParseYAML(data) // normalize to cherry.Input
    scopes := scopeIDs(input)                    // derive from YAML, not from lane
    return producer.BuildResult{
        SourceRevision: hash,
        Scopes:         scopes,
        Input:          input,
    }, nil
}
```

---

## How the pieces fit together

```
YAML file on disk
  │  (read inside MutationCallback)
  ▼
yamlserver.ParseYAML  →  cherry.Input  (secret refs only, never resolved)
  │
  ▼
producer.Builder.Build
  │  cherry.BuildWithManifest
  │  cherry.NewBundle(scopeKind, scopeID, scopes, blob, manifest)
  │  cherry.EncodeBundleZstd
  ▼
snapshot.Manager  (per-lane atomic store)
  │
  ├─ Watcher (fsnotify, 200 ms debounce)  →  Publish on file change
  │
  └─ server.Service
       ├─ SnapshotService.Fetch        ←  Plum data planes poll here
       ├─ ConfigAdminService.Publish   ←  admin callers trigger rebuilds
       └─ /healthz                     ←  embedder-owned mux route
```

---

## Endpoints

| Path | Description |
|------|-------------|
| `/healthz` | Returns 200 OK |
| `/debug/repl` | Development-only stateless Cherry REPL over the current `default` lane snapshot |
| `orange.config.v1.SnapshotService/Fetch` | Polling fetch for Plum data planes |
| `orange.config.admin.v1.ConfigAdminService/PublishSnapshot` | Admin publish trigger |

### Development REPL endpoint

`/debug/repl` runs one Cherry REPL command against the current snapshot in the
example's `default` Orange lane. It is intentionally example-only and protected
only by the development authenticator in `main.go`.

```sh
curl 'http://127.0.0.1:8080/debug/repl?cmd=summary'
curl 'http://127.0.0.1:8080/debug/repl?scope=prod&cmd=llm%20slug:alice%20gpt-4o-mini'
curl 'http://127.0.0.1:8080/debug/repl?scope=prod&cmd=mcp%20call%20github%20github__list_repos'
```

`POST /debug/repl` accepts JSON:

```sh
curl -s http://127.0.0.1:8080/debug/repl \
  -H 'content-type: application/json' \
  -d '{"scope":"prod","line":"inspect principals"}'
```

The endpoint demonstrates how lane and scope compose:

- Orange **lane** selects the current snapshot from `snapshot.Manager`; this
  example always uses lane `default`.
- Cherry **scope** selects the enforcement boundary inside that snapshot; pass it
  as `scope=prod`, or use the returned `scope` field as client-side session
  state.

---

## Config schema

```yaml
providers:
  - id: openai
    kind: openai
    endpoint: https://api.openai.com
    secret_ref: env://OPENAI_API_KEY   # opaque ref; never resolved here
    auth_type: bearer
    path_prefix: /v1

models:
  - id: gpt-4o-mini
    provider: openai
    name: gpt-4o-mini
    mode: chat
    capabilities: [function_calling, tool_choice]
    metadata_json: '{"context_window":128000}'

mcp_servers:
  - id: github
    endpoint: https://mcp.github.example
    secret_ref: sm://github-token
    auth_type: bearer

scopes:
  - id: prod
    principals:
      - slug: "slug:alice"
        # model_routes: explicit per-model route plans (target / chain / split)
        model_routes:
          gpt-4o-mini:
            kind: target
            provider: openai
            model: gpt-4o-mini
            secret_ref: orange://alice/openai
          claude-haiku:
            kind: chain
            retry:
              retry_on: 401,403,5xx
              per_try_timeout_ms: 2500
            children:
              - kind: target
                provider: anthropic
                model: claude-haiku
              - kind: target
                provider: openai
                model: fallback-chat
          fallback-chat:
            kind: split
            split:
              - weight: 80
                plan: {kind: target, provider: openai, model: gpt-4o-mini}
              - weight: 20
                plan: {kind: target, provider: anthropic, model: claude-haiku}
        rate:
          usd_per_day_cents: 1000
          rpm: 60
          on_exceed: reject
      - slug: "slug:bob"
        # route: compatibility single-route shorthand
        route:
          kind: target
          provider: openai
          model: gpt-4o-mini
        rate:
          usd_per_day_cents: 500
          rpm: 30
          on_exceed: queue
    mcp_profiles:
      - path: github
        tools:
          - exposed_name: github__list_repos
            server: github
            tool: list_repos
            secret_ref: sm://github-token
            auth_type: bearer
```

Rules enforced by `ParseYAML`:
- Unknown YAML keys → parse error (no silent ignoring of typos).
- Multi-document YAML (`---` separator) → parse error.
- `secret_ref` values are copied verbatim; they are never resolved to bytes.

---

## Package layout

```
examples/yamlserver/
  main.go          # runnable binary (package main)
  server/          # library imported by main.go (package yamlserver)
    config.go      # ParseYAML — YAML → cherry.Input
    watcher.go     # Watcher — fsnotify + debounce loop
  testdata/
    example.yaml   # kitchen-sink config covering all schema fields
```
