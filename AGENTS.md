# Agent Instructions For Orange

This repository is the Cherry bundle producer used by control planes to serve
snapshots to Plum. Read `DESIGN.md` before making architectural changes.
`IMPLEMENTATION.md` is an execution prompt pack; it is not the durable
architecture record. If an implementation decision should survive the task,
promote it into `DESIGN.md` in the same change.

## Core Invariants

- Orange is producer/control-plane side only. It turns domain config into
  normalized `cherry.Input`, packs Cherry bundles, wraps them in snapshot
  payloads, and exposes SnapshotService handlers for an embedder-owned server.
- Plum is the data plane. Do not add request matching, upstream picking,
  provider auth injection, Envoy bootstrap, or Plum generation publication to
  Orange.
- Orange has no universal tenancy model. Tenant, user, workspace, partition,
  key, and policy ownership belongs to the embedding control plane.
- Snapshot lane selection comes from authenticated client identity:
  principal -> mapped-split lane. Do not add lane selection to request messages
  unless `DESIGN.md` changes first.
- Published snapshots are immutable. Mutation/publish paths must not expose
  partially built snapshots, and failed mutations must leave the previous
  snapshot active.
- Initial data-plane delivery is polling via
  `SnapshotService.FetchMappedSplitMap` and
  `SnapshotService.FetchMappedSplitBundle` with `last_version` and
  `last_checksum`. Do not add server streaming as part of initial
  implementation work.

## Cherry, Plum, And API Boundaries

- Cherry owns the bundle format and validation. Orange should call Cherry APIs
  such as `BuildWithManifest`, `NewBundle`, and `EncodeBundleZstd`; it should
  not reimplement Cherry packing.
- Plum consumes the typed mapped-split map and component bundle bytes from
  `SnapshotService`. Orange should not require Plum to understand
  Orange-specific source or tenancy models.
- `SnapshotEnvelope.payload` should contain a `ConfigPayload` wrapper with
  bundle bytes and operational metadata. Keep metadata diagnostic and
  delivery-oriented.
- Public proto fields should use neutral payload and format names such as
  `format`, `payload`, and `payload_sha256`. Do not leak Cherry-specific names
  into the wire API; Cherry is Orange's internal bundle implementation.
- Use the existing protobuf services:
  - `orange.config.v1.SnapshotService.FetchMappedSplitMap`
  - `orange.config.v1.SnapshotService.FetchMappedSplitBundle`
  Do not introduce parallel `BundleService`, `GetBundle`,
  `SnapshotAdminService`, `ConfigAdminService`, or `MutateSnapshot` APIs.
- Remote admin publication is intentionally deferred. Keep mapped-split
  publication in-process or embedder-owned until the production mutation
  contract is clear.
- Do not add an Orange-owned standalone server helper or recommend running
  Orange in a separate server goroutine. Production embedders should attach
  `config.Server` or `config.SnapshotService` to their own mux/router.

## Secret Handling

- Secret handling is reference-only. Cherry input may carry refs such as
  `env://...`, `file://...`, `literal://...`, `orange://...`, `vault://...`, or
  `sm://...`.
- Orange must not resolve provider or MCP credentials to bytes.
- No secret bytes, auth headers, user key material, full tenant records, or
  normalized config dumps belong in snapshots, metadata, logs, metrics, traces,
  fixtures, or tests.
- Tests that use secret refs should assert refs pass through unchanged and are
  not printed as resolved values.

## Protobuf And Generated Code

- Edit `.proto` files first, then regenerate checked-in Go, Connect, and
  vtprotobuf outputs using the repository's existing buf setup.
- Keep auth annotations in proto definitions unless the task explicitly changes
  the auth model.
- Add validation in proto only when it is stable and domain-neutral. Avoid
  encoding embedder-specific tenancy or policy rules in shared protos.
- Generated file churn should be limited to the proto change being made.

## Testing Expectations

- Prefer `github.com/stretchr/testify` for new Go tests. Use `require` for
  setup and fatal assertions, `assert` for non-fatal comparisons, and suites
  only when they reduce real repetition.
- Every implementation task in `IMPLEMENTATION.md` must include tests unless
  the prompt explicitly says generated-code compilation is sufficient.
- Run focused package tests while iterating. Run `go test ./...` before
  reporting any implementation task complete.
- Run `go test ./... -race` for changes touching snapshot publication,
  goroutines, caches/stores, handler attachment, auth/lane resolution, or
  concurrent fetch/publish behavior.
- Connect service changes need handler-level tests with `httptest.Server`,
  `bufconn`, or an embedder-owned mux mounting `SnapshotService`. Cover success
  and error mapping.
- Snapshot manager changes need concurrency tests proving:
  - fetch during publish never observes a partial snapshot
  - failed publish keeps the previous snapshot active
  - caller mutation cannot change published snapshots
  - lanes do not cross-contaminate
- Producer/builder changes need compatibility tests that build a Cherry bundle
  and open it with Cherry.

## E2E Requirements

- Once the public fetch/publish path exists, e2e coverage must exercise the full
  Orange path:

  ```text
  prepared component input
    -> cherry.Input
    -> Cherry bundle
    -> ConfigPayload
    -> SnapshotEnvelope component resource
    -> typed MappedSplitSnapshot
    -> SnapshotService.FetchMappedSplitMap
    -> SnapshotService.FetchMappedSplitBundle
    -> decode ConfigPayload
    -> cherry.OpenBundleZstd
  ```

- E2E tests should use real generated Connect clients/handlers, not direct
  method calls, for at least one publish/fetch round trip.
- E2E tests must assert mapped-split map and bundle fetches return `Unchanged`
  when `last_version` and `last_checksum` match.
- E2E tests must assert the fetched payload contains a valid Cherry bundle that
  Cherry can open.
- E2E tests must not require Plum to run. Plum integration belongs in a
  separate Plum task; Orange e2e proves Orange serves a Cherry-compatible
  snapshot.
- If an e2e cannot be run because required infrastructure is not implemented
  yet, state that explicitly and do not mark the task done.

## Logging

- Always use `log/slog` (Go 1.21+ structured logger). Do not use `log`, `fmt.Print*`,
  or third-party logging packages.
- Create loggers explicitly (`slog.New(...)`) rather than relying on the global
  default so callers control handler configuration.
- When a function needs to emit log lines, accept the logger as a parameter.
  Follow the ordering convention `(ctx context.Context, logger *slog.Logger, …other params…)`.
  For example: `func A(ctx context.Context, logger *slog.Logger, otherParams ...)`.
- Log structured key/value pairs (`logger.Info("msg", "key", val)`) rather than
  interpolated strings so log lines are machine-parseable.
- Never log secret bytes, auth headers, resolved credential values, or full
  tenant/provider records.

## Editing Guidance

- Keep changes scoped to the requested implementation slice.
- Prefer existing package and generated-code patterns over new abstractions.
- Do not rewrite unrelated dirty files or revert user changes.
- Update `DESIGN.md` for durable architecture decisions.
- Update `IMPLEMENTATION.md` for task status, disposable prompts, and execution
  sequencing.
- Update user-facing README-style docs only when behavior is available to run.

## Local Agent Skill

Use `.agents/skills/integrate-orange` when asked to connect Orange to producer
control-plane code, snapshot servers, mapped-split consumers, lane resolution,
or mapped-split delivery. That skill describes how to keep embedding
source/tenancy logic outside Orange while producing typed maps and
`ConfigPayload` component snapshots safely.
