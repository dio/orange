# Implementation Notes

Orange is now mapped-split first.

Current data-plane delivery uses:

- `SnapshotService.FetchMappedSplitMap`
- `SnapshotService.FetchMappedSplitBundle`

The old generic `SnapshotService.Fetch` path and simple YAML server/client
examples have been removed. Durable architecture lives in `DESIGN.md`; practical
integration guidance lives in `.agents/skills/integrate-orange/SKILL.md` and
`mappedsplit/README.md`.

The multi-replica Postgres store, embeddedpg, lease, and optional PgQue worker
implementation prompt pack lives in `POSTGRES_STORE_IMPLEMENTATION.md`.
