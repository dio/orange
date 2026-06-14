package config

import (
	"bytes"
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/mappedsplit"
	"github.com/dio/orange/snapshot"
)

var pgStoreIdentifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// PgStore is a pgxpool-backed mapped-split Store.
type PgStore struct {
	pool   *pgxpool.Pool
	schema string
}

// PgStoreOption configures PgStore.
type PgStoreOption func(*PgStore)

// NewPgStore creates a Postgres-backed Store. It does not run migrations;
// callers must apply config/postgres/migration before constructing the store.
func NewPgStore(pool *pgxpool.Pool, opts ...PgStoreOption) (*PgStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres store pool is required")
	}
	store := &PgStore{pool: pool}
	for _, opt := range opts {
		opt(store)
	}
	if store.schema != "" && !pgStoreIdentifierPattern.MatchString(store.schema) {
		return nil, fmt.Errorf("invalid postgres store schema identifier %q", store.schema)
	}
	return store, nil
}

// WithPgStoreSchema uses schema as the store search path inside transactions.
func WithPgStoreSchema(schema string) PgStoreOption {
	return func(s *PgStore) {
		s.schema = schema
	}
}

// PublishMappedSplit publishes component resources and then advances the lane's
// current typed map in one transaction.
func (s *PgStore) PublishMappedSplit(ctx context.Context, out MappedSplitPublication) (PublishResult, error) {
	if out.Lane == "" {
		return PublishResult{}, fmt.Errorf("map lane is required")
	}

	tx, err := s.beginTx(ctx)
	if err != nil {
		return PublishResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockLane(ctx, tx, "orange:mapped-split-publish", out.Lane); err != nil {
		return PublishResult{}, err
	}

	resources := make(map[string]*configv1.SnapshotEnvelope, len(out.ComponentSeq))
	for _, componentName := range out.ComponentSeq {
		component, ok := out.Components[componentName]
		if !ok {
			return PublishResult{}, fmt.Errorf("publish %s: component output is missing", componentName)
		}
		envelope, err := publishPgResource(ctx, tx, out.Lane, componentName, component)
		if err != nil {
			return PublishResult{}, err
		}
		resources[component.Ref.Resource] = envelope
	}

	mapVersion, err := nextPgMapVersion(ctx, tx, out.Lane)
	if err != nil {
		return PublishResult{}, err
	}
	typedMap, err := mappedsplit.NewMapSnapshot(mapVersion, out.Map)
	if err != nil {
		return PublishResult{}, fmt.Errorf("publish map: %w", err)
	}
	mapPayload, err := proto.Marshal(typedMap)
	if err != nil {
		return PublishResult{}, fmt.Errorf("publish map: marshal typed snapshot: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO orange_mapped_split_maps (lane, map_version, map_checksum, map_payload)
		VALUES ($1, $2, $3, $4)
	`, out.Lane, typedMap.Version, typedMap.Checksum, mapPayload); err != nil {
		return PublishResult{}, fmt.Errorf("publish map: insert version %d: %w", typedMap.Version, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO orange_mapped_split_current (lane, map_version, map_checksum, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (lane) DO UPDATE
		SET map_version = EXCLUDED.map_version,
		    map_checksum = EXCLUDED.map_checksum,
		    updated_at = EXCLUDED.updated_at
	`, out.Lane, typedMap.Version, typedMap.Checksum); err != nil {
		return PublishResult{}, fmt.Errorf("publish map: update current pointer: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return PublishResult{}, fmt.Errorf("publish mapped split: commit: %w", err)
	}

	return PublishResult{
		Lane:      out.Lane,
		Map:       proto.Clone(typedMap).(*configv1.MappedSplitSnapshot),
		Resources: resources,
	}, nil
}

// FetchMappedSplitMap returns the current typed mapped-split map for lane.
func (s *PgStore) FetchMappedSplitMap(ctx context.Context, lane string, lastVersion uint64, lastChecksum []byte) (*configv1.MappedSplitSnapshot, bool, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var version uint64
	var checksum []byte
	var payload []byte
	err = tx.QueryRow(ctx, `
		SELECT m.map_version, m.map_checksum, m.map_payload
		FROM orange_mapped_split_current c
		JOIN orange_mapped_split_maps m
		  ON m.lane = c.lane AND m.map_version = c.map_version
		WHERE c.lane = $1
	`, lane).Scan(&version, &checksum, &payload)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, false, fmt.Errorf("%w: mapped split map lane %q", snapshot.ErrNoSnapshot, lane)
		}
		return nil, false, fmt.Errorf("fetch mapped split map lane %q: %w", lane, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("fetch mapped split map lane %q: commit: %w", lane, err)
	}

	if lastVersion == version && bytes.Equal(lastChecksum, checksum) {
		return nil, true, nil
	}
	var typedMap configv1.MappedSplitSnapshot
	if err := proto.Unmarshal(payload, &typedMap); err != nil {
		return nil, false, fmt.Errorf("fetch mapped split map lane %q: unmarshal typed snapshot: %w", lane, err)
	}
	return proto.Clone(&typedMap).(*configv1.MappedSplitSnapshot), false, nil
}

// FetchResource returns the current component resource for lane.
func (s *PgStore) FetchResource(ctx context.Context, lane string, resource string, lastVersion uint64, lastChecksum []byte) (*configv1.SnapshotEnvelope, bool, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	version, checksum, payload, err := latestPgResource(ctx, tx, lane, resource)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, false, fmt.Errorf("%w: lane %q resource %q", snapshot.ErrNoSnapshot, lane, resource)
		}
		return nil, false, fmt.Errorf("fetch resource lane %q resource %q: %w", lane, resource, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("fetch resource lane %q resource %q: commit: %w", lane, resource, err)
	}

	if lastVersion == version && bytes.Equal(lastChecksum, checksum) {
		return nil, true, nil
	}
	var envelope configv1.SnapshotEnvelope
	if err := proto.Unmarshal(payload, &envelope); err != nil {
		return nil, false, fmt.Errorf("fetch resource lane %q resource %q: unmarshal envelope: %w", lane, resource, err)
	}
	return proto.Clone(&envelope).(*configv1.SnapshotEnvelope), false, nil
}

func (s *PgStore) beginTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres store: begin transaction: %w", err)
	}
	if s.schema != "" {
		if _, err := tx.Exec(ctx, "SELECT set_config('search_path', $1, true)", s.schema+",public"); err != nil {
			_ = tx.Rollback(ctx)
			return nil, fmt.Errorf("postgres store: set search_path: %w", err)
		}
	}
	return tx, nil
}

func lockLane(ctx context.Context, tx pgx.Tx, scope string, lane string) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))", scope, lane); err != nil {
		return fmt.Errorf("lock lane %q: %w", lane, err)
	}
	return nil
}

func publishPgResource(ctx context.Context, tx pgx.Tx, lane string, componentName string, component BuiltComponent) (*configv1.SnapshotEnvelope, error) {
	if component.Ref.Resource == "" {
		return nil, fmt.Errorf("publish %s: resource is required", componentName)
	}

	currentVersion, currentChecksum, currentPayload, err := latestPgResource(ctx, tx, lane, component.Ref.Resource)
	if err != nil && err != pgx.ErrNoRows {
		return nil, fmt.Errorf("publish %s: read current resource: %w", componentName, err)
	}

	nextVersion := currentVersion + 1
	snap, err := snapshot.New(nextVersion, component.Payload, component.BundleZstd)
	if err != nil {
		return nil, fmt.Errorf("publish %s: %w", componentName, err)
	}
	if currentVersion > 0 && bytes.Equal(currentChecksum, snap.Envelope.Checksum) {
		var envelope configv1.SnapshotEnvelope
		if err := proto.Unmarshal(currentPayload, &envelope); err != nil {
			return nil, fmt.Errorf("publish %s: unmarshal current resource envelope: %w", componentName, err)
		}
		return proto.Clone(&envelope).(*configv1.SnapshotEnvelope), nil
	}

	envelopePayload, err := proto.Marshal(snap.Envelope)
	if err != nil {
		return nil, fmt.Errorf("publish %s: marshal envelope: %w", componentName, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO orange_mapped_split_resources (lane, resource, version, checksum, envelope_payload)
		VALUES ($1, $2, $3, $4, $5)
	`, lane, component.Ref.Resource, snap.Envelope.Version, snap.Envelope.Checksum, envelopePayload); err != nil {
		return nil, fmt.Errorf("publish %s: insert resource version %d: %w", componentName, snap.Envelope.Version, err)
	}
	return proto.Clone(snap.Envelope).(*configv1.SnapshotEnvelope), nil
}

func latestPgResource(ctx context.Context, tx pgx.Tx, lane string, resource string) (uint64, []byte, []byte, error) {
	var version uint64
	var checksum []byte
	var payload []byte
	err := tx.QueryRow(ctx, `
		SELECT version, checksum, envelope_payload
		FROM orange_mapped_split_resources
		WHERE lane = $1 AND resource = $2
		ORDER BY version DESC
		LIMIT 1
	`, lane, resource).Scan(&version, &checksum, &payload)
	if err != nil {
		return 0, nil, nil, err
	}
	return version, checksum, payload, nil
}

func nextPgMapVersion(ctx context.Context, tx pgx.Tx, lane string) (uint64, error) {
	var version uint64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(map_version), 0) + 1
		FROM orange_mapped_split_maps
		WHERE lane = $1
	`, lane).Scan(&version); err != nil {
		return 0, fmt.Errorf("allocate map version for lane %q: %w", lane, err)
	}
	return version, nil
}
