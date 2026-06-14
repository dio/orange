package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/mappedsplit"
	"github.com/dio/orange/snapshot"
)

var pgStoreIdentifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

const (
	defaultPgBuildLeaseDuration = 30 * time.Second
	minPgBuildHeartbeatInterval = 10 * time.Millisecond
)

// PgStore is a pgxpool-backed mapped-split Store.
type PgStore struct {
	pool                   *pgxpool.Pool
	schema                 string
	buildLeaseHolderID     string
	buildLeaseDuration     time.Duration
	buildHeartbeatInterval time.Duration
}

// PgStoreOption configures PgStore.
type PgStoreOption func(*PgStore)

// NewPgStore creates a Postgres-backed Store. It does not run migrations;
// callers must apply config/postgres/migration before constructing the store.
func NewPgStore(pool *pgxpool.Pool, opts ...PgStoreOption) (*PgStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres store pool is required")
	}
	store := &PgStore{
		pool:                   pool,
		buildLeaseHolderID:     defaultPgBuildLeaseHolderID(),
		buildLeaseDuration:     defaultPgBuildLeaseDuration,
		buildHeartbeatInterval: defaultPgBuildLeaseDuration / 3,
	}
	for _, opt := range opts {
		opt(store)
	}
	if store.schema != "" && !pgStoreIdentifierPattern.MatchString(store.schema) {
		return nil, fmt.Errorf("invalid postgres store schema identifier %q", store.schema)
	}
	if store.buildLeaseHolderID == "" {
		return nil, fmt.Errorf("postgres store build lease holder ID is required")
	}
	if store.buildLeaseDuration <= 0 {
		return nil, fmt.Errorf("postgres store build lease duration must be positive")
	}
	if store.buildHeartbeatInterval <= 0 {
		return nil, fmt.Errorf("postgres store build heartbeat interval must be positive")
	}
	if store.buildHeartbeatInterval < minPgBuildHeartbeatInterval {
		store.buildHeartbeatInterval = minPgBuildHeartbeatInterval
	}
	return store, nil
}

// WithPgStoreSchema uses schema as the store search path inside transactions.
func WithPgStoreSchema(schema string) PgStoreOption {
	return func(s *PgStore) {
		s.schema = schema
	}
}

// WithPgStoreBuildLeaseHolderID sets the replica identity written into build
// leases. Multi-replica deployments should pass a stable per-process value.
func WithPgStoreBuildLeaseHolderID(holderID string) PgStoreOption {
	return func(s *PgStore) {
		s.buildLeaseHolderID = holderID
	}
}

// WithPgStoreBuildLeaseDuration sets the lease expiry window for mapped-split
// builds.
func WithPgStoreBuildLeaseDuration(duration time.Duration) PgStoreOption {
	return func(s *PgStore) {
		s.buildLeaseDuration = duration
	}
}

// WithPgStoreBuildHeartbeatInterval sets how often an acquired build lease is
// renewed while the callback is running.
func WithPgStoreBuildHeartbeatInterval(interval time.Duration) PgStoreOption {
	return func(s *PgStore) {
		s.buildHeartbeatInterval = interval
	}
}

// PublishMappedSplit publishes component resources and then advances the lane's
// current typed map in one transaction.
func (s *PgStore) PublishMappedSplit(ctx context.Context, out MappedSplitPublication) (PublishResult, error) {
	return s.publishMappedSplit(ctx, out, nil)
}

// PublishMappedSplitWithLease publishes while asserting a build lease fencing
// token in the same transaction that advances the lane's current map.
func (s *PgStore) PublishMappedSplitWithLease(ctx context.Context, lease BuildLease, out MappedSplitPublication) (PublishResult, error) {
	if lease.Lane != out.Lane {
		return PublishResult{}, fmt.Errorf("%w: lease lane %q does not match publication lane %q", ErrBuildLeaseLost, lease.Lane, out.Lane)
	}
	return s.publishMappedSplit(ctx, out, &lease)
}

func (s *PgStore) publishMappedSplit(ctx context.Context, out MappedSplitPublication, lease *BuildLease) (PublishResult, error) {
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
	if lease != nil {
		if err := assertPgBuildLease(ctx, tx, *lease); err != nil {
			return PublishResult{}, err
		}
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

// MarkMappedSplitDirty records that lane needs a mapped-split rebuild. The row
// is coalesced by lane, with the latest attribution metadata winning.
func (s *PgStore) MarkMappedSplitDirty(ctx context.Context, req BuildRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := markMappedSplitDirtyTx(ctx, tx, req); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("mark mapped split dirty lane %q: commit: %w", req.Lane, err)
	}
	return nil
}

func markMappedSplitDirtyTx(ctx context.Context, tx pgx.Tx, req BuildRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO orange_mapped_split_build_requests (
			lane, dirty, requested_by, source_revision, change_hint, updated_at
		)
		VALUES ($1, true, $2, $3, $4, now())
		ON CONFLICT (lane) DO UPDATE
		SET dirty = true,
		    requested_by = EXCLUDED.requested_by,
		    source_revision = EXCLUDED.source_revision,
		    change_hint = EXCLUDED.change_hint,
		    updated_at = EXCLUDED.updated_at
	`, req.Lane, req.RequestedBy, req.SourceRevision, req.ChangeHint); err != nil {
		return fmt.Errorf("mark mapped split dirty lane %q: %w", req.Lane, err)
	}
	return nil
}

// GetMappedSplitBuildRequest returns the current dirty build request for lane.
// It returns nil when no dirty request exists.
func (s *PgStore) GetMappedSplitBuildRequest(ctx context.Context, lane string) (*BuildRequest, error) {
	if lane == "" {
		return nil, fmt.Errorf("build request lane is required")
	}
	tx, err := s.beginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var req BuildRequest
	err = tx.QueryRow(ctx, `
		SELECT lane, requested_by, source_revision, change_hint
		FROM orange_mapped_split_build_requests
		WHERE lane = $1 AND dirty = true
	`, lane).Scan(&req.Lane, &req.RequestedBy, &req.SourceRevision, &req.ChangeHint)
	if err != nil {
		if err == pgx.ErrNoRows {
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("get mapped split build request lane %q: commit: %w", lane, err)
			}
			return nil, nil
		}
		return nil, fmt.Errorf("get mapped split build request lane %q: %w", lane, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("get mapped split build request lane %q: commit: %w", lane, err)
	}
	return &req, nil
}

// ClearMappedSplitDirty clears the dirty bit after a scheduled build publishes
// mapVersion while still holding the matching lease fencing token.
func (s *PgStore) ClearMappedSplitDirty(ctx context.Context, lease BuildLease, mapVersion uint64) error {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := assertPgBuildLease(ctx, tx, lease); err != nil {
		return err
	}
	if mapVersion > 0 {
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM orange_mapped_split_current
				WHERE lane = $1 AND map_version = $2
			)
		`, lease.Lane, mapVersion).Scan(&exists); err != nil {
			return fmt.Errorf("clear mapped split dirty lane %q: read current map: %w", lease.Lane, err)
		}
		if !exists {
			return fmt.Errorf("%w: lane %q current map version is not %d", ErrBuildLeaseLost, lease.Lane, mapVersion)
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE orange_mapped_split_build_requests
		SET dirty = false,
		    updated_at = now()
		WHERE lane = $1
	`, lease.Lane); err != nil {
		return fmt.Errorf("clear mapped split dirty lane %q: %w", lease.Lane, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("clear mapped split dirty lane %q: commit: %w", lease.Lane, err)
	}
	return nil
}

// WithMappedSplitBuildLease runs fn while holding a controller-style per-lane
// lease. The lease is renewed in the background until fn returns or ctx ends.
func (s *PgStore) WithMappedSplitBuildLease(ctx context.Context, lane string, fn func(context.Context, BuildLease) error) error {
	if lane == "" {
		return fmt.Errorf("build lease lane is required")
	}
	if fn == nil {
		return fmt.Errorf("build lease callback is required")
	}

	lease, err := s.acquireMappedSplitBuildLease(ctx, lane)
	if err != nil {
		return err
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatErr := make(chan error, 1)
	go func() {
		heartbeatErr <- s.heartbeatMappedSplitBuildLease(heartbeatCtx, lease)
	}()

	callbackErr := fn(ctx, lease)
	cancelHeartbeat()
	hbErr := <-heartbeatErr

	if releaseErr := s.releaseMappedSplitBuildLease(context.WithoutCancel(ctx), lease); releaseErr != nil && callbackErr == nil {
		callbackErr = releaseErr
	}
	if callbackErr != nil {
		return callbackErr
	}
	if hbErr != nil && !errors.Is(hbErr, context.Canceled) {
		return hbErr
	}
	return nil
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

func (s *PgStore) acquireMappedSplitBuildLease(ctx context.Context, lane string) (BuildLease, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return BuildLease{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var lease BuildLease
	err = tx.QueryRow(ctx, `
		INSERT INTO orange_mapped_split_build_leases (
			lane, holder_id, lease_version, locked_until, heartbeat_at, generation_started_at
		)
		VALUES ($1, $2, 1, now() + $3::interval, now(), now())
		ON CONFLICT (lane) DO UPDATE
		SET holder_id = EXCLUDED.holder_id,
		    lease_version = orange_mapped_split_build_leases.lease_version + 1,
		    locked_until = EXCLUDED.locked_until,
		    heartbeat_at = EXCLUDED.heartbeat_at,
		    generation_started_at = EXCLUDED.generation_started_at
		WHERE orange_mapped_split_build_leases.locked_until <= now()
		RETURNING lane, holder_id, lease_version, locked_until
	`, lane, s.buildLeaseHolderID, pgInterval(s.buildLeaseDuration)).Scan(
		&lease.Lane,
		&lease.HolderID,
		&lease.LeaseVersion,
		&lease.LockedUntil,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return BuildLease{}, fmt.Errorf("%w: lane %q", ErrBuildLeaseHeld, lane)
		}
		return BuildLease{}, fmt.Errorf("acquire mapped split build lease lane %q: %w", lane, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return BuildLease{}, fmt.Errorf("acquire mapped split build lease lane %q: commit: %w", lane, err)
	}
	return lease, nil
}

func (s *PgStore) heartbeatMappedSplitBuildLease(ctx context.Context, lease BuildLease) error {
	ticker := time.NewTicker(s.buildHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.renewMappedSplitBuildLease(ctx, lease); err != nil {
				return err
			}
		}
	}
}

func (s *PgStore) renewMappedSplitBuildLease(ctx context.Context, lease BuildLease) error {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	commandTag, err := tx.Exec(ctx, `
		UPDATE orange_mapped_split_build_leases
		SET locked_until = now() + $4::interval,
		    heartbeat_at = now()
		WHERE lane = $1
		  AND holder_id = $2
		  AND lease_version = $3
	`, lease.Lane, lease.HolderID, lease.LeaseVersion, pgInterval(s.buildLeaseDuration))
	if err != nil {
		return fmt.Errorf("renew mapped split build lease lane %q: %w", lease.Lane, err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("%w: lane %q", ErrBuildLeaseLost, lease.Lane)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("renew mapped split build lease lane %q: commit: %w", lease.Lane, err)
	}
	return nil
}

func (s *PgStore) releaseMappedSplitBuildLease(ctx context.Context, lease BuildLease) error {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE orange_mapped_split_build_leases
		SET locked_until = LEAST(locked_until, now()),
		    heartbeat_at = now()
		WHERE lane = $1
		  AND holder_id = $2
		  AND lease_version = $3
	`, lease.Lane, lease.HolderID, lease.LeaseVersion); err != nil {
		return fmt.Errorf("release mapped split build lease lane %q: %w", lease.Lane, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("release mapped split build lease lane %q: commit: %w", lease.Lane, err)
	}
	return nil
}

func assertPgBuildLease(ctx context.Context, tx pgx.Tx, lease BuildLease) error {
	if lease.Lane == "" || lease.HolderID == "" || lease.LeaseVersion <= 0 {
		return fmt.Errorf("%w: invalid build lease", ErrBuildLeaseLost)
	}
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM orange_mapped_split_build_leases
			WHERE lane = $1
			  AND holder_id = $2
			  AND lease_version = $3
			  AND locked_until > now()
		)
	`, lease.Lane, lease.HolderID, lease.LeaseVersion).Scan(&exists); err != nil {
		return fmt.Errorf("assert mapped split build lease lane %q: %w", lease.Lane, err)
	}
	if !exists {
		return fmt.Errorf("%w: lane %q", ErrBuildLeaseLost, lease.Lane)
	}
	return nil
}

func pgInterval(duration time.Duration) string {
	return fmt.Sprintf("%f seconds", duration.Seconds())
}

func defaultPgBuildLeaseHolderID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s:%d", hostname, os.Getpid())
}
