package config

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	pgquesetup "github.com/dio/orange/config/pgque"
	"github.com/dio/orange/config/postgres/migration"
	"github.com/dio/orange/internal/embeddedpg/testpg"
	"github.com/dio/orange/snapshot"
)

func TestPgQueMigrationIndependence(t *testing.T) {
	ctx := context.Background()

	t.Run("store only", func(t *testing.T) {
		pool := freshPgQueSchedulerDB(t)
		require.NoError(t, migration.Migrate(ctx, pool))
		store, err := NewPgStore(pool)
		require.NoError(t, err)
		server := testServerWithStore(store)

		_, err = server.PublishMappedSplit(ctx, testMappedSplitRequest("lane-a", 1))
		require.NoError(t, err)
		typedMap, unchanged, err := store.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
		require.NoError(t, err)
		require.False(t, unchanged)
		require.Equal(t, uint64(1), typedMap.Version)
		require.False(t, pgQueSchedulerNamespaceExists(t, pool, "pgque"))
	})

	t.Run("pgque only", func(t *testing.T) {
		pool := freshPgQueSchedulerDB(t)
		require.NoError(t, pgquesetup.Setup(ctx, pool))
		_, err := pgquesetup.SendTestEvent(ctx, pool, pgquesetup.DefaultQueue)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			_, typ, err := pgquesetup.ReceiveOne(ctx, pool, pgquesetup.DefaultQueue, pgquesetup.DefaultConsumer)
			return err == nil && typ == "orange.pgque.test"
		}, 2*time.Second, 20*time.Millisecond)
		require.False(t, pgQueSchedulerTableExists(t, pool, "orange_mapped_split_maps"))
	})

	t.Run("both setup orders work", func(t *testing.T) {
		pool := freshPgQueSchedulerDB(t)
		require.NoError(t, pgquesetup.Setup(ctx, pool))
		require.NoError(t, migration.Migrate(ctx, pool))
		store, err := NewPgStore(pool)
		require.NoError(t, err)
		server := testServerWithStore(store)

		_, err = server.PublishMappedSplit(ctx, testMappedSplitRequest("lane-a", 1))
		require.NoError(t, err)
		_, unchanged, err := store.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
		require.NoError(t, err)
		require.False(t, unchanged)
		_, err = pgquesetup.SendTestEvent(ctx, pool, pgquesetup.DefaultQueue)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			_, typ, err := pgquesetup.ReceiveOne(ctx, pool, pgquesetup.DefaultQueue, pgquesetup.DefaultConsumer)
			return err == nil && typ == "orange.pgque.test"
		}, 2*time.Second, 20*time.Millisecond)
	})
}

func TestPgQueSchedulerScheduleBuildMarksDirtyAndSendsEvent(t *testing.T) {
	ctx := context.Background()
	pool, store := freshPgQueSchedulerStore(t, "holder-a")
	scheduler := newTestPgQueScheduler(t, store, func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
		return testMappedSplitRequest(req.Lane, 1), nil
	})

	for range 8 {
		require.NoError(t, scheduler.ScheduleBuild(ctx, BuildRequest{
			Lane:           "lane-a",
			RequestedBy:    "test",
			SourceRevision: "rev-1",
			ChangeHint:     "diagnostic",
		}))
	}

	require.Equal(t, 1, pgQueSchedulerCount(t, pool, "SELECT count(*) FROM orange_mapped_split_build_requests WHERE lane = $1 AND dirty", "lane-a"))
	require.Eventually(t, func() bool {
		_, typ, err := pgquesetup.ReceiveOne(ctx, pool, pgquesetup.DefaultQueue, pgquesetup.DefaultConsumer)
		return err == nil && typ == PgQueMappedSplitBuildEventType
	}, 2*time.Second, 20*time.Millisecond)
}

func TestPgQueSchedulerTicksOnlyMappedSplitQueue(t *testing.T) {
	ctx := context.Background()
	pool, store := freshPgQueSchedulerStore(t, "holder-a")
	scheduler := newTestPgQueScheduler(t, store, func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
		return testMappedSplitRequest(req.Lane, 1), nil
	})

	_, err := pool.Exec(ctx, "SELECT pgque.create_queue($1)", "unrelated_broken_queue")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "UPDATE pgque.queue SET queue_event_seq = 'pgque.missing_sequence' WHERE queue_name = $1", "unrelated_broken_queue")
	require.NoError(t, err)

	require.NoError(t, scheduler.ScheduleBuild(ctx, BuildRequest{Lane: "lane-a", RequestedBy: "test"}))
	processed, err := scheduler.ProcessOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, uint64(1), pgQueSchedulerCurrentRevision(t, store, "lane-a"))

	_, err = pool.Exec(ctx, "SELECT pgque.ticker()")
	require.Error(t, err)
}

func TestPgQueSchedulerDuplicateEventsProduceOnePublishedRevision(t *testing.T) {
	ctx := context.Background()
	_, store := freshPgQueSchedulerStore(t, "holder-a")
	var builds atomic.Int64
	scheduler := newTestPgQueScheduler(t, store, func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
		builds.Add(1)
		return testMappedSplitRequest(req.Lane, 1), nil
	})

	for range 5 {
		require.NoError(t, scheduler.ScheduleBuild(ctx, BuildRequest{Lane: "lane-a", RequestedBy: "test"}))
	}
	require.Eventually(t, func() bool {
		_, _ = scheduler.ProcessOnce(ctx)
		return pgQueSchedulerCurrentRevision(t, store, "lane-a") == 1
	}, 3*time.Second, 20*time.Millisecond)
	for range 6 {
		_, err := scheduler.ProcessOnce(ctx)
		require.NoError(t, err)
	}

	require.Equal(t, int64(1), builds.Load())
	require.Nil(t, pgQueSchedulerDirtyRequest(t, store, "lane-a"))
}

func TestPgQueSchedulerTwoWorkersRaceOneLane(t *testing.T) {
	ctx := context.Background()
	pool, storeA := freshPgQueSchedulerStore(t, "holder-a")
	storeB, err := NewPgStore(
		pool,
		WithPgStoreBuildLeaseHolderID("holder-b"),
		WithPgStoreBuildLeaseDuration(time.Second),
		WithPgStoreBuildHeartbeatInterval(20*time.Millisecond),
	)
	require.NoError(t, err)

	var builds atomic.Int64
	build := func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
		builds.Add(1)
		time.Sleep(50 * time.Millisecond)
		return testMappedSplitRequest(req.Lane, 1), nil
	}
	schedulerA := newTestPgQueScheduler(t, storeA, build)
	const consumerB = "orange_mapped_split_builder_b"
	_, err = pool.Exec(ctx, "SELECT pgque.subscribe($1, $2)", pgquesetup.DefaultQueue, consumerB)
	require.NoError(t, err)
	schedulerB, err := NewPgQueScheduler(
		storeB,
		build,
		WithPgQueSchedulerConsumer(consumerB),
		WithPgQueSchedulerRetryAfter(10*time.Millisecond),
		WithPgQueSchedulerPollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	require.NoError(t, schedulerA.ScheduleBuild(ctx, BuildRequest{Lane: "lane-a"}))

	var wg sync.WaitGroup
	wg.Add(2)
	for _, scheduler := range []*PgQueScheduler{schedulerA, schedulerB} {
		go func() {
			defer wg.Done()
			for range 5 {
				_, err := scheduler.ProcessOnce(ctx)
				if err != nil {
					require.ErrorIs(t, err, ErrBuildLeaseHeld)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, uint64(1), pgQueSchedulerCurrentRevision(t, storeA, "lane-a"))
	require.Equal(t, int64(1), builds.Load())
}

func TestPgQueSchedulerLeaseHeldIsNoOp(t *testing.T) {
	ctx := context.Background()
	pool, storeA := freshPgQueSchedulerStore(t, "holder-a")
	storeB, err := NewPgStore(
		pool,
		WithPgStoreBuildLeaseHolderID("holder-b"),
		WithPgStoreBuildLeaseDuration(time.Second),
		WithPgStoreBuildHeartbeatInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	schedulerB := newTestPgQueScheduler(t, storeB, func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
		return testMappedSplitRequest(req.Lane, 1), nil
	})
	require.NoError(t, storeA.MarkMappedSplitDirty(ctx, BuildRequest{Lane: "lane-a"}))

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- storeA.WithMappedSplitBuildLease(ctx, "lane-a", func(context.Context, BuildLease) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	err = schedulerB.processMessage(ctx, pgQueMessage{
		Type:    PgQueMappedSplitBuildEventType,
		Payload: `{"lane":"lane-a"}`,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), pgQueSchedulerCurrentRevision(t, storeB, "lane-a"))
	require.NotNil(t, pgQueSchedulerDirtyRequest(t, storeB, "lane-a"))

	close(release)
	require.NoError(t, <-done)
}

func TestPgQueSchedulerBuildsDifferentLanesIndependently(t *testing.T) {
	ctx := context.Background()
	_, store := freshPgQueSchedulerStore(t, "holder-a")
	var builds atomic.Int64
	scheduler := newTestPgQueScheduler(t, store, func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
		builds.Add(1)
		return testMappedSplitRequest(req.Lane, 1), nil
	})

	require.NoError(t, scheduler.ScheduleBuild(ctx, BuildRequest{Lane: "lane-a"}))
	require.NoError(t, scheduler.ScheduleBuild(ctx, BuildRequest{Lane: "lane-b"}))
	require.Eventually(t, func() bool {
		_, _ = scheduler.ProcessOnce(ctx)
		return pgQueSchedulerCurrentRevision(t, store, "lane-a") == 1 &&
			pgQueSchedulerCurrentRevision(t, store, "lane-b") == 1
	}, 3*time.Second, 20*time.Millisecond)
	require.Equal(t, int64(2), builds.Load())
}

func TestPgQueSchedulerPostPublishPreAckFailureRedeliversNoOp(t *testing.T) {
	ctx := context.Background()
	pool, store := freshPgQueSchedulerStore(t, "holder-a")
	var builds atomic.Int64
	failOnce := atomic.Bool{}
	failOnce.Store(true)
	scheduler := newTestPgQueScheduler(t, store, func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
		builds.Add(1)
		return testMappedSplitRequest(req.Lane, 1), nil
	})
	scheduler.afterClearDirtyHook = func(context.Context, BuildLease, PublishResult) error {
		if failOnce.Swap(false) {
			return errors.New("simulated post-publish pre-ack failure")
		}
		return nil
	}

	require.NoError(t, scheduler.ScheduleBuild(ctx, BuildRequest{Lane: "lane-a"}))
	processed, err := scheduler.ProcessOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, uint64(1), pgQueSchedulerCurrentRevision(t, store, "lane-a"))
	require.Nil(t, pgQueSchedulerDirtyRequest(t, store, "lane-a"))

	pgQueSchedulerForceRetry(t, pool)
	require.Eventually(t, func() bool {
		_, err := scheduler.ProcessOnce(ctx)
		require.NoError(t, err)
		return builds.Load() == 1
	}, 3*time.Second, 20*time.Millisecond)
	require.Equal(t, int64(1), builds.Load())
}

func freshPgQueSchedulerStore(t *testing.T, holderID string) (*pgxpool.Pool, *PgStore) {
	t.Helper()
	ctx := context.Background()
	pool := freshPgQueSchedulerDB(t)
	require.NoError(t, migration.Migrate(ctx, pool))
	require.NoError(t, pgquesetup.Setup(ctx, pool))
	store, err := NewPgStore(
		pool,
		WithPgStoreBuildLeaseHolderID(holderID),
		WithPgStoreBuildLeaseDuration(time.Second),
		WithPgStoreBuildHeartbeatInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	return pool, store
}

func newTestPgQueScheduler(t *testing.T, store *PgStore, build OnDemandBuildFunc) *PgQueScheduler {
	t.Helper()
	scheduler, err := NewPgQueScheduler(
		store,
		build,
		WithPgQueSchedulerRetryAfter(10*time.Millisecond),
		WithPgQueSchedulerPollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	return scheduler
}

func freshPgQueSchedulerDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	base := testpg.Pool(t)
	dbName := testPgSchema(t)
	_, err := base.Exec(context.Background(), "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize())
	require.NoError(t, err)
	cfg := base.Config().Copy()
	cfg.ConnConfig.Database = dbName
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize())
	})
	return pool
}

func pgQueSchedulerCount(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var count int
	err := pool.QueryRow(context.Background(), query, args...).Scan(&count)
	require.NoError(t, err)
	return count
}

func pgQueSchedulerNamespaceExists(t *testing.T, pool *pgxpool.Pool, name string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(context.Background(), "SELECT to_regnamespace($1) IS NOT NULL", name).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func pgQueSchedulerTableExists(t *testing.T, pool *pgxpool.Pool, name string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(context.Background(), "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func pgQueSchedulerCurrentRevision(t *testing.T, store *PgStore, lane string) uint64 {
	t.Helper()
	typedMap, _, err := store.FetchMappedSplitMap(context.Background(), lane, 0, nil)
	if err != nil {
		require.ErrorIs(t, err, snapshot.ErrNoSnapshot)
		return 0
	}
	return typedMap.Map.MapRevision
}

func pgQueSchedulerDirtyRequest(t *testing.T, store *PgStore, lane string) *BuildRequest {
	t.Helper()
	req, err := store.GetMappedSplitBuildRequest(context.Background(), lane)
	require.NoError(t, err)
	return req
}

func pgQueSchedulerForceRetry(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, "UPDATE pgque.retry_queue SET ev_retry_after = now() - interval '1 second'")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "SELECT pgque.maint_retry_events()")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "SELECT pgque.force_next_tick($1)", pgquesetup.DefaultQueue)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "SELECT pgque.ticker($1)", pgquesetup.DefaultQueue)
	require.NoError(t, err)
}
