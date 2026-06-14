package config

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	pgquesetup "github.com/dio/orange/config/pgque"
	"github.com/dio/orange/config/postgres/migration"
	"github.com/dio/orange/internal/embeddedpg/testpg"
)

func TestPgStoreMultiReplicaE2EWithServers(t *testing.T) {
	ctx := context.Background()
	schema := testPgSchema(t)
	pool := testpg.Pool(t)
	require.NoError(t, migration.Migrate(ctx, pool, migration.WithSchema(schema)))

	storeA := newPgStoreForMultiReplicaE2E(t, pool, schema, "replica-a")
	storeB := newPgStoreForMultiReplicaE2E(t, pool, schema, "replica-b")
	storeC := newPgStoreForMultiReplicaE2E(t, pool, schema, "replica-c")
	serverA := testServerWithStore(storeA)
	serverB := testServerWithStore(storeB)
	serverC := testServerWithStore(storeC)
	clientB, cleanupB := startServerClient(t, serverB)
	defer cleanupB()

	_, err := serverA.PublishMappedSplit(ctx, testMappedSplitRequest("lane-a", 1))
	require.NoError(t, err)

	mapReq := connectRequestFetchMap()
	mapReq.Header().Set("x-orange-lane", "lane-a")
	mapResp, err := clientB.FetchMappedSplitMap(ctx, mapReq)
	require.NoError(t, err)
	firstMap := mapResp.Msg.GetSnapshot()
	require.NotNil(t, firstMap)
	require.Equal(t, uint64(1), firstMap.Version)
	require.Equal(t, uint64(1), firstMap.Map.MapRevision)

	bundleReq := connect.NewRequest(&configv1.FetchMappedSplitBundleRequest{Resource: "llm-generic"})
	bundleReq.Header().Set("x-orange-lane", "lane-a")
	bundleResp, err := clientB.FetchMappedSplitBundle(ctx, bundleReq)
	require.NoError(t, err)
	firstBundle := bundleResp.Msg.GetSnapshot()
	require.NotNil(t, firstBundle)
	require.Equal(t, uint64(1), firstBundle.Version)

	unchangedBundleReq := connect.NewRequest(&configv1.FetchMappedSplitBundleRequest{
		Resource:     "llm-generic",
		LastVersion:  firstBundle.Version,
		LastChecksum: firstBundle.Checksum,
	})
	unchangedBundleReq.Header().Set("x-orange-lane", "lane-a")
	unchangedBundleResp, err := clientB.FetchMappedSplitBundle(ctx, unchangedBundleReq)
	require.NoError(t, err)
	require.NotNil(t, unchangedBundleResp.Msg.GetUnchanged())

	_, err = serverC.PublishMappedSplit(ctx, testMappedSplitRequest("lane-a", 2))
	require.NoError(t, err)

	newMapReq := connect.NewRequest(&configv1.FetchMappedSplitMapRequest{
		LastVersion:  firstMap.Version,
		LastChecksum: firstMap.Checksum,
	})
	newMapReq.Header().Set("x-orange-lane", "lane-a")
	newMapResp, err := clientB.FetchMappedSplitMap(ctx, newMapReq)
	require.NoError(t, err)
	secondMap := newMapResp.Msg.GetSnapshot()
	require.NotNil(t, secondMap)
	require.Equal(t, uint64(2), secondMap.Version)
	require.Equal(t, uint64(2), secondMap.Map.MapRevision)

	unchangedMapReq := connect.NewRequest(&configv1.FetchMappedSplitMapRequest{
		LastVersion:  secondMap.Version,
		LastChecksum: secondMap.Checksum,
	})
	unchangedMapReq.Header().Set("x-orange-lane", "lane-a")
	unchangedMapResp, err := clientB.FetchMappedSplitMap(ctx, unchangedMapReq)
	require.NoError(t, err)
	require.NotNil(t, unchangedMapResp.Msg.GetUnchanged())

	mapFromA, unchanged, err := storeA.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, uint64(2), mapFromA.Version)
	require.Equal(t, uint64(2), mapFromA.Map.MapRevision)

	mapFromB, unchanged, err := storeB.FetchMappedSplitMap(ctx, "lane-a", secondMap.Version, secondMap.Checksum)
	require.NoError(t, err)
	require.True(t, unchanged)
	require.Nil(t, mapFromB)
}

func TestPgQueSchedulerMultiReplicaE2E(t *testing.T) {
	ctx := context.Background()
	pool := freshPgQueSchedulerDB(t)
	require.NoError(t, migration.Migrate(ctx, pool))

	replicas := []struct {
		holder   string
		consumer string
	}{
		{holder: "worker-a", consumer: "orange_mapped_split_builder_a"},
		{holder: "worker-b", consumer: "orange_mapped_split_builder_b"},
		{holder: "worker-c", consumer: "orange_mapped_split_builder_c"},
	}
	for _, replica := range replicas {
		require.NoError(t, pgquesetup.Setup(ctx, pool, pgquesetup.WithConsumer(replica.consumer)))
		require.NoError(t, pgquesetup.Setup(ctx, pool, pgquesetup.WithConsumer(replica.consumer)))
	}

	var laneABuilds atomic.Int64
	storeA := newPgStoreForMultiReplicaE2E(t, pool, "", "worker-a")
	schedulerA := newPgQueSchedulerForMultiReplicaE2E(t, storeA, replicas[0].consumer, func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
		if req.Lane == "lane-a" {
			laneABuilds.Add(1)
		}
		return testMappedSplitRequest(req.Lane, 1), nil
	})

	for i := range 12 {
		require.NoError(t, schedulerA.ScheduleBuild(ctx, BuildRequest{
			Lane:           "lane-a",
			RequestedBy:    fmt.Sprintf("replica-%02d", i%len(replicas)),
			SourceRevision: fmt.Sprintf("rev-%02d", i),
			ChangeHint:     "duplicate",
		}))
	}
	require.Equal(t, 1, pgQueSchedulerCount(t, pool, "SELECT count(*) FROM orange_mapped_split_build_requests WHERE lane = $1 AND dirty", "lane-a"))
	require.Eventually(t, func() bool {
		_, _ = schedulerA.ProcessOnce(ctx)
		return pgQueSchedulerCurrentRevision(t, storeA, "lane-a") == 1
	}, 4*time.Second, 20*time.Millisecond)
	for range 4 {
		_, err := schedulerA.ProcessOnce(ctx)
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), laneABuilds.Load())
	require.Nil(t, pgQueSchedulerDirtyRequest(t, storeA, "lane-a"))

	storeTakeoverA := newPgStoreForMultiReplicaE2EWithLease(t, pool, "", "takeover-a", 60*time.Millisecond, time.Second)
	storeTakeoverB := newPgStoreForMultiReplicaE2EWithLease(t, pool, "", "takeover-b", time.Second, 20*time.Millisecond)
	takeoverEntered := make(chan struct{})
	allowTakeoverA := make(chan struct{})
	var takeoverBuilds atomic.Int64
	schedulerTakeoverA := newPgQueSchedulerForMultiReplicaE2E(t, storeTakeoverA, replicas[0].consumer, func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
		if req.Lane == "lane-takeover" {
			takeoverBuilds.Add(1)
			close(takeoverEntered)
			<-allowTakeoverA
		}
		return testMappedSplitRequest(req.Lane, 1), nil
	})
	schedulerTakeoverB := newPgQueSchedulerForMultiReplicaE2E(t, storeTakeoverB, replicas[1].consumer, func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
		if req.Lane == "lane-takeover" {
			takeoverBuilds.Add(1)
		}
		return testMappedSplitRequest(req.Lane, 1), nil
	})

	require.NoError(t, schedulerTakeoverA.ScheduleBuild(ctx, BuildRequest{Lane: "lane-takeover", RequestedBy: "test"}))
	aDone := make(chan error, 1)
	go func() {
		_, err := schedulerTakeoverA.ProcessOnce(ctx)
		aDone <- err
	}()
	select {
	case <-takeoverEntered:
	case <-time.After(time.Second):
		t.Fatal("takeover worker A did not enter build")
	}
	time.Sleep(90 * time.Millisecond)
	require.Eventually(t, func() bool {
		_, _ = schedulerTakeoverB.ProcessOnce(ctx)
		return pgQueSchedulerCurrentRevision(t, storeTakeoverB, "lane-takeover") == 1
	}, 4*time.Second, 20*time.Millisecond)
	close(allowTakeoverA)
	err := <-aDone
	require.NoError(t, err)
	require.Equal(t, int64(2), takeoverBuilds.Load())
	require.Nil(t, pgQueSchedulerDirtyRequest(t, storeTakeoverB, "lane-takeover"))

	storeB := newPgStoreForMultiReplicaE2E(t, pool, "", "worker-b-concurrent")
	storeC := newPgStoreForMultiReplicaE2E(t, pool, "", "worker-c-concurrent")
	laneEntered := make(chan string, 2)
	releaseConcurrentBuilds := make(chan struct{})
	var seenMu sync.Mutex
	seenLanes := map[string]bool{}
	buildConcurrent := func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
		if req.Lane == "lane-b" || req.Lane == "lane-c" {
			seenMu.Lock()
			seenLanes[req.Lane] = true
			seenMu.Unlock()
			laneEntered <- req.Lane
			<-releaseConcurrentBuilds
		}
		return testMappedSplitRequest(req.Lane, 1), nil
	}
	schedulerB := newPgQueSchedulerForMultiReplicaE2E(t, storeB, replicas[1].consumer, buildConcurrent)
	schedulerC := newPgQueSchedulerForMultiReplicaE2E(t, storeC, replicas[2].consumer, buildConcurrent)

	require.NoError(t, schedulerA.ScheduleBuild(ctx, BuildRequest{Lane: "lane-b", RequestedBy: "test"}))
	require.NoError(t, schedulerA.ScheduleBuild(ctx, BuildRequest{Lane: "lane-c", RequestedBy: "test"}))

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	for _, work := range []struct {
		scheduler *PgQueScheduler
		lane      string
	}{
		{scheduler: schedulerB, lane: "lane-b"},
		{scheduler: schedulerC, lane: "lane-c"},
	} {
		work := work
		go func() {
			defer wg.Done()
			errs <- work.scheduler.processMessage(ctx, pgQueMessage{
				Type:    PgQueMappedSplitBuildEventType,
				Payload: fmt.Sprintf(`{"lane":%q}`, work.lane),
			})
		}()
	}
	require.Eventually(t, func() bool {
		seenMu.Lock()
		defer seenMu.Unlock()
		return seenLanes["lane-b"] && seenLanes["lane-c"]
	}, 4*time.Second, 20*time.Millisecond)
	close(releaseConcurrentBuilds)
	<-laneEntered
	<-laneEntered
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, uint64(1), pgQueSchedulerCurrentRevision(t, storeB, "lane-b"))
	require.Equal(t, uint64(1), pgQueSchedulerCurrentRevision(t, storeC, "lane-c"))
}

func newPgStoreForMultiReplicaE2E(t *testing.T, pool *pgxpool.Pool, schema string, holderID string) *PgStore {
	t.Helper()
	return newPgStoreForMultiReplicaE2EWithLease(t, pool, schema, holderID, time.Second, 20*time.Millisecond)
}

func newPgStoreForMultiReplicaE2EWithLease(t *testing.T, pool *pgxpool.Pool, schema string, holderID string, leaseDuration time.Duration, heartbeatInterval time.Duration) *PgStore {
	t.Helper()
	opts := []PgStoreOption{
		WithPgStoreBuildLeaseHolderID(holderID),
		WithPgStoreBuildLeaseDuration(leaseDuration),
		WithPgStoreBuildHeartbeatInterval(heartbeatInterval),
	}
	if schema != "" {
		opts = append(opts, WithPgStoreSchema(schema))
	}
	store, err := NewPgStore(pool, opts...)
	require.NoError(t, err)
	return store
}

func newPgQueSchedulerForMultiReplicaE2E(t *testing.T, store *PgStore, consumer string, build OnDemandBuildFunc) *PgQueScheduler {
	t.Helper()
	scheduler, err := NewPgQueScheduler(
		store,
		build,
		WithPgQueSchedulerConsumer(consumer),
		WithPgQueSchedulerMaxMessages(1),
		WithPgQueSchedulerRetryAfter(10*time.Millisecond),
		WithPgQueSchedulerPollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	return scheduler
}
