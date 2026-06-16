package config

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dio/orange/config/postgres/migration"
	"github.com/dio/orange/internal/embeddedpg/testpg"
	"github.com/dio/orange/mappedsplit"
	"github.com/dio/orange/snapshot"
)

var (
	_ Store            = (*PgStore)(nil)
	_ BuildCoordinator = (*PgStore)(nil)
)

func TestMain(m *testing.M) {
	code := m.Run()
	testpg.Cleanup()
	os.Exit(code)
}

func TestNewPgStoreValidation(t *testing.T) {
	_, err := NewPgStore(nil)
	require.Error(t, err)

	_, err = NewPgStore(testpg.Pool(t), WithPgStoreSchema("bad-schema"))
	require.Error(t, err)
}

func TestPgStoreEmptyFetchReturnsNoSnapshot(t *testing.T) {
	ctx := context.Background()
	store := newTestPgStore(t)

	_, unchanged, err := store.FetchMappedSplitMap(ctx, "missing", 0, nil)
	require.ErrorIs(t, err, snapshot.ErrNoSnapshot)
	require.False(t, unchanged)

	_, unchanged, err = store.FetchResource(ctx, "missing", "llm-generic", 0, nil)
	require.ErrorIs(t, err, snapshot.ErrNoSnapshot)
	require.False(t, unchanged)
}

func TestPgStorePublishFetchAndUnchanged(t *testing.T) {
	ctx := context.Background()
	store := newTestPgStore(t)
	server := testServerWithStore(store)

	result, err := server.PublishMappedSplit(ctx, testMappedSplitRequest("lane-a", 1))
	require.NoError(t, err)
	require.Equal(t, "lane-a", result.Lane)
	require.Equal(t, uint64(1), result.Map.Version)
	require.Len(t, result.Resources, 4)

	typedMap, unchanged, err := store.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, uint64(1), typedMap.Version)
	require.Equal(t, uint64(1), typedMap.Map.MapRevision)

	again, unchanged, err := store.FetchMappedSplitMap(ctx, "lane-a", typedMap.Version, typedMap.Checksum)
	require.NoError(t, err)
	require.True(t, unchanged)
	require.Nil(t, again)

	envelope, unchanged, err := store.FetchResource(ctx, "lane-a", "llm-generic", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, uint64(1), envelope.Version)
	require.NotEmpty(t, envelope.Payload)

	envelopeAgain, unchanged, err := store.FetchResource(ctx, "lane-a", "llm-generic", envelope.Version, envelope.Checksum)
	require.NoError(t, err)
	require.True(t, unchanged)
	require.Nil(t, envelopeAgain)
}

func TestPgStoreRepublishUnchangedComponentsReusesResourceVersions(t *testing.T) {
	ctx := context.Background()
	store := newTestPgStore(t)
	server := testServerWithStore(store)

	_, err := server.PublishMappedSplit(ctx, testMappedSplitRequest("lane-a", 1))
	require.NoError(t, err)
	firstResource, unchanged, err := store.FetchResource(ctx, "lane-a", "llm-generic", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)

	_, err = server.PublishMappedSplit(ctx, testMappedSplitRequest("lane-a", 2))
	require.NoError(t, err)

	secondMap, unchanged, err := store.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, uint64(2), secondMap.Version)
	require.Equal(t, uint64(2), secondMap.Map.MapRevision)

	secondResource, unchanged, err := store.FetchResource(ctx, "lane-a", "llm-generic", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, firstResource.Version, secondResource.Version)
	require.Equal(t, firstResource.Checksum, secondResource.Checksum)
}

func TestPgStoreFailedPublishKeepsPreviousCurrent(t *testing.T) {
	ctx := context.Background()
	store := newTestPgStore(t)
	server := testServerWithStore(store)

	_, err := server.PublishMappedSplit(ctx, testMappedSplitRequest("lane-a", 1))
	require.NoError(t, err)
	before, unchanged, err := store.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)

	builder := mappedsplit.NewBuilder(mappedsplit.BuildOptions{Producer: "pg-store-test"})
	out, err := builder.Build(ctx, mappedsplit.BuildRequest(testMappedSplitRequest("lane-a", 2)))
	require.NoError(t, err)
	componentName := out.ComponentSeq[1]
	badComponent := out.Components[componentName]
	badComponent.Payload = nil
	out.Components[componentName] = badComponent

	_, err = store.PublishMappedSplit(ctx, out)
	require.Error(t, err)

	after, unchanged, err := store.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	assert.Equal(t, before.Version, after.Version)
	assert.Equal(t, before.Checksum, after.Checksum)
	assert.Equal(t, before.Map.MapRevision, after.Map.MapRevision)
}

func TestPgStoreMultiReplicaReads(t *testing.T) {
	ctx := context.Background()
	schema := testPgSchema(t)
	pool := testpg.Pool(t)
	require.NoError(t, migration.Migrate(ctx, pool, migration.WithSchema(schema)))

	storeA, err := NewPgStore(pool, WithPgStoreSchema(schema))
	require.NoError(t, err)
	storeB, err := NewPgStore(pool, WithPgStoreSchema(schema))
	require.NoError(t, err)

	serverA := testServerWithStore(storeA)
	serverB := testServerWithStore(storeB)

	_, err = serverA.PublishMappedSplit(ctx, testMappedSplitRequest("lane-a", 1))
	require.NoError(t, err)

	mapFromB, unchanged, err := storeB.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, uint64(1), mapFromB.Version)

	resourceFromB, unchanged, err := storeB.FetchResource(ctx, "lane-a", "llm-generic", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, uint64(1), resourceFromB.Version)

	_, err = serverB.PublishMappedSplit(ctx, testMappedSplitRequest("lane-a", 2))
	require.NoError(t, err)

	mapFromA, unchanged, err := storeA.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, uint64(2), mapFromA.Version)
	require.Equal(t, uint64(2), mapFromA.Map.MapRevision)
}

func TestPgStoreColdStartConcurrentFetchesUseLease(t *testing.T) {
	ctx := context.Background()
	schema := testPgSchema(t)
	pool := testpg.Pool(t)
	require.NoError(t, migration.Migrate(ctx, pool, migration.WithSchema(schema)))

	const callers = 8
	var builds atomic.Int64
	start := make(chan struct{})
	buildEntered := make(chan struct{})
	var closeBuildEntered sync.Once

	var wg sync.WaitGroup
	wg.Add(callers)
	successes := make(chan uint64, callers)
	notFound := make(chan struct{}, callers)
	errs := make(chan error, callers)
	for i := range callers {
		store, err := NewPgStore(
			pool,
			WithPgStoreSchema(schema),
			WithPgStoreBuildLeaseHolderID(fmt.Sprintf("cold-start-holder-%02d", i)),
			WithPgStoreBuildLeaseDuration(time.Second),
			WithPgStoreBuildHeartbeatInterval(20*time.Millisecond),
		)
		require.NoError(t, err)
		server := testServerWithOptions(ServerOptions{
			Store: store,
			OnDemandBuild: func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
				builds.Add(1)
				closeBuildEntered.Do(func() { close(buildEntered) })
				time.Sleep(50 * time.Millisecond)
				return testMappedSplitRequest(req.Lane, 1), nil
			},
		})
		client, cleanup := startServerClient(t, server)
		defer cleanup()

		go func() {
			defer wg.Done()
			<-start
			req := connectRequestFetchMap()
			req.Header().Set("x-orange-lane", "lane-a")
			resp, err := client.FetchMappedSplitMap(ctx, req)
			if err != nil {
				if connect.CodeOf(err) == connect.CodeNotFound {
					notFound <- struct{}{}
					return
				}
				errs <- err
				return
			}
			snap := resp.Msg.GetSnapshot()
			if snap == nil {
				errs <- fmt.Errorf("cold-start fetch returned no snapshot")
				return
			}
			successes <- snap.Map.MapRevision
		}()
	}

	close(start)
	select {
	case <-buildEntered:
	case <-time.After(time.Second):
		t.Fatal("no cold-start build entered")
	}
	wg.Wait()
	close(successes)
	close(notFound)
	close(errs)

	require.Empty(t, errs)
	require.Equal(t, int64(1), builds.Load())
	require.NotEmpty(t, successes)
	require.Equal(t, callers, len(successes)+len(notFound))
	for revision := range successes {
		require.Equal(t, uint64(1), revision)
	}

	store := newTestPgStoreWithOptions(t, WithPgStoreSchema(schema))
	current, unchanged, err := store.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, uint64(1), current.Map.MapRevision)
}

func TestPgStoreBuildRequestCoalescesByLane(t *testing.T) {
	ctx := context.Background()
	store := newTestPgStore(t)

	const calls = 32
	var wg sync.WaitGroup
	wg.Add(calls)
	for i := range calls {
		go func() {
			defer wg.Done()
			err := store.MarkMappedSplitDirty(ctx, BuildRequest{
				Lane:           "lane-a",
				RequestedBy:    fmt.Sprintf("replica-%02d", i),
				SourceRevision: fmt.Sprintf("rev-%02d", i),
				ChangeHint:     fmt.Sprintf("hint-%02d", i),
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	require.Equal(t, 1, countPgRows(t, store, "orange_mapped_split_build_requests", "lane = 'lane-a'"))

	err := store.MarkMappedSplitDirty(ctx, BuildRequest{
		Lane:           "lane-a",
		RequestedBy:    "replica-final",
		SourceRevision: "rev-final",
		ChangeHint:     "hint-final",
	})
	require.NoError(t, err)

	req, err := store.GetMappedSplitBuildRequest(ctx, "lane-a")
	require.NoError(t, err)
	require.Equal(t, &BuildRequest{
		Lane:           "lane-a",
		RequestedBy:    "replica-final",
		SourceRevision: "rev-final",
		ChangeHint:     "hint-final",
	}, req)

	require.NoError(t, store.MarkMappedSplitDirty(ctx, BuildRequest{Lane: "lane-b", RequestedBy: "other"}))
	require.Equal(t, 1, countPgRows(t, store, "orange_mapped_split_build_requests", "lane = 'lane-b'"))

	err = store.MarkMappedSplitDirty(ctx, BuildRequest{})
	require.Error(t, err)
}

func TestPgStoreBuildLeaseAllowsOneHolderPerLane(t *testing.T) {
	ctx := context.Background()
	schema := testPgSchema(t)
	pool := testpg.Pool(t)
	require.NoError(t, migration.Migrate(ctx, pool, migration.WithSchema(schema)))

	holder, err := NewPgStore(
		pool,
		WithPgStoreSchema(schema),
		WithPgStoreBuildLeaseHolderID("holder-active"),
		WithPgStoreBuildLeaseDuration(time.Second),
		WithPgStoreBuildHeartbeatInterval(50*time.Millisecond),
	)
	require.NoError(t, err)

	entered := make(chan struct{})
	release := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- holder.WithMappedSplitBuildLease(ctx, "lane-a", func(context.Context, BuildLease) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	const contenders = 7
	var unexpectedCallbacks atomic.Int64
	var wg sync.WaitGroup
	wg.Add(contenders)
	for i := range contenders {
		store := newTestPgStoreWithOptions(t,
			WithPgStoreSchema(schema),
			WithPgStoreBuildLeaseHolderID(fmt.Sprintf("holder-%02d", i)),
			WithPgStoreBuildLeaseDuration(time.Second),
			WithPgStoreBuildHeartbeatInterval(50*time.Millisecond),
		)
		go func() {
			defer wg.Done()
			err := store.WithMappedSplitBuildLease(ctx, "lane-a", func(context.Context, BuildLease) error {
				unexpectedCallbacks.Add(1)
				return nil
			})
			assert.ErrorIs(t, err, ErrBuildLeaseHeld)
		}()
	}
	wg.Wait()
	require.Equal(t, int64(0), unexpectedCallbacks.Load())

	close(release)
	require.NoError(t, <-holderDone)
}

func TestPgStoreBuildLeaseAllowsDifferentLanesConcurrently(t *testing.T) {
	ctx := context.Background()
	schema := testPgSchema(t)
	pool := testpg.Pool(t)
	require.NoError(t, migration.Migrate(ctx, pool, migration.WithSchema(schema)))
	storeA := newTestPgStoreWithOptions(t,
		WithPgStoreSchema(schema),
		WithPgStoreBuildLeaseHolderID("holder-a"),
		WithPgStoreBuildLeaseDuration(time.Second),
		WithPgStoreBuildHeartbeatInterval(50*time.Millisecond),
	)
	storeB := newTestPgStoreWithOptions(t,
		WithPgStoreSchema(schema),
		WithPgStoreBuildLeaseHolderID("holder-b"),
		WithPgStoreBuildLeaseDuration(time.Second),
		WithPgStoreBuildHeartbeatInterval(50*time.Millisecond),
	)

	entered := make(chan string, 2)
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		require.NoError(t, storeA.WithMappedSplitBuildLease(ctx, "lane-a", func(context.Context, BuildLease) error {
			entered <- "lane-a"
			<-release
			return nil
		}))
	}()
	go func() {
		defer wg.Done()
		require.NoError(t, storeB.WithMappedSplitBuildLease(ctx, "lane-b", func(context.Context, BuildLease) error {
			entered <- "lane-b"
			<-release
			return nil
		}))
	}()

	require.Eventually(t, func() bool {
		return len(entered) == 2
	}, time.Second, 10*time.Millisecond)
	close(release)
	wg.Wait()
}

func TestPgStoreBuildLeaseExpiryFencesHolderWithoutTakeover(t *testing.T) {
	ctx := context.Background()
	schema := testPgSchema(t)
	pool := testpg.Pool(t)
	require.NoError(t, migration.Migrate(ctx, pool, migration.WithSchema(schema)))
	store := newTestPgStoreWithOptions(t,
		WithPgStoreSchema(schema),
		WithPgStoreBuildLeaseHolderID("holder-a"),
		WithPgStoreBuildLeaseDuration(40*time.Millisecond),
		WithPgStoreBuildHeartbeatInterval(time.Second),
	)
	require.NoError(t, store.MarkMappedSplitDirty(ctx, BuildRequest{Lane: "lane-a", RequestedBy: "test"}))

	err := store.WithMappedSplitBuildLease(ctx, "lane-a", func(ctx context.Context, lease BuildLease) error {
		time.Sleep(time.Until(lease.LockedUntil.Add(30 * time.Millisecond)))

		out := testBuildOutput(t, ctx, "lane-a", 1)
		_, err := store.PublishMappedSplitWithLease(ctx, lease, out)
		require.ErrorIs(t, err, ErrBuildLeaseLost)

		err = store.ClearMappedSplitDirty(ctx, lease, 0)
		require.ErrorIs(t, err, ErrBuildLeaseLost)
		return nil
	})
	require.NoError(t, err)

	_, unchanged, err := store.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
	require.ErrorIs(t, err, snapshot.ErrNoSnapshot)
	require.False(t, unchanged)
	req, err := store.GetMappedSplitBuildRequest(ctx, "lane-a")
	require.NoError(t, err)
	require.NotNil(t, req)
}

func TestPgStoreBuildLeaseTakeoverFencesStaleHolder(t *testing.T) {
	ctx := context.Background()
	schema := testPgSchema(t)
	pool := testpg.Pool(t)
	require.NoError(t, migration.Migrate(ctx, pool, migration.WithSchema(schema)))

	storeA := newTestPgStoreWithOptions(t,
		WithPgStoreSchema(schema),
		WithPgStoreBuildLeaseHolderID("holder-a"),
		WithPgStoreBuildLeaseDuration(50*time.Millisecond),
		WithPgStoreBuildHeartbeatInterval(time.Second),
	)
	storeB := newTestPgStoreWithOptions(t,
		WithPgStoreSchema(schema),
		WithPgStoreBuildLeaseHolderID("holder-b"),
		WithPgStoreBuildLeaseDuration(time.Second),
		WithPgStoreBuildHeartbeatInterval(50*time.Millisecond),
	)
	require.NoError(t, storeA.MarkMappedSplitDirty(ctx, BuildRequest{
		Lane:           "lane-a",
		RequestedBy:    "test",
		SourceRevision: "rev-1",
		ChangeHint:     "initial",
	}))

	aAcquired := make(chan BuildLease, 1)
	bDone := make(chan PublishResult, 1)
	aStalePublishErr := make(chan error, 1)
	aStaleClearErr := make(chan error, 1)
	aDone := make(chan error, 1)

	go func() {
		aDone <- storeA.WithMappedSplitBuildLease(ctx, "lane-a", func(ctx context.Context, lease BuildLease) error {
			aAcquired <- lease
			result := <-bDone

			staleOut := testBuildOutput(t, ctx, "lane-a", 2)
			_, err := storeA.PublishMappedSplitWithLease(ctx, lease, staleOut)
			aStalePublishErr <- err

			aStaleClearErr <- storeA.ClearMappedSplitDirty(ctx, lease, result.Map.Version)
			return nil
		})
	}()

	leaseA := <-aAcquired
	time.Sleep(time.Until(leaseA.LockedUntil.Add(30 * time.Millisecond)))

	var resultB PublishResult
	err := storeB.WithMappedSplitBuildLease(ctx, "lane-a", func(ctx context.Context, lease BuildLease) error {
		out := testBuildOutput(t, ctx, "lane-a", 1)
		result, err := storeB.PublishMappedSplitWithLease(ctx, lease, out)
		if err != nil {
			return err
		}
		resultB = result
		return storeB.ClearMappedSplitDirty(ctx, lease, result.Map.Version)
	})
	require.NoError(t, err)
	bDone <- resultB

	require.ErrorIs(t, <-aStalePublishErr, ErrBuildLeaseLost)
	require.ErrorIs(t, <-aStaleClearErr, ErrBuildLeaseLost)
	require.NoError(t, <-aDone)

	req, err := storeB.GetMappedSplitBuildRequest(ctx, "lane-a")
	require.NoError(t, err)
	require.Nil(t, req)

	current, unchanged, err := storeB.FetchMappedSplitMap(ctx, "lane-a", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, resultB.Map.Version, current.Version)
	require.Equal(t, uint64(1), current.Map.MapRevision)
}

func newTestPgStore(t *testing.T) *PgStore {
	t.Helper()
	ctx := context.Background()
	schema := testPgSchema(t)
	pool := testpg.Pool(t)
	require.NoError(t, migration.Migrate(ctx, pool, migration.WithSchema(schema)))
	store, err := NewPgStore(pool, WithPgStoreSchema(schema))
	require.NoError(t, err)
	return store
}

func newTestPgStoreWithOptions(t *testing.T, opts ...PgStoreOption) *PgStore {
	t.Helper()
	store, err := NewPgStore(testpg.Pool(t), opts...)
	require.NoError(t, err)
	return store
}

func testBuildOutput(t *testing.T, ctx context.Context, lane string, revision int) MappedSplitPublication {
	t.Helper()
	builder := mappedsplit.NewBuilder(mappedsplit.BuildOptions{Producer: "pg-store-test"})
	out, err := builder.Build(ctx, mappedsplit.BuildRequest(testMappedSplitRequest(lane, revision)))
	require.NoError(t, err)
	return out
}

func countPgRows(t *testing.T, store *PgStore, table string, where string) int {
	t.Helper()
	if !pgStoreIdentifierPattern.MatchString(table) {
		t.Fatalf("unsafe table identifier %q", table)
	}
	tx, err := store.beginTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()

	var count int
	err = tx.QueryRow(context.Background(), fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", table, where)).Scan(&count)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(context.Background()))
	return count
}

func testServerWithStore(store Store) *Server {
	server := testServer()
	server.store = store
	return server
}

func testPgSchema(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(t.Name())
	name = regexp.MustCompile(`[^a-z0-9_]+`).ReplaceAllString(name, "_")
	name = strings.Trim(name, "_")
	return fmt.Sprintf("orange_%s_%d", name, os.Getpid())
}
