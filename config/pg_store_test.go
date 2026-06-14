package config

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dio/orange/config/postgres/migration"
	"github.com/dio/orange/internal/embeddedpg/testpg"
	"github.com/dio/orange/mappedsplit"
	"github.com/dio/orange/snapshot"
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
