package config

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/mappedsplit"
	"github.com/dio/orange/snapshot"
)

type fakeRPC struct {
	mu             sync.Mutex
	mapCalls       int
	bundleCalls    int
	headers        []http.Header
	mapResults     []*configv1.FetchMappedSplitMapResponse
	bundleResults  []*configv1.FetchMappedSplitBundleResponse
	mapErrors      []error
	bundleErrors   []error
	bundleRequests []*configv1.FetchMappedSplitBundleRequest
}

func (f *fakeRPC) FetchMappedSplitMap(
	_ context.Context,
	req *connect.Request[configv1.FetchMappedSplitMapRequest],
) (*connect.Response[configv1.FetchMappedSplitMapResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mapCalls++
	call := f.mapCalls
	f.headers = append(f.headers, req.Header())
	idx := call - 1
	if idx < len(f.mapErrors) && f.mapErrors[idx] != nil {
		return nil, f.mapErrors[idx]
	}
	if idx < len(f.mapResults) {
		return connect.NewResponse(f.mapResults[idx]), nil
	}
	return connect.NewResponse(f.mapResults[len(f.mapResults)-1]), nil
}

func (f *fakeRPC) FetchMappedSplitBundle(
	_ context.Context,
	req *connect.Request[configv1.FetchMappedSplitBundleRequest],
) (*connect.Response[configv1.FetchMappedSplitBundleResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bundleCalls++
	call := f.bundleCalls
	f.bundleRequests = append(f.bundleRequests, req.Msg)
	f.headers = append(f.headers, req.Header())
	idx := call - 1
	if idx < len(f.bundleErrors) && f.bundleErrors[idx] != nil {
		return nil, f.bundleErrors[idx]
	}
	if idx < len(f.bundleResults) {
		return connect.NewResponse(f.bundleResults[idx]), nil
	}
	return connect.NewResponse(f.bundleResults[len(f.bundleResults)-1]), nil
}

func TestClientFetchMapAndUnchanged(t *testing.T) {
	snapshot := testMapSnapshot(t, 1)
	rpc := &fakeRPC{mapResults: []*configv1.FetchMappedSplitMapResponse{
		{Result: &configv1.FetchMappedSplitMapResponse_Snapshot{Snapshot: snapshot}},
		{Result: &configv1.FetchMappedSplitMapResponse_Unchanged{Unchanged: &configv1.Unchanged{}}},
	}}
	c, err := NewClient(ClientOptions{RPCClient: rpc, RetryPolicy: RetryPolicy{MaxAttempts: 1}})
	require.NoError(t, err)

	first, err := c.FetchMap(context.Background())
	require.NoError(t, err)
	require.False(t, first.Unchanged)
	require.Equal(t, uint64(1), first.Version)
	require.Equal(t, 1, first.Map.MapRevision)

	second, err := c.FetchMap(context.Background())
	require.NoError(t, err)
	require.True(t, second.Unchanged)
	require.Equal(t, first.Checksum, second.Checksum)
}

func TestClientFetchBundleFetchesResource(t *testing.T) {
	rpc := &fakeRPC{bundleResults: []*configv1.FetchMappedSplitBundleResponse{
		{Result: &configv1.FetchMappedSplitBundleResponse_Snapshot{Snapshot: testBundleEnvelope(t, 7)}},
	}}
	c, err := NewClient(ClientOptions{RPCClient: rpc, RetryPolicy: RetryPolicy{MaxAttempts: 1}})
	require.NoError(t, err)

	result, err := c.FetchBundle(context.Background(), "llm-user-key-003")
	require.NoError(t, err)
	require.Equal(t, uint64(7), result.Version)
	require.NotEmpty(t, result.BundleZstd)
	require.Equal(t, "llm-user-key-003", rpc.bundleRequests[0].Resource)
}

func TestClientFetchBundleRequiresResource(t *testing.T) {
	c, err := NewClient(ClientOptions{RPCClient: &fakeRPC{}})
	require.NoError(t, err)

	_, err = c.FetchBundle(context.Background(), "")
	require.Error(t, err)
}

func TestClientFetchMapRetriesTransientError(t *testing.T) {
	rpc := &fakeRPC{
		mapErrors: []error{connect.NewError(connect.CodeUnavailable, errors.New("try again"))},
		mapResults: []*configv1.FetchMappedSplitMapResponse{
			{Result: &configv1.FetchMappedSplitMapResponse_Snapshot{Snapshot: testMapSnapshot(t, 2)}},
		},
	}
	c, err := NewClient(ClientOptions{
		RPCClient:      rpc,
		RetryPolicy:    RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond},
		AttemptTimeout: time.Second,
		sleep:          func(context.Context, time.Duration) error { return nil },
	})
	require.NoError(t, err)

	result, err := c.FetchMap(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(2), result.Version)
	require.Equal(t, 2, rpc.mapCalls)
}

func TestClientSyncWithServer(t *testing.T) {
	server := testServer()
	_, err := server.PublishMappedSplit(context.Background(), testMappedSplitRequest("lane-a", 1))
	require.NoError(t, err)

	mux := http.NewServeMux()
	server.Mount(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	client, err := NewClient(ClientOptions{
		BaseURL: httpServer.URL,
		HeaderFunc: func(_ context.Context, header http.Header) error {
			header.Set("x-orange-lane", "lane-a")
			return nil
		},
		RetryPolicy: RetryPolicy{MaxAttempts: 1},
	})
	require.NoError(t, err)

	first, err := client.Sync(context.Background())
	require.NoError(t, err)
	require.NotNil(t, first.Opened)
	require.Equal(t, 4, first.Stats.Fetched)
	require.Equal(t, "rev-1", first.Opened.LLMGeneric.SourceRevision)

	llm, ok := first.Opened.ResolveLLM("prod", "slug:alice", "gpt-4o-mini")
	require.True(t, ok)
	require.Equal(t, "orange://alice/openai", llm.SecretRef)

	second, err := client.Sync(context.Background())
	require.NoError(t, err)
	require.True(t, second.Unchanged)
	require.Equal(t, 1, second.Stats.Reused)
}

func testMapSnapshot(t *testing.T, version uint64) *configv1.MappedSplitSnapshot {
	t.Helper()
	snapshot, err := mappedsplit.NewMapSnapshot(version, SplitMap{
		FormatVersion:           mappedsplit.SplitMapFormatVersion,
		ScopeKind:               "workspace",
		ScopeID:                 "ws-1",
		Scopes:                  []string{"ws-1"},
		GenerationID:            "gen-1",
		MapRevision:             int(version),
		LLMDefaultPrincipalSlug: "slug:default",
		Partitioning: map[string]PartitionSpec{
			"llm-user-key":     {Algorithm: "fnv1a64", Key: "principal_slug", Partitions: 1},
			"mcp-user-profile": {Algorithm: "fnv1a64", Key: "path_suffix", Partitions: 1},
		},
		Bundles: map[string]BundleRef{
			"llm-generic": {ID: "llm-generic", Resource: "llm-generic", Component: "llm-generic", Checksum: 1, Size: 1},
			"mcp-servers": {ID: "mcp-servers", Resource: "mcp-servers", Component: "mcp-servers", Checksum: 2, Size: 1},
		},
		PartitionBundles: map[string][]PartitionBundleRef{
			"llm-user-key": {{
				Partition: 0,
				BundleRef: BundleRef{
					ID: "llm-user-key-000", Resource: "llm-user-key-000", Component: "llm-user-key-000", Checksum: 3, Size: 1,
				},
			}},
			"mcp-user-profile": {{
				Partition: 0,
				BundleRef: BundleRef{
					ID: "mcp-user-profile-000", Resource: "mcp-user-profile-000", Component: "mcp-user-profile-000", Checksum: 4, Size: 1,
				},
			}},
		},
	})
	require.NoError(t, err)
	return snapshot
}

func testBundleEnvelope(t *testing.T, version uint64) *configv1.SnapshotEnvelope {
	t.Helper()
	key := MappedSplitBundleKey{Lane: MappedSplitLaneLLMUserKey, Partition: 0}
	out, err := mappedsplit.NewBuilder(mappedsplit.BuildOptions{
		Producer: "client-test",
		Clock:    func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	}).Build(context.Background(), mappedsplit.BuildRequest{
		Selection:      Selection{ScopeKind: "workspace", ScopeID: "ws-1"},
		Lane:           "lane-a",
		Scopes:         []string{"ws-1"},
		SourceRevision: "rev-1",
		GenerationID:   "gen-1",
		MapRevision:    1,
		Spec:           MappedSplitSpec{LLMUserKeyPartitions: 1, MCPUserProfilePartitions: 1},
		Components: []ComponentInput{{
			Key: key,
			Input: Input{
				Providers: []Provider{{
					ID: "openai", Kind: "openai", BackendSchema: "openai", Endpoint: "https://api.openai.com", SecretRef: "env://OPENAI_API_KEY",
				}},
				Models: []Model{{ID: "gpt-4o-mini", Provider: "openai", Name: "gpt-4o-mini"}},
				Scopes: []Scope{{
					ID: "ws-1",
					Principals: []Principal{{
						Slug:  "slug:client",
						Route: RoutePlan{Provider: "openai", Model: "gpt-4o-mini"},
						Rate:  RatePolicy{USDPerDayCents: 1000, RPM: 60, OnExceed: "reject"},
					}},
				}},
			},
		}},
	})
	require.NoError(t, err)
	component := out.Components[key.Component()]
	snap, err := snapshot.New(version, component.Payload, component.BundleZstd)
	require.NoError(t, err)
	return snap.Envelope
}
