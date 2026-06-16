package config

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/api/orange/config/v1/configv1connect"
)

func TestServerPublishMappedSplitFetchesMapAndResource(t *testing.T) {
	s := testServer()

	result, err := s.PublishMappedSplit(context.Background(), testMappedSplitRequest("lane-a", 1))
	require.NoError(t, err)
	require.Equal(t, "lane-a", result.Lane)
	require.NotNil(t, result.Map)

	typedMap, unchanged, err := s.Store().FetchMappedSplitMap(context.Background(), "lane-a", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.Equal(t, uint64(5), typedMap.Version)
	require.Equal(t, uint64(1), typedMap.Map.MapRevision)

	again, unchanged, err := s.Store().FetchMappedSplitMap(context.Background(), "lane-a", typedMap.Version, typedMap.Checksum)
	require.NoError(t, err)
	require.True(t, unchanged)
	require.Nil(t, again)

	envelope, unchanged, err := s.Store().FetchResource(context.Background(), "lane-a", "llm-generic", 0, nil)
	require.NoError(t, err)
	require.False(t, unchanged)
	require.NotEmpty(t, envelope.Payload)

	envelopeAgain, unchanged, err := s.Store().FetchResource(context.Background(), "lane-a", "llm-generic", envelope.Version, envelope.Checksum)
	require.NoError(t, err)
	require.True(t, unchanged)
	require.Nil(t, envelopeAgain)
}

func TestServerMountServesSnapshotService(t *testing.T) {
	s := testServer()
	_, err := s.PublishMappedSplit(context.Background(), testMappedSplitRequest("lane-a", 1))
	require.NoError(t, err)

	mux := http.NewServeMux()
	path := s.Mount(mux)
	require.Equal(t, "/orange.config.v1.SnapshotService/", path)

	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	client := configv1connect.NewSnapshotServiceClient(httpServer.Client(), httpServer.URL)
	req := connectRequestFetchMap()
	req.Header().Set("x-orange-lane", "lane-a")

	resp, err := client.FetchMappedSplitMap(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp.Msg.GetSnapshot())
	require.Equal(t, uint64(1), resp.Msg.GetSnapshot().Map.MapRevision)
}

func TestServerDefaultAuthFailsClosed(t *testing.T) {
	s := NewServer(ServerOptions{})
	_, err := s.PublishMappedSplit(context.Background(), testMappedSplitRequest("lane-a", 1))
	require.NoError(t, err)

	mux := http.NewServeMux()
	s.Mount(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	client := configv1connect.NewSnapshotServiceClient(httpServer.Client(), httpServer.URL)
	_, err = client.FetchMappedSplitMap(context.Background(), connectRequestFetchMap())
	require.Error(t, err)
}

func TestServerUsesConfiguredStore(t *testing.T) {
	store := &recordingStore{Store: NewMemoryStore()}
	s := NewServer(ServerOptions{
		Producer: "config-server-test",
		Authenticator: AuthenticatorFunc(func(_ context.Context, _ http.Header) (ServerPrincipal, error) {
			return ServerPrincipal{ID: "lane-a"}, nil
		}),
		LaneResolver: LaneResolverFunc(func(_ context.Context, principal ServerPrincipal) (string, error) { return principal.ID, nil }),
		Store:        store,
	})

	_, err := s.PublishMappedSplit(context.Background(), testMappedSplitRequest("lane-a", 1))
	require.NoError(t, err)
	require.Equal(t, int32(1), store.publishCalls.Load())
}

func TestServerColdStartConcurrentFetchesBuildOnce(t *testing.T) {
	store := NewMemoryStore()
	var builds atomic.Int32
	s := testServerWithOptions(ServerOptions{
		Store: store,
		OnDemandBuild: func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
			builds.Add(1)
			time.Sleep(20 * time.Millisecond)
			return testMappedSplitRequest(req.Lane, 1), nil
		},
	})

	client, cleanup := startServerClient(t, s)
	defer cleanup()

	const callers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	revisions := make(chan uint64, callers)
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			<-start
			req := connectRequestFetchMap()
			req.Header().Set("x-orange-lane", "lane-a")
			resp, err := client.FetchMappedSplitMap(context.Background(), req)
			if err != nil {
				errs <- err
				return
			}
			snap := resp.Msg.GetSnapshot()
			if snap == nil {
				errs <- context.Canceled
				return
			}
			revisions <- snap.Map.MapRevision
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	close(revisions)

	require.Empty(t, errs)
	require.Equal(t, int32(1), builds.Load())
	require.Len(t, revisions, callers)
	for revision := range revisions {
		require.Equal(t, uint64(1), revision)
	}
}

func TestServerColdStartSkipsCallbackWhenCurrentAppearsWhileWaitingForLease(t *testing.T) {
	store := &waitingLeaseStore{
		MemoryStore:    NewMemoryStore(),
		leaseRequested: make(chan struct{}),
		allowLease:     make(chan struct{}),
	}
	var builds atomic.Int32
	s := testServerWithOptions(ServerOptions{
		Store: store,
		OnDemandBuild: func(_ context.Context, req BuildRequest) (MappedSplitRequest, error) {
			builds.Add(1)
			return testMappedSplitRequest(req.Lane, 2), nil
		},
	})
	client, cleanup := startServerClient(t, s)
	defer cleanup()

	respCh := make(chan *configv1.FetchMappedSplitMapResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		req := connectRequestFetchMap()
		req.Header().Set("x-orange-lane", "lane-a")
		resp, err := client.FetchMappedSplitMap(context.Background(), req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp.Msg
	}()

	select {
	case <-store.leaseRequested:
	case <-time.After(time.Second):
		t.Fatal("fetch did not wait for build lease")
	}

	_, err := store.PublishMappedSplit(context.Background(), testBuildOutput(t, context.Background(), "lane-a", 1))
	require.NoError(t, err)
	close(store.allowLease)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case resp := <-respCh:
		snap := resp.GetSnapshot()
		require.NotNil(t, snap)
		require.Equal(t, uint64(1), snap.Map.MapRevision)
	case <-time.After(time.Second):
		t.Fatal("fetch did not complete")
	}
	require.Equal(t, int32(0), builds.Load())
}

func testServer() *Server {
	return testServerWithOptions(ServerOptions{})
}

func testServerWithOptions(opts ServerOptions) *Server {
	if opts.Producer == "" {
		opts.Producer = "config-server-test"
	}
	if opts.Clock == nil {
		opts.Clock = func() time.Time { return time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC) }
	}
	if opts.Authenticator == nil {
		opts.Authenticator = AuthenticatorFunc(func(_ context.Context, header http.Header) (ServerPrincipal, error) {
			lane := header.Get("x-orange-lane")
			if lane == "" {
				return ServerPrincipal{}, ErrUnauthenticated
			}
			return ServerPrincipal{ID: lane}, nil
		})
	}
	if opts.LaneResolver == nil {
		opts.LaneResolver = LaneResolverFunc(func(_ context.Context, principal ServerPrincipal) (string, error) {
			if principal.ID == "" {
				return "", ErrPermissionDenied
			}
			return principal.ID, nil
		})
	}
	return NewServer(opts)
}

func startServerClient(t *testing.T, s *Server) (configv1connect.SnapshotServiceClient, func()) {
	t.Helper()
	mux := http.NewServeMux()
	s.Mount(mux)
	httpServer := httptest.NewServer(mux)
	cleanup := func() { httpServer.Close() }
	return configv1connect.NewSnapshotServiceClient(httpServer.Client(), httpServer.URL), cleanup
}

type waitingLeaseStore struct {
	*MemoryStore
	leaseRequested chan struct{}
	allowLease     chan struct{}
	entered        atomic.Bool
}

func (s *waitingLeaseStore) WithMappedSplitBuildLease(ctx context.Context, lane string, fn func(context.Context, BuildLease) error) error {
	if s.entered.CompareAndSwap(false, true) {
		close(s.leaseRequested)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.allowLease:
		return fn(ctx, BuildLease{Lane: lane, HolderID: "waiting-store", LeaseVersion: 1})
	}
}

type recordingStore struct {
	Store
	publishCalls atomic.Int32
}

func (s *recordingStore) PublishMappedSplit(ctx context.Context, publication MappedSplitPublication) (PublishResult, error) {
	s.publishCalls.Add(1)
	return s.Store.PublishMappedSplit(ctx, publication)
}

func testMappedSplitRequest(lane string, revision int) MappedSplitRequest {
	spec := MappedSplitSpec{LLMUserKeyPartitions: 1, MCPUserProfilePartitions: 1}
	llmGeneric, err := spec.CatalogBundle(MappedSplitLaneLLMGeneric)
	if err != nil {
		panic(err)
	}
	mcpServers, err := spec.CatalogBundle(MappedSplitLaneMCPServers)
	if err != nil {
		panic(err)
	}
	return MappedSplitRequest{
		Selection:               Selection{ScopeKind: "workspace", ScopeID: "prod"},
		Lane:                    lane,
		Scopes:                  []string{"prod"},
		SourceRevision:          "rev-1",
		GenerationID:            "gen-1",
		MapRevision:             revision,
		LLMDefaultPrincipalSlug: "slug:default",
		Spec:                    spec,
		Components: []ComponentInput{
			{Key: llmGeneric, Input: testLLMInput("slug:default", "env://OPENAI_PLATFORM")},
			{Key: mcpServers, Input: testMCPInput("s/github", "env://GITHUB_PLATFORM")},
			{Key: MappedSplitBundleKey{Lane: MappedSplitLaneLLMUserKey, Partition: 0}, Input: testLLMInput("slug:alice", "orange://alice/openai")},
			{Key: MappedSplitBundleKey{Lane: MappedSplitLaneMCPUserProfile, Partition: 0}, Input: testMCPInput("profile-dev-tools", "orange://alice/github")},
		},
	}
}

func testLLMInput(slug string, secretRef string) Input {
	return Input{
		Providers: []Provider{{ID: "openai", Kind: "openai", BackendSchema: "openai", Endpoint: "https://api.openai.example", SecretRef: "env://OPENAI_PLATFORM", AuthType: "bearer"}},
		Models:    []Model{{ID: "gpt-4o-mini", Provider: "openai", Name: "gpt-4o-mini", Mode: "chat"}},
		Scopes: []Scope{{
			ID: "prod",
			Principals: []Principal{{
				Slug: slug,
				ModelRoutes: map[string]RoutePlan{
					"gpt-4o-mini": {Kind: RouteKindTarget, Provider: "openai", Model: "gpt-4o-mini", SecretRef: secretRef},
				},
				Rate: RatePolicy{USDPerDayCents: 1000, RPM: 60, OnExceed: "reject"},
			}},
		}},
	}
}

func testMCPInput(path string, secretRef string) Input {
	return Input{
		MCPServers: []MCPServer{{ID: "github", Endpoint: "https://mcp.github.example", SecretRef: "env://GITHUB_PLATFORM", AuthType: "bearer"}},
		Scopes: []Scope{{
			ID: "prod",
			MCPProfiles: []MCPProfile{{
				Path: path,
				Tools: []MCPToolBinding{{
					ExposedName: "github__list_repos",
					Server:      "github",
					Tool:        "list_repos",
					SecretRef:   secretRef,
					AuthType:    "bearer",
				}},
			}},
		}},
	}
}

func connectRequestFetchMap() *connect.Request[configv1.FetchMappedSplitMapRequest] {
	return connect.NewRequest(&configv1.FetchMappedSplitMapRequest{})
}
