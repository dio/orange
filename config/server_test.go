package config

import (
	"context"
	"net/http"
	"net/http/httptest"
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

func testServer() *Server {
	return NewServer(ServerOptions{
		Producer: "config-server-test",
		Clock:    func() time.Time { return time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC) },
		Authenticator: AuthenticatorFunc(func(_ context.Context, header http.Header) (ServerPrincipal, error) {
			lane := header.Get("x-orange-lane")
			if lane == "" {
				return ServerPrincipal{}, ErrUnauthenticated
			}
			return ServerPrincipal{ID: lane}, nil
		}),
		LaneResolver: LaneResolverFunc(func(_ context.Context, principal ServerPrincipal) (string, error) {
			if principal.ID == "" {
				return "", ErrPermissionDenied
			}
			return principal.ID, nil
		}),
	})
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
		Providers: []Provider{{ID: "openai", Kind: "openai", Endpoint: "https://api.openai.example", SecretRef: "env://OPENAI_PLATFORM", AuthType: "bearer"}},
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
