package config

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/api/orange/config/v1/configv1connect"
	"github.com/dio/orange/mappedsplit"
	"github.com/dio/orange/producer"
	"github.com/dio/orange/snapshot"
)

const snapshotServiceBufSize = 1 << 20

func startSnapshotService(t *testing.T, svc *SnapshotService) configv1connect.SnapshotServiceClient {
	t.Helper()
	mux := http.NewServeMux()
	path, handler := configv1connect.NewSnapshotServiceHandler(svc)
	mux.Handle(path, handler)

	lis := bufconn.Listen(snapshotServiceBufSize)
	srv := &http.Server{Handler: mux}
	t.Cleanup(func() { _ = srv.Close(); _ = lis.Close() })
	go func() { _ = srv.Serve(lis) }()

	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			},
		},
	}
	return configv1connect.NewSnapshotServiceClient(httpClient, "http://bufconn")
}

func TestSnapshotServiceFetchMappedSplitBundleReturnsSnapshot(t *testing.T) {
	bundles := staticBundleProvider{lane: "default", resource: "llm-generic", envelope: serviceBundleEnvelope(t, 1)}
	svc := NewSnapshotServiceWithProviders(bundles,
		&serviceAuthenticator{principal: ServerPrincipal{ID: "p1"}},
		&serviceLaneResolver{lane: "default"},
		nil,
	)
	client := startSnapshotService(t, svc)

	resp, err := client.FetchMappedSplitBundle(context.Background(), connect.NewRequest(&configv1.FetchMappedSplitBundleRequest{
		Resource: "llm-generic",
	}))
	require.NoError(t, err)

	snap := resp.Msg.GetSnapshot()
	require.NotNil(t, snap)
	assert.Greater(t, snap.Version, uint64(0))
	assert.NotEmpty(t, snap.Checksum)
	assert.NotEmpty(t, snap.Payload)
}

func TestSnapshotServiceFetchMappedSplitBundleUnchangedWhenVersionChecksumMatch(t *testing.T) {
	bundles := staticBundleProvider{lane: "default", resource: "llm-generic", envelope: serviceBundleEnvelope(t, 1)}
	svc := NewSnapshotServiceWithProviders(bundles,
		&serviceAuthenticator{principal: ServerPrincipal{ID: "p1"}},
		&serviceLaneResolver{lane: "default"},
		nil,
	)
	client := startSnapshotService(t, svc)

	resp1, err := client.FetchMappedSplitBundle(context.Background(), connect.NewRequest(&configv1.FetchMappedSplitBundleRequest{
		Resource: "llm-generic",
	}))
	require.NoError(t, err)
	snap := resp1.Msg.GetSnapshot()
	require.NotNil(t, snap)

	resp2, err := client.FetchMappedSplitBundle(context.Background(), connect.NewRequest(&configv1.FetchMappedSplitBundleRequest{
		Resource:     "llm-generic",
		LastVersion:  snap.Version,
		LastChecksum: snap.Checksum,
	}))
	require.NoError(t, err)
	assert.NotNil(t, resp2.Msg.GetUnchanged())
	assert.Nil(t, resp2.Msg.GetSnapshot())
}

func TestSnapshotServiceFetchMappedSplitBundleValidatesRequest(t *testing.T) {
	bundles := staticBundleProvider{lane: "default", resource: "llm-generic", envelope: serviceBundleEnvelope(t, 1)}
	svc := NewSnapshotServiceWithProviders(bundles,
		&serviceAuthenticator{principal: ServerPrincipal{ID: "p1"}},
		&serviceLaneResolver{lane: "default"},
		nil,
	)
	client := startSnapshotService(t, svc)

	_, err := client.FetchMappedSplitBundle(context.Background(), connect.NewRequest(&configv1.FetchMappedSplitBundleRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeInvalidArgument, ce.Code())

	_, err = client.FetchMappedSplitBundle(context.Background(), connect.NewRequest(&configv1.FetchMappedSplitBundleRequest{
		Resource:     "llm-generic",
		LastChecksum: []byte("not-32-bytes"),
	}))
	require.Error(t, err)
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeInvalidArgument, ce.Code())
}

func TestSnapshotServiceFetchMappedSplitMapReturnsTypedMapAndUnchanged(t *testing.T) {
	bundles := staticBundleProvider{lane: "default", resource: "llm-generic", envelope: serviceBundleEnvelope(t, 1)}
	provider := staticMappedSplitProvider{lane: "default", snapshot: serviceMappedSplitSnapshot(t, 3)}
	svc := NewSnapshotServiceWithProviders(bundles,
		&serviceAuthenticator{principal: ServerPrincipal{ID: "p1"}},
		&serviceLaneResolver{lane: "default"},
		provider,
	)
	client := startSnapshotService(t, svc)

	resp1, err := client.FetchMappedSplitMap(context.Background(), connect.NewRequest(&configv1.FetchMappedSplitMapRequest{}))
	require.NoError(t, err)
	typedMap := resp1.Msg.GetSnapshot()
	require.NotNil(t, typedMap)
	assert.Equal(t, uint64(3), typedMap.Version)

	resp2, err := client.FetchMappedSplitMap(context.Background(), connect.NewRequest(&configv1.FetchMappedSplitMapRequest{
		LastVersion:  typedMap.Version,
		LastChecksum: typedMap.Checksum,
	}))
	require.NoError(t, err)
	assert.NotNil(t, resp2.Msg.GetUnchanged())
}

func TestSnapshotServiceFetchMappedSplitMapRequiresProvider(t *testing.T) {
	bundles := staticBundleProvider{lane: "default", resource: "llm-generic", envelope: serviceBundleEnvelope(t, 1)}
	svc := NewSnapshotServiceWithProviders(bundles,
		&serviceAuthenticator{principal: ServerPrincipal{ID: "p1"}},
		&serviceLaneResolver{lane: "default"},
		nil,
	)
	client := startSnapshotService(t, svc)

	_, err := client.FetchMappedSplitMap(context.Background(), connect.NewRequest(&configv1.FetchMappedSplitMapRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeUnimplemented, ce.Code())
}

func TestSnapshotServiceAuthAndLaneFailures(t *testing.T) {
	bundles := staticBundleProvider{lane: "default", resource: "llm-generic", envelope: serviceBundleEnvelope(t, 1)}
	client := startSnapshotService(t, NewSnapshotServiceWithProviders(bundles,
		FailClosedAuthenticator{},
		&serviceLaneResolver{lane: "default"},
		nil,
	))

	_, err := client.FetchMappedSplitBundle(context.Background(), connect.NewRequest(&configv1.FetchMappedSplitBundleRequest{
		Resource: "llm-generic",
	}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeUnauthenticated, ce.Code())

	client = startSnapshotService(t, NewSnapshotServiceWithProviders(bundles,
		&serviceAuthenticator{principal: ServerPrincipal{ID: "p1"}},
		&serviceLaneResolver{err: ErrPermissionDenied},
		nil,
	))
	_, err = client.FetchMappedSplitBundle(context.Background(), connect.NewRequest(&configv1.FetchMappedSplitBundleRequest{
		Resource: "llm-generic",
	}))
	require.Error(t, err)
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodePermissionDenied, ce.Code())
}

type staticBundleProvider struct {
	lane     string
	resource string
	envelope *configv1.SnapshotEnvelope
}

func (s staticBundleProvider) FetchResource(_ context.Context, lane string, resource string, lastVersion uint64, lastChecksum []byte) (*configv1.SnapshotEnvelope, bool, error) {
	if lane != s.lane || resource != s.resource {
		return nil, false, snapshot.ErrNoSnapshot
	}
	if lastVersion == s.envelope.Version && bytes.Equal(lastChecksum, s.envelope.Checksum) {
		return nil, true, nil
	}
	return proto.Clone(s.envelope).(*configv1.SnapshotEnvelope), false, nil
}

type staticMappedSplitProvider struct {
	lane     string
	snapshot *configv1.MappedSplitSnapshot
}

func (s staticMappedSplitProvider) FetchMappedSplitMap(
	_ context.Context,
	lane string,
	lastVersion uint64,
	lastChecksum []byte,
) (*configv1.MappedSplitSnapshot, bool, error) {
	if lane != s.lane {
		return nil, false, snapshot.ErrNoSnapshot
	}
	if lastVersion == s.snapshot.Version && bytes.Equal(lastChecksum, s.snapshot.Checksum) {
		return nil, true, nil
	}
	return proto.Clone(s.snapshot).(*configv1.MappedSplitSnapshot), false, nil
}

type serviceAuthenticator struct {
	principal ServerPrincipal
	err       error
}

func (a *serviceAuthenticator) Authenticate(_ context.Context, _ http.Header) (ServerPrincipal, error) {
	if a.err != nil {
		return ServerPrincipal{}, a.err
	}
	return a.principal, nil
}

type serviceLaneResolver struct {
	lane string
	err  error
}

func (r *serviceLaneResolver) ResolveLane(_ context.Context, _ ServerPrincipal) (string, error) {
	if r.err != nil {
		return "", r.err
	}
	return r.lane, nil
}

func serviceBundleEnvelope(t *testing.T, version uint64) *configv1.SnapshotEnvelope {
	t.Helper()
	builder := producer.NewBuilder(producer.Options{
		Producer: "service-test",
		Clock:    func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	})
	out, err := builder.Build(context.Background(),
		Selection{ScopeKind: "workspace", ScopeID: "ws-1"},
		"default",
		producer.BuildResult{
			SourceRevision: "rev-1",
			Scopes:         []string{"ws-1"},
			Input: Input{
				Providers: []Provider{{
					ID: "openai", Kind: "openai",
					Endpoint: "https://api.openai.com", SecretRef: "env://OPENAI_API_KEY",
				}},
				Models: []Model{{ID: "gpt-4o-mini", Provider: "openai", Name: "gpt-4o-mini"}},
				Scopes: []Scope{{
					ID: "ws-1",
					Principals: []Principal{{
						Slug:  "slug:1",
						Route: RoutePlan{Provider: "openai", Model: "gpt-4o-mini"},
						Rate:  RatePolicy{USDPerDayCents: 500, RPM: 30, OnExceed: "reject"},
					}},
				}},
			},
		})
	require.NoError(t, err)
	snap, err := snapshot.New(version, out.Payload, out.BundleZstd)
	require.NoError(t, err)
	return snap.Envelope
}

func serviceMappedSplitSnapshot(t *testing.T, version uint64) *configv1.MappedSplitSnapshot {
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
