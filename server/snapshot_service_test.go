package server_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/api/orange/config/v1/configv1connect"
	"github.com/dio/orange/producer"
	"github.com/dio/orange/server"
	"github.com/dio/orange/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"

	cherry "github.com/dio/cherry"
)

const bufSize = 1 << 20 // 1 MiB

// startSnapshotServer mounts a SnapshotService on an in-memory bufconn listener
// and returns a Connect client wired to it.
func startSnapshotServer(t *testing.T, svc *server.SnapshotService) configv1connect.SnapshotServiceClient {
	t.Helper()
	mux := http.NewServeMux()
	path, handler := configv1connect.NewSnapshotServiceHandler(svc)
	mux.Handle(path, handler)

	lis := bufconn.Listen(bufSize)
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

// newTestManager builds a manager with a pre-published snapshot on the given lane.
func newTestManager(t *testing.T, lane string) *snapshot.Manager {
	t.Helper()
	b := producer.NewBuilder(producer.Options{
		Producer: "test",
		Clock:    func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	})
	cb := func(_ context.Context, req snapshot.MutationRequest) (producer.BuildResult, error) {
		return producer.BuildResult{
			SourceRevision: "rev-1",
			Scopes:         []string{"ws-1"},
			Input: cherry.Input{
				Providers: []cherry.Provider{{
					ID: "openai", Kind: "openai",
					Endpoint: "https://api.openai.com", SecretRef: "env://OPENAI_API_KEY",
				}},
				Models: []cherry.Model{{ID: "gpt-4o-mini", Provider: "openai", Name: "gpt-4o-mini"}},
				Scopes: []cherry.Scope{{
					ID: "ws-1",
					Principals: []cherry.Principal{{
						Slug:  "slug:1",
						Route: cherry.RoutePlan{Provider: "openai", Model: "gpt-4o-mini"},
						Rate:  cherry.RatePolicy{USDPerDayCents: 500, RPM: 30, OnExceed: "reject"},
					}},
				}},
			},
		}, nil
	}
	mgr := snapshot.NewManager(b, cb)
	_, err := mgr.Publish(context.Background(), snapshot.MutationRequest{
		Selection: producer.Selection{ScopeKind: "workspace", ScopeID: "ws-1"},
		Lane:      lane,
	})
	require.NoError(t, err)
	return mgr
}

// --- Tests ---

func TestFetchReturnsSnapshot(t *testing.T) {
	mgr := newTestManager(t, "default")
	svc := server.NewSnapshotService(mgr,
		&staticAuthenticator{principal: server.Principal{ID: "p1"}},
		&staticLaneResolver{lane: "default"},
	)
	client := startSnapshotServer(t, svc)

	resp, err := client.Fetch(context.Background(), connect.NewRequest(&configv1.FetchRequest{}))
	require.NoError(t, err)

	snap := resp.Msg.GetSnapshot()
	require.NotNil(t, snap, "expected snapshot, got unchanged")
	assert.Greater(t, snap.Version, uint64(0))
	assert.NotEmpty(t, snap.Checksum)
	assert.NotEmpty(t, snap.Payload)
}

func TestFetchUnchangedWhenVersionChecksumMatch(t *testing.T) {
	mgr := newTestManager(t, "default")
	svc := server.NewSnapshotService(mgr,
		&staticAuthenticator{principal: server.Principal{ID: "p1"}},
		&staticLaneResolver{lane: "default"},
	)
	client := startSnapshotServer(t, svc)

	// First fetch — get version and checksum.
	resp1, err := client.Fetch(context.Background(), connect.NewRequest(&configv1.FetchRequest{}))
	require.NoError(t, err)
	snap := resp1.Msg.GetSnapshot()
	require.NotNil(t, snap)

	// Second fetch with matching version and checksum — expect unchanged.
	resp2, err := client.Fetch(context.Background(), connect.NewRequest(&configv1.FetchRequest{
		LastVersion:  snap.Version,
		LastChecksum: snap.Checksum,
	}))
	require.NoError(t, err)
	assert.NotNil(t, resp2.Msg.GetUnchanged(), "expected Unchanged response")
	assert.Nil(t, resp2.Msg.GetSnapshot(), "expected no snapshot in Unchanged response")
}

func TestFetchMalformedChecksumReturnsInvalidArgument(t *testing.T) {
	mgr := newTestManager(t, "default")
	svc := server.NewSnapshotService(mgr,
		&staticAuthenticator{principal: server.Principal{ID: "p1"}},
		&staticLaneResolver{lane: "default"},
	)
	client := startSnapshotServer(t, svc)

	_, err := client.Fetch(context.Background(), connect.NewRequest(&configv1.FetchRequest{
		LastChecksum: []byte("not-32-bytes"),
	}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeInvalidArgument, ce.Code())
}

func TestFetchAuthFailureReturnsUnauthenticated(t *testing.T) {
	mgr := newTestManager(t, "default")
	svc := server.NewSnapshotService(mgr,
		server.FailClosedAuthenticator{},
		&staticLaneResolver{lane: "default"},
	)
	client := startSnapshotServer(t, svc)

	_, err := client.Fetch(context.Background(), connect.NewRequest(&configv1.FetchRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeUnauthenticated, ce.Code())
}

func TestFetchLaneResolverErrorReturnsPermissionDenied(t *testing.T) {
	mgr := newTestManager(t, "default")
	svc := server.NewSnapshotService(mgr,
		&staticAuthenticator{principal: server.Principal{ID: "p1"}},
		&staticLaneResolver{err: server.ErrPermissionDenied},
	)
	client := startSnapshotServer(t, svc)

	_, err := client.Fetch(context.Background(), connect.NewRequest(&configv1.FetchRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodePermissionDenied, ce.Code())
}

func TestFetchUnpublishedLaneReturnsNotFound(t *testing.T) {
	mgr := newTestManager(t, "lane-a")
	svc := server.NewSnapshotService(mgr,
		&staticAuthenticator{principal: server.Principal{ID: "p1"}},
		// Resolver points to a lane that has no snapshot published.
		&staticLaneResolver{lane: "lane-b"},
	)
	client := startSnapshotServer(t, svc)

	_, err := client.Fetch(context.Background(), connect.NewRequest(&configv1.FetchRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeNotFound, ce.Code())
}

// TestFetchLaneNotInjectedFromRequest verifies that even if a client tries to
// influence lane selection by crafting the request, the lane is always taken
// from the authenticated principal — not from any FetchRequest field.
// FetchRequest has no lane field in the proto, enforcing this at the API level.
func TestFetchLaneNotInjectedFromRequest(t *testing.T) {
	// Publish on lane-a only.
	mgr := newTestManager(t, "lane-a")

	// Resolver always maps the authenticated principal to lane-a.
	svc := server.NewSnapshotService(mgr,
		&staticAuthenticator{principal: server.Principal{ID: "p1"}},
		&staticLaneResolver{lane: "lane-a"},
	)
	client := startSnapshotServer(t, svc)

	// No matter what the client sends in FetchRequest, they get lane-a's snapshot.
	resp, err := client.Fetch(context.Background(), connect.NewRequest(&configv1.FetchRequest{}))
	require.NoError(t, err)
	assert.NotNil(t, resp.Msg.GetSnapshot())
}
