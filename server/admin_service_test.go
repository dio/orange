package server_test

import (
	"context"
	"net"
	"net/http"
	"testing"

	"connectrpc.com/connect"
	adminv1 "github.com/dio/orange/api/orange/config/admin/v1"
	"github.com/dio/orange/api/orange/config/admin/v1/adminv1connect"
	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/api/orange/config/v1/configv1connect"
	"github.com/dio/orange/server"
	"github.com/dio/orange/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"
)

// startAdminServer mounts the admin service on a bufconn listener and returns
// a Connect client.
func startAdminServer(t *testing.T, svc *server.AdminService) adminv1connect.ConfigAdminServiceClient {
	t.Helper()
	mux := http.NewServeMux()
	path, handler := adminv1connect.NewConfigAdminServiceHandler(svc)
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
	return adminv1connect.NewConfigAdminServiceClient(httpClient, "http://bufconn")
}

// startCombinedServer mounts both services on a shared bufconn listener and
// returns clients for both. Used for the Slice 3 acceptance gate test.
func startCombinedServer(
	t *testing.T,
	snapSvc *server.SnapshotService,
	adminSvc *server.AdminService,
) (configv1connect.SnapshotServiceClient, adminv1connect.ConfigAdminServiceClient) {
	t.Helper()
	mux := http.NewServeMux()

	snapPath, snapHandler := configv1connect.NewSnapshotServiceHandler(snapSvc)
	mux.Handle(snapPath, snapHandler)

	adminPath, adminHandler := adminv1connect.NewConfigAdminServiceHandler(adminSvc)
	mux.Handle(adminPath, adminHandler)

	lis := bufconn.Listen(bufSize)
	srv := &http.Server{Handler: mux}
	t.Cleanup(func() { _ = srv.Close(); _ = lis.Close() })
	go func() { _ = srv.Serve(lis) }()

	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	httpClient := &http.Client{Transport: &http.Transport{DialContext: dial}}

	snapClient := configv1connect.NewSnapshotServiceClient(httpClient, "http://bufconn")
	adminClient := adminv1connect.NewConfigAdminServiceClient(httpClient, "http://bufconn")
	return snapClient, adminClient
}

// adminPrincipal is a Principal with the "admin" scope.
var adminPrincipal = server.Principal{ID: "admin-1", Scopes: []string{"admin"}}

// --- Tests ---

func TestPublishSnapshotSuccess(t *testing.T) {
	mgr := newEmptyManager(t)
	svc := server.NewAdminService(mgr,
		&staticAuthenticator{principal: adminPrincipal},
		&staticLaneResolver{lane: "default"},
	)
	client := startAdminServer(t, svc)

	resp, err := client.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{}))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), resp.Msg.PreviousVersion)
	assert.Equal(t, uint64(1), resp.Msg.PublishedVersion)
	assert.Len(t, resp.Msg.PublishedChecksum, 32)
}

func TestPublishSnapshotAuthFailure(t *testing.T) {
	mgr := newEmptyManager(t)
	svc := server.NewAdminService(mgr, server.FailClosedAuthenticator{},
		&staticLaneResolver{lane: "default"})
	client := startAdminServer(t, svc)

	_, err := client.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeUnauthenticated, ce.Code())
}

func TestPublishSnapshotMissingAdminScopeReturnsPermissionDenied(t *testing.T) {
	mgr := newEmptyManager(t)
	// Principal without "admin" scope.
	svc := server.NewAdminService(mgr,
		&staticAuthenticator{principal: server.Principal{ID: "p1", Scopes: []string{"read"}}},
		&staticLaneResolver{lane: "default"},
	)
	client := startAdminServer(t, svc)

	_, err := client.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodePermissionDenied, ce.Code())
}

func TestPublishSnapshotMalformedChecksumReturnsInvalidArgument(t *testing.T) {
	mgr := newEmptyManager(t)
	svc := server.NewAdminService(mgr,
		&staticAuthenticator{principal: adminPrincipal},
		&staticLaneResolver{lane: "default"},
	)
	client := startAdminServer(t, svc)

	_, err := client.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{
			ExpectedChecksum: []byte("not-32-bytes"),
		}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeInvalidArgument, ce.Code())
}

func TestPublishSnapshotVersionMismatchKeepsOldSnapshot(t *testing.T) {
	mgr := newTestManager(t, "default")

	svc := server.NewAdminService(mgr,
		&staticAuthenticator{principal: adminPrincipal},
		&staticLaneResolver{lane: "default"},
	)
	client := startAdminServer(t, svc)

	// Expect version 99, but current is 1.
	_, err := client.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{ExpectedVersion: 99}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeFailedPrecondition, ce.Code())

	// Old snapshot must still be active.
	current := mgr.Current("default")
	require.NotNil(t, current)
	assert.Equal(t, uint64(1), current.Version)
}

func TestPublishSnapshotMissingCallbackFails(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), nil) // no callback
	svc := server.NewAdminService(mgr,
		&staticAuthenticator{principal: adminPrincipal},
		&staticLaneResolver{lane: "default"},
	)
	client := startAdminServer(t, svc)

	_, err := client.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeUnavailable, ce.Code())
}

// TestPublishThenFetch is the Slice 3 acceptance gate: publish a snapshot
// through admin RPC and fetch it through SnapshotService.Fetch using real
// Connect clients over an in-memory bufconn connection.
func TestPublishThenFetch(t *testing.T) {
	mgr := newEmptyManager(t)
	auth := &staticAuthenticator{principal: adminPrincipal}
	lanes := &staticLaneResolver{lane: "default"}

	snapSvc := server.NewSnapshotService(mgr, auth, lanes)
	adminSvc := server.NewAdminService(mgr, auth, lanes)
	snapClient, adminClient := startCombinedServer(t, snapSvc, adminSvc)

	// Publish via admin RPC.
	pubResp, err := adminClient.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{}))
	require.NoError(t, err)
	require.Greater(t, pubResp.Msg.PublishedVersion, uint64(0))

	// Fetch via SnapshotService.Fetch — must return the published envelope.
	fetchResp, err := snapClient.Fetch(context.Background(),
		connect.NewRequest(&configv1.FetchRequest{}))
	require.NoError(t, err)
	snap := fetchResp.Msg.GetSnapshot()
	require.NotNil(t, snap, "expected snapshot response, got unchanged")
	assert.Equal(t, pubResp.Msg.PublishedVersion, snap.Version)
	assert.Equal(t, pubResp.Msg.PublishedChecksum, snap.Checksum)

	// Second fetch with matching version/checksum — must return Unchanged.
	fetchResp2, err := snapClient.Fetch(context.Background(),
		connect.NewRequest(&configv1.FetchRequest{
			LastVersion:  snap.Version,
			LastChecksum: snap.Checksum,
		}))
	require.NoError(t, err)
	assert.NotNil(t, fetchResp2.Msg.GetUnchanged())
}

// newEmptyManager returns a manager with no snapshot pre-published.
func newEmptyManager(t *testing.T) *snapshot.Manager {
	t.Helper()
	return snapshot.NewManager(testBuilder(), successCallback("default"))
}
