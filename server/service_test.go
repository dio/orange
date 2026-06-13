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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"
)

// startServiceServer mounts both handlers from a Service on a shared bufconn
// listener and returns Connect clients for both services.
func startServiceServer(
	t *testing.T,
	svc *server.Service,
) (configv1connect.SnapshotServiceClient, adminv1connect.ConfigAdminServiceClient) {
	t.Helper()
	mux := http.NewServeMux()

	snapPath, snapHandler := svc.SnapshotServiceHandler()
	mux.Handle(snapPath, snapHandler)

	adminPath, adminHandler := svc.ConfigAdminServiceHandler()
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

func TestServiceHandlersShareManager(t *testing.T) {
	mgr := newEmptyManager(t)
	svc := server.NewService(server.ServiceOptions{
		Manager: mgr,
		Auth:    &staticAuthenticator{principal: adminPrincipal},
		Lanes:   &staticLaneResolver{lane: "default"},
	})
	snapClient, adminClient := startServiceServer(t, svc)

	// Publish via admin handler.
	pubResp, err := adminClient.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{}))
	require.NoError(t, err)
	require.Greater(t, pubResp.Msg.PublishedVersion, uint64(0))

	// Fetch via snapshot handler — must reflect the just-published version.
	fetchResp, err := snapClient.Fetch(context.Background(),
		connect.NewRequest(&configv1.FetchRequest{}))
	require.NoError(t, err)
	snap := fetchResp.Msg.GetSnapshot()
	require.NotNil(t, snap, "expected snapshot response")
	assert.Equal(t, pubResp.Msg.PublishedVersion, snap.Version)
	assert.Equal(t, pubResp.Msg.PublishedChecksum, snap.Checksum)
}

func TestServiceHandlerMethodsReturnDistinctPaths(t *testing.T) {
	mgr := newEmptyManager(t)
	svc := server.NewService(server.ServiceOptions{
		Manager: mgr,
		Auth:    &staticAuthenticator{principal: adminPrincipal},
		Lanes:   &staticLaneResolver{lane: "default"},
	})

	snapPath, _ := svc.SnapshotServiceHandler()
	adminPath, _ := svc.ConfigAdminServiceHandler()
	assert.NotEqual(t, snapPath, adminPath)
	assert.NotEmpty(t, snapPath)
	assert.NotEmpty(t, adminPath)
}

func TestServiceUnpublishedFetchReturnsNotFound(t *testing.T) {
	mgr := newEmptyManager(t)
	svc := server.NewService(server.ServiceOptions{
		Manager: mgr,
		Auth:    &staticAuthenticator{principal: adminPrincipal},
		Lanes:   &staticLaneResolver{lane: "default"},
	})
	snapClient, _ := startServiceServer(t, svc)

	_, err := snapClient.Fetch(context.Background(),
		connect.NewRequest(&configv1.FetchRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeNotFound, ce.Code())
}

func TestServiceNilAuthFailsClosed(t *testing.T) {
	svc := server.NewService(server.ServiceOptions{
		Manager: newEmptyManager(t),
		Lanes:   &staticLaneResolver{lane: "default"},
	})
	snapClient, _ := startServiceServer(t, svc)

	_, err := snapClient.Fetch(context.Background(),
		connect.NewRequest(&configv1.FetchRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodeUnauthenticated, ce.Code())
}

func TestServiceNilLaneResolverFailsClosed(t *testing.T) {
	svc := server.NewService(server.ServiceOptions{
		Manager: newEmptyManager(t),
		Auth:    &staticAuthenticator{principal: adminPrincipal},
	})
	_, adminClient := startServiceServer(t, svc)

	_, err := adminClient.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, connect.CodePermissionDenied, ce.Code())
}
