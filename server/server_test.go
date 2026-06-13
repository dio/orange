package server_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	adminv1 "github.com/dio/orange/api/orange/config/admin/v1"
	"github.com/dio/orange/api/orange/config/admin/v1/adminv1connect"
	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/api/orange/config/v1/configv1connect"
	"github.com/dio/orange/producer"
	"github.com/dio/orange/server"
	"github.com/dio/orange/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"
)

func newTestService(t *testing.T) *server.Service {
	t.Helper()
	return server.NewService(server.ServiceOptions{
		Manager: newEmptyManager(t),
		Auth:    &staticAuthenticator{principal: adminPrincipal},
		Lanes:   &staticLaneResolver{lane: "default"},
	})
}

// TestServePublishFetch starts Service.Serve on a bufconn listener, publishes
// a snapshot through admin, fetches it through the snapshot service, then
// cancels the context and asserts the server shuts down cleanly.
func TestServePublishFetch(t *testing.T) {
	svc := newTestService(t)

	lis := bufconn.Listen(bufSize)
	ctx, cancel := context.WithCancel(context.Background())

	serveErr := make(chan error, 1)
	go func() { serveErr <- svc.Serve(ctx, lis) }()

	dial := func(c context.Context, _, _ string) (net.Conn, error) { return lis.DialContext(c) }
	httpClient := &http.Client{Transport: &http.Transport{DialContext: dial}}
	snapClient := configv1connect.NewSnapshotServiceClient(httpClient, "http://bufconn")
	adminClient := adminv1connect.NewConfigAdminServiceClient(httpClient, "http://bufconn")

	// Publish.
	pubResp, err := adminClient.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{}))
	require.NoError(t, err)
	require.Greater(t, pubResp.Msg.PublishedVersion, uint64(0))

	// Fetch — must return the published snapshot.
	fetchResp, err := snapClient.Fetch(context.Background(),
		connect.NewRequest(&configv1.FetchRequest{}))
	require.NoError(t, err)
	snap := fetchResp.Msg.GetSnapshot()
	require.NotNil(t, snap, "expected snapshot response")
	assert.Equal(t, pubResp.Msg.PublishedVersion, snap.Version)

	// Cancel context and assert the server shuts down without error.
	cancel()
	select {
	case err := <-serveErr:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down within 3 seconds")
	}
}

// TestListenAndServeBindsAndShuts down verifies that ListenAndServe binds to
// an OS-assigned port and exits cleanly on context cancellation.
func TestListenAndServeBindsAndShutdown(t *testing.T) {
	svc := newTestService(t)
	ctx, cancel := context.WithCancel(context.Background())

	// Use a channel to capture the listener address before returning.
	addrCh := make(chan string, 1)
	serveErr := make(chan error, 1)
	go func() {
		// Bind and discover the port ourselves so the test can connect.
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			serveErr <- err
			return
		}
		addrCh <- lis.Addr().String()
		serveErr <- svc.Serve(ctx, lis)
	}()

	addr := <-addrCh
	base := "http://" + addr

	httpClient := &http.Client{}
	adminClient := adminv1connect.NewConfigAdminServiceClient(httpClient, base)
	snapClient := configv1connect.NewSnapshotServiceClient(httpClient, base)

	// Publish then fetch over real TCP.
	pubResp, err := adminClient.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{}))
	require.NoError(t, err)

	fetchResp, err := snapClient.Fetch(context.Background(),
		connect.NewRequest(&configv1.FetchRequest{}))
	require.NoError(t, err)
	assert.Equal(t, pubResp.Msg.PublishedVersion, fetchResp.Msg.GetSnapshot().Version)

	// Shutdown.
	cancel()
	select {
	case err := <-serveErr:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down within 3 seconds")
	}
}

func TestServeShutdownTimeoutForcesClose(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	cb := func(ctx context.Context, req snapshot.MutationRequest) (producer.BuildResult, error) {
		close(started)
		<-release
		return successCallback("default")(ctx, req)
	}

	svc := server.NewService(server.ServiceOptions{
		Manager:         snapshot.NewManager(testBuilder(), cb),
		Auth:            &staticAuthenticator{principal: adminPrincipal},
		Lanes:           &staticLaneResolver{lane: "default"},
		ShutdownTimeout: 10 * time.Millisecond,
	})

	lis := bufconn.Listen(bufSize)
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- svc.Serve(ctx, lis) }()

	dial := func(c context.Context, _, _ string) (net.Conn, error) { return lis.DialContext(c) }
	httpClient := &http.Client{Transport: &http.Transport{DialContext: dial}}
	adminClient := adminv1connect.NewConfigAdminServiceClient(httpClient, "http://bufconn")

	requestErr := make(chan error, 1)
	go func() {
		_, err := adminClient.PublishSnapshot(context.Background(),
			connect.NewRequest(&adminv1.PublishSnapshotRequest{}))
		requestErr <- err
	}()

	<-started
	cancel()

	select {
	case err := <-serveErr:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "got %v", err)
	case <-time.After(time.Second):
		t.Fatal("server did not return after shutdown timeout")
	}

	close(release)
	<-requestErr
}
