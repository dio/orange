package client

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	cherry "github.com/dio/cherry"
	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/producer"
	"github.com/dio/orange/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type fakeRPC struct {
	mu       sync.Mutex
	calls    int
	requests []*configv1.FetchRequest
	headers  []http.Header
	results  []*configv1.FetchResponse
	errors   []error
	block    chan struct{}
	started  chan struct{}
}

func (f *fakeRPC) Fetch(ctx context.Context, req *connect.Request[configv1.FetchRequest]) (*connect.Response[configv1.FetchResponse], error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.requests = append(f.requests, req.Msg)
	f.headers = append(f.headers, req.Header())
	if f.started != nil && call == 1 {
		close(f.started)
	}
	f.mu.Unlock()

	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	idx := call - 1
	if idx < len(f.errors) && f.errors[idx] != nil {
		return nil, f.errors[idx]
	}
	if idx < len(f.results) {
		return connect.NewResponse(f.results[idx]), nil
	}
	return connect.NewResponse(f.results[len(f.results)-1]), nil
}

func (f *fakeRPC) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func testClient(t *testing.T, rpc *fakeRPC, opts ...Option) *Client {
	t.Helper()
	all := []Option{
		WithSnapshotServiceClient(rpc),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 1, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond}),
		withSleep(func(context.Context, time.Duration) error { return nil }),
	}
	all = append(all, opts...)
	c, err := New("", all...)
	require.NoError(t, err)
	return c
}

func snapshotResponse(t *testing.T, version uint64) *configv1.FetchResponse {
	t.Helper()
	b := producer.NewBuilder(producer.Options{
		Producer: "client-test",
		Clock:    func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	})
	out, err := b.Build(context.Background(),
		producer.Selection{ScopeKind: "workspace", ScopeID: "ws-client"},
		"client-lane",
		producer.BuildResult{
			SourceRevision: "rev-client",
			Scopes:         []string{"ws-client"},
			Input: cherry.Input{
				Providers: []cherry.Provider{{
					ID:        "openai",
					Kind:      "openai",
					Endpoint:  "https://api.openai.com",
					SecretRef: "env://OPENAI_API_KEY",
				}},
				Models: []cherry.Model{{ID: "gpt-4o-mini", Provider: "openai", Name: "gpt-4o-mini"}},
				Scopes: []cherry.Scope{{
					ID: "ws-client",
					Principals: []cherry.Principal{{
						Slug:  "slug:client",
						Route: cherry.RoutePlan{Provider: "openai", Model: "gpt-4o-mini"},
						Rate:  cherry.RatePolicy{USDPerDayCents: 1000, RPM: 60, OnExceed: "reject"},
					}},
				}},
			},
		})
	require.NoError(t, err)

	snap, err := snapshot.New(version, out.Payload, out.BundleZstd)
	require.NoError(t, err)
	return &configv1.FetchResponse{
		Result: &configv1.FetchResponse_Snapshot{Snapshot: snap.Envelope},
	}
}

func unchangedResponse() *configv1.FetchResponse {
	return &configv1.FetchResponse{
		Result: &configv1.FetchResponse_Unchanged{Unchanged: &configv1.Unchanged{}},
	}
}

func mutatePayloadResponse(t *testing.T, mutate func(*configv1.ConfigPayload)) *configv1.FetchResponse {
	t.Helper()
	resp := snapshotResponse(t, 1)
	env := resp.GetSnapshot()
	require.NotNil(t, env)

	var payload configv1.ConfigPayload
	require.NoError(t, proto.Unmarshal(env.Payload, &payload))
	mutate(&payload)

	raw, err := proto.Marshal(&payload)
	require.NoError(t, err)
	checksum := sha256.Sum256(raw)
	env.Payload = raw
	env.Checksum = checksum[:]
	return resp
}

func TestFetchReturnsDecodedPayloadAndBundle(t *testing.T) {
	rpc := &fakeRPC{results: []*configv1.FetchResponse{snapshotResponse(t, 1)}}
	c := testClient(t, rpc)

	result, err := c.Fetch(context.Background())
	require.NoError(t, err)
	assert.False(t, result.Unchanged)
	assert.Equal(t, uint64(1), result.Version)
	assert.Len(t, result.Checksum, 32)
	require.NotNil(t, result.Payload)
	assert.NotEmpty(t, result.BundleZstd)
	assert.Equal(t, result.Payload.Payload, result.BundleZstd)
}

func TestFetchUnchangedReusesCachedSnapshot(t *testing.T) {
	rpc := &fakeRPC{results: []*configv1.FetchResponse{
		snapshotResponse(t, 1),
		unchangedResponse(),
	}}
	c := testClient(t, rpc)

	first, err := c.Fetch(context.Background())
	require.NoError(t, err)
	second, err := c.Fetch(context.Background())
	require.NoError(t, err)

	assert.True(t, second.Unchanged)
	assert.Equal(t, first.Version, second.Version)
	assert.Equal(t, first.Checksum, second.Checksum)
	assert.Equal(t, first.BundleZstd, second.BundleZstd)
	require.Len(t, rpc.requests, 2)
	assert.Equal(t, first.Version, rpc.requests[1].LastVersion)
	assert.Equal(t, first.Checksum, rpc.requests[1].LastChecksum)
}

func TestFetchRetriesTransientErrors(t *testing.T) {
	rpc := &fakeRPC{
		errors:  []error{connect.NewError(connect.CodeUnavailable, errors.New("temporary"))},
		results: []*configv1.FetchResponse{nil, snapshotResponse(t, 1)},
	}
	c := testClient(t, rpc,
		WithRetryPolicy(RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond}),
	)

	result, err := c.Fetch(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(1), result.Version)
	assert.Equal(t, 2, rpc.callCount())
}

func TestWithAttemptTimeoutZeroUsesDefaultBound(t *testing.T) {
	rpc := &fakeRPC{results: []*configv1.FetchResponse{snapshotResponse(t, 1)}}
	c := testClient(t, rpc, WithAttemptTimeout(0))

	assert.Equal(t, defaultAttemptTimeout, c.attemptTimeout)
}

func TestWithAttemptTimeoutNegativeUsesDefaultBound(t *testing.T) {
	rpc := &fakeRPC{results: []*configv1.FetchResponse{snapshotResponse(t, 1)}}
	c := testClient(t, rpc, WithAttemptTimeout(-time.Second))

	assert.Equal(t, defaultAttemptTimeout, c.attemptTimeout)
}

func TestFetchInjectsHeaders(t *testing.T) {
	rpc := &fakeRPC{results: []*configv1.FetchResponse{snapshotResponse(t, 1)}}
	c := testClient(t, rpc, WithHeaderFunc(func(_ context.Context, h http.Header) error {
		h.Set("Authorization", "Bearer test")
		return nil
	}))

	_, err := c.Fetch(context.Background())
	require.NoError(t, err)
	require.Len(t, rpc.headers, 1)
	assert.Equal(t, "Bearer test", rpc.headers[0].Get("Authorization"))
}

func TestFetchInvalidChecksumDoesNotReplaceCachedSnapshot(t *testing.T) {
	bad := snapshotResponse(t, 2)
	bad.GetSnapshot().Checksum[0] ^= 0xFF
	rpc := &fakeRPC{results: []*configv1.FetchResponse{
		snapshotResponse(t, 1),
		bad,
		unchangedResponse(),
	}}
	c := testClient(t, rpc)

	first, err := c.Fetch(context.Background())
	require.NoError(t, err)

	_, err = c.Fetch(context.Background())
	require.Error(t, err)

	third, err := c.Fetch(context.Background())
	require.NoError(t, err)
	assert.True(t, third.Unchanged)
	assert.Equal(t, first.Version, third.Version)
	assert.Equal(t, first.Checksum, third.Checksum)
}

func TestFetchRejectsInvalidConfigPayloadShape(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*configv1.ConfigPayload)
	}{
		{
			name: "schema_version_zero",
			mutate: func(p *configv1.ConfigPayload) {
				p.SchemaVersion = 0
			},
		},
		{
			name: "missing_format",
			mutate: func(p *configv1.ConfigPayload) {
				p.Format = nil
			},
		},
		{
			name: "empty_media_type",
			mutate: func(p *configv1.ConfigPayload) {
				p.Format.MediaType = ""
			},
		},
		{
			name: "empty_encoding",
			mutate: func(p *configv1.ConfigPayload) {
				p.Format.Encoding = ""
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rpc := &fakeRPC{results: []*configv1.FetchResponse{
				mutatePayloadResponse(t, tc.mutate),
			}}
			c := testClient(t, rpc)

			_, err := c.Fetch(context.Background())
			require.Error(t, err)
		})
	}
}

func TestFetchSingleflightCoalescesConcurrentCalls(t *testing.T) {
	rpc := &fakeRPC{
		results: []*configv1.FetchResponse{snapshotResponse(t, 1)},
		block:   make(chan struct{}),
		started: make(chan struct{}),
	}
	c := testClient(t, rpc)

	const callers = 8
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Fetch(context.Background())
			errs <- err
		}()
	}

	<-rpc.started
	time.Sleep(20 * time.Millisecond)
	close(rpc.block)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, 1, rpc.callCount())
}

func TestFetchSingleflightFirstCallerCancellationDoesNotCancelSharedRPC(t *testing.T) {
	rpc := &fakeRPC{
		results: []*configv1.FetchResponse{snapshotResponse(t, 1)},
		block:   make(chan struct{}),
		started: make(chan struct{}),
	}
	c := testClient(t, rpc)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstErr := make(chan error, 1)
	go func() {
		_, err := c.Fetch(firstCtx)
		firstErr <- err
	}()

	<-rpc.started
	cancelFirst()
	require.ErrorIs(t, <-firstErr, context.Canceled)

	secondResult := make(chan *FetchResult, 1)
	secondErr := make(chan error, 1)
	go func() {
		result, err := c.Fetch(context.Background())
		if err != nil {
			secondErr <- err
			return
		}
		secondResult <- result
	}()

	time.Sleep(20 * time.Millisecond)
	close(rpc.block)

	select {
	case err := <-secondErr:
		require.NoError(t, err)
	case result := <-secondResult:
		require.NotNil(t, result)
		assert.Equal(t, uint64(1), result.Version)
	case <-time.After(time.Second):
		t.Fatal("second fetch did not receive shared RPC result")
	}
	assert.Equal(t, 1, rpc.callCount())
}

func TestFetchAttemptTimeoutReleasesSingleflight(t *testing.T) {
	rpc := &fakeRPC{
		results: []*configv1.FetchResponse{snapshotResponse(t, 1)},
		block:   make(chan struct{}),
	}
	c := testClient(t, rpc, WithAttemptTimeout(5*time.Millisecond))

	_, err := c.Fetch(context.Background())
	require.Error(t, err)

	_, err = c.Fetch(context.Background())
	require.Error(t, err)
	assert.Equal(t, 2, rpc.callCount())
}
