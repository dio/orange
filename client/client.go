// Package client provides a resilient SnapshotService.Fetch wrapper for data
// planes that poll Orange for published configuration snapshots.
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/api/orange/config/v1/configv1connect"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMaxAttempts    = 3
	defaultAttemptTimeout = 10 * time.Second
	defaultInitialBackoff = 200 * time.Millisecond
	defaultMaxBackoff     = 2 * time.Second
)

// HeaderFunc injects authentication or tracing headers into one fetch request.
type HeaderFunc func(ctx context.Context, header http.Header) error

// RetryPolicy controls retry behavior for transient fetch failures.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type options struct {
	httpClient     connect.HTTPClient
	rpcClient      configv1connect.SnapshotServiceClient
	connectOptions []connect.ClientOption
	header         HeaderFunc
	retry          RetryPolicy
	attemptTimeout time.Duration
	sleep          func(context.Context, time.Duration) error
}

// Option configures a Client.
type Option func(*options)

// WithHTTPClient sets the HTTP client used by the generated Connect client.
func WithHTTPClient(httpClient connect.HTTPClient) Option {
	return func(o *options) {
		o.httpClient = httpClient
	}
}

// WithConnectOptions forwards generated Connect client options.
func WithConnectOptions(opts ...connect.ClientOption) Option {
	return func(o *options) {
		o.connectOptions = append(o.connectOptions, opts...)
	}
}

// WithHeaderFunc injects request headers before each fetch attempt.
func WithHeaderFunc(header HeaderFunc) Option {
	return func(o *options) {
		o.header = header
	}
}

// WithRetryPolicy sets retry limits and backoff for transient errors.
func WithRetryPolicy(policy RetryPolicy) Option {
	return func(o *options) {
		o.retry = policy
	}
}

// WithAttemptTimeout sets the timeout for each RPC attempt. Non-positive values
// use the default timeout so a shared singleflight RPC is always bounded.
func WithAttemptTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.attemptTimeout = timeout
	}
}

// WithSnapshotServiceClient injects a generated SnapshotService client. It is
// mainly useful for tests and non-HTTP transports.
func WithSnapshotServiceClient(rpcClient configv1connect.SnapshotServiceClient) Option {
	return func(o *options) {
		o.rpcClient = rpcClient
	}
}

// withSleep overrides retry sleeping for tests.
func withSleep(sleep func(context.Context, time.Duration) error) Option {
	return func(o *options) {
		o.sleep = sleep
	}
}

// Client wraps SnapshotService.Fetch with local version tracking, payload
// validation, retry, and singleflight coalescing.
type Client struct {
	rpc            configv1connect.SnapshotServiceClient
	header         HeaderFunc
	retry          RetryPolicy
	attemptTimeout time.Duration
	sleep          func(context.Context, time.Duration) error

	group singleflight.Group
	mu    sync.Mutex
	state fetchState
}

type fetchState struct {
	version  uint64
	checksum []byte
	last     *FetchResult
}

// FetchResult is the validated result of one fetch attempt. When Unchanged is
// true, Payload and BundleZstd describe the last cached snapshot.
type FetchResult struct {
	Unchanged  bool
	Version    uint64
	Checksum   []byte
	Envelope   *configv1.SnapshotEnvelope
	Payload    *configv1.ConfigPayload
	BundleZstd []byte
}

// New creates a Client for an Orange base URL. If WithSnapshotServiceClient is
// supplied, baseURL may be empty.
func New(baseURL string, opts ...Option) (*Client, error) {
	cfg := options{
		httpClient:     http.DefaultClient,
		retry:          RetryPolicy{MaxAttempts: defaultMaxAttempts, InitialBackoff: defaultInitialBackoff, MaxBackoff: defaultMaxBackoff},
		attemptTimeout: defaultAttemptTimeout,
		sleep:          sleepContext,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	normalizeOptions(&cfg)

	rpc := cfg.rpcClient
	if rpc == nil {
		if baseURL == "" {
			return nil, fmt.Errorf("base URL is required without an injected SnapshotService client")
		}
		rpc = configv1connect.NewSnapshotServiceClient(cfg.httpClient, baseURL, cfg.connectOptions...)
	}

	return &Client{
		rpc:            rpc,
		header:         cfg.header,
		retry:          cfg.retry,
		attemptTimeout: cfg.attemptTimeout,
		sleep:          cfg.sleep,
	}, nil
}

// Fetch returns the latest snapshot or the cached snapshot when Orange reports
// Unchanged. Concurrent Fetch calls are coalesced with singleflight.
func (c *Client) Fetch(ctx context.Context) (*FetchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opCtx := context.WithoutCancel(ctx)
	ch := c.group.DoChan("fetch", func() (any, error) {
		return c.fetch(opCtx)
	})

	select {
	case out := <-ch:
		if out.Err != nil {
			return nil, out.Err
		}
		result, ok := out.Val.(*FetchResult)
		if !ok {
			return nil, fmt.Errorf("unexpected fetch result type %T", out.Val)
		}
		return result.clone(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) fetch(ctx context.Context) (*FetchResult, error) {
	var lastErr error
	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		result, err := c.fetchOnce(ctx)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if attempt == c.retry.MaxAttempts || !retryable(ctx, err) {
			return nil, err
		}
		if err := c.sleep(ctx, backoff(c.retry, attempt)); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func (c *Client) fetchOnce(ctx context.Context) (*FetchResult, error) {
	version, checksum := c.lastVersion()
	if c.attemptTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.attemptTimeout)
		defer cancel()
	}

	req := connect.NewRequest(&configv1.FetchRequest{
		LastVersion:  version,
		LastChecksum: checksum,
	})
	if c.header != nil {
		if err := c.header(ctx, req.Header()); err != nil {
			return nil, fmt.Errorf("prepare fetch headers: %w", err)
		}
	}

	resp, err := c.rpc.Fetch(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Msg.GetUnchanged() != nil {
		return c.cachedUnchanged()
	}

	result, err := decodeSnapshot(resp.Msg.GetSnapshot())
	if err != nil {
		return nil, err
	}
	c.store(result)
	return result.clone(), nil
}

func decodeSnapshot(env *configv1.SnapshotEnvelope) (*FetchResult, error) {
	if env == nil {
		return nil, fmt.Errorf("fetch response contained neither snapshot nor unchanged")
	}
	if env.Version == 0 {
		return nil, fmt.Errorf("snapshot version must be > 0")
	}
	if len(env.Checksum) != sha256.Size {
		return nil, fmt.Errorf("snapshot checksum must be %d bytes, got %d", sha256.Size, len(env.Checksum))
	}
	got := sha256.Sum256(env.Payload)
	if !bytes.Equal(got[:], env.Checksum) {
		return nil, fmt.Errorf("snapshot checksum mismatch")
	}

	var payload configv1.ConfigPayload
	if err := proto.Unmarshal(env.Payload, &payload); err != nil {
		return nil, fmt.Errorf("decode config payload: %w", err)
	}
	if payload.SchemaVersion == 0 {
		return nil, fmt.Errorf("config payload schema_version must be > 0")
	}
	if payload.Format == nil {
		return nil, fmt.Errorf("config payload format is required")
	}
	if payload.Format.MediaType == "" {
		return nil, fmt.Errorf("config payload format.media_type is required")
	}
	if payload.Format.Encoding == "" {
		return nil, fmt.Errorf("config payload format.encoding is required")
	}
	if len(payload.Payload) == 0 {
		return nil, fmt.Errorf("config payload bytes are empty")
	}
	if payload.Metadata != nil && len(payload.Metadata.PayloadSha256) != 0 {
		payloadSum := sha256.Sum256(payload.Payload)
		if !bytes.Equal(payloadSum[:], payload.Metadata.PayloadSha256) {
			return nil, fmt.Errorf("config payload checksum mismatch")
		}
	}

	return &FetchResult{
		Version:    env.Version,
		Checksum:   append([]byte(nil), env.Checksum...),
		Envelope:   proto.Clone(env).(*configv1.SnapshotEnvelope),
		Payload:    proto.Clone(&payload).(*configv1.ConfigPayload),
		BundleZstd: append([]byte(nil), payload.Payload...),
	}, nil
}

func (c *Client) lastVersion() (uint64, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.version, append([]byte(nil), c.state.checksum...)
}

func (c *Client) store(result *FetchResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.version = result.Version
	c.state.checksum = append([]byte(nil), result.Checksum...)
	c.state.last = result.clone()
}

func (c *Client) cachedUnchanged() (*FetchResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state.last == nil {
		return nil, fmt.Errorf("server returned unchanged before any snapshot was cached")
	}
	result := c.state.last.clone()
	result.Unchanged = true
	return result, nil
}

func (r *FetchResult) clone() *FetchResult {
	if r == nil {
		return nil
	}
	out := &FetchResult{
		Unchanged:  r.Unchanged,
		Version:    r.Version,
		Checksum:   append([]byte(nil), r.Checksum...),
		BundleZstd: append([]byte(nil), r.BundleZstd...),
	}
	if r.Envelope != nil {
		out.Envelope = proto.Clone(r.Envelope).(*configv1.SnapshotEnvelope)
	}
	if r.Payload != nil {
		out.Payload = proto.Clone(r.Payload).(*configv1.ConfigPayload)
	}
	return out
}

func normalizeOptions(o *options) {
	if o.httpClient == nil {
		o.httpClient = http.DefaultClient
	}
	if o.retry.MaxAttempts <= 0 {
		o.retry.MaxAttempts = defaultMaxAttempts
	}
	if o.retry.InitialBackoff <= 0 {
		o.retry.InitialBackoff = defaultInitialBackoff
	}
	if o.retry.MaxBackoff <= 0 {
		o.retry.MaxBackoff = defaultMaxBackoff
	}
	if o.attemptTimeout <= 0 {
		o.attemptTimeout = defaultAttemptTimeout
	}
	if o.sleep == nil {
		o.sleep = sleepContext
	}
}

func retryable(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return false
	}
	switch connectErr.Code() {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeResourceExhausted, connect.CodeAborted, connect.CodeUnknown:
		return true
	default:
		return false
	}
}

func backoff(policy RetryPolicy, attempt int) time.Duration {
	delay := policy.InitialBackoff
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= policy.MaxBackoff {
			return policy.MaxBackoff
		}
	}
	if delay > policy.MaxBackoff {
		return policy.MaxBackoff
	}
	return delay
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
