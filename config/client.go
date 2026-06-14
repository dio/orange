package config

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
	"github.com/dio/cherry"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/api/orange/config/v1/configv1connect"
	"github.com/dio/orange/internal/otelx"
	"github.com/dio/orange/mappedsplit"
)

const (
	defaultMaxAttempts    = 3
	defaultAttemptTimeout = 10 * time.Second
	defaultInitialBackoff = 200 * time.Millisecond
	defaultMaxBackoff     = 2 * time.Second
)

type (
	SplitMap           = mappedsplit.SplitMap
	BundleRef          = mappedsplit.BundleRef
	PartitionBundleRef = mappedsplit.PartitionBundleRef
	PartitionSpec      = mappedsplit.PartitionSpec
	Opened             = mappedsplit.Opened
	ApplyStats         = mappedsplit.ApplyStats
	LLMResult          = cherry.LLMResult
	MCPResult          = cherry.MCPResult
	OpenedBundle       = cherry.OpenedBundle
)

// HeaderFunc injects authentication or tracing headers into one fetch request.
type HeaderFunc func(ctx context.Context, header http.Header) error

// RetryPolicy controls retry behavior for transient fetch failures.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// ClientOptions configures a mapped-split config client.
type ClientOptions struct {
	BaseURL        string
	HTTPClient     connect.HTTPClient
	RPCClient      configv1connect.SnapshotServiceClient
	ConnectOptions []connect.ClientOption
	HeaderFunc     HeaderFunc
	RetryPolicy    RetryPolicy
	AttemptTimeout time.Duration

	sleep func(context.Context, time.Duration) error
}

// Client polls Orange mapped-split snapshots and publishes a local opened view.
type Client struct {
	baseURL        string
	rpc            configv1connect.SnapshotServiceClient
	connectOptions []connect.ClientOption
	httpClient     connect.HTTPClient
	header         HeaderFunc
	retry          RetryPolicy
	attemptTimeout time.Duration
	sleep          func(context.Context, time.Duration) error

	mu            sync.Mutex
	mapClient     *mappedSplitMapClient
	bundleClients map[string]*mappedSplitBundleClient
	current       *Opened
}

// MapResult is one validated typed map fetch result.
type MapResult struct {
	Unchanged bool
	Version   uint64
	Checksum  []byte
	Snapshot  *configv1.MappedSplitSnapshot
	Map       SplitMap
}

// BundleResult is one validated component bundle fetch result.
type BundleResult struct {
	Unchanged  bool
	Version    uint64
	Checksum   []byte
	Envelope   *configv1.SnapshotEnvelope
	Payload    *configv1.ConfigPayload
	BundleZstd []byte
}

// SyncResult is the result of fetching and applying the current mapped-split
// map to the client's active opened view.
type SyncResult struct {
	Unchanged bool
	Map       *MapResult
	Stats     ApplyStats
	Opened    *Opened
}

type fetchState struct {
	version  uint64
	checksum []byte
	last     *BundleResult
}

type mappedSplitMapState struct {
	version  uint64
	checksum []byte
	last     *MapResult
}

type mappedSplitMapClient struct {
	rpc            configv1connect.SnapshotServiceClient
	header         HeaderFunc
	retry          RetryPolicy
	attemptTimeout time.Duration
	sleep          func(context.Context, time.Duration) error

	group singleflight.Group
	mu    sync.Mutex
	state mappedSplitMapState
}

type mappedSplitBundleClient struct {
	rpc            configv1connect.SnapshotServiceClient
	header         HeaderFunc
	resource       string
	retry          RetryPolicy
	attemptTimeout time.Duration
	sleep          func(context.Context, time.Duration) error

	group singleflight.Group
	mu    sync.Mutex
	state fetchState
}

// NewClient creates a mapped-split config client. BaseURL is required unless an
// injected generated SnapshotService client is provided.
func NewClient(opts ClientOptions) (*Client, error) {
	otelx.AutoConfigureFromEnv()

	normalizeClientOptions(&opts)
	rpc := opts.RPCClient
	if rpc == nil {
		if opts.BaseURL == "" {
			return nil, fmt.Errorf("base URL is required without an injected SnapshotService client")
		}
		rpc = configv1connect.NewSnapshotServiceClient(opts.HTTPClient, opts.BaseURL, opts.ConnectOptions...)
	}
	return &Client{
		baseURL:        opts.BaseURL,
		rpc:            rpc,
		connectOptions: append([]connect.ClientOption(nil), opts.ConnectOptions...),
		httpClient:     opts.HTTPClient,
		header:         opts.HeaderFunc,
		retry:          opts.RetryPolicy,
		attemptTimeout: opts.AttemptTimeout,
		sleep:          opts.sleep,
		bundleClients:  map[string]*mappedSplitBundleClient{},
	}, nil
}

// Current returns the current opened mapped-split view, if one has been synced.
func (c *Client) Current() *Opened {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

// FetchMap fetches the latest typed mapped-split map or returns the cached map
// when Orange reports Unchanged.
func (c *Client) FetchMap(ctx context.Context) (*MapResult, error) {
	ctx, span := startConfigOperationSpan(ctx, "orange.config.Client.FetchMap")
	start := time.Now()
	var spanErr error
	mapClient := c.mapFetcher()
	result, err := mapClient.Fetch(ctx)
	captureSpanError(&spanErr, err)
	resultLabel := metricResult(err)
	if err == nil && result.Unchanged {
		resultLabel = "unchanged"
	}
	recordConfigOperation(ctx, "client.fetch_map", resultLabel, start)
	finishConfigOperationSpan(span, resultLabel, spanErr)
	return result, err
}

// FetchBundle fetches one component bundle resource from the authenticated lane.
func (c *Client) FetchBundle(ctx context.Context, resource string) (*BundleResult, error) {
	ctx, span := startConfigOperationSpan(ctx, "orange.config.Client.FetchBundle",
		attribute.String("orange.resource", resource),
	)
	start := time.Now()
	resultLabel := "success"
	var spanErr error
	defer func() {
		recordConfigOperation(ctx, "client.fetch_bundle", resultLabel, start)
		finishConfigOperationSpan(span, resultLabel, spanErr)
	}()

	bundleClient, err := c.bundleFetcher(resource)
	if err != nil {
		resultLabel = "error"
		captureSpanError(&spanErr, err)
		return nil, err
	}
	result, err := bundleClient.Fetch(ctx)
	if err != nil {
		resultLabel = "error"
		captureSpanError(&spanErr, err)
		return nil, err
	}
	if result.Payload.GetFormat().GetMediaType() != mappedsplit.BundleMediaType {
		resultLabel = "error"
		err := fmt.Errorf("resource %s returned media type %q", resource, result.Payload.GetFormat().GetMediaType())
		captureSpanError(&spanErr, err)
		return nil, err
	}
	if result.Unchanged {
		resultLabel = "unchanged"
	}
	return result, nil
}

// Sync fetches the current mapped-split map, fetches only stale or missing
// component resources, and swaps the client's active opened view.
func (c *Client) Sync(ctx context.Context) (*SyncResult, error) {
	ctx, span := startConfigOperationSpan(ctx, "orange.config.Client.Sync")
	start := time.Now()
	resultLabel := "success"
	var spanErr error
	defer func() {
		recordConfigOperation(ctx, "client.sync", resultLabel, start)
		finishConfigOperationSpan(span, resultLabel, spanErr)
	}()

	mapResult, err := c.FetchMap(ctx)
	if err != nil {
		resultLabel = "error"
		err := fmt.Errorf("fetch split map: %w", err)
		captureSpanError(&spanErr, err)
		return nil, err
	}

	c.mu.Lock()
	current := c.current
	c.mu.Unlock()

	if mapResult.Unchanged && current != nil {
		resultLabel = "unchanged"
		return &SyncResult{
			Unchanged: true,
			Map:       mapResult,
			Stats:     ApplyStats{Reused: 1},
			Opened:    current,
		}, nil
	}

	next, stats, err := mappedsplit.Open(ctx, current, mapResult.Map, func(ctx context.Context, ref mappedsplit.BundleRef) ([]byte, bool, error) {
		result, err := c.FetchBundle(ctx, ref.Resource)
		if err != nil {
			return nil, false, fmt.Errorf("fetch component %s resource %s: %w", ref.Component, ref.Resource, err)
		}
		return result.BundleZstd, result.Unchanged, nil
	})
	if err != nil {
		resultLabel = "error"
		captureSpanError(&spanErr, err)
		return nil, err
	}

	c.mu.Lock()
	c.current = next
	c.mu.Unlock()

	return &SyncResult{
		Map:    mapResult,
		Stats:  stats,
		Opened: next,
	}, nil
}

// OpenBundleZstd opens a Cherry zstd bundle. It is exposed for diagnostic
// callers that fetch an individual component resource.
func OpenBundleZstd(payload []byte) (OpenedBundle, error) {
	return cherry.OpenBundleZstd(payload)
}

func (c *Client) mapFetcher() *mappedSplitMapClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapClient != nil {
		return c.mapClient
	}
	c.mapClient = &mappedSplitMapClient{
		rpc:            c.rpc,
		header:         c.header,
		retry:          c.retry,
		attemptTimeout: c.attemptTimeout,
		sleep:          c.sleep,
	}
	return c.mapClient
}

func (c *Client) bundleFetcher(resource string) (*mappedSplitBundleClient, error) {
	if resource == "" {
		return nil, fmt.Errorf("mapped split bundle resource is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if bundleClient := c.bundleClients[resource]; bundleClient != nil {
		return bundleClient, nil
	}
	bundleClient := &mappedSplitBundleClient{
		rpc:            c.rpc,
		header:         c.header,
		resource:       resource,
		retry:          c.retry,
		attemptTimeout: c.attemptTimeout,
		sleep:          c.sleep,
	}
	c.bundleClients[resource] = bundleClient
	return bundleClient, nil
}

func (c *mappedSplitMapClient) Fetch(ctx context.Context) (*MapResult, error) {
	otelx.AutoConfigureFromEnv()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opCtx := context.WithoutCancel(ctx)
	ch := c.group.DoChan("fetch-mapped-split-map", func() (any, error) {
		return c.fetch(opCtx)
	})

	select {
	case out := <-ch:
		if out.Err != nil {
			return nil, out.Err
		}
		result, ok := out.Val.(*MapResult)
		if !ok {
			return nil, fmt.Errorf("unexpected mapped split map result type %T", out.Val)
		}
		return result.clone(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *mappedSplitMapClient) fetch(ctx context.Context) (*MapResult, error) {
	var lastErr error
	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		start := time.Now()
		result, err := c.fetchOnce(ctx)
		resultLabel := metricResult(err)
		if err == nil && result.Unchanged {
			resultLabel = "unchanged"
		}
		recordConfigOperation(ctx, "client.fetch_map_attempt", resultLabel, start,
			attribute.Int("orange.attempt", attempt),
		)
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

func (c *mappedSplitMapClient) fetchOnce(ctx context.Context) (*MapResult, error) {
	version, checksum := c.lastVersion()
	if c.attemptTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.attemptTimeout)
		defer cancel()
	}

	req := connect.NewRequest(&configv1.FetchMappedSplitMapRequest{
		LastVersion:  version,
		LastChecksum: checksum,
	})
	if c.header != nil {
		if err := c.header(ctx, req.Header()); err != nil {
			return nil, fmt.Errorf("prepare fetch mapped split map headers: %w", err)
		}
	}

	resp, err := c.rpc.FetchMappedSplitMap(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Msg.GetUnchanged() != nil {
		return c.cachedUnchanged()
	}

	result, err := decodeMappedSplitSnapshot(resp.Msg.GetSnapshot())
	if err != nil {
		return nil, err
	}
	c.store(result)
	return result.clone(), nil
}

func (c *mappedSplitMapClient) lastVersion() (uint64, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.version, append([]byte(nil), c.state.checksum...)
}

func (c *mappedSplitMapClient) store(result *MapResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.version = result.Version
	c.state.checksum = append([]byte(nil), result.Checksum...)
	c.state.last = result.clone()
}

func (c *mappedSplitMapClient) cachedUnchanged() (*MapResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state.last == nil {
		return nil, fmt.Errorf("server returned unchanged before any mapped split map was cached")
	}
	result := c.state.last.clone()
	result.Unchanged = true
	return result, nil
}

func (c *mappedSplitBundleClient) Fetch(ctx context.Context) (*BundleResult, error) {
	otelx.AutoConfigureFromEnv()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opCtx := context.WithoutCancel(ctx)
	ch := c.group.DoChan("fetch-mapped-split-bundle", func() (any, error) {
		return c.fetch(opCtx)
	})

	select {
	case out := <-ch:
		if out.Err != nil {
			return nil, out.Err
		}
		result, ok := out.Val.(*BundleResult)
		if !ok {
			return nil, fmt.Errorf("unexpected mapped split bundle result type %T", out.Val)
		}
		return result.clone(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *mappedSplitBundleClient) fetch(ctx context.Context) (*BundleResult, error) {
	var lastErr error
	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		start := time.Now()
		result, err := c.fetchOnce(ctx)
		resultLabel := metricResult(err)
		if err == nil && result.Unchanged {
			resultLabel = "unchanged"
		}
		recordConfigOperation(ctx, "client.fetch_bundle_attempt", resultLabel, start,
			attribute.Int("orange.attempt", attempt),
		)
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

func (c *mappedSplitBundleClient) fetchOnce(ctx context.Context) (*BundleResult, error) {
	version, checksum := c.lastVersion()
	if c.attemptTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.attemptTimeout)
		defer cancel()
	}

	req := connect.NewRequest(&configv1.FetchMappedSplitBundleRequest{
		Resource:     c.resource,
		LastVersion:  version,
		LastChecksum: checksum,
	})
	if c.header != nil {
		if err := c.header(ctx, req.Header()); err != nil {
			return nil, fmt.Errorf("prepare fetch mapped split bundle headers: %w", err)
		}
	}

	resp, err := c.rpc.FetchMappedSplitBundle(ctx, req)
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

func (c *mappedSplitBundleClient) lastVersion() (uint64, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.version, append([]byte(nil), c.state.checksum...)
}

func (c *mappedSplitBundleClient) store(result *BundleResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.version = result.Version
	c.state.checksum = append([]byte(nil), result.Checksum...)
	c.state.last = result.clone()
}

func (c *mappedSplitBundleClient) cachedUnchanged() (*BundleResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state.last == nil {
		return nil, fmt.Errorf("server returned unchanged before any mapped split bundle was cached")
	}
	result := c.state.last.clone()
	result.Unchanged = true
	return result, nil
}

func decodeMappedSplitSnapshot(snapshot *configv1.MappedSplitSnapshot) (*MapResult, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("mapped split map response contained neither snapshot nor unchanged")
	}
	if snapshot.Version == 0 {
		return nil, fmt.Errorf("mapped split map version must be > 0")
	}
	if len(snapshot.Checksum) != sha256.Size {
		return nil, fmt.Errorf("mapped split map checksum must be %d bytes, got %d", sha256.Size, len(snapshot.Checksum))
	}
	if snapshot.Map == nil {
		return nil, fmt.Errorf("mapped split map body is required")
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(snapshot.Map)
	if err != nil {
		return nil, fmt.Errorf("marshal typed mapped split map: %w", err)
	}
	got := sha256.Sum256(raw)
	if !bytes.Equal(got[:], snapshot.Checksum) {
		return nil, fmt.Errorf("mapped split map checksum mismatch")
	}
	splitMap, err := mappedsplit.FromProtoMap(snapshot.Map)
	if err != nil {
		return nil, err
	}
	return &MapResult{
		Version:  snapshot.Version,
		Checksum: append([]byte(nil), snapshot.Checksum...),
		Snapshot: proto.Clone(snapshot).(*configv1.MappedSplitSnapshot),
		Map:      splitMap,
	}, nil
}

func decodeSnapshot(env *configv1.SnapshotEnvelope) (*BundleResult, error) {
	if env == nil {
		return nil, fmt.Errorf("mapped split bundle response contained neither snapshot nor unchanged")
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

	return &BundleResult{
		Version:    env.Version,
		Checksum:   append([]byte(nil), env.Checksum...),
		Envelope:   proto.Clone(env).(*configv1.SnapshotEnvelope),
		Payload:    proto.Clone(&payload).(*configv1.ConfigPayload),
		BundleZstd: append([]byte(nil), payload.Payload...),
	}, nil
}

func (r *MapResult) clone() *MapResult {
	if r == nil {
		return nil
	}
	out := &MapResult{
		Unchanged: r.Unchanged,
		Version:   r.Version,
		Checksum:  append([]byte(nil), r.Checksum...),
		Map:       r.Map,
	}
	if r.Snapshot != nil {
		out.Snapshot = proto.Clone(r.Snapshot).(*configv1.MappedSplitSnapshot)
	}
	return out
}

func (r *BundleResult) clone() *BundleResult {
	if r == nil {
		return nil
	}
	out := &BundleResult{
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

func normalizeClientOptions(o *ClientOptions) {
	if o.HTTPClient == nil {
		o.HTTPClient = http.DefaultClient
	}
	if o.RetryPolicy.MaxAttempts <= 0 {
		o.RetryPolicy.MaxAttempts = defaultMaxAttempts
	}
	if o.RetryPolicy.InitialBackoff <= 0 {
		o.RetryPolicy.InitialBackoff = defaultInitialBackoff
	}
	if o.RetryPolicy.MaxBackoff <= 0 {
		o.RetryPolicy.MaxBackoff = defaultMaxBackoff
	}
	if o.AttemptTimeout <= 0 {
		o.AttemptTimeout = defaultAttemptTimeout
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
