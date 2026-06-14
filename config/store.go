package config

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/mappedsplit"
	"github.com/dio/orange/snapshot"
)

type (
	// MappedSplitPublication is the complete built publication unit passed to a Store.
	MappedSplitPublication = mappedsplit.BuildOutput
	// BuiltComponent is one built component bundle and its payload wrapper.
	BuiltComponent = mappedsplit.ComponentOutput
)

// Store is the durable boundary for mapped-split publication and fetch.
//
// Implementations used by multi-replica deployments should publish component
// resources and the typed map atomically: the new map must become visible only
// after every referenced component resource is readable.
type Store interface {
	BundleResourceProvider
	MappedSplitMapProvider
	PublishMappedSplit(ctx context.Context, publication MappedSplitPublication) (PublishResult, error)
}

// MemoryStore is an in-process Store implementation for tests, examples, and
// single-replica deployments.
type MemoryStore struct {
	mu            sync.RWMutex
	version       uint64
	items         map[string]*snapshot.Snapshot
	maps          map[string]*configv1.MappedSplitSnapshot
	buildLocks    map[string]*sync.Mutex
	buildVersions map[string]int64
	dirty         map[string]BuildRequest
}

// NewMemoryStore creates an empty in-memory mapped-split store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		items:         map[string]*snapshot.Snapshot{},
		maps:          map[string]*configv1.MappedSplitSnapshot{},
		buildLocks:    map[string]*sync.Mutex{},
		buildVersions: map[string]int64{},
		dirty:         map[string]BuildRequest{},
	}
}

// PublishMappedSplit publishes component resources first, then the typed map.
// Failed publishes leave the previous state visible.
func (s *MemoryStore) PublishMappedSplit(ctx context.Context, out MappedSplitPublication) (PublishResult, error) {
	ctx, span := startConfigOperationSpan(ctx, "orange.config.MemoryStore.PublishMappedSplit",
		attribute.String("orange.store", "memory"),
		attribute.String("orange.lane", out.Lane),
		attribute.Int("orange.component_count", len(out.ComponentSeq)),
	)
	start := time.Now()
	resultLabel := "success"
	var spanErr error
	defer func() {
		recordConfigOperation(ctx, "store.publish_mapped_split", resultLabel, start,
			attribute.String("orange.store", "memory"),
		)
		finishConfigOperationSpan(span, resultLabel, spanErr)
	}()

	if out.Lane == "" {
		resultLabel = "error"
		err := fmt.Errorf("map lane is required")
		captureSpanError(&spanErr, err)
		return PublishResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	nextVersion := s.version
	nextItems := make(map[string]*snapshot.Snapshot, len(out.ComponentSeq))
	envelopes := make(map[string]*configv1.SnapshotEnvelope, len(out.ComponentSeq))
	for _, component := range out.ComponentSeq {
		bundle := out.Components[component]
		key := itemKey(out.Lane, bundle.Ref.Resource)
		current := s.items[key]
		if current != nil && samePayload(current.Payload, bundle.Payload) {
			continue
		}
		nextVersion++
		snap, err := snapshot.New(nextVersion, bundle.Payload, bundle.BundleZstd)
		if err != nil {
			resultLabel = "error"
			err := fmt.Errorf("publish %s: %w", component, err)
			captureSpanError(&spanErr, err)
			return PublishResult{}, err
		}
		nextItems[key] = snap
		envelopes[bundle.Ref.Resource] = proto.Clone(snap.Envelope).(*configv1.SnapshotEnvelope)
	}

	nextVersion++
	typedMap, err := mappedsplit.NewMapSnapshot(nextVersion, out.Map)
	if err != nil {
		resultLabel = "error"
		err := fmt.Errorf("publish map: %w", err)
		captureSpanError(&spanErr, err)
		return PublishResult{}, err
	}

	maps.Copy(s.items, nextItems)
	s.maps[out.Lane] = typedMap
	s.version = nextVersion

	return PublishResult{
		Lane:      out.Lane,
		Map:       proto.Clone(typedMap).(*configv1.MappedSplitSnapshot),
		Resources: envelopes,
	}, nil
}

// FetchResource returns the current component resource for lane.
func (s *MemoryStore) FetchResource(ctx context.Context, lane string, resource string, lastVersion uint64, lastChecksum []byte) (*configv1.SnapshotEnvelope, bool, error) {
	ctx, span := startConfigOperationSpan(ctx, "orange.config.MemoryStore.FetchResource",
		attribute.String("orange.store", "memory"),
		attribute.String("orange.lane", lane),
		attribute.String("orange.resource", resource),
		attribute.Int64("orange.last_version", int64(lastVersion)),
		attribute.Bool("orange.last_checksum_present", len(lastChecksum) != 0),
	)
	start := time.Now()
	resultLabel := "success"
	var spanErr error
	defer func() {
		recordConfigOperation(ctx, "store.fetch_resource", resultLabel, start,
			attribute.String("orange.store", "memory"),
		)
		finishConfigOperationSpan(span, resultLabel, spanErr)
	}()

	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := s.items[itemKey(lane, resource)]
	if snap == nil {
		resultLabel = "not_found"
		err := fmt.Errorf("%w: lane %q resource %q", snapshot.ErrNoSnapshot, lane, resource)
		captureSpanError(&spanErr, err)
		return nil, false, err
	}
	if lastVersion == snap.Version && bytes.Equal(lastChecksum, snap.Envelope.Checksum) {
		resultLabel = "unchanged"
		return nil, true, nil
	}
	return proto.Clone(snap.Envelope).(*configv1.SnapshotEnvelope), false, nil
}

// FetchMappedSplitMap returns the current typed mapped-split map for lane.
func (s *MemoryStore) FetchMappedSplitMap(ctx context.Context, lane string, lastVersion uint64, lastChecksum []byte) (*configv1.MappedSplitSnapshot, bool, error) {
	ctx, span := startConfigOperationSpan(ctx, "orange.config.MemoryStore.FetchMappedSplitMap",
		attribute.String("orange.store", "memory"),
		attribute.String("orange.lane", lane),
		attribute.Int64("orange.last_version", int64(lastVersion)),
		attribute.Bool("orange.last_checksum_present", len(lastChecksum) != 0),
	)
	start := time.Now()
	resultLabel := "success"
	var spanErr error
	defer func() {
		recordConfigOperation(ctx, "store.fetch_mapped_split_map", resultLabel, start,
			attribute.String("orange.store", "memory"),
		)
		finishConfigOperationSpan(span, resultLabel, spanErr)
	}()

	s.mu.RLock()
	defer s.mu.RUnlock()

	typedMap := s.maps[lane]
	if typedMap == nil {
		resultLabel = "not_found"
		err := fmt.Errorf("%w: mapped split map lane %q", snapshot.ErrNoSnapshot, lane)
		captureSpanError(&spanErr, err)
		return nil, false, err
	}
	if lastVersion == typedMap.Version && bytes.Equal(lastChecksum, typedMap.Checksum) {
		resultLabel = "unchanged"
		return nil, true, nil
	}
	return proto.Clone(typedMap).(*configv1.MappedSplitSnapshot), false, nil
}

// MarkMappedSplitDirty records the latest dirty build request for lane.
func (s *MemoryStore) MarkMappedSplitDirty(ctx context.Context, req BuildRequest) error {
	ctx, span := startConfigOperationSpan(ctx, "orange.config.MemoryStore.MarkMappedSplitDirty",
		attribute.String("orange.store", "memory"),
		attribute.String("orange.lane", req.Lane),
	)
	start := time.Now()
	resultLabel := "success"
	var spanErr error
	defer func() {
		recordConfigOperation(ctx, "coordinator.mark_dirty", resultLabel, start,
			attribute.String("orange.store", "memory"),
		)
		finishConfigOperationSpan(span, resultLabel, spanErr)
	}()

	if err := req.Validate(); err != nil {
		resultLabel = "error"
		captureSpanError(&spanErr, err)
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dirty[req.Lane] = req
	return nil
}

// GetMappedSplitBuildRequest returns the dirty build request for lane.
func (s *MemoryStore) GetMappedSplitBuildRequest(ctx context.Context, lane string) (*BuildRequest, error) {
	ctx, span := startConfigOperationSpan(ctx, "orange.config.MemoryStore.GetMappedSplitBuildRequest",
		attribute.String("orange.store", "memory"),
		attribute.String("orange.lane", lane),
	)
	start := time.Now()
	resultLabel := "success"
	var spanErr error
	defer func() {
		recordConfigOperation(ctx, "coordinator.get_build_request", resultLabel, start,
			attribute.String("orange.store", "memory"),
		)
		finishConfigOperationSpan(span, resultLabel, spanErr)
	}()

	if lane == "" {
		resultLabel = "error"
		err := fmt.Errorf("build request lane is required")
		captureSpanError(&spanErr, err)
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	req, ok := s.dirty[lane]
	if !ok {
		resultLabel = "empty"
		return nil, nil
	}
	return &req, nil
}

// ClearMappedSplitDirty clears the dirty request for lease.Lane.
func (s *MemoryStore) ClearMappedSplitDirty(ctx context.Context, lease BuildLease, _ uint64) error {
	ctx, span := startConfigOperationSpan(ctx, "orange.config.MemoryStore.ClearMappedSplitDirty",
		attribute.String("orange.store", "memory"),
		attribute.String("orange.lane", lease.Lane),
	)
	start := time.Now()
	resultLabel := "success"
	var spanErr error
	defer func() {
		recordConfigOperation(ctx, "coordinator.clear_dirty", resultLabel, start,
			attribute.String("orange.store", "memory"),
		)
		finishConfigOperationSpan(span, resultLabel, spanErr)
	}()

	if lease.Lane == "" {
		resultLabel = "error"
		err := fmt.Errorf("%w: invalid build lease", ErrBuildLeaseLost)
		captureSpanError(&spanErr, err)
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.dirty, lease.Lane)
	return nil
}

// WithMappedSplitBuildLease serializes build callbacks per lane.
func (s *MemoryStore) WithMappedSplitBuildLease(ctx context.Context, lane string, fn func(context.Context, BuildLease) error) error {
	ctx, span := startConfigOperationSpan(ctx, "orange.config.MemoryStore.WithMappedSplitBuildLease",
		attribute.String("orange.store", "memory"),
		attribute.String("orange.lane", lane),
	)
	start := time.Now()
	resultLabel := "success"
	var spanErr error
	defer func() {
		recordConfigOperation(ctx, "coordinator.with_build_lease", resultLabel, start,
			attribute.String("orange.store", "memory"),
		)
		finishConfigOperationSpan(span, resultLabel, spanErr)
	}()

	if lane == "" {
		resultLabel = "error"
		err := fmt.Errorf("build lease lane is required")
		captureSpanError(&spanErr, err)
		return err
	}
	if fn == nil {
		resultLabel = "error"
		err := fmt.Errorf("build lease callback is required")
		captureSpanError(&spanErr, err)
		return err
	}

	lock := s.buildLock(lane)
	lock.Lock()
	defer lock.Unlock()

	s.mu.Lock()
	s.buildVersions[lane]++
	lease := BuildLease{
		Lane:         lane,
		HolderID:     "memory",
		LeaseVersion: s.buildVersions[lane],
	}
	s.mu.Unlock()

	if err := fn(ctx, lease); err != nil {
		resultLabel = "error"
		captureSpanError(&spanErr, err)
		return err
	}
	return nil
}

func (s *MemoryStore) buildLock(lane string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()

	lock := s.buildLocks[lane]
	if lock == nil {
		lock = &sync.Mutex{}
		s.buildLocks[lane] = lock
	}
	return lock
}

func samePayload(current *configv1.ConfigPayload, next *configv1.ConfigPayload) bool {
	return current != nil &&
		next != nil &&
		current.GetMetadata() != nil &&
		next.GetMetadata() != nil &&
		bytes.Equal(current.GetMetadata().GetPayloadSha256(), next.GetMetadata().GetPayloadSha256())
}

func itemKey(lane string, resource string) string {
	if resource == "" {
		return lane
	}
	return lane + "\x00" + resource
}
