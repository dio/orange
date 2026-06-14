package config

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"sync"

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
	mu      sync.RWMutex
	version uint64
	items   map[string]*snapshot.Snapshot
	maps    map[string]*configv1.MappedSplitSnapshot
}

// NewMemoryStore creates an empty in-memory mapped-split store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		items: map[string]*snapshot.Snapshot{},
		maps:  map[string]*configv1.MappedSplitSnapshot{},
	}
}

// PublishMappedSplit publishes component resources first, then the typed map.
// Failed publishes leave the previous state visible.
func (s *MemoryStore) PublishMappedSplit(_ context.Context, out MappedSplitPublication) (PublishResult, error) {
	if out.Lane == "" {
		return PublishResult{}, fmt.Errorf("map lane is required")
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
			return PublishResult{}, fmt.Errorf("publish %s: %w", component, err)
		}
		nextItems[key] = snap
		envelopes[bundle.Ref.Resource] = proto.Clone(snap.Envelope).(*configv1.SnapshotEnvelope)
	}

	nextVersion++
	typedMap, err := mappedsplit.NewMapSnapshot(nextVersion, out.Map)
	if err != nil {
		return PublishResult{}, fmt.Errorf("publish map: %w", err)
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
func (s *MemoryStore) FetchResource(_ context.Context, lane string, resource string, lastVersion uint64, lastChecksum []byte) (*configv1.SnapshotEnvelope, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := s.items[itemKey(lane, resource)]
	if snap == nil {
		return nil, false, fmt.Errorf("%w: lane %q resource %q", snapshot.ErrNoSnapshot, lane, resource)
	}
	if lastVersion == snap.Version && bytes.Equal(lastChecksum, snap.Envelope.Checksum) {
		return nil, true, nil
	}
	return proto.Clone(snap.Envelope).(*configv1.SnapshotEnvelope), false, nil
}

// FetchMappedSplitMap returns the current typed mapped-split map for lane.
func (s *MemoryStore) FetchMappedSplitMap(_ context.Context, lane string, lastVersion uint64, lastChecksum []byte) (*configv1.MappedSplitSnapshot, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	typedMap := s.maps[lane]
	if typedMap == nil {
		return nil, false, fmt.Errorf("%w: mapped split map lane %q", snapshot.ErrNoSnapshot, lane)
	}
	if lastVersion == typedMap.Version && bytes.Equal(lastChecksum, typedMap.Checksum) {
		return nil, true, nil
	}
	return proto.Clone(typedMap).(*configv1.MappedSplitSnapshot), false, nil
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
