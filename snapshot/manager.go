package snapshot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/producer"
)

// ErrVersionMismatch is returned by Publish when the caller supplied an
// expected version or checksum that does not match the current snapshot.
var ErrVersionMismatch = errors.New("expected version or checksum mismatch")

// ErrNoSnapshot is returned by Fetch when no snapshot has been published for
// the requested lane.
var ErrNoSnapshot = errors.New("no snapshot for lane")

// ErrNoCallback is returned by Publish when no mutation callback is registered.
var ErrNoCallback = errors.New("no mutation callback registered")

// ErrNoBuilder is returned by Publish when no producer builder is configured.
var ErrNoBuilder = errors.New("no producer builder configured")

// MutationRequest carries the opaque context for one publication. The callback
// reads from this to know what to build.
type MutationRequest struct {
	Selection producer.Selection
	Lane      string
	// Resource is an optional named resource inside Lane. Empty is the default
	// resource for that lane.
	Resource string
	// ExpectedVersion gates the publish; 0 means no version check.
	ExpectedVersion uint64
	// ExpectedChecksum gates the publish; nil means no checksum check.
	ExpectedChecksum []byte
	// PreparedData is opaque bytes supplied by remote admin callers; in-process
	// callers may leave it nil and use out-of-band typed state instead.
	PreparedData []byte
}

// MutationCallback is the embedding hook. It reads domain config and returns
// normalized cherry.Input with secret refs only. It must not return secret
// bytes and must not mutate previously published snapshots.
type MutationCallback func(ctx context.Context, req MutationRequest) (producer.BuildResult, error)

// PublishResult reports the outcome of one successful publication.
type PublishResult struct {
	PreviousVersion uint64
	Snapshot        *Snapshot
}

// laneState holds the atomically swapped current snapshot for one lane. A nil
// pointer means no snapshot has been published yet.
type laneState struct {
	snap atomic.Pointer[Snapshot]
}

type laneResourceKey struct {
	lane     string
	resource string
}

// Manager owns the per-lane snapshot state and serializes mutations while
// allowing concurrent lock-free reads. A zero Manager is not valid; use
// NewManager.
type Manager struct {
	mu       sync.Mutex
	version  uint64   // monotonic counter, incremented inside mu
	lanes    sync.Map // string -> *laneState
	builder  *producer.Builder
	callback MutationCallback
}

// NewManager creates a Manager backed by builder and callback. Both must be
// non-nil.
func NewManager(builder *producer.Builder, callback MutationCallback) *Manager {
	return &Manager{
		builder:  builder,
		callback: callback,
	}
}

// SetCallback replaces the mutation callback. It is safe to call concurrently;
// the replacement is visible to the next Publish call.
func (m *Manager) SetCallback(cb MutationCallback) {
	m.mu.Lock()
	m.callback = cb
	m.mu.Unlock()
}

// Publish runs the mutation callback, builds a new snapshot, and atomically
// replaces the current snapshot for the request's lane. If the callback or
// builder returns an error, the previous snapshot remains active.
//
// Concurrent Publish calls for any lane are serialized. Concurrent Fetch calls
// always observe either the snapshot before or after a Publish, never an
// in-between state.
func (m *Manager) Publish(ctx context.Context, req MutationRequest) (*Snapshot, error) {
	result, err := m.PublishWithResult(ctx, req)
	if err != nil {
		return nil, err
	}
	return result.Snapshot, nil
}

// PublishWithResult is like Publish, but also reports the version that was
// active for the lane inside the serialized publication critical section.
func (m *Manager) PublishWithResult(ctx context.Context, req MutationRequest) (PublishResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.callback == nil {
		return PublishResult{}, ErrNoCallback
	}
	if m.builder == nil {
		return PublishResult{}, ErrNoBuilder
	}

	// Check optimistic concurrency before calling the callback so the callback
	// does not run against a stale precondition.
	ls := m.getOrCreateLane(req.Lane, req.Resource)
	current := ls.snap.Load()
	var previousVersion uint64
	if current != nil {
		previousVersion = current.Version
	}

	if err := checkPrecondition(current, req.ExpectedVersion, req.ExpectedChecksum); err != nil {
		return PublishResult{}, err
	}

	buildResult, err := m.callback(ctx, req)
	if err != nil {
		return PublishResult{}, fmt.Errorf("mutation callback: %w", err)
	}

	out, err := m.builder.Build(ctx, req.Selection, req.Lane, buildResult)
	if err != nil {
		return PublishResult{}, fmt.Errorf("build snapshot: %w", err)
	}

	m.version++
	snap, err := New(m.version, out.Payload, out.BundleZstd)
	if err != nil {
		m.version-- // rollback; next successful publish reuses this slot
		return PublishResult{}, fmt.Errorf("assemble snapshot: %w", err)
	}

	ls.snap.Store(snap)
	// Return a clone so the caller cannot corrupt the stored snapshot through
	// the returned pointer.
	return PublishResult{
		PreviousVersion: previousVersion,
		Snapshot:        snap.clone(),
	}, nil
}

// Fetch returns the current snapshot envelope for the given lane. When
// lastVersion and lastChecksum both match the current snapshot, it returns
// (nil, true, nil) to signal unchanged. When no snapshot has been published
// for the lane, it returns ErrNoSnapshot.
//
// Fetch is lock-free on the read path; Publish does not block Fetch.
func (m *Manager) Fetch(lane string, lastVersion uint64, lastChecksum []byte) (*configv1.SnapshotEnvelope, bool, error) {
	return m.FetchResource(lane, "", lastVersion, lastChecksum)
}

// FetchResource returns the current snapshot envelope for a named resource
// inside lane. Empty resource is the lane's default resource.
func (m *Manager) FetchResource(lane string, resource string, lastVersion uint64, lastChecksum []byte) (*configv1.SnapshotEnvelope, bool, error) {
	ls, ok := m.loadLane(lane, resource)
	if !ok {
		return nil, false, ErrNoSnapshot
	}

	snap := ls.snap.Load()
	if snap == nil {
		return nil, false, ErrNoSnapshot
	}

	if lastVersion == snap.Version && bytes.Equal(lastChecksum, snap.Envelope.Checksum) {
		return nil, true, nil // unchanged
	}

	// Clone the envelope so the caller cannot corrupt the stored snapshot.
	return proto.Clone(snap.Envelope).(*configv1.SnapshotEnvelope), false, nil
}

// Current returns a clone of the current snapshot for lane, or nil if none
// has been published. It never blocks. The returned snapshot is independent of
// the stored copy.
func (m *Manager) Current(lane string) *Snapshot {
	return m.CurrentResource(lane, "")
}

// CurrentResource returns a clone of the current snapshot for resource inside
// lane, or nil if none has been published.
func (m *Manager) CurrentResource(lane string, resource string) *Snapshot {
	ls, ok := m.loadLane(lane, resource)
	if !ok {
		return nil
	}
	snap := ls.snap.Load()
	if snap == nil {
		return nil
	}
	return snap.clone()
}

func (m *Manager) getOrCreateLane(lane string, resource string) *laneState {
	v, _ := m.lanes.LoadOrStore(resourceKey(lane, resource), &laneState{})
	return v.(*laneState)
}

func (m *Manager) loadLane(lane string, resource string) (*laneState, bool) {
	v, ok := m.lanes.Load(resourceKey(lane, resource))
	if !ok {
		return nil, false
	}
	return v.(*laneState), true
}

func resourceKey(lane string, resource string) laneResourceKey {
	return laneResourceKey{lane: lane, resource: resource}
}

func checkPrecondition(current *Snapshot, expectedVersion uint64, expectedChecksum []byte) error {
	if expectedVersion == 0 && len(expectedChecksum) == 0 {
		return nil // no precondition
	}

	var currentVersion uint64
	var currentChecksum []byte
	if current != nil {
		currentVersion = current.Version
		currentChecksum = current.Envelope.Checksum
	}

	if expectedVersion != 0 && expectedVersion != currentVersion {
		return fmt.Errorf("%w: expected version %d, current %d", ErrVersionMismatch, expectedVersion, currentVersion)
	}
	if len(expectedChecksum) != 0 && !bytes.Equal(expectedChecksum, currentChecksum) {
		return fmt.Errorf("%w: checksum mismatch", ErrVersionMismatch)
	}
	return nil
}
