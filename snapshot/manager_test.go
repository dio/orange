package snapshot_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dio/cherry"
	"github.com/dio/orange/producer"
	"github.com/dio/orange/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testBuilder returns a producer.Builder with a fixed clock.
func testBuilder() *producer.Builder {
	return producer.NewBuilder(producer.Options{
		Producer: "test",
		Clock:    func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	})
}

// minimalBuildResult returns a BuildResult with a small valid cherry.Input.
func minimalBuildResult(_ string) producer.BuildResult {
	return producer.BuildResult{
		SourceRevision: "rev-1",
		Scopes:         []string{"ws-1"},
		Input: cherry.Input{
			Providers: []cherry.Provider{{
				ID:        "openai",
				Kind:      "openai",
				Endpoint:  "https://api.openai.com",
				SecretRef: "env://OPENAI_API_KEY",
			}},
			Models: []cherry.Model{{
				ID:       "gpt-4o-mini",
				Provider: "openai",
				Name:     "gpt-4o-mini",
			}},
			Scopes: []cherry.Scope{{
				ID: "ws-1",
				Principals: []cherry.Principal{{
					Slug:  "slug:user:1",
					Route: cherry.RoutePlan{Provider: "openai", Model: "gpt-4o-mini"},
					Rate:  cherry.RatePolicy{USDPerDayCents: 1000, RPM: 60, OnExceed: "reject"},
				}},
			}},
		},
	}
}

func successCallback(lane string) snapshot.MutationCallback {
	return func(_ context.Context, req snapshot.MutationRequest) (producer.BuildResult, error) {
		return minimalBuildResult(lane), nil
	}
}

func failingCallback(msg string) snapshot.MutationCallback {
	return func(_ context.Context, _ snapshot.MutationRequest) (producer.BuildResult, error) {
		return producer.BuildResult{}, errors.New(msg)
	}
}

func defaultSel() producer.Selection {
	return producer.Selection{ScopeKind: "workspace", ScopeID: "ws-1"}
}

// TestPublishAndFetch verifies the basic publish → fetch cycle.
func TestPublishAndFetch(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("default"))

	req := snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"}
	snap, err := mgr.Publish(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(1), snap.Version)

	envelope, unchanged, err := mgr.Fetch("default", 0, nil)
	require.NoError(t, err)
	assert.False(t, unchanged)
	require.NotNil(t, envelope)
	assert.Equal(t, uint64(1), envelope.Version)
}

func TestPublishWithResultReportsPreviousVersion(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("default"))

	first, err := mgr.PublishWithResult(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
	require.NoError(t, err)
	assert.Equal(t, uint64(0), first.PreviousVersion)
	require.NotNil(t, first.Snapshot)
	assert.Equal(t, uint64(1), first.Snapshot.Version)

	second, err := mgr.PublishWithResult(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), second.PreviousVersion)
	require.NotNil(t, second.Snapshot)
	assert.Equal(t, uint64(2), second.Snapshot.Version)
}

// TestFetchUnchanged verifies that matching version+checksum returns unchanged.
func TestFetchUnchanged(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("default"))
	snap, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
	require.NoError(t, err)

	_, unchanged, err := mgr.Fetch("default", snap.Version, snap.Envelope.Checksum)
	require.NoError(t, err)
	assert.True(t, unchanged)
}

// TestFetchNoSnapshot returns ErrNoSnapshot for an unknown lane.
func TestFetchNoSnapshot(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("default"))
	_, _, err := mgr.Fetch("unknown-lane", 0, nil)
	require.ErrorIs(t, err, snapshot.ErrNoSnapshot)
}

// TestFailedPublishKeepsOldSnapshot ensures a callback failure leaves the
// previous snapshot active.
func TestFailedPublishKeepsOldSnapshot(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("default"))

	// First publish succeeds.
	snap1, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
	require.NoError(t, err)

	// Second publish fails.
	mgr.SetCallback(failingCallback("build error"))
	_, err = mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
	require.Error(t, err)

	// Current snapshot must still be the first one.
	current := mgr.Current("default")
	require.NotNil(t, current)
	assert.Equal(t, snap1.Version, current.Version)
}

// TestExpectedVersionMismatch verifies the failed-precondition path.
func TestExpectedVersionMismatch(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("default"))
	_, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
	require.NoError(t, err)

	// Request expects version 99, current is 1.
	_, err = mgr.Publish(context.Background(), snapshot.MutationRequest{
		Selection:       defaultSel(),
		Lane:            "default",
		ExpectedVersion: 99,
	})
	require.ErrorIs(t, err, snapshot.ErrVersionMismatch)
}

// TestExpectedChecksumMismatch verifies checksum-based concurrency gate.
func TestExpectedChecksumMismatch(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("default"))
	_, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
	require.NoError(t, err)

	_, err = mgr.Publish(context.Background(), snapshot.MutationRequest{
		Selection:        defaultSel(),
		Lane:             "default",
		ExpectedChecksum: []byte("wrong-checksum-not-32-bytes"),
	})
	require.ErrorIs(t, err, snapshot.ErrVersionMismatch)
}

// TestNoCallbackFails verifies publish fails closed when no callback is set.
func TestNoCallbackFails(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), nil)
	_, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
	require.ErrorIs(t, err, snapshot.ErrNoCallback)
}

// TestMultipleLanesIsolated verifies that publishes to different lanes do not
// cross-contaminate each other.
func TestMultipleLanesIsolated(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("any"))

	// Publish to lane A twice, lane B once.
	_, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "lane-a"})
	require.NoError(t, err)
	_, err = mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "lane-a"})
	require.NoError(t, err)
	_, err = mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "lane-b"})
	require.NoError(t, err)

	snapA := mgr.Current("lane-a")
	snapB := mgr.Current("lane-b")
	require.NotNil(t, snapA)
	require.NotNil(t, snapB)

	// lane-a had two publishes; lane-b had one. Versions are global so they
	// should differ, and each lane's current snapshot is independent.
	assert.NotEqual(t, snapA.Version, snapB.Version)

	// Fetching an unrelated lane returns ErrNoSnapshot.
	_, _, err = mgr.Fetch("lane-c", 0, nil)
	require.ErrorIs(t, err, snapshot.ErrNoSnapshot)
}

// TestMultiLaneLaneMetadataCorrect verifies that each lane's snapshot carries
// the correct lane label in its metadata when a single Builder serves multiple
// lanes. This guards against the static Builder.Options.Lane bug.
func TestMultiLaneLaneMetadataCorrect(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("any"))

	for _, lane := range []string{"lane-a", "lane-b"} {
		lane := lane
		_, err := mgr.Publish(context.Background(), snapshot.MutationRequest{
			Selection: defaultSel(),
			Lane:      lane,
		})
		require.NoError(t, err)

		snap := mgr.Current(lane)
		require.NotNil(t, snap)
		assert.Equal(t, lane, snap.Lane, "Snapshot.Lane must match the published lane")
		assert.Equal(t, lane, snap.Payload.Metadata.Lane, "ConfigPayload metadata lane must match")
		assert.True(t, snap.Envelope.Version > 0)
	}

	// The lanes must not share state.
	snapA := mgr.Current("lane-a")
	snapB := mgr.Current("lane-b")
	assert.NotEqual(t, snapA.Version, snapB.Version)
	assert.Equal(t, "lane-a", snapA.Lane)
	assert.Equal(t, "lane-b", snapB.Lane)
}

// TestPublishReturnIsolated verifies that mutating the *Snapshot returned by
// Publish does not corrupt the stored snapshot observed by future Fetch calls.
func TestPublishReturnIsolated(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("default"))
	returned, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
	require.NoError(t, err)

	originalChecksum := make([]byte, len(returned.Envelope.Checksum))
	copy(originalChecksum, returned.Envelope.Checksum)

	// Corrupt the returned snapshot.
	returned.Envelope.Checksum[0] ^= 0xFF
	returned.BundleZstd = []byte("corrupted")

	// The stored snapshot must be unaffected.
	env, unchanged, err := mgr.Fetch("default", 0, nil)
	require.NoError(t, err)
	assert.False(t, unchanged)
	assert.Equal(t, originalChecksum, env.Checksum, "stored checksum must not be corrupted by caller mutation")
}

// TestFetchReturnIsolated verifies that mutating the *SnapshotEnvelope
// returned by Fetch does not corrupt the stored snapshot.
func TestFetchReturnIsolated(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("default"))
	snap, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
	require.NoError(t, err)

	env, _, err := mgr.Fetch("default", 0, nil)
	require.NoError(t, err)
	require.NotNil(t, env)

	originalChecksum := make([]byte, len(env.Checksum))
	copy(originalChecksum, env.Checksum)

	// Corrupt the returned envelope.
	env.Checksum[0] ^= 0xFF
	env.Payload = []byte("corrupted")

	// A second Fetch must still return the original valid envelope.
	env2, unchanged, err := mgr.Fetch("default", snap.Version, originalChecksum)
	require.NoError(t, err)
	assert.True(t, unchanged, "second fetch with correct checksum should be unchanged")
	_ = env2
}

// TestConcurrentFetchDuringPublish runs fetch goroutines concurrently with a
// publish goroutine and asserts that no partial snapshot is ever observed.
// Run with -race to catch data races.
func TestConcurrentFetchDuringPublish(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("default"))

	// Prime with an initial snapshot.
	_, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
	require.NoError(t, err)

	const readers = 20
	const publishes = 10

	var wg sync.WaitGroup
	var fetchErrors atomic.Int32

	// Reader goroutines run continuously during publishes.
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				env, _, err := mgr.Fetch("default", 0, nil)
				if err != nil {
					fetchErrors.Add(1)
					return
				}
				// Every envelope must have a positive version and non-empty checksum.
				if env != nil && (env.Version == 0 || len(env.Checksum) == 0) {
					fetchErrors.Add(1)
				}
			}
		}()
	}

	// Publisher goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		for i := 0; i < publishes; i++ {
			_, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Selection: defaultSel(), Lane: "default"})
			if err != nil {
				fetchErrors.Add(1)
				return
			}
		}
	}()

	wg.Wait()
	assert.Equal(t, int32(0), fetchErrors.Load(), "fetch goroutines encountered errors")
}
