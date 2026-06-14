package snapshot_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/dio/cherry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dio/orange/producer"
	"github.com/dio/orange/snapshot"
)

// TestSecretRefNotInMetadata verifies that a secret-bearing ref in cherry.Input
// does not appear in the snapshot's diagnostic metadata fields. The cherry
// bundle may contain the opaque ref string, but metadata must stay structural.
func TestSecretRefNotInMetadata(t *testing.T) {
	const sensitiveRef = "literal://my-actual-secret-value"

	cb := func(_ context.Context, _ snapshot.MutationRequest) (producer.BuildResult, error) {
		return producer.BuildResult{
			SourceRevision: "rev-secret-test",
			Scopes:         []string{"ws-secret"},
			Input: cherry.Input{
				Providers: []cherry.Provider{{
					ID:        "openai",
					Kind:      "openai",
					Endpoint:  "https://api.openai.com",
					SecretRef: sensitiveRef,
				}},
				Models: []cherry.Model{{
					ID:       "gpt-4o-mini",
					Provider: "openai",
					Name:     "gpt-4o-mini",
				}},
				Scopes: []cherry.Scope{{
					ID: "ws-secret",
					Principals: []cherry.Principal{{
						Slug:  "slug:1",
						Route: cherry.RoutePlan{Provider: "openai", Model: "gpt-4o-mini"},
						Rate:  cherry.RatePolicy{USDPerDayCents: 500, RPM: 30, OnExceed: "reject"},
					}},
				}},
			},
		}, nil
	}

	mgr := snapshot.NewManager(testBuilder(), cb)
	snap, err := mgr.Publish(context.Background(), snapshot.MutationRequest{
		Lane: "secret-lane",
	})
	require.NoError(t, err)
	require.NotNil(t, snap)

	meta := snap.Payload.Metadata
	secretValue := strings.TrimPrefix(sensitiveRef, "literal://")

	// Metadata fields must not carry the resolved secret value.
	assert.NotContains(t, meta.Producer, secretValue)
	assert.NotContains(t, meta.SourceRevision, secretValue)
	assert.NotContains(t, meta.Lane, secretValue)
	assert.NotContains(t, meta.ScopeKind, secretValue)
	assert.NotContains(t, meta.ScopeId, secretValue)
	for _, s := range meta.Scopes {
		assert.NotContains(t, s, secretValue)
	}
}

// TestConcurrentPublishFetchStress exercises many goroutines publishing and
// fetching concurrently under the race detector. Each Fetch must observe a
// complete, valid snapshot — never a partial one.
func TestConcurrentPublishFetchStress(t *testing.T) {
	const (
		publishers = 4
		fetchers   = 16
		rounds     = 10
	)

	mgr := snapshot.NewManager(testBuilder(), successCallback("stress"))

	// Seed an initial snapshot so fetchers never race with an empty lane.
	_, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Lane: "stress"})
	require.NoError(t, err)

	var wg sync.WaitGroup

	for range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				_, _ = mgr.Publish(context.Background(), snapshot.MutationRequest{
					Lane: "stress",
				})
			}
		}()
	}

	for range fetchers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds * 5 {
				env, _, err := mgr.Fetch("stress", 0, nil)
				if err != nil {
					continue // transient; a publish may have just started
				}
				if env != nil {
					// Every observed envelope must have a non-zero version and
					// a 32-byte checksum — partial states are not valid.
					assert.Greater(t, env.Version, uint64(0))
					assert.Len(t, env.Checksum, 32)
				}
			}
		}()
	}

	wg.Wait()
}

// TestFailedPublishDoesNotIncrementVersion verifies that a callback failure
// leaves the version counter unchanged so the next successful publish gets the
// expected next version.
func TestFailedPublishDoesNotIncrementVersion(t *testing.T) {
	mgr := snapshot.NewManager(testBuilder(), successCallback("v-lane"))

	first, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Lane: "v-lane"})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), first.Version)

	// Inject a failing callback, attempt a publish.
	mgr.SetCallback(failingCallback("simulated error"))
	_, err = mgr.Publish(context.Background(), snapshot.MutationRequest{Lane: "v-lane"})
	require.Error(t, err)

	// Restore working callback; next publish must increment from 1 → 2.
	mgr.SetCallback(successCallback("v-lane"))
	second, err := mgr.Publish(context.Background(), snapshot.MutationRequest{Lane: "v-lane"})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), second.Version)
}
