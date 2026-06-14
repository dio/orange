package producer_test

import (
	"context"
	"testing"
	"time"

	"github.com/dio/cherry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dio/orange/producer"
)

var fixedTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func fixedClock() func() time.Time {
	return func() time.Time { return fixedTime }
}

// minimalInput returns a small but valid cherry.Input for producer tests.
func minimalInput() cherry.Input {
	return cherry.Input{
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
	}
}

func TestBuildSuccess(t *testing.T) {
	b := producer.NewBuilder(producer.Options{
		Producer: "orange-test/v0",
		Clock:    fixedClock(),
	})

	sel := producer.Selection{ScopeKind: "workspace", ScopeID: "ws-1"}
	result := producer.BuildResult{
		SourceRevision: "abc123",
		Scopes:         []string{"ws-1"},
		Input:          minimalInput(),
	}

	out, err := b.Build(context.Background(), sel, "default", result)
	require.NoError(t, err)

	// Payload wrapper fields.
	require.NotNil(t, out.Payload)
	assert.EqualValues(t, producer.ConfigPayloadSchemaVersion, out.Payload.SchemaVersion)

	require.NotNil(t, out.Payload.Format)
	assert.Equal(t, producer.BundleMediaType, out.Payload.Format.MediaType)
	assert.Equal(t, producer.BundleEncoding, out.Payload.Format.Encoding)
	assert.Equal(t, cherry.BundleFormatVersion, out.Payload.Format.FormatVersion)

	// Metadata fields.
	require.NotNil(t, out.Payload.Metadata)
	meta := out.Payload.Metadata
	assert.Equal(t, "orange-test/v0", meta.Producer)
	assert.Equal(t, "abc123", meta.SourceRevision)
	assert.Equal(t, "default", meta.Lane)
	assert.Equal(t, "workspace", meta.ScopeKind)
	assert.Equal(t, "ws-1", meta.ScopeId)
	assert.Equal(t, []string{"ws-1"}, meta.Scopes)
	require.NotNil(t, meta.CreatedAt)
	assert.Equal(t, fixedTime.Unix(), meta.CreatedAt.AsTime().Unix())

	// Checksum and size.
	assert.NotEmpty(t, out.BundleZstd)
	assert.Equal(t, uint64(len(out.BundleZstd)), meta.PayloadSize)
	assert.Len(t, meta.PayloadSha256, 32)
	assert.Equal(t, out.Checksum[:], meta.PayloadSha256)

	// Payload bytes in ConfigPayload match BundleZstd.
	assert.Equal(t, out.BundleZstd, out.Payload.Payload)

	// Bundle must be openable by Cherry (compatibility check).
	_, err = cherry.OpenBundleZstd(out.BundleZstd)
	require.NoError(t, err)
}

func TestBuildInvalidInput(t *testing.T) {
	b := producer.NewBuilder(producer.Options{Clock: fixedClock()})
	sel := producer.Selection{ScopeKind: "workspace", ScopeID: "ws-1"}

	// Model references a provider that is not in the input — cherry rejects this.
	result := producer.BuildResult{
		Input: cherry.Input{
			Models: []cherry.Model{{
				ID:       "gpt-4o",
				Provider: "nonexistent",
				Name:     "gpt-4o",
			}},
		},
	}

	_, err := b.Build(context.Background(), sel, "", result)
	require.Error(t, err)
}

func TestBuildCanceledContext(t *testing.T) {
	b := producer.NewBuilder(producer.Options{Clock: fixedClock()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := b.Build(ctx,
		producer.Selection{ScopeKind: "workspace", ScopeID: "ws-1"},
		"default",
		producer.BuildResult{Scopes: []string{"ws-1"}, Input: minimalInput()},
	)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBuildSecretRefsPassThrough(t *testing.T) {
	secretRef := "env://OPENAI_API_KEY"
	b := producer.NewBuilder(producer.Options{Clock: fixedClock()})
	sel := producer.Selection{ScopeKind: "workspace", ScopeID: "ws-1"}

	result := producer.BuildResult{
		Scopes: []string{"ws-1"},
		Input:  minimalInput(),
	}

	out, err := b.Build(context.Background(), sel, "", result)
	require.NoError(t, err)

	opened, err := cherry.OpenBundleZstd(out.BundleZstd)
	require.NoError(t, err)

	providers := opened.Reader.Providers()
	require.Len(t, providers, 1)
	// SecretRef must survive packing as-is — never resolved to a real credential.
	assert.Equal(t, secretRef, providers[0].SecretRef)
}

func TestBuildLaneInMetadata(t *testing.T) {
	b := producer.NewBuilder(producer.Options{Clock: fixedClock()})
	sel := producer.Selection{ScopeKind: "workspace", ScopeID: "ws-1"}

	for _, lane := range []string{"lane-a", "lane-b", ""} {
		out, err := b.Build(context.Background(), sel, lane, producer.BuildResult{
			Scopes: []string{"ws-1"},
			Input:  minimalInput(),
		})
		require.NoError(t, err)
		assert.Equal(t, lane, out.Payload.Metadata.Lane, "metadata lane must match the lane passed to Build")
	}
}

func TestBuildOutputBundleZstdNotAliasPayload(t *testing.T) {
	b := producer.NewBuilder(producer.Options{Clock: fixedClock()})
	out, err := b.Build(context.Background(),
		producer.Selection{ScopeKind: "workspace", ScopeID: "ws-1"},
		"",
		producer.BuildResult{Scopes: []string{"ws-1"}, Input: minimalInput()},
	)
	require.NoError(t, err)

	// Mutating BundleZstd must not corrupt Payload.Payload, and vice versa.
	original := make([]byte, len(out.BundleZstd))
	copy(original, out.BundleZstd)

	out.BundleZstd[0] ^= 0xFF
	assert.Equal(t, original[0], out.Payload.Payload[0], "BundleZstd and Payload.Payload must not alias")
}

func TestBuildOutputSlicesImmutable(t *testing.T) {
	b := producer.NewBuilder(producer.Options{Clock: fixedClock()})
	sel := producer.Selection{ScopeKind: "workspace", ScopeID: "ws-1"}

	scopes := []string{"ws-1"}
	result := producer.BuildResult{
		Scopes: scopes,
		Input:  minimalInput(),
	}

	out, err := b.Build(context.Background(), sel, "", result)
	require.NoError(t, err)

	// Caller mutation of the original scopes slice must not change published metadata.
	want := make([]string, len(out.Payload.Metadata.Scopes))
	copy(want, out.Payload.Metadata.Scopes)
	scopes[0] = "mutated"
	assert.Equal(t, want, out.Payload.Metadata.Scopes)
}
