package snapshot_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/snapshot"
)

func makePayload(lane string, scopes []string) *configv1.ConfigPayload {
	return &configv1.ConfigPayload{
		SchemaVersion: 1,
		Format: &configv1.PayloadFormat{
			MediaType:     "application/vnd.dio.orange.config-bundle",
			Encoding:      "zstd",
			FormatVersion: "pack-v1",
		},
		Payload: []byte("fake-bundle-zstd-bytes"),
		Metadata: &configv1.SnapshotMetadata{
			Producer:       "orange-test",
			SourceRevision: "rev-abc",
			CreatedAt:      timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
			Lane:           lane,
			ScopeKind:      "workspace",
			ScopeId:        "ws-1",
			Scopes:         append([]string{}, scopes...),
			PayloadSize:    22,
		},
	}
}

func TestNew_RoundTrip(t *testing.T) {
	payload := makePayload("default", []string{"ws-1"})
	bundleZstd := []byte("fake-bundle-zstd-bytes")

	snap, err := snapshot.New(1, payload, bundleZstd)
	require.NoError(t, err)

	// Unmarshal envelope payload back into a ConfigPayload and compare.
	got := &configv1.ConfigPayload{}
	require.NoError(t, proto.Unmarshal(snap.Envelope.Payload, got))
	assert.True(t, proto.Equal(payload, got), "round-trip payload mismatch")
}

func TestNew_VersionSetCorrectly(t *testing.T) {
	snap, err := snapshot.New(42, makePayload("", nil), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), snap.Version)
	assert.Equal(t, uint64(42), snap.Envelope.Version)
}

func TestNew_ChecksumMatchesEnvelope(t *testing.T) {
	snap, err := snapshot.New(1, makePayload("", nil), nil)
	require.NoError(t, err)
	// Checksum field must equal the SHA-256 recorded in the envelope.
	assert.Equal(t, snap.Checksum[:], snap.Envelope.Checksum)
	require.NoError(t, snap.VerifyChecksum())
}

func TestNew_ChecksumMismatchDetected(t *testing.T) {
	snap, err := snapshot.New(1, makePayload("", nil), nil)
	require.NoError(t, err)

	// Tamper with the envelope's payload bytes after construction.
	snap.Envelope.Payload[0] ^= 0xFF

	assert.Error(t, snap.VerifyChecksum())
}

func TestNew_LaneAndScopesCopied(t *testing.T) {
	payload := makePayload("lane-x", []string{"ws-1", "ws-2"})
	snap, err := snapshot.New(1, payload, nil)
	require.NoError(t, err)

	assert.Equal(t, "lane-x", snap.Lane)
	assert.Equal(t, []string{"ws-1", "ws-2"}, snap.Scopes)

	// Mutating the *source* payload after New must not affect the snapshot.
	payload.Metadata.Scopes[0] = "mutated"
	assert.Equal(t, "ws-1", snap.Scopes[0], "snapshot scopes must be independent of source payload")
	assert.Equal(t, "ws-1", snap.Payload.Metadata.Scopes[0], "snapshot Payload must be an independent clone of source")
}

func TestNew_BundleZstdDerivedFromPayloadWhenNil(t *testing.T) {
	payload := makePayload("", nil)
	snap, err := snapshot.New(1, payload, nil)
	require.NoError(t, err)

	assert.Equal(t, payload.Payload, snap.BundleZstd)
}

func TestNew_BundleZstdCopied(t *testing.T) {
	payload := makePayload("", nil)
	orig := append([]byte(nil), payload.Payload...)
	snap, err := snapshot.New(1, payload, orig)
	require.NoError(t, err)

	// Mutating the original slice must not change the snapshot's copy.
	want := make([]byte, len(snap.BundleZstd))
	copy(want, snap.BundleZstd)
	orig[0] ^= 0xFF
	assert.Equal(t, want, snap.BundleZstd)
}

func TestNew_BundleZstdMustMatchPayload(t *testing.T) {
	_, err := snapshot.New(1, makePayload("", nil), []byte("different-bundle"))
	require.Error(t, err)
}

func TestNew_ZeroVersionRejected(t *testing.T) {
	_, err := snapshot.New(0, makePayload("", nil), nil)
	require.Error(t, err)
}

func TestNew_NilPayloadRejected(t *testing.T) {
	_, err := snapshot.New(1, nil, nil)
	require.Error(t, err)
}
