package server_test

// TestPlumCompatibility is the Slice 5 acceptance gate. It exercises the full
// Orange path:
//
//	admin RPC -> MutationCallback -> cherry.Input -> Cherry bundle
//	  -> ConfigPayload -> SnapshotEnvelope -> SnapshotService.Fetch
//	  -> decode ConfigPayload -> cherry.OpenBundleZstd
//
// The test proves that the bytes served by SnapshotService.Fetch contain a
// Cherry bundle that Cherry can open, and that version/checksum metadata is
// self-consistent.

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	cherry "github.com/dio/cherry"
	adminv1 "github.com/dio/orange/api/orange/config/admin/v1"
	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestPlumCompatibility(t *testing.T) {
	// --- Setup: shared manager wired to both services. ---
	mgr := newEmptyManager(t)
	auth := &staticAuthenticator{principal: adminPrincipal}
	lanes := &staticLaneResolver{lane: "default"}

	snapSvc := server.NewSnapshotService(mgr, auth, lanes)
	adminSvc := server.NewAdminService(mgr, auth, lanes)
	snapClient, adminClient := startCombinedServer(t, snapSvc, adminSvc)

	// --- Step 1: publish via admin RPC. ---
	pubResp, err := adminClient.PublishSnapshot(context.Background(),
		connect.NewRequest(&adminv1.PublishSnapshotRequest{}))
	require.NoError(t, err)
	require.Greater(t, pubResp.Msg.PublishedVersion, uint64(0), "published version must be > 0")
	require.Len(t, pubResp.Msg.PublishedChecksum, 32, "published checksum must be 32 bytes")

	// --- Step 2: fetch via SnapshotService.Fetch. ---
	fetchResp, err := snapClient.Fetch(context.Background(),
		connect.NewRequest(&configv1.FetchRequest{}))
	require.NoError(t, err)
	env := fetchResp.Msg.GetSnapshot()
	require.NotNil(t, env, "expected snapshot response from Fetch")

	// --- Step 3: verify version and checksum match the publish response. ---
	assert.Equal(t, pubResp.Msg.PublishedVersion, env.Version)
	assert.Equal(t, pubResp.Msg.PublishedChecksum, env.Checksum)

	// --- Step 4: decode ConfigPayload from envelope bytes. ---
	// SnapshotEnvelope.Payload contains the raw proto-marshalled ConfigPayload.
	var cp configv1.ConfigPayload
	require.NoError(t, proto.Unmarshal(env.Payload, &cp),
		"envelope payload must unmarshal as ConfigPayload")

	require.NotNil(t, cp.Metadata, "ConfigPayload must have metadata")
	assert.Greater(t, cp.SchemaVersion, uint32(0), "schema_version must be > 0")
	assert.NotEmpty(t, cp.Payload, "ConfigPayload.payload must be non-empty (Cherry bundle bytes)")

	// --- Step 5: open the Cherry bundle. ---
	opened, err := cherry.OpenBundleZstd(cp.Payload)
	require.NoError(t, err, "cherry.OpenBundleZstd must succeed on the fetched payload bytes")

	// The opened bundle must carry the expected format version and non-empty content.
	assert.Equal(t, cherry.BundleFormatVersion, opened.Metadata.FormatVersion,
		"opened bundle must have expected format version")
	assert.NotEmpty(t, opened.Metadata.Scopes, "opened bundle must have scopes")
	assert.NotEmpty(t, opened.Blob, "opened bundle must have pack blob bytes")

	// --- Step 6: checksum self-consistency. ---
	// SnapshotEnvelope.Checksum is SHA-256 of the raw envelope payload bytes.
	// Verify this matches what the manager computes by round-tripping through
	// snapshot.VerifyChecksum via a direct manager current-snapshot check.
	current := mgr.Current("default")
	require.NotNil(t, current)
	require.NoError(t, current.VerifyChecksum(),
		"stored snapshot checksum must be self-consistent")
	assert.Equal(t, current.Checksum[:], env.Checksum,
		"envelope checksum must match stored snapshot checksum")

	// --- Step 7: second fetch returns Unchanged. ---
	fetchResp2, err := snapClient.Fetch(context.Background(),
		connect.NewRequest(&configv1.FetchRequest{
			LastVersion:  env.Version,
			LastChecksum: env.Checksum,
		}))
	require.NoError(t, err)
	assert.NotNil(t, fetchResp2.Msg.GetUnchanged(),
		"second fetch with matching version/checksum must return Unchanged")
	assert.Nil(t, fetchResp2.Msg.GetSnapshot(),
		"Unchanged response must carry no snapshot body")
}
