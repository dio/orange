// Package snapshot builds and holds immutable published snapshots.
// A Snapshot is the fully assembled artifact: ConfigPayload, SnapshotEnvelope,
// Cherry bundle bytes, and the envelope checksum. Once built, none of its
// fields are mutated.
package snapshot

import (
	"crypto/sha256"
	"fmt"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"google.golang.org/protobuf/proto"
)

// Snapshot is an immutable, fully assembled snapshot ready for publication and
// fetch. All byte slices are defensive copies; callers cannot mutate the
// published artifact.
type Snapshot struct {
	Lane     string
	Version  uint64
	Scopes   []string
	Payload  *configv1.ConfigPayload
	Envelope *configv1.SnapshotEnvelope
	// BundleZstd is the raw zstd-compressed Cherry bundle embedded in
	// Payload.payload. Retained here so the manager can pass it to callers
	// that need the bundle bytes directly without re-extracting from the
	// envelope.
	BundleZstd []byte
	// Checksum is SHA-256 of the raw (marshalled) ConfigPayload proto bytes,
	// matching SnapshotEnvelope.checksum.
	Checksum [32]byte
}

// New marshals payload into a SnapshotEnvelope and assembles an immutable
// Snapshot. bundleZstd must be the cherry bundle bytes already stored inside
// payload.payload; it is copied and retained for direct-access callers.
//
// version must be > 0; callers own version sequencing.
func New(version uint64, payload *configv1.ConfigPayload, bundleZstd []byte) (*Snapshot, error) {
	if version == 0 {
		return nil, fmt.Errorf("snapshot version must be > 0")
	}
	if payload == nil {
		return nil, fmt.Errorf("payload must not be nil")
	}

	// Clone the payload so the snapshot owns its own copy; mutating the
	// caller's payload after New cannot affect the stored snapshot.
	ownedPayload := proto.Clone(payload).(*configv1.ConfigPayload)

	raw, err := proto.Marshal(ownedPayload)
	if err != nil {
		return nil, fmt.Errorf("marshal config payload: %w", err)
	}

	checksum := sha256.Sum256(raw)

	// Defensive copies of all byte slices.
	rawCopy := make([]byte, len(raw))
	copy(rawCopy, raw)

	bundleCopy := make([]byte, len(bundleZstd))
	copy(bundleCopy, bundleZstd)

	var scopes []string
	if ownedPayload.Metadata != nil {
		scopes = make([]string, len(ownedPayload.Metadata.Scopes))
		copy(scopes, ownedPayload.Metadata.Scopes)
	}

	lane := ""
	if ownedPayload.Metadata != nil {
		lane = ownedPayload.Metadata.Lane
	}

	envelope := &configv1.SnapshotEnvelope{
		Version:  version,
		Payload:  rawCopy,
		Checksum: checksum[:],
	}

	return &Snapshot{
		Lane:       lane,
		Version:    version,
		Scopes:     scopes,
		Payload:    ownedPayload,
		Envelope:   envelope,
		BundleZstd: bundleCopy,
		Checksum:   checksum,
	}, nil
}

// clone returns a deep copy of s. All byte slices and proto messages are
// independently copied so the caller cannot mutate the stored snapshot.
func (s *Snapshot) clone() *Snapshot {
	if s == nil {
		return nil
	}
	scopes := make([]string, len(s.Scopes))
	copy(scopes, s.Scopes)
	bundleZstd := make([]byte, len(s.BundleZstd))
	copy(bundleZstd, s.BundleZstd)
	return &Snapshot{
		Lane:       s.Lane,
		Version:    s.Version,
		Scopes:     scopes,
		Payload:    proto.Clone(s.Payload).(*configv1.ConfigPayload),
		Envelope:   proto.Clone(s.Envelope).(*configv1.SnapshotEnvelope),
		BundleZstd: bundleZstd,
		Checksum:   s.Checksum,
	}
}

// VerifyChecksum recomputes the SHA-256 of the envelope's payload bytes and
// returns an error if they do not match the stored checksum. This is a
// diagnostic helper; the snapshot is already verified when New returns without
// error.
func (s *Snapshot) VerifyChecksum() error {
	got := sha256.Sum256(s.Envelope.Payload)
	if got != s.Checksum {
		return fmt.Errorf("checksum mismatch: stored %x, computed %x", s.Checksum, got)
	}
	return nil
}
