// Package producer builds Cherry bundles from normalized cherry.Input and wraps
// the result in a ConfigPayload for snapshot publication.
package producer

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/dio/cherry"
	"google.golang.org/protobuf/types/known/timestamppb"

	configv1 "github.com/dio/orange/api/orange/config/v1"
)

const (
	// BundleMediaType identifies the payload format in ConfigPayload.format.
	BundleMediaType = "application/vnd.dio.orange.config-bundle"
	// BundleEncoding is the byte encoding of the embedded bundle.
	BundleEncoding = "zstd"
	// ConfigPayloadSchemaVersion versions the ConfigPayload wrapper.
	ConfigPayloadSchemaVersion = 1
)

// Selection describes what the caller wants built. ScopeKind and ScopeID are
// opaque labels owned by the embedding control plane.
type Selection struct {
	ScopeKind string
	ScopeID   string
}

// BuildResult is the normalized cherry.Input plus associated metadata returned
// by the embedding application's mutation callback before Cherry packing.
type BuildResult struct {
	SourceRevision string
	Scopes         []string
	Input          cherry.Input
}

// BuildOutput holds the packed artifacts produced by Builder.Build.
type BuildOutput struct {
	Payload    *configv1.ConfigPayload
	BundleZstd []byte
	// Checksum is SHA-256 of BundleZstd, echoed in ConfigPayload.metadata.payload_sha256.
	Checksum [32]byte
}

// Options configures a Builder. All fields are optional.
type Options struct {
	// Producer is the name/version string stored in snapshot metadata.
	Producer string
	// Clock provides the current time for metadata; defaults to time.Now.
	Clock func() time.Time
}

// Builder converts normalized cherry.Input into a ConfigPayload.
type Builder struct {
	opts Options
}

// NewBuilder creates a Builder with the given options.
func NewBuilder(opts Options) *Builder {
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &Builder{opts: opts}
}

// Build packs result.Input into a Cherry bundle and wraps it in a
// ConfigPayload. lane is the snapshot lane for this publish; it is written
// into SnapshotMetadata so readers can verify which lane they received.
// The returned BuildOutput contains immutable copies of all byte slices.
func (b *Builder) Build(ctx context.Context, sel Selection, lane string, result BuildResult) (BuildOutput, error) {
	if err := ctx.Err(); err != nil {
		return BuildOutput{}, err
	}

	blob, manifest, err := cherry.BuildWithManifest(result.Input)
	if err != nil {
		return BuildOutput{}, fmt.Errorf("build cherry pack: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return BuildOutput{}, err
	}

	bundle := cherry.NewBundle(sel.ScopeKind, sel.ScopeID, result.Scopes, blob, manifest)

	compressed, err := cherry.EncodeBundleZstd(bundle)
	if err != nil {
		return BuildOutput{}, fmt.Errorf("encode cherry bundle zstd: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return BuildOutput{}, err
	}

	checksum := sha256.Sum256(compressed)

	scopes := make([]string, len(result.Scopes))
	copy(scopes, result.Scopes)

	// Two independent copies: ConfigPayload.Payload and BuildOutput.BundleZstd
	// must not alias so mutating one cannot corrupt the other.
	payloadBytes := make([]byte, len(compressed))
	copy(payloadBytes, compressed)
	bundleZstd := make([]byte, len(compressed))
	copy(bundleZstd, compressed)

	payload := &configv1.ConfigPayload{
		SchemaVersion: ConfigPayloadSchemaVersion,
		Format: &configv1.PayloadFormat{
			MediaType:     BundleMediaType,
			Encoding:      BundleEncoding,
			FormatVersion: cherry.BundleFormatVersion,
		},
		Payload: payloadBytes,
		Metadata: &configv1.SnapshotMetadata{
			Producer:       b.opts.Producer,
			SourceRevision: result.SourceRevision,
			CreatedAt:      timestamppb.New(b.opts.Clock()),
			Lane:           lane,
			ScopeKind:      sel.ScopeKind,
			ScopeId:        sel.ScopeID,
			Scopes:         scopes,
			PayloadSize:    uint64(len(payloadBytes)),
			PayloadSha256:  checksum[:],
		},
	}

	return BuildOutput{
		Payload:    payload,
		BundleZstd: bundleZstd,
		Checksum:   checksum,
	}, nil
}
