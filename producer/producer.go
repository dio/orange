// Package producer builds Cherry bundles from normalized cherry.Input and wraps
// the result in a ConfigPayload for snapshot publication.
package producer

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/dio/cherry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/internal/otelx"
)

const (
	// BundleMediaType identifies the payload format in ConfigPayload.format.
	BundleMediaType = "application/vnd.dio.orange.config-bundle"
	// BundleEncoding is the byte encoding of the embedded bundle.
	BundleEncoding = "zstd"
	// ConfigPayloadSchemaVersion versions the ConfigPayload wrapper.
	ConfigPayloadSchemaVersion = 1
)

var producerTracer = otelx.Tracer("producer")

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
	otelx.AutoConfigureFromEnv()

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
	otelx.AutoConfigureFromEnv()
	start := time.Now()
	resultLabel := "success"
	defer func() {
		recordProducerOperation(ctx, "producer.build", resultLabel, start)
	}()

	ctx, span := producerTracer.Start(ctx, "orange.producer.Builder.Build",
		trace.WithAttributes(
			attribute.String("orange.lane", lane),
			attribute.String("orange.scope_kind", sel.ScopeKind),
			attribute.Int("orange.scope_count", len(result.Scopes)),
		),
	)
	defer span.End()

	if err := ctx.Err(); err != nil {
		resultLabel = "error"
		otelx.RecordError(span, err)
		return BuildOutput{}, err
	}

	blob, manifest, err := cherry.BuildWithManifest(result.Input)
	if err != nil {
		resultLabel = "error"
		otelx.RecordError(span, err)
		return BuildOutput{}, fmt.Errorf("build cherry pack: %w", err)
	}
	span.SetAttributes(
		attribute.Int64("orange.manifest_size_bytes", int64(manifest.SizeBytes)),
		attribute.Int64("orange.manifest_checksum", int64(manifest.Checksum)),
	)
	if err := ctx.Err(); err != nil {
		resultLabel = "error"
		otelx.RecordError(span, err)
		return BuildOutput{}, err
	}

	bundle := cherry.NewBundle(sel.ScopeKind, sel.ScopeID, result.Scopes, blob, manifest)

	compressed, err := cherry.EncodeBundleZstd(bundle)
	if err != nil {
		resultLabel = "error"
		otelx.RecordError(span, err)
		return BuildOutput{}, fmt.Errorf("encode cherry bundle zstd: %w", err)
	}
	span.SetAttributes(attribute.Int("orange.payload_size_bytes", len(compressed)))
	if err := ctx.Err(); err != nil {
		resultLabel = "error"
		otelx.RecordError(span, err)
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
