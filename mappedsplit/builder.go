package mappedsplit

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/dio/cherry"
	"google.golang.org/protobuf/types/known/timestamppb"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/producer"
)

// ComponentInput is one already-normalized Cherry input layer for a mapped
// split component. Domain-specific source loading and slicing happen before
// this type is created.
type ComponentInput struct {
	Key   cherry.MappedSplitBundleKey
	Input cherry.Input
}

// BuildRequest describes one mapped-split generation to build.
type BuildRequest struct {
	Selection               producer.Selection
	Lane                    string
	Scopes                  []string
	SourceRevision          string
	GenerationID            string
	MapRevision             int
	LLMDefaultPrincipalSlug string
	Spec                    cherry.MappedSplitSpec
	Components              []ComponentInput
}

// ComponentOutput is one built component bundle and its Orange payload wrapper.
type ComponentOutput struct {
	Ref        BundleRef
	Payload    *configv1.ConfigPayload
	BundleZstd []byte
	Manifest   cherry.Manifest
}

// BuildOutput is the complete mapped-split publication unit.
type BuildOutput struct {
	Map          SplitMap
	Lane         string
	Components   map[string]ComponentOutput
	ComponentSeq []string
}

// BuildOptions configures a mapped-split builder.
type BuildOptions struct {
	Producer string
	Clock    func() time.Time
	// ResourceForComponent maps a Cherry component name such as
	// "llm-user-key-003" to the resource that serves it inside the
	// authenticated lane. If nil, the default resource is the component name.
	ResourceForComponent func(component string) string
}

// Builder builds mapped-split map and component ConfigPayload values.
type Builder struct {
	opts BuildOptions
}

// NewBuilder creates a mapped-split Builder.
func NewBuilder(opts BuildOptions) *Builder {
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.ResourceForComponent == nil {
		opts.ResourceForComponent = func(component string) string {
			return component
		}
	}
	return &Builder{opts: opts}
}

// Build builds every supplied component and a split map that references those
// components. It does not publish snapshots; callers publish each
// ComponentOutput.Payload on the resources referenced by the map, then publish a
// typed map snapshot with NewMapSnapshot.
func (b *Builder) Build(_ context.Context, req BuildRequest) (BuildOutput, error) {
	if err := req.Spec.Validate(); err != nil {
		return BuildOutput{}, err
	}
	if req.Selection.ScopeKind == "" || req.Selection.ScopeID == "" {
		return BuildOutput{}, fmt.Errorf("selection scope kind and scope ID are required")
	}
	if len(req.Scopes) == 0 {
		return BuildOutput{}, fmt.Errorf("at least one concrete scope is required")
	}
	if req.GenerationID == "" {
		return BuildOutput{}, fmt.Errorf("generation ID is required")
	}
	if req.MapRevision <= 0 {
		return BuildOutput{}, fmt.Errorf("map revision must be positive")
	}

	splitMap := SplitMap{
		FormatVersion:           SplitMapFormatVersion,
		ScopeKind:               req.Selection.ScopeKind,
		ScopeID:                 req.Selection.ScopeID,
		Scopes:                  append([]string(nil), req.Scopes...),
		GenerationID:            req.GenerationID,
		MapRevision:             req.MapRevision,
		LLMDefaultPrincipalSlug: req.LLMDefaultPrincipalSlug,
		Partitioning: map[string]PartitionSpec{
			string(cherry.MappedSplitLaneLLMUserKey): {
				Algorithm:  "fnv1a64",
				Key:        "principal_slug",
				Partitions: req.Spec.LLMUserKeyPartitions,
			},
			string(cherry.MappedSplitLaneMCPUserProfile): {
				Algorithm:  "fnv1a64",
				Key:        "path_suffix",
				Partitions: req.Spec.MCPUserProfilePartitions,
			},
		},
		Bundles:          map[string]BundleRef{},
		PartitionBundles: map[string][]PartitionBundleRef{},
	}

	components := make(map[string]ComponentOutput, len(req.Components))
	componentSeq := make([]string, 0, len(req.Components))
	seen := map[string]struct{}{}
	for _, component := range req.Components {
		name := component.Key.Component()
		if _, ok := seen[name]; ok {
			return BuildOutput{}, fmt.Errorf("duplicate mapped split component %q", name)
		}
		seen[name] = struct{}{}

		out, err := b.buildComponent(req, component)
		if err != nil {
			return BuildOutput{}, fmt.Errorf("build %s: %w", name, err)
		}
		components[name] = out
		componentSeq = append(componentSeq, name)
		if component.Key.IsPartitioned() {
			splitMap.PartitionBundles[string(component.Key.Lane)] = append(
				splitMap.PartitionBundles[string(component.Key.Lane)],
				PartitionBundleRef{Partition: component.Key.Partition, BundleRef: out.Ref},
			)
			continue
		}
		splitMap.Bundles[string(component.Key.Lane)] = out.Ref
	}

	return BuildOutput{
		Map:          splitMap,
		Lane:         req.Lane,
		Components:   components,
		ComponentSeq: componentSeq,
	}, nil
}

func (b *Builder) buildComponent(req BuildRequest, component ComponentInput) (ComponentOutput, error) {
	blob, manifest, err := cherry.BuildWithManifest(component.Input)
	if err != nil {
		return ComponentOutput{}, fmt.Errorf("build cherry pack: %w", err)
	}

	bundle := cherry.NewBundle(req.Selection.ScopeKind, req.Selection.ScopeID, req.Scopes, blob, manifest)
	bundle.Metadata.GenerationID = req.GenerationID

	compressed, err := cherry.EncodeBundleZstd(bundle)
	if err != nil {
		return ComponentOutput{}, fmt.Errorf("encode cherry bundle zstd: %w", err)
	}

	componentName := component.Key.Component()
	ref := BundleRef{
		ID:        componentName,
		Resource:  b.opts.ResourceForComponent(componentName),
		Component: componentName,
		Checksum:  manifest.Checksum,
		Size:      manifest.SizeBytes,
	}
	payload := b.configPayload(req, BundleMediaType, cherry.BundleFormatVersion, compressed)
	return ComponentOutput{
		Ref:        ref,
		Payload:    payload,
		BundleZstd: append([]byte(nil), compressed...),
		Manifest:   manifest,
	}, nil
}

func (b *Builder) configPayload(req BuildRequest, mediaType string, formatVersion string, payload []byte) *configv1.ConfigPayload {
	sum := sha256.Sum256(payload)
	return &configv1.ConfigPayload{
		SchemaVersion: producer.ConfigPayloadSchemaVersion,
		Format: &configv1.PayloadFormat{
			MediaType:     mediaType,
			Encoding:      PayloadEncoding,
			FormatVersion: formatVersion,
		},
		Payload: append([]byte(nil), payload...),
		Metadata: &configv1.SnapshotMetadata{
			Producer:       b.opts.Producer,
			SourceRevision: req.SourceRevision,
			CreatedAt:      timestamppb.New(b.opts.Clock()),
			Lane:           req.Lane,
			ScopeKind:      req.Selection.ScopeKind,
			ScopeId:        req.Selection.ScopeID,
			Scopes:         append([]string(nil), req.Scopes...),
			PayloadSize:    uint64(len(payload)),
			PayloadSha256:  sum[:],
		},
	}
}
