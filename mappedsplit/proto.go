package mappedsplit

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"google.golang.org/protobuf/proto"

	configv1 "github.com/dio/orange/api/orange/config/v1"
)

// NewMapSnapshot wraps splitMap in the typed mapped-split snapshot used by
// SnapshotService.FetchMappedSplitMap. The checksum is SHA-256 over the
// deterministic marshaled MappedSplitMap proto.
func NewMapSnapshot(version uint64, splitMap SplitMap) (*configv1.MappedSplitSnapshot, error) {
	if version == 0 {
		return nil, fmt.Errorf("mapped split map version must be > 0")
	}
	if splitMap.FormatVersion == "" {
		splitMap.FormatVersion = SplitMapFormatVersion
	}
	if _, err := splitMap.Spec(); err != nil {
		return nil, err
	}

	typed := ToProtoMap(splitMap)
	raw, err := marshalTypedMap(typed)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	return &configv1.MappedSplitSnapshot{
		Version:  version,
		Checksum: sum[:],
		Map:      typed,
	}, nil
}

// DecodeMapSnapshot validates a typed mapped-split snapshot and returns its Go
// map form for mappedsplit.Open.
func DecodeMapSnapshot(snapshot *configv1.MappedSplitSnapshot) (SplitMap, error) {
	if snapshot == nil {
		return SplitMap{}, fmt.Errorf("mapped split snapshot is nil")
	}
	if snapshot.Version == 0 {
		return SplitMap{}, fmt.Errorf("mapped split snapshot version must be > 0")
	}
	if len(snapshot.Checksum) != sha256.Size {
		return SplitMap{}, fmt.Errorf("mapped split snapshot checksum must be %d bytes, got %d", sha256.Size, len(snapshot.Checksum))
	}
	if snapshot.Map == nil {
		return SplitMap{}, fmt.Errorf("mapped split snapshot map is required")
	}
	raw, err := marshalTypedMap(snapshot.Map)
	if err != nil {
		return SplitMap{}, err
	}
	got := sha256.Sum256(raw)
	if !bytes.Equal(got[:], snapshot.Checksum) {
		return SplitMap{}, fmt.Errorf("mapped split snapshot checksum mismatch")
	}

	splitMap, err := FromProtoMap(snapshot.Map)
	if err != nil {
		return SplitMap{}, err
	}
	if splitMap.FormatVersion != SplitMapFormatVersion {
		return SplitMap{}, fmt.Errorf("unsupported split map format %q", splitMap.FormatVersion)
	}
	if _, err := splitMap.Spec(); err != nil {
		return SplitMap{}, err
	}
	if err := validateRefs(splitMap); err != nil {
		return SplitMap{}, err
	}
	return splitMap, nil
}

// ToProtoMap converts a Go split map to the protobuf API type.
func ToProtoMap(splitMap SplitMap) *configv1.MappedSplitMap {
	out := &configv1.MappedSplitMap{
		FormatVersion:           splitMap.FormatVersion,
		ScopeKind:               splitMap.ScopeKind,
		ScopeId:                 splitMap.ScopeID,
		Scopes:                  append([]string(nil), splitMap.Scopes...),
		GenerationId:            splitMap.GenerationID,
		MapRevision:             uint64(splitMap.MapRevision),
		LlmDefaultPrincipalSlug: splitMap.LLMDefaultPrincipalSlug,
		Partitioning:            make(map[string]*configv1.MappedSplitPartitionSpec, len(splitMap.Partitioning)),
		Bundles:                 make(map[string]*configv1.MappedSplitBundleRef, len(splitMap.Bundles)),
		PartitionBundles:        make(map[string]*configv1.MappedSplitPartitionBundleRefs, len(splitMap.PartitionBundles)),
	}
	for lane, spec := range splitMap.Partitioning {
		out.Partitioning[lane] = &configv1.MappedSplitPartitionSpec{
			Algorithm:  spec.Algorithm,
			Key:        spec.Key,
			Partitions: uint32(spec.Partitions),
		}
	}
	for lane, ref := range splitMap.Bundles {
		out.Bundles[lane] = toProtoRef(ref)
	}
	for lane, refs := range splitMap.PartitionBundles {
		wrapper := &configv1.MappedSplitPartitionBundleRefs{Refs: make([]*configv1.MappedSplitPartitionBundleRef, 0, len(refs))}
		for _, ref := range refs {
			wrapper.Refs = append(wrapper.Refs, &configv1.MappedSplitPartitionBundleRef{
				Partition: uint32(ref.Partition),
				Ref:       toProtoRef(ref.BundleRef),
			})
		}
		out.PartitionBundles[lane] = wrapper
	}
	return out
}

// FromProtoMap converts the protobuf API type to the Go split map used by
// mappedsplit.Open.
func FromProtoMap(in *configv1.MappedSplitMap) (SplitMap, error) {
	if in == nil {
		return SplitMap{}, fmt.Errorf("mapped split map is nil")
	}
	if overflowsInt(in.GetMapRevision()) {
		return SplitMap{}, fmt.Errorf("map revision %d overflows int", in.GetMapRevision())
	}
	out := SplitMap{
		FormatVersion:           in.GetFormatVersion(),
		ScopeKind:               in.GetScopeKind(),
		ScopeID:                 in.GetScopeId(),
		Scopes:                  append([]string(nil), in.GetScopes()...),
		GenerationID:            in.GetGenerationId(),
		MapRevision:             int(in.GetMapRevision()),
		LLMDefaultPrincipalSlug: in.GetLlmDefaultPrincipalSlug(),
		Partitioning:            make(map[string]PartitionSpec, len(in.GetPartitioning())),
		Bundles:                 make(map[string]BundleRef, len(in.GetBundles())),
		PartitionBundles:        make(map[string][]PartitionBundleRef, len(in.GetPartitionBundles())),
	}
	for lane, spec := range in.GetPartitioning() {
		if overflowsInt(uint64(spec.GetPartitions())) {
			return SplitMap{}, fmt.Errorf("partition count for %s overflows int", lane)
		}
		out.Partitioning[lane] = PartitionSpec{
			Algorithm:  spec.GetAlgorithm(),
			Key:        spec.GetKey(),
			Partitions: int(spec.GetPartitions()),
		}
	}
	for lane, ref := range in.GetBundles() {
		out.Bundles[lane] = fromProtoRef(ref)
	}
	for lane, wrapper := range in.GetPartitionBundles() {
		refs := wrapper.GetRefs()
		out.PartitionBundles[lane] = make([]PartitionBundleRef, 0, len(refs))
		for _, ref := range refs {
			if overflowsInt(uint64(ref.GetPartition())) {
				return SplitMap{}, fmt.Errorf("partition for %s overflows int", lane)
			}
			out.PartitionBundles[lane] = append(out.PartitionBundles[lane], PartitionBundleRef{
				Partition: int(ref.GetPartition()),
				BundleRef: fromProtoRef(ref.GetRef()),
			})
		}
	}
	return out, nil
}

func marshalTypedMap(splitMap *configv1.MappedSplitMap) ([]byte, error) {
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(splitMap)
	if err != nil {
		return nil, fmt.Errorf("marshal typed split map: %w", err)
	}
	return raw, nil
}

func toProtoRef(ref BundleRef) *configv1.MappedSplitBundleRef {
	return &configv1.MappedSplitBundleRef{
		Id:        ref.ID,
		Resource:  ref.Resource,
		Component: ref.Component,
		Checksum:  ref.Checksum,
		Size:      ref.Size,
	}
}

func fromProtoRef(ref *configv1.MappedSplitBundleRef) BundleRef {
	if ref == nil {
		return BundleRef{}
	}
	return BundleRef{
		ID:        ref.GetId(),
		Resource:  ref.GetResource(),
		Component: ref.GetComponent(),
		Checksum:  ref.GetChecksum(),
		Size:      ref.GetSize(),
	}
}

func overflowsInt(v uint64) bool {
	maxInt := uint64(^uint(0) >> 1)
	return v > maxInt
}
