// Package mappedsplit provides first-class Orange helpers for delivering
// Cherry mapped-split bundles through Orange's mapped-split SnapshotService API.
package mappedsplit

import (
	"fmt"

	"github.com/dio/cherry"
)

const (
	// MapLane is retained as the example development lane name for older
	// callers. Production embedders derive lane from authenticated identity.
	MapLane = "bundle-map"

	// BundleMediaType identifies an individual Cherry component bundle payload.
	BundleMediaType = "application/vnd.dio.orange.cherry-bundle"
	// PayloadEncoding is used for zstd Cherry component payload bytes because
	// the ConfigPayload already carries the exact bytes.
	PayloadEncoding = "identity"

	// SplitMapFormatVersion is the experimental mapped-split map format.
	SplitMapFormatVersion = "experimental-split-map-v1"
)

// SplitMap describes the full state of mapped-split resources for one
// authenticated lane. It is SoTW: omitted component refs are absent from the
// next active view.
type SplitMap struct {
	FormatVersion           string                          `json:"format_version"`
	ScopeKind               string                          `json:"scope_kind"`
	ScopeID                 string                          `json:"scope_id"`
	Scopes                  []string                        `json:"scopes"`
	GenerationID            string                          `json:"generation_id"`
	MapRevision             int                             `json:"map_revision"`
	LLMDefaultPrincipalSlug string                          `json:"llm_default_principal_slug"`
	Partitioning            map[string]PartitionSpec        `json:"partitioning"`
	Bundles                 map[string]BundleRef            `json:"bundles"`
	PartitionBundles        map[string][]PartitionBundleRef `json:"partition_bundles"`
}

// PartitionSpec describes the deterministic cohorting for one partitioned lane.
type PartitionSpec struct {
	Algorithm  string `json:"algorithm"`
	Key        string `json:"key"`
	Partitions int    `json:"partitions"`
}

// BundleRef identifies one component bundle payload and the resource that
// FetchMappedSplitBundle serves inside the authenticated lane.
type BundleRef struct {
	// ID is the stable component identity, for example "llm-user-key-003".
	ID        string `json:"id"`
	Resource  string `json:"resource"`
	Component string `json:"component"`
	Checksum  uint64 `json:"checksum"`
	Size      uint64 `json:"size"`
}

// PartitionBundleRef identifies one partitioned component bundle.
type PartitionBundleRef struct {
	Partition int `json:"partition"`
	BundleRef
}

// Spec returns the Cherry mapped-split partitioning spec encoded in m.
func (m SplitMap) Spec() (cherry.MappedSplitSpec, error) {
	llm, err := partitionCount(m.Partitioning, cherry.MappedSplitLaneLLMUserKey)
	if err != nil {
		return cherry.MappedSplitSpec{}, err
	}
	mcp, err := partitionCount(m.Partitioning, cherry.MappedSplitLaneMCPUserProfile)
	if err != nil {
		return cherry.MappedSplitSpec{}, err
	}
	spec := cherry.MappedSplitSpec{
		LLMUserKeyPartitions:     llm,
		MCPUserProfilePartitions: mcp,
	}
	if err := spec.Validate(); err != nil {
		return cherry.MappedSplitSpec{}, err
	}
	return spec, nil
}

func partitionCount(partitioning map[string]PartitionSpec, lane cherry.MappedSplitLane) (int, error) {
	spec, ok := partitioning[string(lane)]
	if !ok {
		return 0, fmt.Errorf("split map missing partitioning for %s", lane)
	}
	if spec.Algorithm != "fnv1a64" {
		return 0, fmt.Errorf("split map partitioning for %s uses unsupported algorithm %q", lane, spec.Algorithm)
	}
	if spec.Partitions <= 0 {
		return 0, fmt.Errorf("split map partitioning for %s must be positive", lane)
	}
	return spec.Partitions, nil
}
