package mappedsplit

import (
	"context"
	"fmt"
	"strings"

	"github.com/dio/cherry"
)

// ComponentPayload is the fetched component bundle bytes plus diagnostic
// metadata from the Orange snapshot wrapper.
type ComponentPayload struct {
	BundleZstd     []byte
	SourceRevision string
}

// ComponentFetcher fetches the component bundle referenced by ref. It is
// commonly backed by one FetchMappedSplitBundle client per resource inside the
// authenticated lane.
type ComponentFetcher func(ctx context.Context, ref BundleRef) (ComponentPayload, bool, error)

// ApplyStats reports how a split map was applied relative to the previous view.
type ApplyStats struct {
	Fetched int
	Reused  int
	Omitted int
}

// OpenedComponent is an opened Cherry component plus the ref that selected it.
type OpenedComponent struct {
	Ref            BundleRef
	SourceRevision string
	Opened         cherry.OpenedBundle
}

// Opened is the active, queryable mapped-split view.
type Opened struct {
	Map            SplitMap
	Spec           cherry.MappedSplitSpec
	LLMGeneric     OpenedComponent
	MCPServers     OpenedComponent
	LLMUserKey     []OpenedComponent
	MCPUserProfile []OpenedComponent
}

// Open opens a mapped-split view, reusing unchanged components from previous
// and fetching only missing or stale refs.
func Open(ctx context.Context, previous *Opened, splitMap SplitMap, fetch ComponentFetcher) (*Opened, ApplyStats, error) {
	if fetch == nil {
		return nil, ApplyStats{}, fmt.Errorf("component fetcher is required")
	}
	spec, err := splitMap.Spec()
	if err != nil {
		return nil, ApplyStats{}, err
	}

	out := &Opened{
		Map:            splitMap,
		Spec:           spec,
		LLMUserKey:     make([]OpenedComponent, spec.LLMUserKeyPartitions),
		MCPUserProfile: make([]OpenedComponent, spec.MCPUserProfilePartitions),
	}
	stats := ApplyStats{}

	llmGenericRef, ok := splitMap.Bundles[string(cherry.MappedSplitLaneLLMGeneric)]
	if !ok {
		return nil, stats, fmt.Errorf("split map missing %s bundle", cherry.MappedSplitLaneLLMGeneric)
	}
	out.LLMGeneric, err = openRef(ctx, previousCatalog(previous, cherry.MappedSplitLaneLLMGeneric), splitMap, llmGenericRef, fetch, &stats)
	if err != nil {
		return nil, stats, fmt.Errorf("open llm-generic: %w", err)
	}

	mcpServersRef, ok := splitMap.Bundles[string(cherry.MappedSplitLaneMCPServers)]
	if !ok {
		return nil, stats, fmt.Errorf("split map missing %s bundle", cherry.MappedSplitLaneMCPServers)
	}
	out.MCPServers, err = openRef(ctx, previousCatalog(previous, cherry.MappedSplitLaneMCPServers), splitMap, mcpServersRef, fetch, &stats)
	if err != nil {
		return nil, stats, fmt.Errorf("open mcp-servers: %w", err)
	}

	if err := openPartitioned(ctx, previous, splitMap, cherry.MappedSplitLaneLLMUserKey, out.LLMUserKey, fetch, &stats); err != nil {
		return nil, stats, err
	}
	if err := openPartitioned(ctx, previous, splitMap, cherry.MappedSplitLaneMCPUserProfile, out.MCPUserProfile, fetch, &stats); err != nil {
		return nil, stats, err
	}
	if previous != nil {
		stats.Omitted += countOmitted(previous.LLMUserKey, out.LLMUserKey)
		stats.Omitted += countOmitted(previous.MCPUserProfile, out.MCPUserProfile)
	}
	return out, stats, nil
}

// ResolveLLM resolves a key-specific LLM route first, then falls back to the
// generic/default LLM component.
func (o *Opened) ResolveLLM(scopeID string, principalSlug string, modelID string) (cherry.LLMResult, bool) {
	if o == nil {
		return cherry.LLMResult{}, false
	}
	if key, err := o.Spec.LLMUserKeyBundle(principalSlug); err == nil && key.Partition >= 0 && key.Partition < len(o.LLMUserKey) {
		if component := o.LLMUserKey[key.Partition]; component.Ref.Resource != "" {
			if result, ok := component.Opened.Reader.ResolveLLM(scopeID, principalSlug, modelID); ok {
				return result, true
			}
		}
	}
	genericSlug := principalSlug
	if o.Map.LLMDefaultPrincipalSlug != "" {
		genericSlug = o.Map.LLMDefaultPrincipalSlug
	}
	return o.LLMGeneric.Opened.Reader.ResolveLLM(scopeID, genericSlug, modelID)
}

// ResolveMCP resolves direct server paths from the MCP server component and
// profile paths from the cohort-selected MCP profile component.
func (o *Opened) ResolveMCP(scopeID string, pathSuffix string) (cherry.MCPResult, bool) {
	if o == nil {
		return cherry.MCPResult{}, false
	}
	if strings.HasPrefix(pathSuffix, "s/") {
		return o.MCPServers.Opened.Reader.ResolveMCP(scopeID, pathSuffix)
	}
	if key, err := o.Spec.MCPUserProfileBundle(pathSuffix); err == nil && key.Partition >= 0 && key.Partition < len(o.MCPUserProfile) {
		if component := o.MCPUserProfile[key.Partition]; component.Ref.Resource != "" {
			return component.Opened.Reader.ResolveMCP(scopeID, pathSuffix)
		}
	}
	return cherry.MCPResult{}, false
}

func openPartitioned(
	ctx context.Context,
	previous *Opened,
	splitMap SplitMap,
	lane cherry.MappedSplitLane,
	target []OpenedComponent,
	fetch ComponentFetcher,
	stats *ApplyStats,
) error {
	refs := splitMap.PartitionBundles[string(lane)]
	seen := map[int]struct{}{}
	for _, ref := range refs {
		if ref.Partition < 0 || ref.Partition >= len(target) {
			return fmt.Errorf("%s partition %d out of range", lane, ref.Partition)
		}
		if _, ok := seen[ref.Partition]; ok {
			return fmt.Errorf("%s partition %d appears more than once", lane, ref.Partition)
		}
		seen[ref.Partition] = struct{}{}
		component, err := openRef(ctx, previousPartition(previous, lane, ref.Partition), splitMap, ref.BundleRef, fetch, stats)
		if err != nil {
			return fmt.Errorf("open %s partition %d: %w", lane, ref.Partition, err)
		}
		target[ref.Partition] = component
	}
	return nil
}

func openRef(
	ctx context.Context,
	previous OpenedComponent,
	splitMap SplitMap,
	ref BundleRef,
	fetch ComponentFetcher,
	stats *ApplyStats,
) (OpenedComponent, error) {
	if ref.ID == "" || ref.Resource == "" || ref.Component == "" {
		return OpenedComponent{}, fmt.Errorf("bundle ref must include id, resource, and component")
	}
	if sameRef(previous.Ref, ref) {
		stats.Reused++
		return previous, nil
	}
	payload, unchanged, err := fetch(ctx, ref)
	if err != nil {
		return OpenedComponent{}, err
	}
	if !unchanged {
		stats.Fetched++
	}
	opened, err := cherry.OpenBundleZstd(payload.BundleZstd)
	if err != nil {
		return OpenedComponent{}, err
	}
	if err := validateOpened(ref, splitMap, opened); err != nil {
		return OpenedComponent{}, err
	}
	return OpenedComponent{Ref: ref, SourceRevision: payload.SourceRevision, Opened: opened}, nil
}

func validateOpened(ref BundleRef, splitMap SplitMap, opened cherry.OpenedBundle) error {
	if opened.Metadata.ScopeKind != splitMap.ScopeKind {
		return fmt.Errorf("scope kind mismatch: map=%q bundle=%q", splitMap.ScopeKind, opened.Metadata.ScopeKind)
	}
	if opened.Metadata.ScopeID != splitMap.ScopeID {
		return fmt.Errorf("scope ID mismatch: map=%q bundle=%q", splitMap.ScopeID, opened.Metadata.ScopeID)
	}
	if !sameStrings(opened.Metadata.Scopes, splitMap.Scopes) {
		return fmt.Errorf("concrete scopes mismatch")
	}
	if opened.Metadata.PackManifest.Checksum != ref.Checksum {
		return fmt.Errorf("manifest checksum mismatch for %s", ref.Component)
	}
	if opened.Metadata.PackManifest.SizeBytes != ref.Size {
		return fmt.Errorf("manifest size mismatch for %s", ref.Component)
	}
	return nil
}

func previousCatalog(previous *Opened, lane cherry.MappedSplitLane) OpenedComponent {
	if previous == nil {
		return OpenedComponent{}
	}
	switch lane {
	case cherry.MappedSplitLaneLLMGeneric:
		return previous.LLMGeneric
	case cherry.MappedSplitLaneMCPServers:
		return previous.MCPServers
	default:
		return OpenedComponent{}
	}
}

func previousPartition(previous *Opened, lane cherry.MappedSplitLane, partition int) OpenedComponent {
	if previous == nil || partition < 0 {
		return OpenedComponent{}
	}
	switch lane {
	case cherry.MappedSplitLaneLLMUserKey:
		if partition < len(previous.LLMUserKey) {
			return previous.LLMUserKey[partition]
		}
	case cherry.MappedSplitLaneMCPUserProfile:
		if partition < len(previous.MCPUserProfile) {
			return previous.MCPUserProfile[partition]
		}
	}
	return OpenedComponent{}
}

func countOmitted(previous []OpenedComponent, next []OpenedComponent) int {
	count := 0
	for i, component := range previous {
		if component.Ref.Resource == "" {
			continue
		}
		if i >= len(next) || next[i].Ref.Resource == "" {
			count++
		}
	}
	return count
}

func sameRef(a BundleRef, b BundleRef) bool {
	return a.ID == b.ID &&
		a.Resource == b.Resource &&
		a.Component == b.Component &&
		a.Checksum == b.Checksum &&
		a.Size == b.Size
}

func validateRefs(splitMap SplitMap) error {
	seen := map[string]struct{}{}
	for lane, ref := range splitMap.Bundles {
		if err := validateRefIdentity(lane, ref); err != nil {
			return err
		}
		if _, ok := seen[ref.ID]; ok {
			return fmt.Errorf("split map duplicate bundle ref id %q", ref.ID)
		}
		seen[ref.ID] = struct{}{}
	}
	for lane, refs := range splitMap.PartitionBundles {
		for _, ref := range refs {
			if err := validateRefIdentity(lane, ref.BundleRef); err != nil {
				return err
			}
			if _, ok := seen[ref.ID]; ok {
				return fmt.Errorf("split map duplicate bundle ref id %q", ref.ID)
			}
			seen[ref.ID] = struct{}{}
		}
	}
	return nil
}

func validateRefIdentity(lane string, ref BundleRef) error {
	if ref.ID == "" {
		return fmt.Errorf("split map %s ref missing stable id", lane)
	}
	if ref.Resource == "" {
		return fmt.Errorf("split map %s ref %s missing resource", lane, ref.ID)
	}
	if ref.Component == "" {
		return fmt.Errorf("split map %s ref %s missing component", lane, ref.ID)
	}
	if ref.Component != ref.ID {
		return fmt.Errorf("split map %s ref has id %q but component %q", lane, ref.ID, ref.Component)
	}
	return nil
}

func sameStrings(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
