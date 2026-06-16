package mappedsplit

import (
	"context"
	"testing"

	"github.com/dio/cherry"
	"github.com/stretchr/testify/require"

	"github.com/dio/orange/producer"
)

func TestBuildOpenReuseAndOmit(t *testing.T) {
	ctx := context.Background()
	spec := cherry.MappedSplitSpec{LLMUserKeyPartitions: 2, MCPUserProfilePartitions: 2}
	builder := NewBuilder(BuildOptions{Producer: "test"})

	initial, err := builder.Build(ctx, buildRequest(spec, "gen1", 1, testInput("orange://alice/openai"), -1))
	require.NoError(t, err)

	current, stats, err := Open(ctx, nil, initial.Map, componentFetcher(initial))
	require.NoError(t, err)
	require.Equal(t, ApplyStats{Fetched: 6}, stats)
	require.Equal(t, "test", current.LLMGeneric.SourceRevision)

	llm, ok := current.ResolveLLM("prod", "slug:alice", "gpt-4o-mini")
	require.True(t, ok)
	require.Equal(t, "orange://alice/openai", llm.SecretRef)
	mcp, ok := current.ResolveMCP("prod", "profile-dev-tools")
	require.True(t, ok)
	require.Len(t, mcp.Tools, 1)

	removed, err := spec.MCPUserProfileBundle("profile-dev-tools")
	require.NoError(t, err)
	next, err := builder.Build(ctx, buildRequest(spec, "gen1", 2, testInput("orange://alice/openai-updated"), removed.Partition))
	require.NoError(t, err)

	current, stats, err = Open(ctx, current, next.Map, componentFetcher(next))
	require.NoError(t, err)
	require.Equal(t, 1, stats.Fetched)
	require.Equal(t, 4, stats.Reused)
	require.Equal(t, 1, stats.Omitted)

	llm, ok = current.ResolveLLM("prod", "slug:alice", "gpt-4o-mini")
	require.True(t, ok)
	require.Equal(t, "orange://alice/openai-updated", llm.SecretRef)
	_, ok = current.ResolveMCP("prod", "profile-dev-tools")
	require.False(t, ok)
}

func TestOpenReusesMatchingRefsAcrossMapGenerationChange(t *testing.T) {
	ctx := context.Background()
	spec := cherry.MappedSplitSpec{LLMUserKeyPartitions: 2, MCPUserProfilePartitions: 2}
	builder := NewBuilder(BuildOptions{Producer: "test"})

	initial, err := builder.Build(ctx, buildRequest(spec, "gen1", 1, testInput("orange://alice/openai"), -1))
	require.NoError(t, err)

	current, stats, err := Open(ctx, nil, initial.Map, componentFetcher(initial))
	require.NoError(t, err)
	require.Equal(t, ApplyStats{Fetched: 6}, stats)

	nextMap := initial.Map
	nextMap.GenerationID = "gen2"
	nextMap.MapRevision = 2

	current, stats, err = Open(ctx, current, nextMap, func(context.Context, BundleRef) (ComponentPayload, bool, error) {
		t.Fatal("matching refs should be reused without fetching")
		return ComponentPayload{}, false, nil
	})
	require.NoError(t, err)
	require.Equal(t, ApplyStats{Reused: 6}, stats)
	require.Equal(t, "gen2", current.Map.GenerationID)
	require.Equal(t, "gen1", current.LLMGeneric.Opened.Metadata.GenerationID)
	require.Equal(t, "test", current.LLMGeneric.SourceRevision)
}

func buildRequest(spec cherry.MappedSplitSpec, generation string, revision int, input cherry.Input, omitMCPPartition int) BuildRequest {
	components := make([]ComponentInput, 0, 2+spec.LLMUserKeyPartitions+spec.MCPUserProfilePartitions)
	llmGeneric, _ := spec.CatalogBundle(cherry.MappedSplitLaneLLMGeneric)
	mcpServers, _ := spec.CatalogBundle(cherry.MappedSplitLaneMCPServers)
	components = append(components,
		ComponentInput{Key: llmGeneric, Input: testLLMGenericInput(input)},
		ComponentInput{Key: mcpServers, Input: testMCPServersInput(input)},
	)
	for partition := range spec.LLMUserKeyPartitions {
		components = append(components, ComponentInput{
			Key:   cherry.MappedSplitBundleKey{Lane: cherry.MappedSplitLaneLLMUserKey, Partition: partition},
			Input: testLLMPartitionInput(input, spec, partition),
		})
	}
	for partition := range spec.MCPUserProfilePartitions {
		if partition == omitMCPPartition {
			continue
		}
		components = append(components, ComponentInput{
			Key:   cherry.MappedSplitBundleKey{Lane: cherry.MappedSplitLaneMCPUserProfile, Partition: partition},
			Input: testMCPProfilePartitionInput(input, spec, partition),
		})
	}
	return BuildRequest{
		Selection:               producer.Selection{ScopeKind: "workspace", ScopeID: "prod"},
		Scopes:                  []string{"prod"},
		SourceRevision:          "test",
		GenerationID:            generation,
		MapRevision:             revision,
		LLMDefaultPrincipalSlug: "slug:default",
		Spec:                    spec,
		Components:              components,
	}
}

func componentFetcher(out BuildOutput) ComponentFetcher {
	return func(_ context.Context, ref BundleRef) (ComponentPayload, bool, error) {
		component := out.Components[ref.Component]
		return ComponentPayload{
			BundleZstd:     component.BundleZstd,
			SourceRevision: component.Payload.GetMetadata().GetSourceRevision(),
		}, false, nil
	}
}

func testInput(aliceSecret string) cherry.Input {
	return cherry.Input{
		Providers: []cherry.Provider{{
			ID:            "openai",
			Kind:          "openai",
			BackendSchema: "openai",
			Endpoint:      "https://api.openai.com",
			SecretRef:     "env://OPENAI_PLATFORM",
			AuthType:      "bearer",
		}},
		Models: []cherry.Model{{ID: "gpt-4o-mini", Provider: "openai", Name: "gpt-4o-mini", Mode: "chat"}},
		MCPServers: []cherry.MCPServer{{
			ID:        "github",
			Endpoint:  "https://mcp.github.example",
			SecretRef: "env://GITHUB_PLATFORM",
			AuthType:  "bearer",
		}},
		Scopes: []cherry.Scope{{
			ID: "prod",
			Principals: []cherry.Principal{
				testPrincipal("slug:alice", aliceSecret),
				testPrincipal("slug:bob", "orange://bob/openai"),
			},
			MCPProfiles: []cherry.MCPProfile{
				{
					Path: "s/github",
					Tools: []cherry.MCPToolBinding{{
						ExposedName: "github__list_repos",
						Server:      "github",
						Tool:        "list_repos",
						SecretRef:   "env://GITHUB_PLATFORM",
						AuthType:    "bearer",
					}},
				},
				{
					Path: "profile-dev-tools",
					Tools: []cherry.MCPToolBinding{{
						ExposedName: "github__list_repos",
						Server:      "github",
						Tool:        "list_repos",
						SecretRef:   "orange://alice/github",
						AuthType:    "bearer",
					}},
				},
			},
		}},
	}
}

func testPrincipal(slug string, secret string) cherry.Principal {
	return cherry.Principal{
		Slug: slug,
		ModelRoutes: map[string]cherry.RoutePlan{
			"gpt-4o-mini": {
				Kind:      cherry.RouteKindTarget,
				Provider:  "openai",
				Model:     "gpt-4o-mini",
				SecretRef: secret,
			},
		},
		Rate: cherry.RatePolicy{USDPerDayCents: 1000, RPM: 60, OnExceed: "reject"},
	}
}

func testLLMGenericInput(input cherry.Input) cherry.Input {
	return cherry.Input{
		Providers: input.Providers,
		Models:    input.Models,
		Scopes: []cherry.Scope{{
			ID: "prod",
			Principals: []cherry.Principal{{
				Slug: "slug:default",
				ModelRoutes: map[string]cherry.RoutePlan{
					"gpt-4o-mini": {Kind: cherry.RouteKindTarget, Provider: "openai", Model: "gpt-4o-mini"},
				},
				Rate: cherry.RatePolicy{USDPerDayCents: 1000, RPM: 300, OnExceed: "reject"},
			}},
		}},
	}
}

func testLLMPartitionInput(input cherry.Input, spec cherry.MappedSplitSpec, partition int) cherry.Input {
	out := cherry.Input{Providers: input.Providers, Models: input.Models, Scopes: []cherry.Scope{{ID: "prod"}}}
	for _, principal := range input.Scopes[0].Principals {
		got, _ := spec.LLMUserKeyPartition(principal.Slug)
		if got == partition {
			out.Scopes[0].Principals = append(out.Scopes[0].Principals, principal)
		}
	}
	return out
}

func testMCPServersInput(input cherry.Input) cherry.Input {
	return cherry.Input{
		MCPServers: input.MCPServers,
		Scopes: []cherry.Scope{{
			ID:          "prod",
			MCPProfiles: []cherry.MCPProfile{input.Scopes[0].MCPProfiles[0]},
		}},
	}
}

func testMCPProfilePartitionInput(input cherry.Input, spec cherry.MappedSplitSpec, partition int) cherry.Input {
	out := cherry.Input{MCPServers: input.MCPServers, Scopes: []cherry.Scope{{ID: "prod"}}}
	for _, profile := range input.Scopes[0].MCPProfiles[1:] {
		got, _ := spec.MCPUserProfilePartition(profile.Path)
		if got == partition {
			out.Scopes[0].MCPProfiles = append(out.Scopes[0].MCPProfiles, profile)
		}
	}
	return out
}
