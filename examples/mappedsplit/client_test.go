package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dio/orange/config"
	"github.com/dio/orange/mappedsplit"
)

func TestMappedSplitCompletionCandidatesIncludeDynamicViewData(t *testing.T) {
	opened := testMappedSplitOpened(t)

	assert.Contains(t, mappedSplitCompletionCandidates(opened, defaultScope, nil), "llm")
	assert.Contains(t, mappedSplitCompletionCandidates(opened, defaultScope, []string{"use"}), defaultScope)

	llmTopLevel := mappedSplitCompletionCandidates(opened, defaultScope, []string{"llm"})
	assert.Contains(t, llmTopLevel, "models")
	assert.Contains(t, llmTopLevel, defaultScope)
	assert.Contains(t, llmTopLevel, "slug:alice")

	assert.Contains(t, mappedSplitCompletionCandidates(opened, defaultScope, []string{"llm", defaultScope}), "slug:bob")
	assert.Contains(t, mappedSplitCompletionCandidates(opened, defaultScope, []string{"llm", defaultScope, "slug:alice"}), defaultModel)
	assert.Contains(t, mappedSplitCompletionCandidates(opened, defaultScope, []string{"llm", "models"}), "--provider=openai")

	mcpTopLevel := mappedSplitCompletionCandidates(opened, defaultScope, []string{"mcp"})
	assert.Contains(t, mcpTopLevel, "profile-dev-tools")
	assert.Contains(t, mcpTopLevel, "server=github")
	assert.Contains(t, mappedSplitCompletionCandidates(opened, defaultScope, []string{"mcp", "call", defaultScope, "profile-dev-tools"}), "github__list_repos")
}

func TestCompletionMatchesReturnsReadlineSuffixes(t *testing.T) {
	matches := completionMatches([]string{"models", "model", "providers"}, "mod")
	require.Len(t, matches, 2)
	assert.Equal(t, "el ", string(matches[0]))
	assert.Equal(t, "els ", string(matches[1]))

	matches = completionMatches([]string{"summary"}, "summary")
	require.Len(t, matches, 1)
	assert.Equal(t, " ", string(matches[0]))
}

func testMappedSplitOpened(t *testing.T) *config.Opened {
	t.Helper()

	ctx := context.Background()
	input, _, err := loadYAMLInputDir("data")
	require.NoError(t, err)

	req, err := buildMappedSplit(defaultLane, input, 4, exampleGenerationID, 1)
	require.NoError(t, err)

	out, err := mappedsplit.NewBuilder(mappedsplit.BuildOptions{Producer: "completion-test"}).Build(ctx, mappedsplit.BuildRequest(req))
	require.NoError(t, err)

	opened, _, err := mappedsplit.Open(ctx, nil, out.Map, func(_ context.Context, ref mappedsplit.BundleRef) (mappedsplit.ComponentPayload, bool, error) {
		component, ok := out.Components[ref.Component]
		if !ok {
			return mappedsplit.ComponentPayload{}, false, nil
		}
		return mappedsplit.ComponentPayload{BundleZstd: component.BundleZstd}, true, nil
	})
	require.NoError(t, err)
	return opened
}
