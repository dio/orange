package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadYAMLInputDirLoadsDefaultExample(t *testing.T) {
	input, digest, err := loadYAMLInputDir("data")
	require.NoError(t, err)
	require.NotEmpty(t, digest)
	require.Len(t, input.Providers, 1)
	require.Len(t, input.Models, 2)
	require.Len(t, input.MCPServers, 1)
	require.Len(t, input.Scopes, 1)
	assert.Equal(t, "gpt-4o-mini", input.Models[0].ID)
	assert.Equal(t, "gpt-4o-not-mini", input.Models[1].ID)

	scope := input.Scopes[0]
	require.Len(t, scope.Principals, 2)
	require.Len(t, scope.MCPProfiles, 2)
	assert.Equal(t, "orange://alice/openai", scope.Principals[0].ModelRoutes[defaultModel].SecretRef)
	assert.Equal(t, "orange://alice/github", scope.MCPProfiles[1].Tools[0].SecretRef)
}

func TestYAMLBuildSourceIncrementsRevisionOnContentChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.yaml")
	require.NoError(t, os.WriteFile(path, []byte(defaultYAML("orange://alice/openai", true)), 0o644))

	source := newYAMLBuildSource(defaultLane, 2, dir)
	first, err := source.Build(context.Background(), source.CurrentBuildRequest("test"))
	require.NoError(t, err)
	assert.Equal(t, 1, first.MapRevision)

	second, err := source.Build(context.Background(), source.CurrentBuildRequest("test"))
	require.NoError(t, err)
	assert.Equal(t, 1, second.MapRevision)

	require.NoError(t, os.WriteFile(path, []byte(defaultYAML("orange://alice/openai-updated", false)), 0o644))
	third, err := source.Build(context.Background(), source.CurrentBuildRequest("test"))
	require.NoError(t, err)
	assert.Equal(t, 2, third.MapRevision)
}

func defaultYAML(aliceSecret string, includeProfile bool) string {
	profile := ""
	if includeProfile {
		profile = `
      - path: profile-dev-tools
        tools:
          - exposed_name: github__list_repos
            server: github
            tool: list_repos
            secret_ref: orange://alice/github
            auth_type: bearer`
	}
	return `providers:
  - id: openai
    kind: openai
    endpoint: https://api.openai.com
    secret_ref: env://OPENAI_PLATFORM
    auth_type: bearer
models:
  - id: gpt-4o-mini
    provider: openai
    name: gpt-4o-mini
    mode: chat
mcp_servers:
  - id: github
    endpoint: https://mcp.github.example
    secret_ref: env://GITHUB_PLATFORM
    auth_type: bearer
scopes:
  - id: prod
    principals:
      - slug: slug:alice
        model_routes:
          gpt-4o-mini:
            kind: target
            provider: openai
            model: gpt-4o-mini
            secret_ref: ` + aliceSecret + `
        rate:
          usd_per_day_cents: 1000
          rpm: 60
          on_exceed: reject
    mcp_profiles:
      - path: s/github
        tools:
          - exposed_name: github__list_repos
            server: github
            tool: list_repos
            secret_ref: env://GITHUB_PLATFORM
            auth_type: bearer` + profile + `
`
}
