package yamlserver

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dio/cherry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseYAML_ExampleFile(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "example.yaml"))
	require.NoError(t, err)

	input, revision, err := ParseYAML(data)
	require.NoError(t, err)
	assert.Len(t, revision, 64, "SHA-256 hex is 64 chars")

	// Providers
	require.Len(t, input.Providers, 2)
	openai := input.Providers[0]
	assert.Equal(t, "openai", openai.ID)
	assert.Equal(t, "bearer", openai.AuthType)
	assert.Equal(t, "/v1", openai.PathPrefix)

	// Models
	require.Len(t, input.Models, 3)
	gptModel := input.Models[0]
	assert.Equal(t, "gpt-4o-mini", gptModel.ID)
	assert.Equal(t, "chat", gptModel.Mode)
	assert.ElementsMatch(t, []string{"function_calling", "tool_choice"}, gptModel.Capabilities)
	assert.Contains(t, gptModel.MetadataJSON, "128000")

	// MCP servers
	require.Len(t, input.MCPServers, 1)
	assert.Equal(t, "github", input.MCPServers[0].ID)
	assert.Equal(t, "sm://github-token", input.MCPServers[0].SecretRef)

	// Scopes
	require.Len(t, input.Scopes, 1)
	scope := input.Scopes[0]
	assert.Equal(t, "prod", scope.ID)

	// Principals
	require.Len(t, scope.Principals, 2)
	alice := scope.Principals[0]
	assert.Equal(t, "slug:alice", alice.Slug)
	assert.NotNil(t, alice.ModelRoutes)

	// model_routes: target route
	gptRoute, ok := alice.ModelRoutes["gpt-4o-mini"]
	require.True(t, ok)
	assert.Equal(t, cherry.RouteKindTarget, gptRoute.Kind)
	assert.Equal(t, "openai", gptRoute.Provider)
	assert.Equal(t, "orange://alice/openai", gptRoute.SecretRef)

	// model_routes: chain route with retry
	chainRoute, ok := alice.ModelRoutes["claude-haiku"]
	require.True(t, ok)
	assert.Equal(t, cherry.RouteKindChain, chainRoute.Kind)
	require.NotNil(t, chainRoute.Retry)
	assert.Equal(t, "401,403,5xx", chainRoute.Retry.RetryOn)
	assert.Equal(t, uint32(2500), chainRoute.Retry.PerTryTimeoutMS)
	require.Len(t, chainRoute.Children, 2)
	assert.Equal(t, cherry.RouteKindTarget, chainRoute.Children[0].Kind)

	// model_routes: split route
	splitRoute, ok := alice.ModelRoutes["fallback-chat"]
	require.True(t, ok)
	assert.Equal(t, cherry.RouteKindSplit, splitRoute.Kind)
	require.Len(t, splitRoute.Split, 2)
	assert.Equal(t, uint32(80), splitRoute.Split[0].Weight)
	assert.Equal(t, uint32(20), splitRoute.Split[1].Weight)

	// bob uses compatibility route
	bob := scope.Principals[1]
	assert.Equal(t, "slug:bob", bob.Slug)
	assert.Equal(t, cherry.RouteKindTarget, bob.Route.Kind)
	assert.Equal(t, "openai", bob.Route.Provider)

	// MCP profiles
	require.Len(t, scope.MCPProfiles, 1)
	profile := scope.MCPProfiles[0]
	assert.Equal(t, "github", profile.Path)
	require.Len(t, profile.Tools, 1)
	tool := profile.Tools[0]
	assert.Equal(t, "github__list_repos", tool.ExposedName)
	assert.Equal(t, "github", tool.Server)
	assert.Equal(t, "list_repos", tool.Tool)
	assert.Equal(t, "sm://github-token", tool.SecretRef)
}

func TestParseYAML_CherryRoundtrip(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "example.yaml"))
	require.NoError(t, err)

	input, _, err := ParseYAML(data)
	require.NoError(t, err)

	blob, manifest, err := cherry.BuildWithManifest(input)
	require.NoError(t, err)

	bundle := cherry.NewBundle("", "", []string{"prod"}, blob, manifest)
	compressed, err := cherry.EncodeBundleZstd(bundle)
	require.NoError(t, err)

	opened, err := cherry.OpenBundleZstd(compressed)
	require.NoError(t, err)
	assert.NotNil(t, opened.Reader)
}

func TestParseYAML_SecretRefVerbatim(t *testing.T) {
	data := []byte(`
providers:
  - id: openai
    kind: openai
    endpoint: https://api.openai.com
    secret_ref: env://OPENAI_API_KEY
mcp_servers:
  - id: github
    endpoint: https://mcp.github.example
    secret_ref: sm://github-token
models: []
scopes:
  - id: dev
    principals:
      - slug: "slug:alice"
        route:
          kind: target
          provider: openai
          model: gpt-4o-mini
          secret_ref: orange://alice/openai
        rate:
          usd_per_day_cents: 100
          rpm: 10
          on_exceed: reject
`)
	input, _, err := ParseYAML(data)
	require.NoError(t, err)

	// secret_refs must be copied verbatim, never resolved
	assert.Equal(t, "env://OPENAI_API_KEY", input.Providers[0].SecretRef)
	assert.Equal(t, "sm://github-token", input.MCPServers[0].SecretRef)
	assert.Equal(t, "orange://alice/openai", input.Scopes[0].Principals[0].Route.SecretRef)
}

func TestParseYAML_UnknownTopLevelKey(t *testing.T) {
	data := []byte(`
providers: []
models: []
scopes: []
unknown_field: value
`)
	_, _, err := ParseYAML(data)
	require.Error(t, err)
}

func TestParseYAML_UnknownNestedKey(t *testing.T) {
	data := []byte(`
providers:
  - id: openai
    kind: openai
    endpoint: https://api.openai.com
    secret_ref: env://OPENAI_API_KEY
    unknown_provider_field: surprise
models: []
scopes: []
`)
	_, _, err := ParseYAML(data)
	require.Error(t, err)
}

func TestParseYAML_MultiDocument(t *testing.T) {
	data := []byte(`
providers: []
models: []
scopes: []
---
providers: []
models: []
scopes: []
`)
	_, _, err := ParseYAML(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multi-document")
}

func TestParseYAML_Empty(t *testing.T) {
	_, _, err := ParseYAML([]byte{})
	require.Error(t, err)

	_, _, err = ParseYAML([]byte("   \n\t  "))
	require.Error(t, err)
}

func TestParseYAML_Minimal(t *testing.T) {
	data := []byte(`
providers:
  - id: openai
    kind: openai
    endpoint: https://api.openai.com
    secret_ref: env://OPENAI_API_KEY
models:
  - id: gpt-4o-mini
    provider: openai
    name: gpt-4o-mini
scopes:
  - id: prod
    principals:
      - slug: "slug:alice"
        route:
          provider: openai
          model: gpt-4o-mini
        rate:
          usd_per_day_cents: 1000
          rpm: 60
          on_exceed: reject
`)
	input, revision, err := ParseYAML(data)
	require.NoError(t, err)
	assert.NotEmpty(t, revision)
	require.Len(t, input.Providers, 1)
	require.Len(t, input.Models, 1)
	require.Len(t, input.Scopes, 1)
	assert.Equal(t, "prod", input.Scopes[0].ID)
	require.Len(t, input.Scopes[0].Principals, 1)
	assert.Equal(t, "slug:alice", input.Scopes[0].Principals[0].Slug)
}
