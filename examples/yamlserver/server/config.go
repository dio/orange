package yamlserver

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/dio/cherry"
	"gopkg.in/yaml.v3"
)

// YAML schema structs — thin snake_case projection of cherry.Input fields.

type yamlProvider struct {
	ID         string `yaml:"id"`
	Kind       string `yaml:"kind"`
	Endpoint   string `yaml:"endpoint"`
	SecretRef  string `yaml:"secret_ref"`
	AuthType   string `yaml:"auth_type"`
	PathPrefix string `yaml:"path_prefix"`
}

type yamlModel struct {
	ID           string   `yaml:"id"`
	Provider     string   `yaml:"provider"`
	Name         string   `yaml:"name"`
	Mode         string   `yaml:"mode"`
	Capabilities []string `yaml:"capabilities"`
	MetadataJSON string   `yaml:"metadata_json"`
}

type yamlMCPServer struct {
	ID        string `yaml:"id"`
	Endpoint  string `yaml:"endpoint"`
	SecretRef string `yaml:"secret_ref"`
	AuthType  string `yaml:"auth_type"`
}

type yamlRetryPolicy struct {
	RetryOn         string `yaml:"retry_on"`
	PerTryTimeoutMS uint32 `yaml:"per_try_timeout_ms"`
}

type yamlWeightedRoutePlan struct {
	Weight uint32         `yaml:"weight"`
	Plan   *yamlRoutePlan `yaml:"plan"`
}

// yamlRoutePlan is recursive: chain nodes have Children, split nodes have Split.
type yamlRoutePlan struct {
	Kind      string                  `yaml:"kind"`
	Provider  string                  `yaml:"provider"`
	Model     string                  `yaml:"model"`
	SecretRef string                  `yaml:"secret_ref"`
	Retry     *yamlRetryPolicy        `yaml:"retry"`
	Children  []*yamlRoutePlan        `yaml:"children"`
	Split     []yamlWeightedRoutePlan `yaml:"split"`
}

type yamlRatePolicy struct {
	USDPerDayCents uint64 `yaml:"usd_per_day_cents"`
	RPM            uint32 `yaml:"rpm"`
	OnExceed       string `yaml:"on_exceed"`
}

type yamlPrincipal struct {
	Slug        string                    `yaml:"slug"`
	Route       *yamlRoutePlan            `yaml:"route"`
	ModelRoutes map[string]*yamlRoutePlan `yaml:"model_routes"`
	Rate        yamlRatePolicy            `yaml:"rate"`
}

type yamlMCPToolBinding struct {
	ExposedName string `yaml:"exposed_name"`
	Server      string `yaml:"server"`
	Tool        string `yaml:"tool"`
	SecretRef   string `yaml:"secret_ref"`
	AuthType    string `yaml:"auth_type"`
}

type yamlMCPProfile struct {
	Path  string               `yaml:"path"`
	Tools []yamlMCPToolBinding `yaml:"tools"`
}

type yamlScope struct {
	ID          string           `yaml:"id"`
	Principals  []yamlPrincipal  `yaml:"principals"`
	MCPProfiles []yamlMCPProfile `yaml:"mcp_profiles"`
}

type yamlConfig struct {
	Providers  []yamlProvider  `yaml:"providers"`
	Models     []yamlModel     `yaml:"models"`
	MCPServers []yamlMCPServer `yaml:"mcp_servers"`
	Scopes     []yamlScope     `yaml:"scopes"`
}

// ParseYAML parses YAML config data into cherry.Input.
// The returned string is a SHA-256 hex digest of data for use as SourceRevision.
// Unknown YAML keys and empty input are errors. Secret refs are copied verbatim.
func ParseYAML(data []byte) (cherry.Input, string, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return cherry.Input{}, "", errors.New("empty YAML input")
	}

	var cfg yamlConfig
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return cherry.Input{}, "", fmt.Errorf("parse YAML: %w", err)
	}
	// Reject multi-document YAML: a second document would silently bypass the
	// unknown-key contract for everything past the first --- separator.
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			return cherry.Input{}, "", errors.New("multi-document YAML is not supported")
		}
		return cherry.Input{}, "", fmt.Errorf("parse YAML: %w", err)
	}

	input, err := toInput(cfg)
	if err != nil {
		return cherry.Input{}, "", err
	}

	digest := sha256.Sum256(data)
	return input, hex.EncodeToString(digest[:]), nil
}

func toInput(cfg yamlConfig) (cherry.Input, error) {
	providers := make([]cherry.Provider, len(cfg.Providers))
	for i, p := range cfg.Providers {
		providers[i] = cherry.Provider{
			ID:         p.ID,
			Kind:       p.Kind,
			Endpoint:   p.Endpoint,
			SecretRef:  p.SecretRef,
			AuthType:   p.AuthType,
			PathPrefix: p.PathPrefix,
		}
	}

	models := make([]cherry.Model, len(cfg.Models))
	for i, m := range cfg.Models {
		models[i] = cherry.Model{
			ID:           m.ID,
			Provider:     m.Provider,
			Name:         m.Name,
			Mode:         m.Mode,
			Capabilities: m.Capabilities,
			MetadataJSON: m.MetadataJSON,
		}
	}

	mcpServers := make([]cherry.MCPServer, len(cfg.MCPServers))
	for i, s := range cfg.MCPServers {
		mcpServers[i] = cherry.MCPServer{
			ID:        s.ID,
			Endpoint:  s.Endpoint,
			SecretRef: s.SecretRef,
			AuthType:  s.AuthType,
		}
	}

	scopes := make([]cherry.Scope, len(cfg.Scopes))
	for i, s := range cfg.Scopes {
		scope, err := toScope(s)
		if err != nil {
			return cherry.Input{}, fmt.Errorf("scope %q: %w", s.ID, err)
		}
		scopes[i] = scope
	}

	return cherry.Input{
		Providers:  providers,
		Models:     models,
		MCPServers: mcpServers,
		Scopes:     scopes,
	}, nil
}

func toScope(s yamlScope) (cherry.Scope, error) {
	principals := make([]cherry.Principal, len(s.Principals))
	for i, p := range s.Principals {
		principal, err := toPrincipal(p)
		if err != nil {
			return cherry.Scope{}, fmt.Errorf("principal %q: %w", p.Slug, err)
		}
		principals[i] = principal
	}

	mcpProfiles := make([]cherry.MCPProfile, len(s.MCPProfiles))
	for i, mp := range s.MCPProfiles {
		tools := make([]cherry.MCPToolBinding, len(mp.Tools))
		for j, t := range mp.Tools {
			tools[j] = cherry.MCPToolBinding{
				ExposedName: t.ExposedName,
				Server:      t.Server,
				Tool:        t.Tool,
				SecretRef:   t.SecretRef,
				AuthType:    t.AuthType,
			}
		}
		mcpProfiles[i] = cherry.MCPProfile{
			Path:  mp.Path,
			Tools: tools,
		}
	}

	return cherry.Scope{
		ID:          s.ID,
		Principals:  principals,
		MCPProfiles: mcpProfiles,
	}, nil
}

func toPrincipal(p yamlPrincipal) (cherry.Principal, error) {
	var defaultRoute cherry.RoutePlan
	if p.Route != nil {
		r, err := toRoutePlan(p.Route)
		if err != nil {
			return cherry.Principal{}, fmt.Errorf("route: %w", err)
		}
		defaultRoute = r
	}

	var modelRoutes map[string]cherry.RoutePlan
	if len(p.ModelRoutes) > 0 {
		modelRoutes = make(map[string]cherry.RoutePlan, len(p.ModelRoutes))
		for model, yr := range p.ModelRoutes {
			r, err := toRoutePlan(yr)
			if err != nil {
				return cherry.Principal{}, fmt.Errorf("model_routes[%q]: %w", model, err)
			}
			modelRoutes[model] = r
		}
	}

	return cherry.Principal{
		Slug:        p.Slug,
		Route:       defaultRoute,
		ModelRoutes: modelRoutes,
		Rate: cherry.RatePolicy{
			USDPerDayCents: p.Rate.USDPerDayCents,
			RPM:            p.Rate.RPM,
			OnExceed:       p.Rate.OnExceed,
		},
	}, nil
}

func toRoutePlan(yr *yamlRoutePlan) (cherry.RoutePlan, error) {
	if yr == nil {
		return cherry.RoutePlan{}, nil
	}

	var retry *cherry.RetryPolicy
	if yr.Retry != nil {
		retry = &cherry.RetryPolicy{
			RetryOn:         yr.Retry.RetryOn,
			PerTryTimeoutMS: yr.Retry.PerTryTimeoutMS,
		}
	}

	children := make([]cherry.RoutePlan, len(yr.Children))
	for i, c := range yr.Children {
		child, err := toRoutePlan(c)
		if err != nil {
			return cherry.RoutePlan{}, fmt.Errorf("children[%d]: %w", i, err)
		}
		children[i] = child
	}

	split := make([]cherry.WeightedRoutePlan, len(yr.Split))
	for i, s := range yr.Split {
		plan, err := toRoutePlan(s.Plan)
		if err != nil {
			return cherry.RoutePlan{}, fmt.Errorf("split[%d].plan: %w", i, err)
		}
		split[i] = cherry.WeightedRoutePlan{
			Weight: s.Weight,
			Plan:   plan,
		}
	}

	return cherry.RoutePlan{
		Kind:      cherry.RouteKind(yr.Kind),
		Provider:  yr.Provider,
		Model:     yr.Model,
		SecretRef: yr.SecretRef,
		Retry:     retry,
		Children:  children,
		Split:     split,
	}, nil
}
