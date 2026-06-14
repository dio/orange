package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dio/cherry"
	"github.com/dio/orange/config"
	"gopkg.in/yaml.v3"
)

const exampleGenerationID = "gen-demo"

type yamlBuildSource struct {
	mu         sync.Mutex
	lane       string
	partitions int
	dir        string
	revision   int
	digest     string
}

func newYAMLBuildSource(lane string, partitions int, dir string) *yamlBuildSource {
	return &yamlBuildSource{lane: lane, partitions: partitions, dir: filepath.Clean(dir)}
}

func (s *yamlBuildSource) CurrentBuildRequest(requestedBy string) config.BuildRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	revision := s.revision
	if revision == 0 {
		revision = 1
	}
	return config.BuildRequest{
		Lane:           s.lane,
		RequestedBy:    requestedBy,
		SourceRevision: fmt.Sprintf("%s-r%d", exampleGenerationID, revision),
		ChangeHint:     "example mapped-split YAML rebuild",
	}
}

func (s *yamlBuildSource) Build(_ context.Context, req config.BuildRequest) (config.MappedSplitRequest, error) {
	input, digest, err := loadYAMLInputDir(s.dir)
	if err != nil {
		return config.MappedSplitRequest{}, err
	}

	s.mu.Lock()
	lane := req.Lane
	if lane == "" {
		lane = s.lane
	}
	if s.revision == 0 {
		s.revision = 1
		s.digest = digest
	} else if digest != s.digest {
		s.revision++
		s.digest = digest
	}
	partitions := s.partitions
	revision := s.revision
	s.mu.Unlock()

	return buildMappedSplit(lane, input, partitions, exampleGenerationID, revision)
}

func (s *yamlBuildSource) Digest() (string, error) {
	_, digest, err := loadYAMLInputDir(s.dir)
	return digest, err
}

func (s *yamlBuildSource) WriteNPlusOne() error {
	input, _, err := loadYAMLInputDir(s.dir)
	if err != nil {
		return err
	}
	for scopeIndex := range input.Scopes {
		scope := &input.Scopes[scopeIndex]
		for principalIndex := range scope.Principals {
			principal := &scope.Principals[principalIndex]
			if principal.Slug != "slug:alice" {
				continue
			}
			for model, route := range principal.ModelRoutes {
				route.SecretRef = "orange://alice/openai-updated"
				principal.ModelRoutes[model] = route
			}
			if principal.Route.Provider != "" || principal.Route.Model != "" {
				principal.Route.SecretRef = "orange://alice/openai-updated"
			}
		}
		profiles := scope.MCPProfiles[:0]
		for _, profile := range scope.MCPProfiles {
			if profile.Path != defaultMCPPath {
				profiles = append(profiles, profile)
			}
		}
		scope.MCPProfiles = profiles
	}

	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("create input dir: %w", err)
	}
	body, err := yaml.Marshal(fromConfigInput(input))
	if err != nil {
		return fmt.Errorf("encode n+1 YAML: %w", err)
	}
	path := filepath.Join(s.dir, "99-nplus1.yaml")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return fmt.Errorf("write n+1 YAML: %w", err)
	}
	return nil
}

func watchYAMLInput(ctx context.Context, logger *slog.Logger, source *yamlBuildSource, interval time.Duration, publish func(context.Context) error) error {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	lastDigest, err := source.Digest()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastFailedDigest string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		digest, err := source.Digest()
		if err != nil {
			logger.Warn("yaml input read failed", "error", err)
			continue
		}
		if digest == lastDigest {
			continue
		}
		if err := publish(ctx); err != nil {
			if digest != lastFailedDigest {
				logger.Warn("yaml input publish failed", "error", err)
				lastFailedDigest = digest
			}
			continue
		}
		logger.Info("yaml input published", "digest", digest[:12])
		lastDigest = digest
		lastFailedDigest = ""
	}
}

func loadYAMLInputDir(dir string) (config.Input, string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return config.Input{}, "", fmt.Errorf("read input dir %q: %w", dir, err)
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		ext := strings.ToLower(filepath.Ext(name))
		if ext == ".yaml" || ext == ".yml" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return config.Input{}, "", fmt.Errorf("input dir %q contains no .yaml files", dir)
	}

	hash := sha256.New()
	var merged config.Input
	for _, name := range names {
		path := filepath.Join(dir, name)
		body, err := os.ReadFile(path)
		if err != nil {
			return config.Input{}, "", fmt.Errorf("read %s: %w", path, err)
		}
		hash.Write([]byte(name))
		hash.Write([]byte{0})
		hash.Write(body)
		hash.Write([]byte{0})

		var raw yamlInput
		decoder := yaml.NewDecoder(bytes.NewReader(body))
		decoder.KnownFields(true)
		if err := decoder.Decode(&raw); err != nil {
			return config.Input{}, "", fmt.Errorf("decode %s: %w", path, err)
		}
		merged = mergeInput(merged, raw.toConfig())
	}
	return merged, hex.EncodeToString(hash.Sum(nil)), nil
}

func mergeInput(dst config.Input, src config.Input) config.Input {
	dst.Providers = mergeByKey(dst.Providers, src.Providers, func(v config.Provider) string { return v.ID })
	dst.Models = mergeByKey(dst.Models, src.Models, func(v config.Model) string { return v.ID })
	dst.MCPServers = mergeByKey(dst.MCPServers, src.MCPServers, func(v config.MCPServer) string { return v.ID })
	dst.Scopes = mergeByKey(dst.Scopes, src.Scopes, func(v config.Scope) string { return v.ID })
	return dst
}

func mergeByKey[T any](dst []T, src []T, key func(T) string) []T {
	index := make(map[string]int, len(dst)+len(src))
	for i, item := range dst {
		index[key(item)] = i
	}
	for _, item := range src {
		k := key(item)
		if i, ok := index[k]; ok {
			dst[i] = item
			continue
		}
		index[k] = len(dst)
		dst = append(dst, item)
	}
	return dst
}

type yamlInput struct {
	Providers  []yamlProvider  `yaml:"providers"`
	Models     []yamlModel     `yaml:"models"`
	MCPServers []yamlMCPServer `yaml:"mcp_servers"`
	Scopes     []yamlScope     `yaml:"scopes"`
}

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

type yamlRoutePlan struct {
	Kind      string                  `yaml:"kind"`
	Provider  string                  `yaml:"provider"`
	Model     string                  `yaml:"model"`
	SecretRef string                  `yaml:"secret_ref"`
	Retry     *yamlRetryPolicy        `yaml:"retry"`
	Children  []yamlRoutePlan         `yaml:"children"`
	Split     []yamlWeightedRoutePlan `yaml:"split"`
}

type yamlRetryPolicy struct {
	RetryOn         string `yaml:"retry_on"`
	PerTryTimeoutMS uint32 `yaml:"per_try_timeout_ms"`
}

type yamlWeightedRoutePlan struct {
	Weight uint32        `yaml:"weight"`
	Plan   yamlRoutePlan `yaml:"plan"`
}

type yamlRatePolicy struct {
	USDPerDayCents uint64 `yaml:"usd_per_day_cents"`
	RPM            uint32 `yaml:"rpm"`
	OnExceed       string `yaml:"on_exceed"`
}

type yamlPrincipal struct {
	Slug        string                   `yaml:"slug"`
	ModelRoutes map[string]yamlRoutePlan `yaml:"model_routes"`
	Route       yamlRoutePlan            `yaml:"route"`
	Rate        yamlRatePolicy           `yaml:"rate"`
}

type yamlMCPServer struct {
	ID        string `yaml:"id"`
	Endpoint  string `yaml:"endpoint"`
	SecretRef string `yaml:"secret_ref"`
	AuthType  string `yaml:"auth_type"`
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

func (in yamlInput) toConfig() config.Input {
	out := config.Input{
		Providers:  make([]config.Provider, 0, len(in.Providers)),
		Models:     make([]config.Model, 0, len(in.Models)),
		MCPServers: make([]config.MCPServer, 0, len(in.MCPServers)),
		Scopes:     make([]config.Scope, 0, len(in.Scopes)),
	}
	for _, provider := range in.Providers {
		out.Providers = append(out.Providers, config.Provider(provider))
	}
	for _, model := range in.Models {
		out.Models = append(out.Models, config.Model(model))
	}
	for _, server := range in.MCPServers {
		out.MCPServers = append(out.MCPServers, config.MCPServer(server))
	}
	for _, scope := range in.Scopes {
		out.Scopes = append(out.Scopes, scope.toConfig())
	}
	return out
}

func (s yamlScope) toConfig() config.Scope {
	out := config.Scope{
		ID:          s.ID,
		Principals:  make([]config.Principal, 0, len(s.Principals)),
		MCPProfiles: make([]config.MCPProfile, 0, len(s.MCPProfiles)),
	}
	for _, principal := range s.Principals {
		out.Principals = append(out.Principals, principal.toConfig())
	}
	for _, profile := range s.MCPProfiles {
		out.MCPProfiles = append(out.MCPProfiles, profile.toConfig())
	}
	return out
}

func (p yamlPrincipal) toConfig() config.Principal {
	routes := make(map[string]config.RoutePlan, len(p.ModelRoutes))
	for model, route := range p.ModelRoutes {
		routes[model] = route.toConfig()
	}
	return config.Principal{
		Slug:        p.Slug,
		ModelRoutes: routes,
		Route:       p.Route.toConfig(),
		Rate: config.RatePolicy{
			USDPerDayCents: p.Rate.USDPerDayCents,
			RPM:            p.Rate.RPM,
			OnExceed:       p.Rate.OnExceed,
		},
	}
}

func (p yamlRoutePlan) toConfig() config.RoutePlan {
	out := config.RoutePlan{
		Kind:      cherry.RouteKind(p.Kind),
		Provider:  p.Provider,
		Model:     p.Model,
		SecretRef: p.SecretRef,
		Children:  make([]config.RoutePlan, 0, len(p.Children)),
		Split:     make([]cherry.WeightedRoutePlan, 0, len(p.Split)),
	}
	if p.Retry != nil {
		out.Retry = &cherry.RetryPolicy{
			RetryOn:         p.Retry.RetryOn,
			PerTryTimeoutMS: p.Retry.PerTryTimeoutMS,
		}
	}
	for _, child := range p.Children {
		out.Children = append(out.Children, child.toConfig())
	}
	for _, split := range p.Split {
		out.Split = append(out.Split, cherry.WeightedRoutePlan{
			Weight: split.Weight,
			Plan:   split.Plan.toConfig(),
		})
	}
	return out
}

func (p yamlMCPProfile) toConfig() config.MCPProfile {
	out := config.MCPProfile{
		Path:  p.Path,
		Tools: make([]config.MCPToolBinding, 0, len(p.Tools)),
	}
	for _, tool := range p.Tools {
		out.Tools = append(out.Tools, config.MCPToolBinding(tool))
	}
	return out
}

func fromConfigInput(in config.Input) yamlInput {
	out := yamlInput{
		Providers:  make([]yamlProvider, 0, len(in.Providers)),
		Models:     make([]yamlModel, 0, len(in.Models)),
		MCPServers: make([]yamlMCPServer, 0, len(in.MCPServers)),
		Scopes:     make([]yamlScope, 0, len(in.Scopes)),
	}
	for _, provider := range in.Providers {
		out.Providers = append(out.Providers, yamlProvider(provider))
	}
	for _, model := range in.Models {
		out.Models = append(out.Models, yamlModel(model))
	}
	for _, server := range in.MCPServers {
		out.MCPServers = append(out.MCPServers, yamlMCPServer(server))
	}
	for _, scope := range in.Scopes {
		out.Scopes = append(out.Scopes, fromConfigScope(scope))
	}
	return out
}

func fromConfigScope(s config.Scope) yamlScope {
	out := yamlScope{
		ID:          s.ID,
		Principals:  make([]yamlPrincipal, 0, len(s.Principals)),
		MCPProfiles: make([]yamlMCPProfile, 0, len(s.MCPProfiles)),
	}
	for _, principal := range s.Principals {
		out.Principals = append(out.Principals, fromConfigPrincipal(principal))
	}
	for _, profile := range s.MCPProfiles {
		out.MCPProfiles = append(out.MCPProfiles, fromConfigMCPProfile(profile))
	}
	return out
}

func fromConfigPrincipal(p config.Principal) yamlPrincipal {
	routes := make(map[string]yamlRoutePlan, len(p.ModelRoutes))
	for model, route := range p.ModelRoutes {
		routes[model] = fromConfigRoutePlan(route)
	}
	return yamlPrincipal{
		Slug:        p.Slug,
		ModelRoutes: routes,
		Route:       fromConfigRoutePlan(p.Route),
		Rate: yamlRatePolicy{
			USDPerDayCents: p.Rate.USDPerDayCents,
			RPM:            p.Rate.RPM,
			OnExceed:       p.Rate.OnExceed,
		},
	}
}

func fromConfigRoutePlan(p config.RoutePlan) yamlRoutePlan {
	out := yamlRoutePlan{
		Kind:      string(p.Kind),
		Provider:  p.Provider,
		Model:     p.Model,
		SecretRef: p.SecretRef,
		Children:  make([]yamlRoutePlan, 0, len(p.Children)),
		Split:     make([]yamlWeightedRoutePlan, 0, len(p.Split)),
	}
	if p.Retry != nil {
		out.Retry = &yamlRetryPolicy{
			RetryOn:         p.Retry.RetryOn,
			PerTryTimeoutMS: p.Retry.PerTryTimeoutMS,
		}
	}
	for _, child := range p.Children {
		out.Children = append(out.Children, fromConfigRoutePlan(child))
	}
	for _, split := range p.Split {
		out.Split = append(out.Split, yamlWeightedRoutePlan{
			Weight: split.Weight,
			Plan:   fromConfigRoutePlan(split.Plan),
		})
	}
	return out
}

func fromConfigMCPProfile(p config.MCPProfile) yamlMCPProfile {
	out := yamlMCPProfile{
		Path:  p.Path,
		Tools: make([]yamlMCPToolBinding, 0, len(p.Tools)),
	}
	for _, tool := range p.Tools {
		out.Tools = append(out.Tools, yamlMCPToolBinding(tool))
	}
	return out
}
