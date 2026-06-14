package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dio/orange/config"
)

func runServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("addr", "127.0.0.1:8090", "listen address")
	lane := fs.String("lane", defaultLane, "development lane identity")
	partitions := fs.Int("partitions", 4, "mapped split partition count")
	if err := fs.Parse(args); err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	store := config.NewMemoryStore()
	snapshotServer := config.NewServer(config.ServerOptions{
		Producer:      "mappedsplit-example",
		Authenticator: laneAuthenticator{defaultLane: *lane},
		LaneResolver:  laneResolver{},
		Store:         store,
	})
	if err := publishInitial(context.Background(), snapshotServer, *lane, *partitions); err != nil {
		return fmt.Errorf("publish initial mapped split: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	path := snapshotServer.Mount(mux)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /debug/nplus1", func(w http.ResponseWriter, _ *http.Request) {
		if err := publishNPlusOne(context.Background(), snapshotServer, *lane, *partitions); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	logger.Info("mappedsplit server starting", "addr", *addr, "snapshot_path", path)

	serveErr := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()

	select {
	case err := <-serveErr:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		logger.Info("shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			logger.Warn("shutdown error", "error", err)
			_ = srv.Close()
		}
		if err := <-serveErr; err != nil {
			logger.Warn("serve error after shutdown", "error", err)
		}
	}
	return nil
}

type laneAuthenticator struct {
	defaultLane string
}

func (a laneAuthenticator) Authenticate(_ context.Context, header http.Header) (config.ServerPrincipal, error) {
	lane := header.Get("x-orange-lane")
	if lane == "" {
		lane = a.defaultLane
	}
	return config.ServerPrincipal{ID: lane}, nil
}

type laneResolver struct{}

func (laneResolver) ResolveLane(_ context.Context, principal config.ServerPrincipal) (string, error) {
	if principal.ID == "" {
		return "", config.ErrPermissionDenied
	}
	return principal.ID, nil
}

func publishInitial(ctx context.Context, s *config.Server, lane string, partitions int) error {
	input := exampleInput("orange://alice/openai")
	generationID := "gen-demo"
	req, err := buildMappedSplit(lane, input, partitions, generationID, 1, "")
	if err != nil {
		return err
	}
	_, err = s.PublishMappedSplit(ctx, req)
	return err
}

func publishNPlusOne(ctx context.Context, s *config.Server, lane string, partitions int) error {
	input := exampleInput("orange://alice/openai-updated")
	generationID := "gen-demo"
	// Simulate one removed profile partition: profile-dev-tools no longer appears
	// in the map, so clients must not keep serving the old reader.
	req, err := buildMappedSplit(lane, input, partitions, generationID, 2, "profile-dev-tools")
	if err != nil {
		return err
	}
	_, err = s.PublishMappedSplit(ctx, req)
	return err
}

func buildMappedSplit(lane string, input config.Input, partitions int, generationID string, revision int, omitMCPProfilePath string) (config.MappedSplitRequest, error) {
	spec := config.MappedSplitSpec{
		LLMUserKeyPartitions:     partitions,
		MCPUserProfilePartitions: partitions,
	}
	if err := spec.Validate(); err != nil {
		return config.MappedSplitRequest{}, err
	}

	components := make([]config.ComponentInput, 0, 2+partitions*2)
	llmGeneric, err := spec.CatalogBundle(config.MappedSplitLaneLLMGeneric)
	if err != nil {
		return config.MappedSplitRequest{}, err
	}
	mcpServers, err := spec.CatalogBundle(config.MappedSplitLaneMCPServers)
	if err != nil {
		return config.MappedSplitRequest{}, err
	}
	components = append(components,
		config.ComponentInput{Key: llmGeneric, Input: llmGenericInput(input)},
		config.ComponentInput{Key: mcpServers, Input: mcpServersInput(input)},
	)

	omitMCPPartition := -1
	if omitMCPProfilePath != "" {
		removed, err := spec.MCPUserProfileBundle(omitMCPProfilePath)
		if err != nil {
			return config.MappedSplitRequest{}, err
		}
		omitMCPPartition = removed.Partition
	}
	for partition := range partitions {
		key := config.MappedSplitBundleKey{Lane: config.MappedSplitLaneLLMUserKey, Partition: partition}
		components = append(components, config.ComponentInput{Key: key, Input: llmPartitionInput(input, partitions, partition)})
		if partition != omitMCPPartition {
			key = config.MappedSplitBundleKey{Lane: config.MappedSplitLaneMCPUserProfile, Partition: partition}
			components = append(components, config.ComponentInput{Key: key, Input: mcpProfilePartitionInput(input, partitions, partition)})
		}
	}
	return config.MappedSplitRequest{
		Selection:               config.Selection{ScopeKind: defaultScopeKind, ScopeID: defaultScopeID},
		Lane:                    lane,
		Scopes:                  []string{defaultScope},
		SourceRevision:          fmt.Sprintf("%s-r%d", generationID, revision),
		GenerationID:            generationID,
		MapRevision:             revision,
		LLMDefaultPrincipalSlug: defaultSlug,
		Spec:                    spec,
		Components:              components,
	}, nil
}

func exampleInput(aliceSecret string) config.Input {
	return config.Input{
		Providers: []config.Provider{{
			ID:        "openai",
			Kind:      "openai",
			Endpoint:  "https://api.openai.com",
			SecretRef: "env://OPENAI_PLATFORM",
			AuthType:  "bearer",
		}},
		Models: []config.Model{{ID: "gpt-4o-mini", Provider: "openai", Name: "gpt-4o-mini", Mode: "chat"}},
		MCPServers: []config.MCPServer{{
			ID:        "github",
			Endpoint:  "https://mcp.github.example",
			SecretRef: "env://GITHUB_PLATFORM",
			AuthType:  "bearer",
		}},
		Scopes: []config.Scope{{
			ID: defaultScope,
			Principals: []config.Principal{
				principal("slug:alice", aliceSecret, 60),
				principal("slug:bob", "orange://bob/openai", 30),
			},
			MCPProfiles: []config.MCPProfile{
				{
					Path: "s/github",
					Tools: []config.MCPToolBinding{{
						ExposedName: "github__list_repos",
						Server:      "github",
						Tool:        "list_repos",
						SecretRef:   "env://GITHUB_PLATFORM",
						AuthType:    "bearer",
					}},
				},
				{
					Path: "profile-dev-tools",
					Tools: []config.MCPToolBinding{{
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

func principal(slug string, secret string, rpm uint32) config.Principal {
	return config.Principal{
		Slug: slug,
		ModelRoutes: map[string]config.RoutePlan{
			"gpt-4o-mini": {
				Kind:      config.RouteKindTarget,
				Provider:  "openai",
				Model:     "gpt-4o-mini",
				SecretRef: secret,
			},
		},
		Rate: config.RatePolicy{USDPerDayCents: 1000, RPM: rpm, OnExceed: "reject"},
	}
}

func llmGenericInput(input config.Input) config.Input {
	routes := map[string]config.RoutePlan{}
	for _, model := range input.Models {
		routes[model.ID] = config.RoutePlan{Kind: config.RouteKindTarget, Provider: model.Provider, Model: model.ID}
	}
	out := config.Input{
		Providers: append([]config.Provider(nil), input.Providers...),
		Models:    append([]config.Model(nil), input.Models...),
		Scopes:    make([]config.Scope, 0, len(input.Scopes)),
	}
	for _, scope := range input.Scopes {
		out.Scopes = append(out.Scopes, config.Scope{
			ID: scope.ID,
			Principals: []config.Principal{{
				Slug:        defaultSlug,
				ModelRoutes: cloneRouteMap(routes),
				Rate:        config.RatePolicy{USDPerDayCents: 50000, RPM: 300, OnExceed: "reject"},
			}},
		})
	}
	return out
}

func llmPartitionInput(input config.Input, partitions int, partition int) config.Input {
	spec := config.MappedSplitSpec{LLMUserKeyPartitions: partitions, MCPUserProfilePartitions: 1}
	out := config.Input{
		Providers: append([]config.Provider(nil), input.Providers...),
		Models:    append([]config.Model(nil), input.Models...),
		Scopes:    make([]config.Scope, 0, len(input.Scopes)),
	}
	for _, scope := range input.Scopes {
		outScope := config.Scope{ID: scope.ID}
		for _, principal := range scope.Principals {
			got, err := spec.LLMUserKeyPartition(principal.Slug)
			if err == nil && got == partition {
				outScope.Principals = append(outScope.Principals, clonePrincipal(principal))
			}
		}
		out.Scopes = append(out.Scopes, outScope)
	}
	return out
}

func mcpServersInput(input config.Input) config.Input {
	out := config.Input{
		MCPServers: append([]config.MCPServer(nil), input.MCPServers...),
		Scopes:     make([]config.Scope, 0, len(input.Scopes)),
	}
	for _, scope := range input.Scopes {
		outScope := config.Scope{ID: scope.ID}
		for _, profile := range scope.MCPProfiles {
			if strings.HasPrefix(profile.Path, "s/") {
				outScope.MCPProfiles = append(outScope.MCPProfiles, cloneMCPProfile(profile))
			}
		}
		out.Scopes = append(out.Scopes, outScope)
	}
	return out
}

func mcpProfilePartitionInput(input config.Input, partitions int, partition int) config.Input {
	spec := config.MappedSplitSpec{LLMUserKeyPartitions: 1, MCPUserProfilePartitions: partitions}
	out := config.Input{
		MCPServers: append([]config.MCPServer(nil), input.MCPServers...),
		Scopes:     make([]config.Scope, 0, len(input.Scopes)),
	}
	for _, scope := range input.Scopes {
		outScope := config.Scope{ID: scope.ID}
		for _, profile := range scope.MCPProfiles {
			got, err := spec.MCPUserProfilePartition(profile.Path)
			if !strings.HasPrefix(profile.Path, "s/") && err == nil && got == partition {
				outScope.MCPProfiles = append(outScope.MCPProfiles, cloneMCPProfile(profile))
			}
		}
		out.Scopes = append(out.Scopes, outScope)
	}
	return out
}

func clonePrincipal(principal config.Principal) config.Principal {
	out := principal
	out.ModelRoutes = cloneRouteMap(principal.ModelRoutes)
	return out
}

func cloneRouteMap(routes map[string]config.RoutePlan) map[string]config.RoutePlan {
	if routes == nil {
		return nil
	}
	out := make(map[string]config.RoutePlan, len(routes))
	for k, v := range routes {
		out[k] = v
	}
	return out
}

func cloneMCPProfile(profile config.MCPProfile) config.MCPProfile {
	return config.MCPProfile{
		Path:  profile.Path,
		Tools: append([]config.MCPToolBinding(nil), profile.Tools...),
	}
}
