// Package config provides facade APIs for producing and serving Orange
// mapped-split configuration snapshots.
package config

import (
	"context"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/dio/cherry"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/api/orange/config/v1/configv1connect"
	"github.com/dio/orange/mappedsplit"
	"github.com/dio/orange/producer"
)

// Cherry input type aliases let embedders build mapped-split components while
// importing only this facade package.
type (
	Input          = cherry.Input
	Provider       = cherry.Provider
	Model          = cherry.Model
	RoutePlan      = cherry.RoutePlan
	RatePolicy     = cherry.RatePolicy
	Scope          = cherry.Scope
	Principal      = cherry.Principal
	MCPServer      = cherry.MCPServer
	MCPProfile     = cherry.MCPProfile
	MCPToolBinding = cherry.MCPToolBinding

	MappedSplitSpec      = cherry.MappedSplitSpec
	MappedSplitBundleKey = cherry.MappedSplitBundleKey
	MappedSplitLane      = cherry.MappedSplitLane

	Selection      = producer.Selection
	ComponentInput = mappedsplit.ComponentInput
)

const (
	RouteKindTarget = cherry.RouteKindTarget

	MappedSplitCatalogPartition = cherry.MappedSplitCatalogPartition

	MappedSplitLaneLLMGeneric     = cherry.MappedSplitLaneLLMGeneric
	MappedSplitLaneLLMUserKey     = cherry.MappedSplitLaneLLMUserKey
	MappedSplitLaneMCPServers     = cherry.MappedSplitLaneMCPServers
	MappedSplitLaneMCPUserProfile = cherry.MappedSplitLaneMCPUserProfile
)

// ServerOptions configures a mapped-split producer/server facade.
type ServerOptions struct {
	Producer string
	Clock    func() time.Time

	Authenticator Authenticator
	LaneResolver  LaneResolver
	Store         Store

	ResourceForComponent func(component string) string
	HandlerOptions       []connect.HandlerOption
	OnDemandBuild        OnDemandBuildFunc
}

// MappedSplitRequest describes one complete mapped-split publication.
// Components must already contain normalized Cherry input layers.
type MappedSplitRequest struct {
	Selection               Selection
	Lane                    string
	Scopes                  []string
	SourceRevision          string
	GenerationID            string
	MapRevision             int
	LLMDefaultPrincipalSlug string
	Spec                    MappedSplitSpec
	Components              []ComponentInput
}

// PublishResult reports the versions made visible by a successful publish.
type PublishResult struct {
	Lane      string
	Map       *configv1.MappedSplitSnapshot
	Resources map[string]*configv1.SnapshotEnvelope
}

// Server builds, publishes, and serves mapped-split snapshots for an
// embedder-owned HTTP process.
type Server struct {
	builder        *mappedsplit.Builder
	store          Store
	auth           Authenticator
	lanes          LaneResolver
	handlerOptions []connect.HandlerOption
	onDemandBuild  OnDemandBuildFunc
}

// NewServer creates a mapped-split server facade. Without auth hooks, fetches
// fail closed.
func NewServer(opts ServerOptions) *Server {
	auth := opts.Authenticator
	if auth == nil {
		auth = FailClosedAuthenticator{}
	}
	lanes := opts.LaneResolver
	if lanes == nil {
		lanes = FailClosedLaneResolver{}
	}
	store := opts.Store
	if store == nil {
		store = NewMemoryStore()
	}
	return &Server{
		builder: mappedsplit.NewBuilder(mappedsplit.BuildOptions{
			Producer:             opts.Producer,
			Clock:                opts.Clock,
			ResourceForComponent: opts.ResourceForComponent,
		}),
		store:          store,
		auth:           auth,
		lanes:          lanes,
		handlerOptions: append([]connect.HandlerOption(nil), opts.HandlerOptions...),
		onDemandBuild:  opts.OnDemandBuild,
	}
}

// Handler returns the Connect SnapshotService mount path and handler.
func (s *Server) Handler() (string, http.Handler) {
	mappedSplit := MappedSplitMapProvider(s.store)
	if s.onDemandBuild != nil {
		if coordinator, ok := s.store.(BuildCoordinator); ok {
			mappedSplit = &coldStartMappedSplitProvider{
				store:       s.store,
				coordinator: coordinator,
				builder:     s.builder,
				build:       s.onDemandBuild,
			}
		}
	}
	svc := NewSnapshotServiceWithProviders(s.store, s.auth, s.lanes, mappedSplit)
	return configv1connect.NewSnapshotServiceHandler(svc, s.handlerOptions...)
}

// Mount attaches the SnapshotService handler to mux.
func (s *Server) Mount(mux *http.ServeMux) string {
	path, handler := s.Handler()
	mux.Handle(path, handler)
	return path
}

// PublishMappedSplit builds every component, publishes component resources,
// then publishes the typed map. Failed publishes leave the previous state
// visible.
func (s *Server) PublishMappedSplit(ctx context.Context, req MappedSplitRequest) (PublishResult, error) {
	out, err := s.builder.Build(ctx, mappedsplit.BuildRequest{
		Selection:               req.Selection,
		Lane:                    req.Lane,
		Scopes:                  req.Scopes,
		SourceRevision:          req.SourceRevision,
		GenerationID:            req.GenerationID,
		MapRevision:             req.MapRevision,
		LLMDefaultPrincipalSlug: req.LLMDefaultPrincipalSlug,
		Spec:                    req.Spec,
		Components:              req.Components,
	})
	if err != nil {
		return PublishResult{}, err
	}
	return s.store.PublishMappedSplit(ctx, out)
}

// Store returns the configured mapped-split store.
func (s *Server) Store() Store {
	return s.store
}
