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
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/dio/orange/config"
	pgquesetup "github.com/dio/orange/config/pgque"
	"github.com/dio/orange/config/postgres/migration"
	"github.com/dio/orange/internal/embeddedpg"
	"github.com/jackc/pgx/v5/pgxpool"
)

func runServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("addr", "127.0.0.1:8090", "listen address")
	lane := fs.String("lane", defaultLane, "development lane identity")
	partitions := fs.Int("partitions", 4, "mapped split partition count")
	inputDir := fs.String("input-dir", filepath.Join("examples", "mappedsplit", "data"), "directory of watched .yaml mapped-split input files")
	watchInterval := fs.Duration("watch-interval", 500*time.Millisecond, "interval for polling --input-dir changes")
	local := fs.Bool("local", false, "use embedded Postgres, PgStore, and PgQue with data under .mappedsplit")
	localDir := fs.String("local-dir", ".mappedsplit", "embedded Postgres root used with --local")
	postgresDSN := fs.String("postgres-dsn", "", "existing Postgres DSN for PgStore and PgQue; use this for additional replicas")
	workerID := fs.String("worker-id", "", "optional PgStore lease holder identity; defaults to hostname:pid")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *local && *postgresDSN != "" {
		return fmt.Errorf("--local and --postgres-dsn are mutually exclusive")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	source := newYAMLBuildSource(*lane, *partitions, *inputDir)
	var store config.Store = config.NewMemoryStore()
	var scheduler *config.PgQueScheduler
	var localRuntime *localPostgresRuntime
	var workerErr <-chan error

	if *local || *postgresDSN != "" {
		runtime, err := startDurablePostgres(ctx, logger, *local, *localDir, *postgresDSN)
		if err != nil {
			return err
		}
		localRuntime = runtime
		defer localRuntime.Close()

		storeOpts := []config.PgStoreOption{
			config.WithPgStoreBuildLeaseDuration(30 * time.Second),
			config.WithPgStoreBuildHeartbeatInterval(5 * time.Second),
		}
		if *workerID != "" {
			storeOpts = append(storeOpts, config.WithPgStoreBuildLeaseHolderID(*workerID))
		}
		pgStore, err := config.NewPgStore(runtime.pool, storeOpts...)
		if err != nil {
			return err
		}
		store = pgStore

		scheduler, err = config.NewPgQueScheduler(
			pgStore,
			source.Build,
			config.WithPgQueSchedulerConsumer("mappedsplit_example_server"),
			config.WithPgQueSchedulerRetryAfter(time.Second),
			config.WithPgQueSchedulerPollInterval(200*time.Millisecond),
		)
		if err != nil {
			return err
		}
		errs := make(chan error, 1)
		go func() {
			err := scheduler.Run(ctx)
			if errors.Is(err, context.Canceled) {
				err = nil
			}
			errs <- err
		}()
		workerErr = errs
	}

	snapshotServer := config.NewServer(config.ServerOptions{
		Producer:      "mappedsplit-example",
		Authenticator: laneAuthenticator{defaultLane: *lane},
		LaneResolver:  laneResolver{},
		Store:         store,
		OnDemandBuild: source.Build,
	})
	if scheduler != nil {
		if err := scheduleExampleBuild(ctx, scheduler, source.CurrentBuildRequest("server-start")); err != nil {
			return err
		}
		if err := waitForInitialMap(ctx, store, *lane); err != nil {
			return err
		}
	} else {
		if err := publishCurrent(context.Background(), snapshotServer, source, "server-start"); err != nil {
			return fmt.Errorf("publish initial mapped split: %w", err)
		}
	}

	watchErr := make(chan error, 1)
	go func() {
		err := watchYAMLInput(ctx, logger, source, *watchInterval, func(ctx context.Context) error {
			if scheduler != nil {
				return scheduleExampleBuild(ctx, scheduler, source.CurrentBuildRequest("yaml-watch"))
			}
			return publishCurrent(ctx, snapshotServer, source, "yaml-watch")
		})
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		watchErr <- err
	}()

	mux := http.NewServeMux()
	path := snapshotServer.Mount(mux)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /debug/nplus1", func(w http.ResponseWriter, _ *http.Request) {
		var err error
		if err = source.WriteNPlusOne(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if scheduler != nil {
			err = scheduleExampleBuild(context.Background(), scheduler, source.CurrentBuildRequest("debug-nplus1"))
		} else {
			err = publishCurrent(context.Background(), snapshotServer, source, "debug-nplus1")
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "scheduled": scheduler != nil})
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	logger.Info("mappedsplit server starting", "addr", *addr, "snapshot_path", path, "local", *local)

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
	case err := <-workerErr:
		if err != nil {
			return fmt.Errorf("pgque worker: %w", err)
		}
	case err := <-watchErr:
		if err != nil {
			return fmt.Errorf("yaml input watcher: %w", err)
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

type localPostgresRuntime struct {
	inst *embeddedpg.Instance
	pool *pgxpool.Pool
}

func startDurablePostgres(ctx context.Context, logger *slog.Logger, local bool, root string, dsn string) (*localPostgresRuntime, error) {
	var inst *embeddedpg.Instance
	if local {
		if root == "" {
			root = ".mappedsplit"
		}
		root = filepath.Clean(root)
		started, err := embeddedpg.Start(embeddedpg.Config{Root: root})
		if err != nil {
			return nil, fmt.Errorf("start embedded postgres: %w", err)
		}
		inst = started
		dsn = inst.DSN()
	} else if dsn == "" {
		return nil, fmt.Errorf("postgres DSN is required")
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		if inst != nil {
			_ = inst.Stop()
		}
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	runtime := &localPostgresRuntime{inst: inst, pool: pool}
	if err := migration.Migrate(ctx, pool); err != nil {
		runtime.Close()
		return nil, fmt.Errorf("migrate local postgres store: %w", err)
	}
	if err := pgquesetup.Setup(ctx, pool, pgquesetup.WithConsumer("mappedsplit_example_server")); err != nil {
		runtime.Close()
		return nil, fmt.Errorf("setup local pgque: %w", err)
	}
	if local {
		fmt.Fprintf(os.Stderr, "local postgres root: %s\n", root)
		fmt.Fprintf(os.Stderr, "local postgres dsn: %s\n", dsn)
		fmt.Fprintf(os.Stderr, "psql: psql %q\n", dsn)
		logger.Info("local embedded postgres ready", "root", root, "dsn", dsn)
	} else {
		logger.Info("postgres store and pgque ready", "dsn", dsn)
	}
	return runtime, nil
}

func (r *localPostgresRuntime) Close() {
	if r == nil {
		return
	}
	if r.pool != nil {
		r.pool.Close()
	}
	if r.inst != nil {
		_ = r.inst.Stop()
	}
}

func scheduleExampleBuild(ctx context.Context, scheduler *config.PgQueScheduler, req config.BuildRequest) error {
	if err := scheduler.ScheduleBuild(ctx, req); err != nil {
		return fmt.Errorf("schedule mapped split build: %w", err)
	}
	return nil
}

func waitForInitialMap(ctx context.Context, store config.Store, lane string) error {
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		typedMap, _, err := store.FetchMappedSplitMap(deadline, lane, 0, nil)
		if err == nil && typedMap != nil {
			return nil
		}
		select {
		case <-deadline.Done():
			return fmt.Errorf("wait for initial mapped split map: %w", deadline.Err())
		case <-ticker.C:
		}
	}
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

func publishCurrent(ctx context.Context, s *config.Server, source *yamlBuildSource, requestedBy string) error {
	req, err := source.Build(ctx, source.CurrentBuildRequest(requestedBy))
	if err != nil {
		return err
	}
	_, err = s.PublishMappedSplit(ctx, req)
	return err
}

func buildMappedSplit(lane string, input config.Input, partitions int, generationID string, revision int) (config.MappedSplitRequest, error) {
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

	for partition := range partitions {
		key := config.MappedSplitBundleKey{Lane: config.MappedSplitLaneLLMUserKey, Partition: partition}
		components = append(components, config.ComponentInput{Key: key, Input: llmPartitionInput(input, partitions, partition)})
		key = config.MappedSplitBundleKey{Lane: config.MappedSplitLaneMCPUserProfile, Partition: partition}
		components = append(components, config.ComponentInput{Key: key, Input: mcpProfilePartitionInput(input, partitions, partition)})
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
