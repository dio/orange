// yamlserver is a development-only example. The devAuthenticator and
// singleLaneResolver defined below bypass all credential checks. Do not use
// them in production.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	cherry "github.com/dio/cherry"
	"github.com/dio/cherry/repl"
	yamlserver "github.com/dio/orange/examples/yamlserver/server"
	"github.com/dio/orange/producer"
	"github.com/dio/orange/server"
	"github.com/dio/orange/snapshot"
)

// devAuthenticator grants every request admin and read access.
// For development use only — it bypasses all credential checks.
type devAuthenticator struct{}

func (devAuthenticator) Authenticate(_ context.Context, _ http.Header) (server.Principal, error) {
	return server.Principal{ID: "dev", Scopes: []string{"admin", "read"}}, nil
}

// singleLaneResolver always resolves to the configured lane.
type singleLaneResolver struct{ lane string }

func (r singleLaneResolver) ResolveLane(_ context.Context, _ server.Principal) (string, error) {
	return r.lane, nil
}

type replRequest struct {
	Line  string `json:"line"`
	Scope string `json:"scope,omitempty"`
}

func main() {
	configPath := flag.String("config", "", "path to YAML config file (required)")
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "error: --config is required")
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	builder := producer.NewBuilder(producer.Options{Producer: "yamlserver"})

	callback := func(ctx context.Context, _ snapshot.MutationRequest) (producer.BuildResult, error) {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			return producer.BuildResult{}, fmt.Errorf("read config: %w", err)
		}
		input, contentHash, err := yamlserver.ParseYAML(data)
		if err != nil {
			return producer.BuildResult{}, fmt.Errorf("parse config: %w", err)
		}
		// Derive Cherry bundle scopes from parsed input; lane and Cherry scope
		// are distinct concepts.
		scopes := make([]string, len(input.Scopes))
		for i, s := range input.Scopes {
			scopes[i] = s.ID
		}
		return producer.BuildResult{
			SourceRevision: contentHash,
			Scopes:         scopes,
			Input:          input,
		}, nil
	}

	mgr := snapshot.NewManager(builder, callback)

	selection := producer.Selection{ScopeKind: "lane", ScopeID: "default"}
	if _, err := mgr.Publish(ctx, snapshot.MutationRequest{Selection: selection, Lane: "default"}); err != nil {
		logger.Error("initial publish failed", "error", err)
		os.Exit(1)
	}
	logger.Info("initial snapshot published")

	w := yamlserver.NewWatcher(*configPath, 0)
	go func() {
		err := w.Run(ctx, logger, func() {
			if _, pubErr := mgr.Publish(ctx, snapshot.MutationRequest{Selection: selection, Lane: "default"}); pubErr != nil {
				logger.Warn("republish failed", "error", pubErr)
			} else {
				logger.Info("snapshot rebuilt", "config", *configPath)
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Warn("watcher stopped", "error", err)
		}
	}()

	svc := server.NewService(server.ServiceOptions{
		Manager: mgr,
		Auth:    devAuthenticator{},
		Lanes:   singleLaneResolver{lane: "default"},
	})

	mux := http.NewServeMux()
	snapshotPath, snapshotHandler := svc.SnapshotServiceHandler()
	mux.Handle(snapshotPath, snapshotHandler)
	adminPath, adminHandler := svc.ConfigAdminServiceHandler()
	mux.Handle(adminPath, adminHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/debug/repl", func(w http.ResponseWriter, r *http.Request) {
		handleServerREPL(w, r, mgr, "default")
	})

	srv := &http.Server{Addr: *addr, Handler: mux}

	logger.Info("yamlserver starting", "addr", *addr, "config", *configPath)

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
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutErr := srv.Shutdown(shutCtx); shutErr != nil {
			logger.Warn("shutdown error", "error", shutErr)
			_ = srv.Close()
		}
		if err := <-serveErr; err != nil {
			logger.Warn("serve error after shutdown", "error", err)
		}
	}
}

func handleServerREPL(w http.ResponseWriter, r *http.Request, mgr *snapshot.Manager, lane string) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	req, err := readREPLRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	snap := mgr.Current(lane)
	if snap == nil {
		http.Error(w, "no snapshot published yet", http.StatusServiceUnavailable)
		return
	}

	opened, err := cherry.OpenBundleZstd(snap.BundleZstd)
	if err != nil {
		http.Error(w, fmt.Sprintf("open cherry bundle: %v", err), http.StatusInternalServerError)
		return
	}

	session, err := repl.NewSession(repl.Config{
		Backend:      repl.NewLocalBackend(opened),
		DefaultScope: defaultREPLScope(req.Scope, opened.Metadata.Scopes),
		Context:      replContext(lane, snap.Version, snap.Checksum, "yamlserver"),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	result, err := session.Execute(r.Context(), req.Line)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func readREPLRequest(r *http.Request) (replRequest, error) {
	if r.Method == http.MethodGet {
		line := r.URL.Query().Get("cmd")
		if line == "" {
			line = r.URL.Query().Get("line")
		}
		return replRequest{
			Line:  line,
			Scope: r.URL.Query().Get("scope"),
		}, nil
	}
	defer func() {
		_ = r.Body.Close()
	}()
	var req replRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return replRequest{}, fmt.Errorf("decode request: %w", err)
	}
	return req, nil
}

func defaultREPLScope(requested string, scopes []string) string {
	if requested != "" {
		return requested
	}
	if len(scopes) == 1 {
		return scopes[0]
	}
	return ""
}

func replContext(lane string, version uint64, checksum [32]byte, source string) repl.Context {
	return repl.Context{
		Lane:             lane,
		SnapshotVersion:  version,
		SnapshotChecksum: hex.EncodeToString(checksum[:]),
		Source:           source,
	}
}
