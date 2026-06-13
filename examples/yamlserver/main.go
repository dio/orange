// yamlserver is a development-only example. The devAuthenticator and
// singleLaneResolver defined below bypass all credential checks. Do not use
// them in production.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	if _, err := mgr.Publish(ctx, snapshot.MutationRequest{Lane: "default"}); err != nil {
		logger.Error("initial publish failed", "error", err)
		os.Exit(1)
	}
	logger.Info("initial snapshot published")

	w := yamlserver.NewWatcher(*configPath, 0)
	go func() {
		err := w.Run(ctx, logger, func() {
			if _, pubErr := mgr.Publish(ctx, snapshot.MutationRequest{Lane: "default"}); pubErr != nil {
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
