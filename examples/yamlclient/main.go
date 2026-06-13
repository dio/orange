// yamlclient is a development-only example. It polls an Orange server for
// configuration snapshots, opens each received Cherry bundle, and exposes a
// small HTTP API so you can inspect the currently active bundle while the
// server is running.
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
	"sync/atomic"
	"syscall"
	"time"

	cherry "github.com/dio/cherry"
	"github.com/dio/orange/client"
)

// snapshotView is the JSON shape served by GET /snapshot.
type snapshotView struct {
	// Version is the monotonic snapshot counter from the orange server.
	Version uint64 `json:"version"`
	// Checksum is the hex-encoded SHA-256 of the raw ConfigPayload bytes.
	Checksum string `json:"checksum"`
	// FetchedAt is when this client last received a changed snapshot.
	FetchedAt time.Time `json:"fetched_at"`
	// Metadata mirrors SnapshotMetadata from the ConfigPayload wrapper.
	Metadata metadataView `json:"metadata"`
	// Bundle describes the opened Cherry bundle envelope.
	Bundle bundleView `json:"bundle"`
}

type metadataView struct {
	Producer       string    `json:"producer,omitempty"`
	SourceRevision string    `json:"source_revision,omitempty"`
	Lane           string    `json:"lane,omitempty"`
	ScopeKind      string    `json:"scope_kind,omitempty"`
	ScopeID        string    `json:"scope_id,omitempty"`
	Scopes         []string  `json:"scopes,omitempty"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
	PayloadSize    uint64    `json:"payload_size,omitempty"`
}

type bundleView struct {
	// FormatVersion is the Cherry bundle format string, e.g. "1.0".
	FormatVersion string   `json:"format_version,omitempty"`
	ScopeKind     string   `json:"scope_kind,omitempty"`
	ScopeID       string   `json:"scope_id,omitempty"`
	Scopes        []string `json:"scopes,omitempty"`
	Pack          packView `json:"pack"`
}

type packView struct {
	// SizeBytes is the uncompressed pack blob size reported in the Cherry manifest.
	SizeBytes uint64 `json:"size_bytes"`
}

func main() {
	serverURL := flag.String("server", "http://127.0.0.1:8080", "Orange server base URL")
	addr := flag.String("addr", "127.0.0.1:8081", "inspection HTTP server listen address")
	interval := flag.Duration("interval", 2*time.Second, "poll interval")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := client.New(*serverURL)
	if err != nil {
		logger.Error("create client failed", "error", err)
		os.Exit(1)
	}

	var current atomic.Pointer[snapshotView]

	go runPoller(ctx, logger, c, *interval, &current)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, _ *http.Request) {
		view := current.Load()
		if view == nil {
			http.Error(w, "no snapshot received yet", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(view)
	})

	srv := &http.Server{Addr: *addr, Handler: mux}

	logger.Info("yamlclient starting",
		"server", *serverURL,
		"addr", *addr,
		"interval", *interval)

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

// runPoller fetches snapshots on every tick and updates current on change.
func runPoller(ctx context.Context, logger *slog.Logger, c *client.Client, interval time.Duration, current *atomic.Pointer[snapshotView]) {
	poll(ctx, logger, c, current)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			poll(ctx, logger, c, current)
		}
	}
}

// poll performs a single fetch and updates current if a new snapshot arrived.
func poll(ctx context.Context, logger *slog.Logger, c *client.Client, current *atomic.Pointer[snapshotView]) {
	result, err := c.Fetch(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			logger.Warn("fetch failed", "error", err)
		}
		return
	}

	if result.Unchanged {
		logger.Debug("snapshot unchanged", "version", result.Version)
		return
	}

	view, err := buildView(result)
	if err != nil {
		logger.Warn("open bundle failed", "error", err)
		return
	}

	current.Store(view)
	logger.Info("new snapshot",
		"version", view.Version,
		"source_revision", view.Metadata.SourceRevision,
		"lane", view.Metadata.Lane,
		"scopes", fmt.Sprintf("%v", view.Metadata.Scopes),
		"bundle_scopes", fmt.Sprintf("%v", view.Bundle.Scopes),
		"pack_size_bytes", view.Bundle.Pack.SizeBytes)
}

// buildView opens the Cherry bundle from result and assembles a snapshotView.
func buildView(result *client.FetchResult) (*snapshotView, error) {
	opened, err := cherry.OpenBundleZstd(result.BundleZstd)
	if err != nil {
		return nil, fmt.Errorf("open cherry bundle: %w", err)
	}

	view := &snapshotView{
		Version:   result.Version,
		Checksum:  hex.EncodeToString(result.Checksum),
		FetchedAt: time.Now().UTC(),
		Bundle: bundleView{
			FormatVersion: opened.Metadata.FormatVersion,
			ScopeKind:     opened.Metadata.ScopeKind,
			ScopeID:       opened.Metadata.ScopeID,
			Scopes:        opened.Metadata.Scopes,
			Pack: packView{
				SizeBytes: opened.Metadata.PackManifest.SizeBytes,
			},
		},
	}

	if md := result.Payload.GetMetadata(); md != nil {
		view.Metadata = metadataView{
			Producer:       md.Producer,
			SourceRevision: md.SourceRevision,
			Lane:           md.Lane,
			ScopeKind:      md.ScopeKind,
			ScopeID:        md.ScopeId,
			Scopes:         md.Scopes,
			PayloadSize:    md.PayloadSize,
		}
		if md.CreatedAt != nil {
			view.Metadata.CreatedAt = md.CreatedAt.AsTime()
		}
	}

	return view, nil
}
