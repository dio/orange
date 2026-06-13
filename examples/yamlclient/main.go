// yamlclient is a development-only example. It polls an Orange server for
// configuration snapshots, opens each received Cherry bundle, and exposes a
// small HTTP API so you can inspect the currently active bundle while the
// server is running.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	cherry "github.com/dio/cherry"
	"github.com/dio/cherry/repl"
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

type activeSnapshot struct {
	view   *snapshotView
	opened cherry.OpenedBundle
}

type replRequest struct {
	Line  string `json:"line"`
	Scope string `json:"scope,omitempty"`
}

func main() {
	serverURL := flag.String("server", "http://127.0.0.1:8080", "Orange server base URL")
	addr := flag.String("addr", "127.0.0.1:8081", "inspection HTTP server listen address")
	interval := flag.Duration("interval", 2*time.Second, "poll interval")
	replMode := flag.Bool("repl", false, "run an interactive REPL over the downloaded bundle instead of the inspection HTTP server")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := client.New(*serverURL)
	if err != nil {
		logger.Error("create client failed", "error", err)
		os.Exit(1)
	}

	var current atomic.Pointer[activeSnapshot]

	go runPoller(ctx, logger, c, *interval, &current)
	if *replMode {
		if err := runInteractiveREPL(ctx, logger, &current); err != nil {
			logger.Error("repl failed", "error", err)
			os.Exit(1)
		}
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, _ *http.Request) {
		active := current.Load()
		if active == nil {
			http.Error(w, "no snapshot received yet", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(active.view)
	})
	mux.HandleFunc("/repl", func(w http.ResponseWriter, r *http.Request) {
		handleLocalREPL(w, r, current.Load())
	})
	mux.HandleFunc("/server-repl", func(w http.ResponseWriter, r *http.Request) {
		handleRemoteREPL(w, r, *serverURL)
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

func runInteractiveREPL(ctx context.Context, logger *slog.Logger, current *atomic.Pointer[activeSnapshot]) error {
	logger.Info("waiting for first snapshot")
	active, err := waitForSnapshot(ctx, current)
	if err != nil {
		return err
	}

	session, err := newLocalSession(active, current)
	if err != nil {
		return err
	}

	fmt.Printf("loaded snapshot version=%d lane=%s scopes=%s\n", active.view.Version, active.view.Metadata.Lane, strings.Join(active.opened.Metadata.Scopes, ","))
	if scope := session.ActiveScope(); scope != "" {
		fmt.Printf("using scope %s\n", scope)
	}
	fmt.Println("commands: summary, scopes, use <scope>, llm, mcp, inspect, reload, help, quit")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("orange yamlclient> ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return err
			}
			return nil
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		result, err := session.Execute(ctx, line)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			continue
		}
		if result.Text != "" {
			fmt.Print(result.Text)
		}
		if !result.Continue {
			return nil
		}
	}
}

func waitForSnapshot(ctx context.Context, current *atomic.Pointer[activeSnapshot]) (*activeSnapshot, error) {
	if active := current.Load(); active != nil {
		return active, nil
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if active := current.Load(); active != nil {
				return active, nil
			}
		}
	}
}

func newLocalSession(active *activeSnapshot, current *atomic.Pointer[activeSnapshot]) (*repl.Session, error) {
	return repl.NewSession(repl.Config{
		Backend:      repl.NewLocalBackend(active.opened),
		DefaultScope: defaultREPLScope("", active.opened.Metadata.Scopes),
		Context:      localREPLContext(active.view),
		Reload: func(context.Context) (repl.Backend, repl.Context, error) {
			latest := current.Load()
			if latest == nil {
				return nil, repl.Context{}, errors.New("no snapshot received yet")
			}
			return repl.NewLocalBackend(latest.opened), localREPLContext(latest.view), nil
		},
	})
}

// runPoller fetches snapshots on every tick and updates current on change.
func runPoller(ctx context.Context, logger *slog.Logger, c *client.Client, interval time.Duration, current *atomic.Pointer[activeSnapshot]) {
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
func poll(ctx context.Context, logger *slog.Logger, c *client.Client, current *atomic.Pointer[activeSnapshot]) {
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

	active, err := buildActiveSnapshot(result)
	if err != nil {
		logger.Warn("open bundle failed", "error", err)
		return
	}

	current.Store(active)
	logger.Info("new snapshot",
		"version", active.view.Version,
		"source_revision", active.view.Metadata.SourceRevision,
		"lane", active.view.Metadata.Lane,
		"scopes", fmt.Sprintf("%v", active.view.Metadata.Scopes),
		"bundle_scopes", fmt.Sprintf("%v", active.view.Bundle.Scopes),
		"pack_size_bytes", active.view.Bundle.Pack.SizeBytes)
}

// buildActiveSnapshot opens the Cherry bundle from result and assembles the
// client-side active snapshot used by HTTP inspection and local REPL commands.
func buildActiveSnapshot(result *client.FetchResult) (*activeSnapshot, error) {
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

	return &activeSnapshot{view: view, opened: opened}, nil
}

func handleLocalREPL(w http.ResponseWriter, r *http.Request, active *activeSnapshot) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if active == nil {
		http.Error(w, "no snapshot received yet", http.StatusServiceUnavailable)
		return
	}
	req, err := readREPLRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	session, err := repl.NewSession(repl.Config{
		Backend:      repl.NewLocalBackend(active.opened),
		DefaultScope: defaultREPLScope(req.Scope, active.opened.Metadata.Scopes),
		Context:      localREPLContext(active.view),
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

func handleRemoteREPL(w http.ResponseWriter, r *http.Request, serverURL string) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	req, err := readREPLRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	body, err := json.Marshal(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	endpoint := strings.TrimRight(serverURL, "/") + "/debug/repl"
	upstreamReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	upstreamReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(upstreamReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
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

func localREPLContext(view *snapshotView) repl.Context {
	if view == nil {
		return repl.Context{Source: "yamlclient"}
	}
	return repl.Context{
		Lane:             view.Metadata.Lane,
		SnapshotVersion:  view.Version,
		SnapshotChecksum: view.Checksum,
		Source:           "yamlclient",
	}
}
