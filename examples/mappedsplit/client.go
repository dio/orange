package main

import (
	"context"
	"crypto/sha256"
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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chzyer/readline"
	"github.com/dio/cherry"
	cherryrepl "github.com/dio/cherry/repl"
	"github.com/dio/orange/config"
	"github.com/dio/orange/mappedsplit"
)

func runClient(args []string) error {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		switch args[0] {
		case "fetch-map":
			return runClientFetchMap(args[1:])
		case "fetch-bundle", "fetch-resource":
			return runClientFetchBundle(args[1:])
		case "apply", "watch":
			return runClientApply(args[1:])
		case "repl":
			return runClientREPL(args[1:])
		default:
			return fmt.Errorf("unknown client command %q", args[0])
		}
	}
	return runClientREPL(args)
}

func runClientFetchMap(args []string) error {
	fs := flag.NewFlagSet("client fetch-map", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	serverURL := fs.String("server", defaultServerURL, "Orange server base URL")
	lane := fs.String("lane", defaultLane, "authorized development lane identity")
	out := fs.String("out", "", "write split map JSON to this path; stdout when empty")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := newLaneClient(*serverURL, *lane)
	if err != nil {
		return err
	}
	result, err := c.FetchMap(ctx)
	if err != nil {
		return fmt.Errorf("fetch typed split map: %w", err)
	}
	mapJSON, err := json.MarshalIndent(result.Map, "", "  ")
	if err != nil {
		return fmt.Errorf("encode split map JSON: %w", err)
	}

	if *out != "" {
		if err := os.WriteFile(*out, mapJSON, 0o644); err != nil {
			return err
		}
	} else {
		fmt.Println(string(mapJSON))
	}

	fmt.Fprintf(os.Stderr, "fetched map lane=%s resource=\"\" version=%d checksum=%s generation=%s revision=%d\n",
		*lane,
		result.Version,
		hex.EncodeToString(result.Checksum[:8]),
		result.Map.GenerationID,
		result.Map.MapRevision,
	)
	printMapRefs(os.Stderr, result.Map)
	return nil
}

func runClientFetchBundle(args []string) error {
	fs := flag.NewFlagSet("client fetch-bundle", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	serverURL := fs.String("server", defaultServerURL, "Orange server base URL")
	lane := fs.String("lane", defaultLane, "authorized development lane identity")
	resource := fs.String("resource", "", "component resource from the fetched split map, e.g. llm-user-key-003")
	out := fs.String("out", "", "write Cherry zstd bundle bytes to this path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *resource == "" {
		return fmt.Errorf("--resource is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := newLaneClient(*serverURL, *lane)
	if err != nil {
		return err
	}
	result, err := c.FetchBundle(ctx, *resource)
	if err != nil {
		return fmt.Errorf("fetch bundle resource %q: %w", *resource, err)
	}
	opened, err := config.OpenBundleZstd(result.BundleZstd)
	if err != nil {
		return fmt.Errorf("open fetched Cherry bundle: %w", err)
	}
	if *out != "" {
		if err := os.WriteFile(*out, result.BundleZstd, 0o644); err != nil {
			return err
		}
	}

	payloadChecksum := sha256.Sum256(result.BundleZstd)
	fmt.Printf("fetched bundle lane=%s resource=%s version=%d checksum=%s payload_sha256=%s generation=%s pack_checksum=%d pack_size=%d\n",
		*lane,
		*resource,
		result.Version,
		hex.EncodeToString(result.Checksum[:8]),
		hex.EncodeToString(payloadChecksum[:8]),
		opened.Metadata.GenerationID,
		opened.Metadata.PackManifest.Checksum,
		opened.Metadata.PackManifest.SizeBytes,
	)
	if *out != "" {
		fmt.Printf("wrote %s\n", *out)
	}
	return nil
}

func runClientApply(args []string) error {
	fs := flag.NewFlagSet("client", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	serverURL := fs.String("server", defaultServerURL, "Orange server base URL")
	lane := fs.String("lane", defaultLane, "authorized development lane identity")
	interval := fs.Duration("interval", 2*time.Second, "poll interval")
	once := fs.Bool("once", false, "fetch once and exit")
	triggerUpdate := fs.Bool("trigger-update", false, "POST /debug/nplus1 after the first successful load")
	scope := fs.String("scope", defaultScope, "Cherry enforcement scope to query")
	principal := fs.String("principal", defaultPrincipal, "principal slug to query")
	model := fs.String("model", defaultModel, "LLM model to query")
	mcpPath := fs.String("mcp-path", defaultMCPPath, "MCP path suffix to query")
	if err := fs.Parse(args); err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := newLaneClient(*serverURL, *lane)
	if err != nil {
		return err
	}
	updated := false

	for {
		result, err := c.Sync(ctx)
		if err != nil {
			if *once {
				return err
			}
			logger.Error("fetch mapped split failed", "error", err)
		} else {
			printStatus(result)
			printQueries(result.Opened, *scope, *principal, *model, *mcpPath)
			if *triggerUpdate && !updated {
				updated = true
				if err := triggerNPlusOne(ctx, *serverURL); err != nil {
					logger.Error("trigger n+1 failed", "error", err)
				} else {
					logger.Info("triggered n+1 update")
				}
			}
			if *once {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(*interval):
		}
	}
}

func runClientREPL(args []string) error {
	fs := flag.NewFlagSet("client repl", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	serverURL := fs.String("server", defaultServerURL, "Orange server base URL")
	lane := fs.String("lane", defaultLane, "authorized development lane identity")
	interval := fs.Duration("interval", 2*time.Second, "background sync interval")
	scope := fs.String("scope", defaultScope, "initial Cherry enforcement scope")
	triggerUpdate := fs.Bool("trigger-update", false, "POST /debug/nplus1 after the first successful load")
	if err := fs.Parse(args); err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := newLaneClient(*serverURL, *lane)
	if err != nil {
		return err
	}
	initial, err := c.Sync(ctx)
	if err != nil {
		return fmt.Errorf("initial sync: %w", err)
	}
	state, err := newMappedSplitREPLState(*lane, *scope, initial, func(ctx context.Context) (*config.SyncResult, error) {
		return c.Sync(ctx)
	})
	if err != nil {
		return err
	}

	fmt.Printf("loaded mapped split lane=%s map_version=%d checksum=%s generation=%s revision=%d fetched=%d reused=%d omitted=%d\n",
		*lane,
		initial.Map.Version,
		shortChecksum(initial.Map.Checksum),
		initial.Opened.Map.GenerationID,
		initial.Opened.Map.MapRevision,
		initial.Stats.Fetched,
		initial.Stats.Reused,
		initial.Stats.Omitted,
	)
	fmt.Println("commands: summary, scopes, use <scope>, llm ..., mcp ..., inspect ..., reload, sync, help, quit")

	if *triggerUpdate {
		if err := triggerNPlusOne(ctx, *serverURL); err != nil {
			logger.Error("trigger n+1 failed", "error", err)
		} else {
			logger.Info("triggered n+1 update")
		}
	}

	go pollMappedSplitChanges(ctx, logger, c, state, *interval)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:       "orange> ",
		AutoComplete: state,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = rl.Close()
	}()
	for {
		line, err := rl.Readline()
		if errors.Is(err, io.EOF) || errors.Is(err, readline.ErrInterrupt) {
			return nil
		}
		if err != nil {
			return err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		result, err := state.Execute(ctx, line)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			continue
		}
		if result.Text != "" {
			fmt.Print(result.Text)
			if !strings.HasSuffix(result.Text, "\n") {
				fmt.Println()
			}
		}
		if !result.Continue {
			return nil
		}
	}
}

func pollMappedSplitChanges(ctx context.Context, logger *slog.Logger, c *config.Client, state *mappedSplitREPLState, interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		result, err := c.Sync(ctx)
		if err != nil {
			logger.Error("background sync failed", "error", err)
			continue
		}
		if result.Unchanged {
			continue
		}
		if err := state.Replace(result); err != nil {
			logger.Error("refresh repl view failed", "error", err)
			continue
		}
		fmt.Fprintf(os.Stderr, "\nnotification: mapped split changed lane=%s map_version=%d checksum=%s generation=%s revision=%d fetched=%d reused=%d omitted=%d\n",
			state.Lane(),
			result.Map.Version,
			shortChecksum(result.Map.Checksum),
			result.Opened.Map.GenerationID,
			result.Opened.Map.MapRevision,
			result.Stats.Fetched,
			result.Stats.Reused,
			result.Stats.Omitted,
		)
	}
}

func newLaneClient(serverURL string, lane string) (*config.Client, error) {
	return config.NewClient(config.ClientOptions{
		BaseURL: serverURL,
		HeaderFunc: func(_ context.Context, h http.Header) error {
			h.Set("x-orange-lane", lane)
			return nil
		},
	})
}

func printMapRefs(out *os.File, splitMap config.SplitMap) {
	for lane, ref := range splitMap.Bundles {
		_, _ = fmt.Fprintf(out, "bundle lane=%s id=%s resource=%s checksum=%d size=%d\n",
			lane, ref.ID, ref.Resource, ref.Checksum, ref.Size)
	}
	for lane, refs := range splitMap.PartitionBundles {
		for _, ref := range refs {
			_, _ = fmt.Fprintf(out, "partition lane=%s partition=%d id=%s resource=%s checksum=%d size=%d\n",
				lane, ref.Partition, ref.ID, ref.Resource, ref.Checksum, ref.Size)
		}
	}
}

func printStatus(result *config.SyncResult) {
	fmt.Printf(
		"map version=%d checksum=%s unchanged=%v generation=%s revision=%d fetched=%d reused=%d omitted=%d\n",
		result.Map.Version,
		hex.EncodeToString(result.Map.Checksum[:8]),
		result.Unchanged,
		result.Opened.Map.GenerationID,
		result.Opened.Map.MapRevision,
		result.Stats.Fetched,
		result.Stats.Reused,
		result.Stats.Omitted,
	)
}

func printQueries(opened *config.Opened, scope string, principal string, model string, mcpPath string) {
	llm, ok := opened.ResolveLLM(scope, principal, model)
	if ok {
		fmt.Printf(
			"llm scope=%s principal=%s model=%s provider=%s secret_ref=%s rpm=%d\n",
			scope,
			principal,
			model,
			llm.Provider,
			llm.SecretRef,
			llm.Rate.RPM,
		)
	} else {
		fmt.Printf("llm scope=%s principal=%s model=%s not found\n", scope, principal, model)
	}

	mcp, ok := opened.ResolveMCP(scope, mcpPath)
	if !ok {
		fmt.Printf("mcp scope=%s path=%s not found\n", scope, mcpPath)
		return
	}
	tools := make([]string, 0, len(mcp.Tools))
	for _, tool := range mcp.Tools {
		tools = append(tools, fmt.Sprintf("%s->%s/%s secret_ref=%s", tool.ExposedName, tool.Server, tool.Tool, tool.SecretRef))
	}
	fmt.Printf("mcp scope=%s path=%s tools=%s\n", scope, mcpPath, strings.Join(tools, ", "))
}

func triggerNPlusOne(ctx context.Context, serverURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(serverURL, "/")+"/debug/nplus1", http.NoBody)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	return nil
}

type mappedSplitREPLState struct {
	mu     sync.Mutex
	lane   string
	sync   func(context.Context) (*config.SyncResult, error)
	latest *config.SyncResult
	repl   *cherryrepl.Session
}

func newMappedSplitREPLState(
	lane string,
	defaultScope string,
	result *config.SyncResult,
	syncFn func(context.Context) (*config.SyncResult, error),
) (*mappedSplitREPLState, error) {
	if result == nil || result.Opened == nil || result.Map == nil {
		return nil, fmt.Errorf("mapped split repl requires an opened view")
	}
	state := &mappedSplitREPLState{lane: lane, sync: syncFn}
	if err := state.set(result, defaultScope); err != nil {
		return nil, err
	}
	return state, nil
}

func (s *mappedSplitREPLState) Lane() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lane
}

func (s *mappedSplitREPLState) Execute(ctx context.Context, line string) (cherryrepl.Result, error) {
	switch strings.TrimSpace(line) {
	case "sync", "reload":
		return s.syncCommand(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.repl == nil {
		return cherryrepl.Result{}, fmt.Errorf("repl session is not initialized")
	}
	return s.repl.Execute(ctx, line)
}

func (s *mappedSplitREPLState) Do(line []rune, pos int) ([][]rune, int) {
	if pos < 0 {
		pos = 0
	}
	if pos > len(line) {
		pos = len(line)
	}

	words, partial := completionWords(string(line[:pos]))
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil || s.latest.Opened == nil {
		return nil, 0
	}
	activeScope := ""
	if s.repl != nil {
		activeScope = s.repl.ActiveScope()
	}
	candidates := mappedSplitCompletionCandidates(s.latest.Opened, activeScope, words)
	return completionMatches(candidates, partial), len([]rune(partial))
}

func (s *mappedSplitREPLState) Replace(result *config.SyncResult) error {
	s.mu.Lock()
	scope := ""
	if s.repl != nil {
		scope = s.repl.ActiveScope()
	}
	s.mu.Unlock()
	return s.set(result, scope)
}

func (s *mappedSplitREPLState) syncCommand(ctx context.Context) (cherryrepl.Result, error) {
	if s.sync == nil {
		return cherryrepl.Result{Continue: true, Text: "sync is not configured\n"}, nil
	}
	result, err := s.sync(ctx)
	if err != nil {
		return cherryrepl.Result{}, err
	}
	if err := s.Replace(result); err != nil {
		return cherryrepl.Result{}, err
	}
	text := fmt.Sprintf("synced map_version=%d checksum=%s unchanged=%v fetched=%d reused=%d omitted=%d\n",
		result.Map.Version,
		shortChecksum(result.Map.Checksum),
		result.Unchanged,
		result.Stats.Fetched,
		result.Stats.Reused,
		result.Stats.Omitted,
	)
	return cherryrepl.Result{Continue: true, Text: text, Lane: s.lane}, nil
}

func (s *mappedSplitREPLState) set(result *config.SyncResult, defaultScope string) error {
	if result == nil || result.Opened == nil || result.Map == nil {
		return fmt.Errorf("mapped split repl requires an opened view")
	}
	if defaultScope == "" {
		defaultScope = defaultScopeFor(result.Opened.Map.Scopes)
	}
	backend := mappedSplitREPLBackend{opened: result.Opened}
	session, err := cherryrepl.NewSession(cherryrepl.Config{
		Backend:      backend,
		DefaultScope: defaultScope,
		Context: cherryrepl.Context{
			Lane:             s.lane,
			SnapshotVersion:  result.Map.Version,
			SnapshotChecksum: shortChecksum(result.Map.Checksum),
			Source:           "orange mappedsplit client",
		},
		Reload: func(ctx context.Context) (cherryrepl.Backend, cherryrepl.Context, error) {
			if s.sync == nil {
				return nil, cherryrepl.Context{}, fmt.Errorf("sync is not configured")
			}
			refreshed, err := s.sync(ctx)
			if err != nil {
				return nil, cherryrepl.Context{}, err
			}
			if err := s.Replace(refreshed); err != nil {
				return nil, cherryrepl.Context{}, err
			}
			return mappedSplitREPLBackend{opened: refreshed.Opened}, cherryrepl.Context{
				Lane:             s.lane,
				SnapshotVersion:  refreshed.Map.Version,
				SnapshotChecksum: shortChecksum(refreshed.Map.Checksum),
				Source:           "orange mappedsplit client",
			}, nil
		},
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.latest = result
	s.repl = session
	s.mu.Unlock()
	return nil
}

func completionWords(line string) ([]string, string) {
	if line == "" {
		return nil, ""
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil, ""
	}
	if isCompletionBoundary(line[len(line)-1]) {
		return fields, ""
	}
	return fields[:len(fields)-1], fields[len(fields)-1]
}

func isCompletionBoundary(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

func mappedSplitCompletionCandidates(opened *config.Opened, activeScope string, words []string) []string {
	if len(words) == 0 {
		return []string{"summary", "scopes", "use", "llm", "mcp", "inspect", "reload", "sync", "help", "quit", "exit"}
	}
	switch words[0] {
	case "use":
		if len(words) == 1 {
			return completionScopes(opened)
		}
	case "inspect":
		if len(words) == 1 {
			return []string{"metadata", "principals", "mcp", "all"}
		}
	case "llm":
		return llmCompletionCandidates(opened, activeScope, words[1:])
	case "mcp":
		return mcpCompletionCandidates(opened, activeScope, words[1:])
	}
	return nil
}

func llmCompletionCandidates(opened *config.Opened, activeScope string, args []string) []string {
	if len(args) == 0 {
		return combineCompletionCandidates(
			[]string{"principals", "providers", "models", "model", "capability"},
			completionScopes(opened),
			completionPrincipals(opened, activeScope),
		)
	}
	switch args[0] {
	case "principals":
		if len(args) == 1 {
			return completionScopes(opened)
		}
	case "providers":
		return nil
	case "models":
		if len(args) == 1 {
			return completionProviderFlags(opened)
		}
		if args[len(args)-1] == "--provider" {
			return completionProviders(opened)
		}
	case "model":
		if len(args) == 1 {
			return combineCompletionCandidates(completionModels(opened), completionProviderFlags(opened))
		}
		if args[len(args)-1] == "--provider" {
			return completionProviders(opened)
		}
		if len(args) == 2 && !strings.HasPrefix(args[1], "--provider") {
			return completionProviderFlags(opened)
		}
	case "capability":
		if len(args) == 1 {
			return completionModels(opened)
		}
	}

	scope := activeScope
	offset := 0
	if len(args) > 0 && containsString(completionScopes(opened), args[0]) {
		scope = args[0]
		offset = 1
	}
	count := len(args) - offset
	if offset == 0 {
		switch count {
		case 0:
			return combineCompletionCandidates(completionScopes(opened), completionPrincipals(opened, activeScope))
		case 1:
			return completionModels(opened)
		}
		return nil
	}
	switch count {
	case 0:
		return completionPrincipals(opened, scope)
	case 1:
		return completionModels(opened)
	}
	return nil
}

func mcpCompletionCandidates(opened *config.Opened, activeScope string, args []string) []string {
	if len(args) == 0 {
		return combineCompletionCandidates(
			[]string{"paths", "initialize", "list", "call"},
			completionScopes(opened),
			completionMCPPathTargets(opened, activeScope),
		)
	}
	switch args[0] {
	case "paths":
		if len(args) == 1 {
			return combineCompletionCandidates(completionScopes(opened), []string{"--tools"})
		}
		if len(args) == 2 && args[1] != "--tools" {
			return []string{"--tools"}
		}
		return nil
	case "initialize", "list":
		return mcpPathCommandCompletionCandidates(opened, activeScope, args[1:], false)
	case "call":
		return mcpPathCommandCompletionCandidates(opened, activeScope, args[1:], true)
	default:
		return mcpPathCommandCompletionCandidates(opened, activeScope, args, true)
	}
}

func mcpPathCommandCompletionCandidates(opened *config.Opened, activeScope string, args []string, includeTool bool) []string {
	scope := activeScope
	offset := 0
	if len(args) > 0 && containsString(completionScopes(opened), args[0]) {
		scope = args[0]
		offset = 1
	}
	count := len(args) - offset
	switch count {
	case 0:
		if offset == 1 {
			return completionMCPPathTargets(opened, scope)
		}
		return combineCompletionCandidates(completionScopes(opened), completionMCPPathTargets(opened, activeScope))
	case 1:
		if includeTool {
			return completionMCPTools(opened, scope, normalizeCompletionMCPTarget(args[offset]))
		}
		return nil
	}
	return nil
}

func completionScopes(opened *config.Opened) []string {
	if opened == nil {
		return nil
	}
	return sortedStrings(opened.Map.Scopes)
}

func completionPrincipals(opened *config.Opened, scope string) []string {
	if opened == nil || scope == "" {
		return nil
	}
	principals, err := (mappedSplitREPLBackend{opened: opened}).LLMPrincipals(context.Background(), scope)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(principals))
	for _, principal := range principals {
		out = append(out, principal.PrincipalSlug)
	}
	return sortedStrings(out)
}

func completionProviders(opened *config.Opened) []string {
	if opened == nil {
		return nil
	}
	providers, err := (mappedSplitREPLBackend{opened: opened}).Providers(context.Background())
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(providers))
	for _, provider := range providers {
		out = append(out, provider.ID)
	}
	return sortedStrings(out)
}

func completionProviderFlags(opened *config.Opened) []string {
	providers := completionProviders(opened)
	out := make([]string, 0, len(providers)*2+1)
	out = append(out, "--provider")
	for _, provider := range providers {
		out = append(out, "--provider="+provider)
	}
	return sortedStrings(out)
}

func completionModels(opened *config.Opened) []string {
	if opened == nil {
		return nil
	}
	models, err := (mappedSplitREPLBackend{opened: opened}).Models(context.Background())
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(models))
	for _, model := range models {
		out = append(out, model.ID)
	}
	return sortedStrings(out)
}

func completionMCPPathTargets(opened *config.Opened, scope string) []string {
	if opened == nil || scope == "" {
		return nil
	}
	paths, err := (mappedSplitREPLBackend{opened: opened}).MCPPaths(context.Background(), scope)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(paths)*2)
	for _, path := range paths {
		out = append(out, path.Path)
		if strings.HasPrefix(path.Path, "s/") {
			out = append(out, "server="+strings.TrimPrefix(path.Path, "s/"))
		} else {
			out = append(out, "profile="+path.Path)
		}
	}
	return sortedStrings(out)
}

func completionMCPTools(opened *config.Opened, scope string, path string) []string {
	if opened == nil || scope == "" || path == "" {
		return nil
	}
	paths, err := (mappedSplitREPLBackend{opened: opened}).MCPPaths(context.Background(), scope)
	if err != nil {
		return nil
	}
	out := []string{}
	for _, candidate := range paths {
		if candidate.Path != path {
			continue
		}
		for _, tool := range candidate.Tools {
			out = append(out, tool.ExposedName)
		}
	}
	return sortedStrings(out)
}

func normalizeCompletionMCPTarget(value string) string {
	switch {
	case strings.HasPrefix(value, "server="):
		return "s/" + strings.TrimPrefix(value, "server=")
	case strings.HasPrefix(value, "profile="):
		return strings.TrimPrefix(value, "profile=")
	default:
		return value
	}
}

func combineCompletionCandidates(groups ...[]string) []string {
	total := 0
	for _, group := range groups {
		total += len(group)
	}
	out := make([]string, 0, total)
	for _, group := range groups {
		out = append(out, group...)
	}
	return sortedStrings(out)
}

func completionMatches(candidates []string, partial string) [][]rune {
	candidates = sortedStrings(candidates)
	out := make([][]rune, 0, len(candidates))
	for _, candidate := range candidates {
		if !strings.HasPrefix(candidate, partial) {
			continue
		}
		suffix := candidate[len(partial):]
		if suffix == "" {
			suffix = " "
		} else if !strings.ContainsAny(suffix, " \t") {
			suffix += " "
		}
		out = append(out, []rune(suffix))
	}
	return out
}

func sortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

type mappedSplitREPLBackend struct {
	opened *config.Opened
}

func (b mappedSplitREPLBackend) Metadata(context.Context) (cherry.BundleMetadata, error) {
	if b.opened == nil {
		return cherry.BundleMetadata{}, fmt.Errorf("no opened mapped split view")
	}
	return b.opened.LLMGeneric.Opened.Metadata, nil
}

func (b mappedSplitREPLBackend) Scopes(context.Context) ([]string, error) {
	if b.opened == nil {
		return nil, fmt.Errorf("no opened mapped split view")
	}
	return append([]string(nil), b.opened.Map.Scopes...), nil
}

func (b mappedSplitREPLBackend) LLMPrincipals(ctx context.Context, scope string) ([]cherry.PrincipalInfo, error) {
	routes, err := b.PrincipalRoutes(ctx, scope)
	if err != nil {
		return nil, err
	}
	bySlug := map[string]map[string]struct{}{}
	for _, route := range routes {
		models := bySlug[route.PrincipalSlug]
		if models == nil {
			models = map[string]struct{}{}
			bySlug[route.PrincipalSlug] = models
		}
		models[route.RequestedModel] = struct{}{}
	}
	out := make([]cherry.PrincipalInfo, 0, len(bySlug))
	for slug, modelSet := range bySlug {
		models := make([]string, 0, len(modelSet))
		for model := range modelSet {
			models = append(models, model)
		}
		sort.Strings(models)
		out = append(out, cherry.PrincipalInfo{ScopeID: scope, PrincipalSlug: slug, RequestedModels: models})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].PrincipalSlug < out[j].PrincipalSlug
	})
	return out, nil
}

func (b mappedSplitREPLBackend) ResolveLLMPlan(_ context.Context, scope string, principalSlug string, modelID string) (cherry.LLMPlan, bool, error) {
	if b.opened == nil {
		return cherry.LLMPlan{}, false, fmt.Errorf("no opened mapped split view")
	}
	if key, err := b.opened.Spec.LLMUserKeyBundle(principalSlug); err == nil && key.Partition >= 0 && key.Partition < len(b.opened.LLMUserKey) {
		component := b.opened.LLMUserKey[key.Partition]
		if component.Ref.Resource != "" {
			if plan, ok := component.Opened.Reader.ResolveLLMPlan(scope, principalSlug, modelID); ok {
				return plan, true, nil
			}
		}
	}
	genericSlug := principalSlug
	if b.opened.Map.LLMDefaultPrincipalSlug != "" {
		genericSlug = b.opened.Map.LLMDefaultPrincipalSlug
	}
	plan, ok := b.opened.LLMGeneric.Opened.Reader.ResolveLLMPlan(scope, genericSlug, modelID)
	if ok {
		plan.PrincipalSlug = principalSlug
	}
	return plan, ok, nil
}

func (b mappedSplitREPLBackend) Providers(context.Context) ([]cherry.ProviderInfo, error) {
	return b.opened.LLMGeneric.Opened.Reader.Providers(), nil
}

func (b mappedSplitREPLBackend) Models(context.Context) ([]cherry.ModelInfo, error) {
	return b.opened.LLMGeneric.Opened.Reader.Models(), nil
}

func (b mappedSplitREPLBackend) ResolveModel(_ context.Context, modelID string) (cherry.ModelInfo, bool, error) {
	model, ok := b.opened.LLMGeneric.Opened.Reader.ResolveModel(modelID)
	return model, ok, nil
}

func (b mappedSplitREPLBackend) ModelCapability(_ context.Context, modelID string, capability string) (bool, error) {
	return b.opened.LLMGeneric.Opened.Reader.ModelCapability(modelID, capability), nil
}

func (b mappedSplitREPLBackend) V1ModelsJSON(_ context.Context, providerID string) ([]byte, error) {
	if providerID == "" {
		return b.opened.LLMGeneric.Opened.Reader.V1ModelsJSON()
	}
	return b.opened.LLMGeneric.Opened.Reader.V1ModelsJSONForProvider(providerID)
}

func (b mappedSplitREPLBackend) MCPPaths(_ context.Context, scope string) ([]cherry.MCPPath, error) {
	paths := map[string]cherry.MCPPath{}
	for _, component := range append([]mappedsplit.OpenedComponent{b.opened.MCPServers}, b.opened.MCPUserProfile...) {
		if component.Ref.Resource == "" {
			continue
		}
		got, ok := component.Opened.Reader.MCPPaths(scope)
		if !ok {
			continue
		}
		for _, path := range got {
			paths[path.Path] = path
		}
	}
	out := make([]cherry.MCPPath, 0, len(paths))
	for _, path := range paths {
		out = append(out, path)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Path < out[j].Path
	})
	return out, nil
}

func (b mappedSplitREPLBackend) ResolveMCP(_ context.Context, scope string, path string) (cherry.MCPResult, bool, error) {
	component := b.mcpComponent(path)
	if component.Ref.Resource == "" {
		return cherry.MCPResult{}, false, nil
	}
	result, ok := component.Opened.Reader.ResolveMCP(scope, path)
	return result, ok, nil
}

func (b mappedSplitREPLBackend) ResolveMCPInitialize(_ context.Context, scope string, path string) (cherry.MCPInitializeResult, bool, error) {
	component := b.mcpComponent(path)
	if component.Ref.Resource == "" {
		return cherry.MCPInitializeResult{}, false, nil
	}
	result, ok := component.Opened.Reader.ResolveMCPInitialize(scope, path)
	return result, ok, nil
}

func (b mappedSplitREPLBackend) ResolveMCPTool(_ context.Context, scope string, path string, exposedTool string) (cherry.MCPTool, bool, error) {
	component := b.mcpComponent(path)
	if component.Ref.Resource == "" {
		return cherry.MCPTool{}, false, nil
	}
	ids, ok := component.Opened.Reader.ResolveMCPToolIDs(scope, path, exposedTool)
	if !ok {
		return cherry.MCPTool{}, false, nil
	}
	reader := component.Opened.Reader
	return cherry.MCPTool{
		ExposedName:    reader.String(ids.ExposedNameSID),
		Server:         reader.String(ids.ServerSID),
		ServerEndpoint: reader.String(ids.ServerEndpointSID),
		Tool:           reader.String(ids.ToolSID),
		SecretRef:      reader.String(ids.SecretSID),
		AuthType:       reader.String(ids.AuthTypeSID),
	}, true, nil
}

func (b mappedSplitREPLBackend) PrincipalRoutes(_ context.Context, scope string) ([]cherry.PrincipalRoute, error) {
	routes := map[string]cherry.PrincipalRoute{}
	for _, component := range append([]mappedsplit.OpenedComponent{b.opened.LLMGeneric}, b.opened.LLMUserKey...) {
		if component.Ref.Resource == "" {
			continue
		}
		got, ok := component.Opened.Reader.PrincipalRoutes(scope)
		if !ok {
			continue
		}
		for _, route := range got {
			key := route.PrincipalSlug + "\x00" + route.RequestedModel
			routes[key] = route
		}
	}
	out := make([]cherry.PrincipalRoute, 0, len(routes))
	for _, route := range routes {
		out = append(out, route)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PrincipalSlug == out[j].PrincipalSlug {
			return out[i].RequestedModel < out[j].RequestedModel
		}
		return out[i].PrincipalSlug < out[j].PrincipalSlug
	})
	return out, nil
}

func (b mappedSplitREPLBackend) mcpComponent(path string) mappedsplit.OpenedComponent {
	if b.opened == nil {
		return mappedsplit.OpenedComponent{}
	}
	if strings.HasPrefix(path, "s/") {
		return b.opened.MCPServers
	}
	if key, err := b.opened.Spec.MCPUserProfileBundle(path); err == nil && key.Partition >= 0 && key.Partition < len(b.opened.MCPUserProfile) {
		return b.opened.MCPUserProfile[key.Partition]
	}
	return mappedsplit.OpenedComponent{}
}

func defaultScopeFor(scopes []string) string {
	if len(scopes) == 1 {
		return scopes[0]
	}
	for _, scope := range scopes {
		if scope == defaultScope {
			return scope
		}
	}
	return ""
}

func shortChecksum(checksum []byte) string {
	if len(checksum) == 0 {
		return ""
	}
	if len(checksum) > 8 {
		checksum = checksum[:8]
	}
	return hex.EncodeToString(checksum)
}
