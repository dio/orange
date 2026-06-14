package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

func runClient(args []string) error {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		switch args[0] {
		case "fetch-map":
			return runClientFetchMap(args[1:])
		case "fetch-bundle", "fetch-resource":
			return runClientFetchBundle(args[1:])
		case "apply", "watch":
			return runClientApply(args[1:])
		default:
			return fmt.Errorf("unknown client command %q", args[0])
		}
	}
	return runClientApply(args)
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
