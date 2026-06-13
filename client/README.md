# Orange Client

Package `client` wraps `orange.config.v1.SnapshotService.Fetch` for data-plane
pollers such as Plum.

It handles the boring fetch loop pieces:

- local `last_version` and `last_checksum` tracking
- `Unchanged` responses using the cached snapshot
- ConfigPayload decoding
- envelope and payload checksum validation
- retry for transient Connect errors
- per-attempt timeout
- request header injection for auth
- `singleflight` coalescing so concurrent callers share one in-flight fetch

Basic use:

```go
c, err := client.New("https://orange.example.com",
	client.WithHeaderFunc(func(ctx context.Context, h http.Header) error {
		h.Set("Authorization", "Bearer "+token)
		return nil
	}),
)
if err != nil {
	return err
}

result, err := c.Fetch(ctx)
if err != nil {
	return err
}

bundleBytes := result.BundleZstd
```
