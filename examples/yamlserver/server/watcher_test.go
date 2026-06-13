package yamlserver

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWatcher_Debounce(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "test.yaml")
	require.NoError(t, os.WriteFile(path, []byte("initial"), 0600))

	w := NewWatcher(path, 100*time.Millisecond)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	var count atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx, logger, func() { count.Add(1) })
	}()

	// Give the watcher goroutine time to register before writing.
	time.Sleep(50 * time.Millisecond)

	// Write multiple times in rapid succession within the debounce window.
	for i := 0; i < 3; i++ {
		require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf("v%d", i+2)), 0600))
	}

	// Wait for debounce to fire plus a generous buffer.
	time.Sleep(500 * time.Millisecond)
	cancel()
	<-done

	assert.Equal(t, int32(1), count.Load())
}

func TestWatcher_ContextCancel(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "test.yaml")
	require.NoError(t, os.WriteFile(path, []byte("initial"), 0600))

	w := NewWatcher(path, 200*time.Millisecond)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx, logger, func() {})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
