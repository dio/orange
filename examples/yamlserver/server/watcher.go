package yamlserver

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

const defaultDebounce = 200 * time.Millisecond

// Watcher monitors a single file path and triggers a callback after a
// configurable debounce window collapses rapid change events into one.
type Watcher struct {
	path     string
	debounce time.Duration
}

// NewWatcher creates a Watcher for path. If debounce is zero, it defaults to
// 200ms.
func NewWatcher(path string, debounce time.Duration) *Watcher {
	if debounce <= 0 {
		debounce = defaultDebounce
	}
	return &Watcher{path: path, debounce: debounce}
}

// Run blocks until ctx is cancelled, calling onChange after each debounced
// change event. Multiple fsnotify events within the debounce window are
// collapsed into one onChange call. Transient watch errors are logged as
// warnings and do not stop the watcher. Run returns ctx.Err() on normal
// shutdown.
//
// The parent directory is watched rather than the file itself so that atomic
// editor saves (rename + create) never leave the watcher without an active
// watch. Events from other files in the same directory are filtered out.
func (w *Watcher) Run(ctx context.Context, logger *slog.Logger, onChange func()) error {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create fsnotify watcher: %w", err)
	}
	defer fw.Close()

	dir := filepath.Dir(w.path)
	base := filepath.Base(w.path)

	if err := fw.Add(dir); err != nil {
		return fmt.Errorf("watch directory %s: %w", dir, err)
	}

	var timer *time.Timer
	resetTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		timer = time.AfterFunc(w.debounce, onChange)
	}

	for {
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return ctx.Err()

		case event, ok := <-fw.Events:
			if !ok {
				return nil
			}
			// Only react to events for the specific file being watched.
			if filepath.Base(event.Name) != base {
				continue
			}
			resetTimer()

		case watchErr, ok := <-fw.Errors:
			if !ok {
				return nil
			}
			logger.Warn("fsnotify error", "path", w.path, "error", watchErr)
		}
	}
}
