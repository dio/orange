// Package embeddedpg wraps fergusstrange/embedded-postgres so Orange tests can
// run a self-contained PostgreSQL instance without depending on a system
// install.
//
// All on-disk paths live under a single root directory. The root is resolved in
// this order:
//
//  1. The Root field on [Config] if non-empty.
//  2. The ORANGE_EMBEDDED_PG_DIR environment variable if set.
//  3. The OS user cache directory.
package embeddedpg

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

// DefaultPort is the listen port used when [Config.Port] is zero.
const DefaultPort uint32 = 5433

// Config configures the embedded postgres instance.
type Config struct {
	// Root is the parent directory for cache, bin, runtime, and data
	// subdirectories. Empty means "resolve from env/defaults" (see package docs).
	Root string

	Username string
	Password string
	Database string
	Port     uint32

	// StartTimeout bounds how long Start waits for postgres to accept
	// connections before giving up. Zero means use the upstream library default.
	StartTimeout time.Duration

	// ResetData deletes the data directory before starting.
	ResetData bool
}

func (c Config) withDefaults() (Config, error) {
	if c.Username == "" {
		c.Username = "orange"
	}
	if c.Password == "" {
		c.Password = "orange"
	}
	if c.Database == "" {
		c.Database = "orange"
	}
	if c.Port == 0 {
		c.Port = DefaultPort
	}
	if c.Root == "" {
		root, err := defaultRoot()
		if err != nil {
			return c, err
		}
		c.Root = root
	}
	return c, nil
}

func defaultRoot() (string, error) {
	if v := os.Getenv("ORANGE_EMBEDDED_PG_DIR"); v != "" {
		return v, nil
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("embeddedpg: resolve user cache dir: %w", err)
	}
	return filepath.Join(cache, "orange", "pg"), nil
}

// Instance is a running embedded postgres.
type Instance struct {
	cfg Config
	db  *embeddedpostgres.EmbeddedPostgres
}

// Start launches an embedded postgres according to cfg.
func Start(cfg Config) (*Instance, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return nil, fmt.Errorf("embeddedpg: mkdir root: %w", err)
	}
	dataDir := filepath.Join(cfg.Root, "data")
	if cfg.ResetData {
		if err := os.RemoveAll(dataDir); err != nil {
			return nil, fmt.Errorf("embeddedpg: reset data dir: %w", err)
		}
	}

	pgCfg := embeddedpostgres.DefaultConfig().
		Username(cfg.Username).
		Password(cfg.Password).
		Database(cfg.Database).
		Port(cfg.Port).
		CachePath(filepath.Join(cfg.Root, "cache")).
		BinariesPath(filepath.Join(cfg.Root, "bin")).
		RuntimePath(filepath.Join(cfg.Root, "runtime")).
		DataPath(dataDir)
	if cfg.StartTimeout > 0 {
		pgCfg = pgCfg.StartTimeout(cfg.StartTimeout)
	}

	db := embeddedpostgres.NewDatabase(pgCfg)
	if err := db.Start(); err != nil {
		return nil, fmt.Errorf("embeddedpg: start: %w", err)
	}
	return &Instance{cfg: cfg, db: db}, nil
}

// Stop shuts down the embedded postgres. Safe to call on a nil Instance.
func (i *Instance) Stop() error {
	if i == nil || i.db == nil {
		return nil
	}
	return i.db.Stop()
}

// DSN returns a libpq-style connection URL suitable for pgxpool.New.
func (i *Instance) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable",
		i.cfg.Username, i.cfg.Password, i.cfg.Port, i.cfg.Database)
}

// Config returns the resolved configuration with defaults applied.
func (i *Instance) Config() Config { return i.cfg }
