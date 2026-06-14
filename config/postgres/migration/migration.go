// Package migration applies Orange Postgres store migrations.
package migration

import (
	"context"
	"crypto/sha256"
	"embed"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sql/postgres/*.sql
var migrationFS embed.FS

const defaultSchemaTable = "orange_schema_migrations"

var identifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// Migration is one embedded SQL migration.
type Migration struct {
	Version  int64
	Name     string
	Checksum [32]byte
}

// StatusResult reports which embedded migrations are applied and pending.
type StatusResult struct {
	Applied []Migration
	Pending []Migration
}

// Option configures migration behavior.
type Option func(*options)

type options struct {
	schema string
	table  string
}

// WithSchema applies migrations in schema. The schema is created by Migrate if
// it does not exist.
func WithSchema(schema string) Option {
	return func(o *options) {
		o.schema = schema
	}
}

// WithSchemaTable stores migration metadata in table.
func WithSchemaTable(table string) Option {
	return func(o *options) {
		o.table = table
	}
}

// Plan returns the embedded migrations that have not been applied.
func Plan(ctx context.Context, pool *pgxpool.Pool, opts ...Option) ([]Migration, error) {
	cfg, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, fmt.Errorf("migration: nil pool")
	}
	migrations, err := loadMigrations()
	if err != nil {
		return nil, err
	}
	applied, err := appliedVersions(ctx, pool, cfg)
	if err != nil {
		return nil, err
	}
	return pendingMigrations(migrations, applied), nil
}

// Status returns applied and pending embedded migrations.
func Status(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (StatusResult, error) {
	cfg, err := applyOptions(opts)
	if err != nil {
		return StatusResult{}, err
	}
	if pool == nil {
		return StatusResult{}, fmt.Errorf("migration: nil pool")
	}
	migrations, err := loadMigrations()
	if err != nil {
		return StatusResult{}, err
	}
	appliedVersions, err := appliedVersions(ctx, pool, cfg)
	if err != nil {
		return StatusResult{}, err
	}

	var out StatusResult
	for _, m := range migrations {
		if _, ok := appliedVersions[m.Version]; ok {
			out.Applied = append(out.Applied, m)
			continue
		}
		out.Pending = append(out.Pending, m)
	}
	return out, nil
}

// Migrate applies all pending embedded migrations in one transaction.
func Migrate(ctx context.Context, pool *pgxpool.Pool, opts ...Option) error {
	cfg, err := applyOptions(opts)
	if err != nil {
		return err
	}
	if pool == nil {
		return fmt.Errorf("migration: nil pool")
	}
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migration: begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))", "orange:postgres-store:migration", cfg.lockName()); err != nil {
		return fmt.Errorf("migration: acquire advisory lock: %w", err)
	}
	if cfg.schema != "" {
		if _, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{cfg.schema}.Sanitize()); err != nil {
			return fmt.Errorf("migration: create schema %q: %w", cfg.schema, err)
		}
		if err := setSearchPath(ctx, tx, cfg); err != nil {
			return err
		}
	}
	if err := ensureMetadataTable(ctx, tx, cfg); err != nil {
		return err
	}

	applied, err := appliedVersionsTx(ctx, tx, cfg)
	if err != nil {
		return err
	}
	for _, m := range pendingMigrations(migrations, applied) {
		sql, err := migrationFS.ReadFile(path.Join("sql/postgres", migrationFileName(m)))
		if err != nil {
			return fmt.Errorf("migration: read %s: %w", m.Name, err)
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration: apply %s: %w", m.Name, err)
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO "+cfg.qualifiedTable()+" (version, name, checksum) VALUES ($1, $2, $3)",
			m.Version, m.Name, m.Checksum[:],
		); err != nil {
			return fmt.Errorf("migration: record %s: %w", m.Name, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migration: commit: %w", err)
	}
	return nil
}

func applyOptions(opts []Option) (options, error) {
	cfg := options{table: defaultSchemaTable}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.schema != "" && !identifierPattern.MatchString(cfg.schema) {
		return options{}, fmt.Errorf("migration: invalid schema identifier %q", cfg.schema)
	}
	if !identifierPattern.MatchString(cfg.table) {
		return options{}, fmt.Errorf("migration: invalid schema table identifier %q", cfg.table)
	}
	return cfg, nil
}

func loadMigrations() ([]Migration, error) {
	files, err := migrationFS.ReadDir("sql/postgres")
	if err != nil {
		return nil, fmt.Errorf("migration: read embedded migrations: %w", err)
	}
	migrations := make([]Migration, 0, len(files))
	seen := map[int64]struct{}{}
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}
		parts := strings.SplitN(file.Name(), "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("migration: invalid migration filename %q", file.Name())
		}
		version, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("migration: invalid migration version in %q", file.Name())
		}
		if _, ok := seen[version]; ok {
			return nil, fmt.Errorf("migration: duplicate migration version %d", version)
		}
		seen[version] = struct{}{}
		body, err := migrationFS.ReadFile(path.Join("sql/postgres", file.Name()))
		if err != nil {
			return nil, fmt.Errorf("migration: read %s: %w", file.Name(), err)
		}
		migrations = append(migrations, Migration{
			Version:  version,
			Name:     strings.TrimSuffix(file.Name(), ".sql"),
			Checksum: sha256.Sum256(body),
		})
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})
	return migrations, nil
}

func appliedVersions(ctx context.Context, pool *pgxpool.Pool, cfg options) (map[int64]struct{}, error) {
	var regclass *string
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1)::text", cfg.regclassName()).Scan(&regclass); err != nil {
		return nil, fmt.Errorf("migration: check metadata table: %w", err)
	}
	if regclass == nil {
		return map[int64]struct{}{}, nil
	}
	rows, err := pool.Query(ctx, "SELECT version FROM "+cfg.qualifiedTable())
	if err != nil {
		return nil, fmt.Errorf("migration: query applied migrations: %w", err)
	}
	defer rows.Close()

	out := map[int64]struct{}{}
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("migration: scan applied migration: %w", err)
		}
		out[version] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("migration: iterate applied migrations: %w", err)
	}
	return out, nil
}

func appliedVersionsTx(ctx context.Context, tx pgx.Tx, cfg options) (map[int64]struct{}, error) {
	rows, err := tx.Query(ctx, "SELECT version FROM "+cfg.qualifiedTable())
	if err != nil {
		return nil, fmt.Errorf("migration: query applied migrations: %w", err)
	}
	defer rows.Close()

	out := map[int64]struct{}{}
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("migration: scan applied migration: %w", err)
		}
		out[version] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("migration: iterate applied migrations: %w", err)
	}
	return out, nil
}

func pendingMigrations(migrations []Migration, applied map[int64]struct{}) []Migration {
	pending := make([]Migration, 0, len(migrations))
	for _, m := range migrations {
		if _, ok := applied[m.Version]; !ok {
			pending = append(pending, m)
		}
	}
	return pending
}

func ensureMetadataTable(ctx context.Context, tx pgx.Tx, cfg options) error {
	_, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+cfg.qualifiedTable()+` (
		version bigint PRIMARY KEY,
		name text NOT NULL,
		checksum bytea NOT NULL CHECK (length(checksum) = 32),
		applied_at timestamptz NOT NULL DEFAULT now()
	)`)
	if err != nil {
		return fmt.Errorf("migration: create metadata table: %w", err)
	}
	return nil
}

func setSearchPath(ctx context.Context, tx pgx.Tx, cfg options) error {
	if cfg.schema == "" {
		return nil
	}
	if _, err := tx.Exec(ctx, "SELECT set_config('search_path', $1, true)", cfg.schema+",public"); err != nil {
		return fmt.Errorf("migration: set search_path: %w", err)
	}
	return nil
}

func migrationFileName(m Migration) string {
	return m.Name + ".sql"
}

func (o options) qualifiedTable() string {
	if o.schema == "" {
		return pgx.Identifier{"public", o.table}.Sanitize()
	}
	return pgx.Identifier{o.schema, o.table}.Sanitize()
}

func (o options) regclassName() string {
	if o.schema == "" {
		return "public." + o.table
	}
	return o.schema + "." + o.table
}

func (o options) lockName() string {
	if o.schema == "" {
		return "public." + o.table
	}
	return o.schema + "." + o.table
}
