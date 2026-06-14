// Package pgque installs and wires PgQue for Orange's optional mapped-split
// scheduler.
package pgque

import (
	"context"
	"embed"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sql/pgque.sql
var sqlFS embed.FS

const (
	DefaultQueue    = "orange_mapped_split_builds"
	DefaultConsumer = "orange_mapped_split_builder"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)

// SetupOption configures PgQue setup.
type SetupOption func(*setupOptions)

type setupOptions struct {
	queue       string
	consumer    string
	subconsumer string
}

// WithQueue sets the PgQue queue name created during setup.
func WithQueue(queue string) SetupOption {
	return func(o *setupOptions) {
		o.queue = queue
	}
}

// WithConsumer sets the logical PgQue consumer registered during setup.
func WithConsumer(consumer string) SetupOption {
	return func(o *setupOptions) {
		o.consumer = consumer
	}
}

// WithSubconsumer registers a cooperative PgQue subconsumer under the logical
// consumer. It is optional; schedulers can also use plain receive.
func WithSubconsumer(subconsumer string) SetupOption {
	return func(o *setupOptions) {
		o.subconsumer = subconsumer
	}
}

// Setup installs or upgrades PgQue, creates the Orange mapped-split queue, and
// registers the logical consumer. It does not create or modify Orange store
// tables.
func Setup(ctx context.Context, pool *pgxpool.Pool, opts ...SetupOption) error {
	if pool == nil {
		return fmt.Errorf("pgque setup: nil pool")
	}
	cfg, err := applySetupOptions(opts)
	if err != nil {
		return err
	}
	body, err := sqlFS.ReadFile("sql/pgque.sql")
	if err != nil {
		return fmt.Errorf("pgque setup: read embedded sql: %w", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgque setup: begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// PgQue setup is safe to run from every replica, but the upstream SQL file
	// is large and performs DDL. Serialize the install and queue wiring per
	// database/queue so two replicas do not race through CREATE TABLE/FUNCTION
	// and queue registration at startup.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))", "orange:pgque:setup", cfg.queue); err != nil {
		return fmt.Errorf("pgque setup: acquire advisory lock: %w", err)
	}
	// This is vendored from https://github.com/NikolayS/PgQue/blob/e8ee488d2c1d87ab09eed2581ec4bbc1e68315f6/sql/pgque.sql.
	// Keep it separate from Orange store migrations: a database should be able
	// to install PgQue without any orange_mapped_split_* tables, and the store
	// should migrate without any pgque schema.
	if _, err := tx.Exec(ctx, string(body)); err != nil {
		return fmt.Errorf("pgque setup: install pgque sql: %w", err)
	}
	if _, err := tx.Exec(ctx, "SELECT pgque.create_queue($1)", cfg.queue); err != nil {
		return fmt.Errorf("pgque setup: create queue %q: %w", cfg.queue, err)
	}
	if _, err := tx.Exec(ctx, "SELECT pgque.set_queue_config($1, 'ticker_max_count', '1')", cfg.queue); err != nil {
		return fmt.Errorf("pgque setup: configure queue %q ticker_max_count: %w", cfg.queue, err)
	}
	// Embedded integration tests and low-latency control-plane schedulers need
	// prompt batch windows after send. These are queue-local settings, not
	// Orange store state, and are safe to apply idempotently during setup.
	if _, err := tx.Exec(ctx, "SELECT pgque.set_queue_config($1, 'ticker_max_lag', '1ms')", cfg.queue); err != nil {
		return fmt.Errorf("pgque setup: configure queue %q ticker_max_lag: %w", cfg.queue, err)
	}
	if _, err := tx.Exec(ctx, "SELECT pgque.subscribe($1, $2)", cfg.queue, cfg.consumer); err != nil {
		return fmt.Errorf("pgque setup: subscribe consumer %q: %w", cfg.consumer, err)
	}
	if cfg.subconsumer != "" {
		if _, err := tx.Exec(ctx, "SELECT pgque.subscribe_subconsumer($1, $2, $3)", cfg.queue, cfg.consumer, cfg.subconsumer); err != nil {
			return fmt.Errorf("pgque setup: subscribe subconsumer %q: %w", cfg.subconsumer, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgque setup: commit: %w", err)
	}
	return nil
}

func applySetupOptions(opts []SetupOption) (setupOptions, error) {
	cfg := setupOptions{
		queue:    DefaultQueue,
		consumer: DefaultConsumer,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := validateName("queue", cfg.queue); err != nil {
		return setupOptions{}, err
	}
	if err := validateName("consumer", cfg.consumer); err != nil {
		return setupOptions{}, err
	}
	if cfg.subconsumer != "" {
		if err := validateName("subconsumer", cfg.subconsumer); err != nil {
			return setupOptions{}, err
		}
	}
	return cfg, nil
}

func validateName(kind, name string) error {
	if name == "" {
		return fmt.Errorf("pgque setup: %s is required", kind)
	}
	if len(name) > 57 {
		return fmt.Errorf("pgque setup: %s %q is too long", kind, name)
	}
	if !namePattern.MatchString(name) {
		return fmt.Errorf("pgque setup: invalid %s %q", kind, name)
	}
	return nil
}

// SendTestEvent sends one simple event for independence tests and setup
// verification. Production scheduling uses config.PgQueScheduler.
func SendTestEvent(ctx context.Context, pool *pgxpool.Pool, queue string) (int64, error) {
	if queue == "" {
		queue = DefaultQueue
	}
	var id int64
	if err := pool.QueryRow(ctx, "SELECT pgque.send($1, $2, $3::jsonb)", queue, "orange.pgque.test", `{"ok":true}`).Scan(&id); err != nil {
		return 0, fmt.Errorf("pgque setup: send test event: %w", err)
	}
	return id, nil
}

// ReceiveOne fetches one event after forcing a PgQue tick.
func ReceiveOne(ctx context.Context, pool *pgxpool.Pool, queue, consumer string) (int64, string, error) {
	if queue == "" {
		queue = DefaultQueue
	}
	if consumer == "" {
		consumer = DefaultConsumer
	}
	// PgQue/PgQ separates event insertion from delivery. send() writes an event
	// row; ticker() materializes a tick window; receive() opens the next batch
	// for the consumer. force_next_tick()+ticker() is the PgQue-documented
	// smoke-test path for making a just-sent event visible without waiting for
	// normal ticker thresholds.
	if _, err := pool.Exec(ctx, "SELECT pgque.force_next_tick($1)", queue); err != nil {
		return 0, "", fmt.Errorf("pgque setup: force tick: %w", err)
	}
	if _, err := pool.Exec(ctx, "SELECT pgque.ticker()"); err != nil {
		return 0, "", fmt.Errorf("pgque setup: global tick: %w", err)
	}
	rows, err := pool.Query(ctx, "SELECT msg_id, type FROM pgque.receive($1, $2, 1)", queue, consumer)
	if err != nil {
		return 0, "", fmt.Errorf("pgque setup: receive: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, "", fmt.Errorf("pgque setup: receive rows: %w", err)
		}
		return 0, "", pgx.ErrNoRows
	}
	var msgID int64
	var typ string
	if err := rows.Scan(&msgID, &typ); err != nil {
		return 0, "", fmt.Errorf("pgque setup: scan received event: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, "", fmt.Errorf("pgque setup: receive rows: %w", err)
	}
	return msgID, typ, nil
}
