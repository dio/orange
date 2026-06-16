package pgque

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/dio/orange/internal/embeddedpg/testpg"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testpg.Cleanup()
	os.Exit(code)
}

func TestPgQueSetupConcurrentIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := startPgQueTestDB(t)
	poolB := newPgQueTestPool(t)
	defer poolB.Close()

	queue := pgQueTestName(t, "queue")
	consumer := pgQueTestName(t, "consumer")

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, p := range []*pgxpool.Pool{pool, poolB} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- Setup(ctx, p, WithQueue(queue), WithConsumer(consumer))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Equal(t, 1, pgQueCount(t, pool, "SELECT count(*) FROM pgque.queue WHERE queue_name = $1", queue))
	require.Equal(t, 1, pgQueCount(t, pool, `
		SELECT count(*)
		FROM pgque.subscription s
		JOIN pgque.queue q ON q.queue_id = s.sub_queue
		JOIN pgque.consumer c ON c.co_id = s.sub_consumer
		WHERE q.queue_name = $1 AND c.co_name = $2
	`, queue, consumer))

	_, err := SendTestEvent(ctx, pool, queue)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, typ, err := ReceiveOne(ctx, poolB, queue, consumer)
		return err == nil && typ == "orange.pgque.test"
	}, 2*time.Second, 20*time.Millisecond)
}

var pgQueTestDB struct {
	base   *pgxpool.Pool
	dbName string
}

func startPgQueTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	base := testpg.Pool(t)
	dbName := pgQueTestName(t, "db")
	_, err := base.Exec(context.Background(), "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize())
	require.NoError(t, err)

	cfg := base.Config().Copy()
	cfg.ConnConfig.Database = dbName
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	pgQueTestDB.base = base
	pgQueTestDB.dbName = dbName
	t.Cleanup(func() {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize())
	})
	return pool
}

func newPgQueTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if pgQueTestDB.base == nil || pgQueTestDB.dbName == "" {
		t.Fatal("pgque test database not started")
	}
	cfg := pgQueTestDB.base.Config().Copy()
	cfg.ConnConfig.Database = pgQueTestDB.dbName
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	return pool
}

func pgQueTestName(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), time.Now().UnixNano())
}

func pgQueCount(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var count int
	err := pool.QueryRow(context.Background(), query, args...).Scan(&count)
	require.NoError(t, err)
	return count
}
