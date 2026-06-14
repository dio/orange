package migration_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dio/orange/config/postgres/migration"
	"github.com/dio/orange/internal/embeddedpg/testpg"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testpg.Cleanup()
	os.Exit(code)
}

func TestMigratePlanStatusAndIdempotency(t *testing.T) {
	ctx := context.Background()
	pool := testpg.Pool(t)
	table := fmt.Sprintf("orange_schema_migrations_%d", os.Getpid())

	plan, err := migration.Plan(ctx, pool, migration.WithSchemaTable(table))
	require.NoError(t, err)
	require.Len(t, plan, 1)

	require.NoError(t, migration.Migrate(ctx, pool, migration.WithSchemaTable(table)))

	status, err := migration.Status(ctx, pool, migration.WithSchemaTable(table))
	require.NoError(t, err)
	require.Len(t, status.Applied, 1)
	require.Empty(t, status.Pending)

	require.NoError(t, migration.Migrate(ctx, pool, migration.WithSchemaTable(table)))

	status, err = migration.Status(ctx, pool, migration.WithSchemaTable(table))
	require.NoError(t, err)
	require.Len(t, status.Applied, 1)
	require.Empty(t, status.Pending)

	var currentTable string
	err = pool.QueryRow(ctx, "SELECT to_regclass('public.orange_mapped_split_current')::text").Scan(&currentTable)
	require.NoError(t, err)
	assert.Equal(t, "orange_mapped_split_current", currentTable)
}

func TestMigrateWithSchema(t *testing.T) {
	ctx := context.Background()
	pool := testpg.Pool(t)
	schema := fmt.Sprintf("orange_migration_test_%d", os.Getpid())

	plan, err := migration.Plan(ctx, pool, migration.WithSchema(schema))
	require.NoError(t, err)
	require.Len(t, plan, 1)

	require.NoError(t, migration.Migrate(ctx, pool, migration.WithSchema(schema)))

	status, err := migration.Status(ctx, pool, migration.WithSchema(schema))
	require.NoError(t, err)
	require.Len(t, status.Applied, 1)
	require.Empty(t, status.Pending)

	var currentTable string
	err = pool.QueryRow(ctx, "SELECT to_regclass($1)::text", schema+".orange_mapped_split_current").Scan(&currentTable)
	require.NoError(t, err)
	assert.Equal(t, schema+".orange_mapped_split_current", currentTable)
}

func TestInvalidIdentifiers(t *testing.T) {
	ctx := context.Background()
	pool := testpg.Pool(t)

	_, err := migration.Plan(ctx, pool, migration.WithSchema("bad-schema"))
	require.Error(t, err)

	err = migration.Migrate(ctx, pool, migration.WithSchemaTable("bad.table"))
	require.Error(t, err)
}
