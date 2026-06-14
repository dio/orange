package embeddedpg_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dio/orange/internal/embeddedpg/testpg"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testpg.Cleanup()
	os.Exit(code)
}

func TestPoolSelectOne(t *testing.T) {
	var got int
	err := testpg.Pool(t).QueryRow(context.Background(), "SELECT 1").Scan(&got)
	require.NoError(t, err)
	require.Equal(t, 1, got)
}
