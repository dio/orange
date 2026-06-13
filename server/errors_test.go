package server

import (
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/dio/orange/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode connect.Code
	}{
		{
			name:     "unauthenticated",
			err:      ErrUnauthenticated,
			wantCode: connect.CodeUnauthenticated,
		},
		{
			name:     "unauthenticated wrapped",
			err:      fmt.Errorf("wrapped: %w", ErrUnauthenticated),
			wantCode: connect.CodeUnauthenticated,
		},
		{
			name:     "permission denied",
			err:      ErrPermissionDenied,
			wantCode: connect.CodePermissionDenied,
		},
		{
			name:     "permission denied wrapped",
			err:      fmt.Errorf("outer: %w", ErrPermissionDenied),
			wantCode: connect.CodePermissionDenied,
		},
		{
			name:     "no snapshot",
			err:      snapshot.ErrNoSnapshot,
			wantCode: connect.CodeNotFound,
		},
		{
			name:     "version mismatch",
			err:      snapshot.ErrVersionMismatch,
			wantCode: connect.CodeFailedPrecondition,
		},
		{
			name:     "version mismatch wrapped",
			err:      fmt.Errorf("publish: %w", snapshot.ErrVersionMismatch),
			wantCode: connect.CodeFailedPrecondition,
		},
		{
			name:     "no callback",
			err:      snapshot.ErrNoCallback,
			wantCode: connect.CodeUnavailable,
		},
		{
			name:     "unknown error becomes internal",
			err:      errors.New("some unexpected internal failure"),
			wantCode: connect.CodeInternal,
		},
		{
			name:     "already a connect error passes through",
			err:      connect.NewError(connect.CodeNotFound, errors.New("custom not found")),
			wantCode: connect.CodeNotFound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := connectError(tc.err)
			require.Error(t, got)

			var ce *connect.Error
			require.ErrorAs(t, got, &ce)
			assert.Equal(t, tc.wantCode, ce.Code())
		})
	}
}

func TestConnectErrorNilReturnsNil(t *testing.T) {
	assert.Nil(t, connectError(nil))
}

func TestConnectErrorInternalDoesNotLeakDetails(t *testing.T) {
	sensitiveErr := errors.New("db password=hunter2 exposed")
	got := connectError(sensitiveErr)

	var ce *connect.Error
	require.ErrorAs(t, got, &ce)
	assert.Equal(t, connect.CodeInternal, ce.Code())
	// The error message exposed to callers must not contain the raw internal text.
	assert.NotContains(t, ce.Error(), "hunter2")
	assert.NotContains(t, ce.Error(), "password")
}
