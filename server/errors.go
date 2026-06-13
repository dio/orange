package server

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/dio/orange/snapshot"
)

// connectError maps Orange sentinel errors to stable Connect codes. Unknown
// errors become CodeInternal so raw internal details never leak to callers.
func connectError(err error) error {
	if err == nil {
		return nil
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		return err // already a connect error
	}
	switch {
	case errors.Is(err, ErrUnauthenticated):
		return connect.NewError(connect.CodeUnauthenticated, err)
	case errors.Is(err, ErrPermissionDenied):
		return connect.NewError(connect.CodePermissionDenied, err)
	case errors.Is(err, snapshot.ErrNoSnapshot):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, snapshot.ErrVersionMismatch):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, snapshot.ErrNoCallback):
		return connect.NewError(connect.CodeUnavailable, err)
	default:
		return connect.NewError(connect.CodeInternal, fmt.Errorf("internal error"))
	}
}
