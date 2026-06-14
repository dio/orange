package config

import (
	"context"
	"errors"
	"net/http"
)

// ServerPrincipal is the verified identity of an authenticated caller.
type ServerPrincipal struct {
	ID     string
	Scopes []string
}

// HasScope reports whether p carries the named scope.
func (p ServerPrincipal) HasScope(scope string) bool {
	for _, s := range p.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// Authenticator validates request credentials and returns the caller's
// principal. Implementations must fail closed.
type Authenticator interface {
	Authenticate(ctx context.Context, header http.Header) (ServerPrincipal, error)
}

// LaneResolver maps a verified principal to a snapshot lane.
type LaneResolver interface {
	ResolveLane(ctx context.Context, principal ServerPrincipal) (string, error)
}

// ErrUnauthenticated is the sentinel for missing or invalid credentials.
var ErrUnauthenticated = errors.New("unauthenticated")

// ErrPermissionDenied is the sentinel for authenticated but unauthorised access.
var ErrPermissionDenied = errors.New("permission denied")

// FailClosedAuthenticator rejects every request.
type FailClosedAuthenticator struct{}

// Authenticate always returns ErrUnauthenticated.
func (FailClosedAuthenticator) Authenticate(_ context.Context, _ http.Header) (ServerPrincipal, error) {
	return ServerPrincipal{}, ErrUnauthenticated
}

// FailClosedLaneResolver rejects every lane lookup.
type FailClosedLaneResolver struct{}

// ResolveLane always returns ErrPermissionDenied.
func (FailClosedLaneResolver) ResolveLane(_ context.Context, _ ServerPrincipal) (string, error) {
	return "", ErrPermissionDenied
}

// AuthenticatorFunc adapts a function to Authenticator.
type AuthenticatorFunc func(context.Context, http.Header) (ServerPrincipal, error)

// Authenticate implements Authenticator.
func (f AuthenticatorFunc) Authenticate(ctx context.Context, header http.Header) (ServerPrincipal, error) {
	return f(ctx, header)
}

// LaneResolverFunc adapts a function to LaneResolver.
type LaneResolverFunc func(context.Context, ServerPrincipal) (string, error)

// ResolveLane implements LaneResolver.
func (f LaneResolverFunc) ResolveLane(ctx context.Context, principal ServerPrincipal) (string, error) {
	return f(ctx, principal)
}
