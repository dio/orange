// Package server implements the Connect service handlers for Orange.
package server

import (
	"context"
	"errors"
	"net/http"
)

// Principal is the verified identity of an authenticated caller.
type Principal struct {
	ID     string
	Scopes []string
}

// HasScope reports whether p carries the named scope.
func (p Principal) HasScope(scope string) bool {
	for _, s := range p.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// Authenticator validates request credentials and returns the caller's
// Principal. Implementations must fail closed: an unrecognised or missing
// credential returns an error rather than a zero Principal.
//
// The header parameter is the HTTP request header from a Connect request
// (connect.Request[T].Header()). Using http.Header keeps this interface
// independent of any specific proto message type.
type Authenticator interface {
	Authenticate(ctx context.Context, header http.Header) (Principal, error)
}

// LaneResolver maps a verified Principal to a snapshot lane. Lane selection
// must come from the authenticated principal identity, never from client-
// provided fields in the fetch request.
type LaneResolver interface {
	ResolveLane(ctx context.Context, principal Principal) (string, error)
}

// ErrUnauthenticated is the sentinel for missing or invalid credentials.
var ErrUnauthenticated = errors.New("unauthenticated")

// ErrPermissionDenied is the sentinel for authenticated but unauthorised access.
var ErrPermissionDenied = errors.New("permission denied")

// FailClosedAuthenticator rejects every request. It is the safe production
// default when no real Authenticator is configured.
type FailClosedAuthenticator struct{}

// Authenticate always returns ErrUnauthenticated.
func (FailClosedAuthenticator) Authenticate(_ context.Context, _ http.Header) (Principal, error) {
	return Principal{}, ErrUnauthenticated
}

// FailClosedLaneResolver rejects every lane lookup. It is the safe production
// default when no real LaneResolver is configured.
type FailClosedLaneResolver struct{}

// ResolveLane always returns ErrPermissionDenied.
func (FailClosedLaneResolver) ResolveLane(_ context.Context, _ Principal) (string, error) {
	return "", ErrPermissionDenied
}
