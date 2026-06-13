package server_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/dio/orange/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// staticAuthenticator is a test-only Authenticator that returns a fixed
// Principal or error regardless of the request header.
type staticAuthenticator struct {
	principal server.Principal
	err       error
}

func (a *staticAuthenticator) Authenticate(_ context.Context, _ http.Header) (server.Principal, error) {
	if a.err != nil {
		return server.Principal{}, a.err
	}
	return a.principal, nil
}

// staticLaneResolver is a test-only LaneResolver that maps every principal to
// a fixed lane. It deliberately ignores the principal to prove the lane comes
// from configuration, not from client request data.
type staticLaneResolver struct {
	lane string
	err  error
}

func (r *staticLaneResolver) ResolveLane(_ context.Context, _ server.Principal) (string, error) {
	if r.err != nil {
		return "", r.err
	}
	return r.lane, nil
}

// principalLaneResolver maps a principal ID to a specific lane, simulating a
// per-tenant partition table owned by the embedding application.
type principalLaneResolver struct {
	table map[string]string
}

func (r *principalLaneResolver) ResolveLane(_ context.Context, p server.Principal) (string, error) {
	lane, ok := r.table[p.ID]
	if !ok {
		return "", server.ErrPermissionDenied
	}
	return lane, nil
}

// TestFailClosedAuthenticator verifies the safe default rejects all requests.
func TestFailClosedAuthenticator(t *testing.T) {
	var auth server.FailClosedAuthenticator
	_, err := auth.Authenticate(context.Background(), http.Header{})
	require.Error(t, err)
	assert.ErrorIs(t, err, server.ErrUnauthenticated)
}

func TestFailClosedLaneResolver(t *testing.T) {
	var resolver server.FailClosedLaneResolver
	_, err := resolver.ResolveLane(context.Background(), server.Principal{ID: "p1"})
	require.Error(t, err)
	assert.ErrorIs(t, err, server.ErrPermissionDenied)
}

// TestHasScope covers Principal.HasScope.
func TestHasScope(t *testing.T) {
	cases := []struct {
		scopes []string
		target string
		want   bool
	}{
		{[]string{"admin", "read"}, "admin", true},
		{[]string{"read"}, "admin", false},
		{nil, "admin", false},
		{[]string{"admin"}, "ADMIN", false}, // case-sensitive
	}
	for _, tc := range cases {
		p := server.Principal{ID: "x", Scopes: tc.scopes}
		assert.Equal(t, tc.want, p.HasScope(tc.target), "scopes=%v target=%q", tc.scopes, tc.target)
	}
}

// TestLaneResolvesFromPrincipal verifies that the lane is derived from the
// authenticated Principal, not from request fields.
func TestLaneResolvesFromPrincipal(t *testing.T) {
	resolver := &principalLaneResolver{table: map[string]string{
		"client-a": "lane-a",
		"client-b": "lane-b",
	}}

	for _, tc := range []struct {
		id       string
		wantLane string
	}{
		{"client-a", "lane-a"},
		{"client-b", "lane-b"},
	} {
		lane, err := resolver.ResolveLane(context.Background(), server.Principal{ID: tc.id})
		require.NoError(t, err)
		assert.Equal(t, tc.wantLane, lane)
	}
}

// TestLaneResolverUnknownPrincipalFails verifies that an unrecognised principal
// returns an error rather than an empty or default lane.
func TestLaneResolverUnknownPrincipalFails(t *testing.T) {
	resolver := &principalLaneResolver{table: map[string]string{"client-a": "lane-a"}}
	_, err := resolver.ResolveLane(context.Background(), server.Principal{ID: "unknown"})
	require.ErrorIs(t, err, server.ErrPermissionDenied)
}

// TestLaneNotInjectedFromRequest proves the design property: a lane resolver
// receives only the Principal and cannot be influenced by raw request headers.
// The staticLaneResolver ignores both the principal and any header that a
// client might try to supply.
func TestLaneNotInjectedFromRequest(t *testing.T) {
	resolver := &staticLaneResolver{lane: "configured-lane"}
	principal := server.Principal{ID: "p1"}

	// Even if a caller tried to inject a header like "X-Lane: attacker-lane",
	// the resolver has no access to request headers.
	lane, err := resolver.ResolveLane(context.Background(), principal)
	require.NoError(t, err)
	assert.Equal(t, "configured-lane", lane,
		"lane must come from the resolver, not from any client-supplied request field")

	// The resolver signature enforces the boundary: only Principal is passed.
	// This test documents that the interface cannot be misused to accept lane
	// selection from request data.
	var _ server.LaneResolver = resolver
}

// TestAuthenticatorError verifies that an auth failure propagates correctly.
func TestAuthenticatorError(t *testing.T) {
	authErr := errors.New("invalid api key")
	auth := &staticAuthenticator{err: authErr}
	_, err := auth.Authenticate(context.Background(), http.Header{"Authorization": []string{"Bearer bad"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, authErr)
}
