package config

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type principalLaneResolver struct {
	lanes map[string]string
}

func (r *principalLaneResolver) ResolveLane(_ context.Context, p ServerPrincipal) (string, error) {
	lane, ok := r.lanes[p.ID]
	if !ok {
		return "", ErrPermissionDenied
	}
	return lane, nil
}

func TestFailClosedAuthAndLaneResolver(t *testing.T) {
	var auth FailClosedAuthenticator
	_, err := auth.Authenticate(context.Background(), http.Header{})
	assert.ErrorIs(t, err, ErrUnauthenticated)

	var resolver FailClosedLaneResolver
	_, err = resolver.ResolveLane(context.Background(), ServerPrincipal{ID: "p1"})
	assert.ErrorIs(t, err, ErrPermissionDenied)
}

func TestPrincipalHasScope(t *testing.T) {
	tests := []struct {
		name   string
		scopes []string
		scope  string
		want   bool
	}{
		{name: "present", scopes: []string{"read", "write"}, scope: "write", want: true},
		{name: "missing", scopes: []string{"read"}, scope: "admin", want: false},
		{name: "empty", scope: "read", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := ServerPrincipal{ID: "x", Scopes: tc.scopes}
			assert.Equal(t, tc.want, p.HasScope(tc.scope))
		})
	}
}

func TestPrincipalLaneResolver(t *testing.T) {
	resolver := &principalLaneResolver{lanes: map[string]string{
		"plum-a": "lane-a",
		"plum-b": "lane-b",
	}}

	for _, tc := range []struct {
		id   string
		want string
	}{
		{id: "plum-a", want: "lane-a"},
		{id: "plum-b", want: "lane-b"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			lane, err := resolver.ResolveLane(context.Background(), ServerPrincipal{ID: tc.id})
			require.NoError(t, err)
			assert.Equal(t, tc.want, lane)
		})
	}

	_, err := resolver.ResolveLane(context.Background(), ServerPrincipal{ID: "unknown"})
	require.ErrorIs(t, err, ErrPermissionDenied)
}

func TestFuncAdapters(t *testing.T) {
	auth := AuthenticatorFunc(func(_ context.Context, h http.Header) (ServerPrincipal, error) {
		if h.Get("authorization") == "" {
			return ServerPrincipal{}, ErrUnauthenticated
		}
		return ServerPrincipal{ID: "p1"}, nil
	})
	principal, err := auth.Authenticate(context.Background(), http.Header{"Authorization": []string{"bearer token"}})
	require.NoError(t, err)
	assert.Equal(t, "p1", principal.ID)

	resolver := LaneResolverFunc(func(_ context.Context, principal ServerPrincipal) (string, error) {
		return fmt.Sprintf("lane:%s", principal.ID), nil
	})
	lane, err := resolver.ResolveLane(context.Background(), principal)
	require.NoError(t, err)
	assert.Equal(t, "lane:p1", lane)
}

var _ LaneResolver = (*principalLaneResolver)(nil)
