package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/snapshot"
)

// SnapshotService implements configv1connect.SnapshotServiceHandler.
type SnapshotService struct {
	manager *snapshot.Manager
	auth    Authenticator
	lanes   LaneResolver
}

// NewSnapshotService creates a SnapshotService. manager is required; nil auth and lanes fail closed.
func NewSnapshotService(manager *snapshot.Manager, auth Authenticator, lanes LaneResolver) *SnapshotService {
	if auth == nil {
		auth = FailClosedAuthenticator{}
	}
	if lanes == nil {
		lanes = FailClosedLaneResolver{}
	}
	return &SnapshotService{manager: manager, auth: auth, lanes: lanes}
}

// Fetch implements orange.config.v1.SnapshotService.Fetch.
//
// Authentication and lane resolution are performed on every call. The lane is
// derived exclusively from the authenticated principal; FetchRequest carries no
// lane field and clients cannot influence lane selection.
func (s *SnapshotService) Fetch(ctx context.Context, req *connect.Request[configv1.FetchRequest]) (*connect.Response[configv1.FetchResponse], error) {
	principal, err := s.auth.Authenticate(ctx, req.Header())
	if err != nil {
		return nil, connectError(err)
	}

	lane, err := s.lanes.ResolveLane(ctx, principal)
	if err != nil {
		return nil, connectError(err)
	}

	if len(req.Msg.LastChecksum) != 0 && len(req.Msg.LastChecksum) != 32 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("last_checksum must be empty or exactly 32 bytes, got %d", len(req.Msg.LastChecksum)))
	}

	envelope, unchanged, err := s.manager.Fetch(lane, req.Msg.LastVersion, req.Msg.LastChecksum)
	if err != nil {
		return nil, connectError(err)
	}

	resp := &configv1.FetchResponse{}
	if unchanged {
		resp.Result = &configv1.FetchResponse_Unchanged{Unchanged: &configv1.Unchanged{}}
	} else {
		resp.Result = &configv1.FetchResponse_Snapshot{Snapshot: envelope}
	}
	return connect.NewResponse(resp), nil
}
