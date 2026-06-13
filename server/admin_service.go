package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	adminv1 "github.com/dio/orange/api/orange/config/admin/v1"
	"github.com/dio/orange/snapshot"
)

// AdminService implements adminv1connect.ConfigAdminServiceHandler.
type AdminService struct {
	manager *snapshot.Manager
	auth    Authenticator
	lanes   LaneResolver
}

// NewAdminService creates an AdminService. manager is required; nil auth and lanes fail closed.
func NewAdminService(manager *snapshot.Manager, auth Authenticator, lanes LaneResolver) *AdminService {
	if auth == nil {
		auth = FailClosedAuthenticator{}
	}
	if lanes == nil {
		lanes = FailClosedLaneResolver{}
	}
	return &AdminService{manager: manager, auth: auth, lanes: lanes}
}

// PublishSnapshot implements orange.config.admin.v1.ConfigAdminService.PublishSnapshot.
//
// The caller must authenticate and carry the "admin" scope. The lane is
// derived from the authenticated principal via LaneResolver — the same
// principal-to-partition boundary as SnapshotService.Fetch, so admin publish
// and data-plane fetch for a given principal always target the same lane.
func (s *AdminService) PublishSnapshot(ctx context.Context, req *connect.Request[adminv1.PublishSnapshotRequest]) (*connect.Response[adminv1.PublishSnapshotResponse], error) {
	principal, err := s.auth.Authenticate(ctx, req.Header())
	if err != nil {
		return nil, connectError(err)
	}

	if !principal.HasScope("admin") {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("admin scope required"))
	}

	lane, err := s.lanes.ResolveLane(ctx, principal)
	if err != nil {
		return nil, connectError(err)
	}

	if len(req.Msg.ExpectedChecksum) != 0 && len(req.Msg.ExpectedChecksum) != 32 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("expected_checksum must be empty or exactly 32 bytes, got %d",
				len(req.Msg.ExpectedChecksum)))
	}

	mutReq := snapshot.MutationRequest{
		Lane:             lane,
		ExpectedVersion:  req.Msg.ExpectedVersion,
		ExpectedChecksum: req.Msg.ExpectedChecksum,
		PreparedData:     req.Msg.PreparedData,
	}

	result, err := s.manager.PublishWithResult(ctx, mutReq)
	if err != nil {
		return nil, connectError(err)
	}
	published := result.Snapshot

	return connect.NewResponse(&adminv1.PublishSnapshotResponse{
		PreviousVersion:   result.PreviousVersion,
		PublishedVersion:  published.Version,
		PublishedChecksum: published.Checksum[:],
		Scopes:            published.Scopes,
	}), nil
}
