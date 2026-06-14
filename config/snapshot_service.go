package config

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	configv1 "github.com/dio/orange/api/orange/config/v1"
)

// SnapshotService implements configv1connect.SnapshotServiceHandler.
type SnapshotService struct {
	bundles     BundleResourceProvider
	auth        Authenticator
	lanes       LaneResolver
	mappedSplit MappedSplitMapProvider
}

// BundleResourceProvider serves component bundle resources for authenticated lanes.
type BundleResourceProvider interface {
	FetchResource(ctx context.Context, lane string, resource string, lastVersion uint64, lastChecksum []byte) (*configv1.SnapshotEnvelope, bool, error)
}

// MappedSplitMapProvider serves typed mapped-split maps for authenticated lanes.
type MappedSplitMapProvider interface {
	FetchMappedSplitMap(ctx context.Context, lane string, lastVersion uint64, lastChecksum []byte) (*configv1.MappedSplitSnapshot, bool, error)
}

// NewSnapshotServiceWithProviders creates a SnapshotService from embedder-owned
// bundle and map providers.
func NewSnapshotServiceWithProviders(
	bundles BundleResourceProvider,
	auth Authenticator,
	lanes LaneResolver,
	mappedSplit MappedSplitMapProvider,
) *SnapshotService {
	if auth == nil {
		auth = FailClosedAuthenticator{}
	}
	if lanes == nil {
		lanes = FailClosedLaneResolver{}
	}
	return &SnapshotService{bundles: bundles, auth: auth, lanes: lanes, mappedSplit: mappedSplit}
}

// FetchMappedSplitMap implements orange.config.v1.SnapshotService.FetchMappedSplitMap.
func (s *SnapshotService) FetchMappedSplitMap(
	ctx context.Context,
	req *connect.Request[configv1.FetchMappedSplitMapRequest],
) (*connect.Response[configv1.FetchMappedSplitMapResponse], error) {
	if s.mappedSplit == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("mapped split map provider is not configured"))
	}

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

	typedMap, unchanged, err := s.mappedSplit.FetchMappedSplitMap(ctx, lane, req.Msg.LastVersion, req.Msg.LastChecksum)
	if err != nil {
		return nil, connectError(err)
	}

	resp := &configv1.FetchMappedSplitMapResponse{}
	if unchanged {
		resp.Result = &configv1.FetchMappedSplitMapResponse_Unchanged{Unchanged: &configv1.Unchanged{}}
	} else {
		resp.Result = &configv1.FetchMappedSplitMapResponse_Snapshot{Snapshot: typedMap}
	}
	return connect.NewResponse(resp), nil
}

// FetchMappedSplitBundle implements orange.config.v1.SnapshotService.FetchMappedSplitBundle.
func (s *SnapshotService) FetchMappedSplitBundle(
	ctx context.Context,
	req *connect.Request[configv1.FetchMappedSplitBundleRequest],
) (*connect.Response[configv1.FetchMappedSplitBundleResponse], error) {
	principal, err := s.auth.Authenticate(ctx, req.Header())
	if err != nil {
		return nil, connectError(err)
	}

	lane, err := s.lanes.ResolveLane(ctx, principal)
	if err != nil {
		return nil, connectError(err)
	}

	if req.Msg.Resource == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("mapped split bundle resource is required"))
	}
	if len(req.Msg.LastChecksum) != 0 && len(req.Msg.LastChecksum) != 32 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("last_checksum must be empty or exactly 32 bytes, got %d", len(req.Msg.LastChecksum)))
	}

	if s.bundles == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("mapped split bundle provider is not configured"))
	}

	envelope, unchanged, err := s.bundles.FetchResource(ctx, lane, req.Msg.Resource, req.Msg.LastVersion, req.Msg.LastChecksum)
	if err != nil {
		return nil, connectError(err)
	}

	resp := &configv1.FetchMappedSplitBundleResponse{}
	if unchanged {
		resp.Result = &configv1.FetchMappedSplitBundleResponse_Unchanged{Unchanged: &configv1.Unchanged{}}
	} else {
		resp.Result = &configv1.FetchMappedSplitBundleResponse_Snapshot{Snapshot: envelope}
	}
	return connect.NewResponse(resp), nil
}
