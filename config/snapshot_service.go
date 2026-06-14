package config

import (
	"context"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/internal/otelx"
)

var configTracer = otelx.Tracer("config")

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
	otelx.AutoConfigureFromEnv()

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
	start := time.Now()
	resultLabel := "snapshot"
	defer func() {
		recordConfigOperation(ctx, "server.fetch_mapped_split_map", resultLabel, start)
	}()

	ctx, span := configTracer.Start(ctx, "orange.config.SnapshotService.FetchMappedSplitMap",
		trace.WithAttributes(
			attribute.Int64("orange.last_version", int64(req.Msg.LastVersion)),
			attribute.Bool("orange.last_checksum_present", len(req.Msg.LastChecksum) != 0),
		),
	)
	defer span.End()

	if s.mappedSplit == nil {
		resultLabel = "error"
		err := connect.NewError(connect.CodeUnimplemented, fmt.Errorf("mapped split map provider is not configured"))
		otelx.RecordError(span, err)
		return nil, err
	}

	principal, err := s.auth.Authenticate(ctx, req.Header())
	if err != nil {
		resultLabel = "error"
		otelx.RecordError(span, err)
		return nil, connectError(err)
	}

	lane, err := s.lanes.ResolveLane(ctx, principal)
	if err != nil {
		resultLabel = "error"
		otelx.RecordError(span, err)
		return nil, connectError(err)
	}
	span.SetAttributes(attribute.String("orange.lane", lane))

	if len(req.Msg.LastChecksum) != 0 && len(req.Msg.LastChecksum) != 32 {
		resultLabel = "error"
		err := connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("last_checksum must be empty or exactly 32 bytes, got %d", len(req.Msg.LastChecksum)))
		otelx.RecordError(span, err)
		return nil, err
	}

	typedMap, unchanged, err := s.mappedSplit.FetchMappedSplitMap(ctx, lane, req.Msg.LastVersion, req.Msg.LastChecksum)
	if err != nil {
		resultLabel = "error"
		otelx.RecordError(span, err)
		return nil, connectError(err)
	}

	resp := &configv1.FetchMappedSplitMapResponse{}
	if unchanged {
		resultLabel = "unchanged"
		span.SetAttributes(attribute.String("orange.result", "unchanged"))
		resp.Result = &configv1.FetchMappedSplitMapResponse_Unchanged{Unchanged: &configv1.Unchanged{}}
	} else {
		span.SetAttributes(
			attribute.String("orange.result", "snapshot"),
			attribute.Int64("orange.map_version", int64(typedMap.GetVersion())),
		)
		resp.Result = &configv1.FetchMappedSplitMapResponse_Snapshot{Snapshot: typedMap}
	}
	return connect.NewResponse(resp), nil
}

// FetchMappedSplitBundle implements orange.config.v1.SnapshotService.FetchMappedSplitBundle.
func (s *SnapshotService) FetchMappedSplitBundle(
	ctx context.Context,
	req *connect.Request[configv1.FetchMappedSplitBundleRequest],
) (*connect.Response[configv1.FetchMappedSplitBundleResponse], error) {
	start := time.Now()
	resultLabel := "snapshot"
	defer func() {
		recordConfigOperation(ctx, "server.fetch_mapped_split_bundle", resultLabel, start)
	}()

	ctx, span := configTracer.Start(ctx, "orange.config.SnapshotService.FetchMappedSplitBundle",
		trace.WithAttributes(
			attribute.String("orange.resource", req.Msg.Resource),
			attribute.Int64("orange.last_version", int64(req.Msg.LastVersion)),
			attribute.Bool("orange.last_checksum_present", len(req.Msg.LastChecksum) != 0),
		),
	)
	defer span.End()

	principal, err := s.auth.Authenticate(ctx, req.Header())
	if err != nil {
		resultLabel = "error"
		otelx.RecordError(span, err)
		return nil, connectError(err)
	}

	lane, err := s.lanes.ResolveLane(ctx, principal)
	if err != nil {
		resultLabel = "error"
		otelx.RecordError(span, err)
		return nil, connectError(err)
	}
	span.SetAttributes(attribute.String("orange.lane", lane))

	if req.Msg.Resource == "" {
		resultLabel = "error"
		err := connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("mapped split bundle resource is required"))
		otelx.RecordError(span, err)
		return nil, err
	}
	if len(req.Msg.LastChecksum) != 0 && len(req.Msg.LastChecksum) != 32 {
		resultLabel = "error"
		err := connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("last_checksum must be empty or exactly 32 bytes, got %d", len(req.Msg.LastChecksum)))
		otelx.RecordError(span, err)
		return nil, err
	}

	if s.bundles == nil {
		resultLabel = "error"
		err := connect.NewError(connect.CodeUnimplemented, fmt.Errorf("mapped split bundle provider is not configured"))
		otelx.RecordError(span, err)
		return nil, err
	}

	envelope, unchanged, err := s.bundles.FetchResource(ctx, lane, req.Msg.Resource, req.Msg.LastVersion, req.Msg.LastChecksum)
	if err != nil {
		resultLabel = "error"
		otelx.RecordError(span, err)
		return nil, connectError(err)
	}

	resp := &configv1.FetchMappedSplitBundleResponse{}
	if unchanged {
		resultLabel = "unchanged"
		span.SetAttributes(attribute.String("orange.result", "unchanged"))
		resp.Result = &configv1.FetchMappedSplitBundleResponse_Unchanged{Unchanged: &configv1.Unchanged{}}
	} else {
		span.SetAttributes(
			attribute.String("orange.result", "snapshot"),
			attribute.Int64("orange.resource_version", int64(envelope.GetVersion())),
		)
		resp.Result = &configv1.FetchMappedSplitBundleResponse_Snapshot{Snapshot: envelope}
	}
	return connect.NewResponse(resp), nil
}
