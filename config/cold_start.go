package config

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	configv1 "github.com/dio/orange/api/orange/config/v1"
	"github.com/dio/orange/mappedsplit"
	"github.com/dio/orange/snapshot"
)

// OnDemandBuildFunc prepares a complete mapped-split publication request for a
// lane whose current map is missing.
type OnDemandBuildFunc func(ctx context.Context, req BuildRequest) (MappedSplitRequest, error)

type mappedSplitLeasePublisher interface {
	PublishMappedSplitWithLease(ctx context.Context, lease BuildLease, publication MappedSplitPublication) (PublishResult, error)
}

type coldStartMappedSplitProvider struct {
	store       Store
	coordinator BuildCoordinator
	builder     *mappedsplit.Builder
	build       OnDemandBuildFunc
}

func (p *coldStartMappedSplitProvider) FetchMappedSplitMap(
	ctx context.Context,
	lane string,
	lastVersion uint64,
	lastChecksum []byte,
) (*configv1.MappedSplitSnapshot, bool, error) {
	ctx, span := startConfigOperationSpan(ctx, "orange.config.coldStartMappedSplitProvider.FetchMappedSplitMap",
		attribute.String("orange.lane", lane),
		attribute.Int64("orange.last_version", int64(lastVersion)),
		attribute.Bool("orange.last_checksum_present", len(lastChecksum) != 0),
	)
	start := time.Now()
	resultLabel := "hit"
	var spanErr error
	defer func() {
		recordConfigOperation(ctx, "cold_start.fetch_mapped_split_map", resultLabel, start)
		finishConfigOperationSpan(span, resultLabel, spanErr)
	}()

	typedMap, unchanged, err := p.store.FetchMappedSplitMap(ctx, lane, lastVersion, lastChecksum)
	if err == nil || !errors.Is(err, snapshot.ErrNoSnapshot) {
		if err != nil {
			resultLabel = "error"
			captureSpanError(&spanErr, err)
		} else if unchanged {
			resultLabel = "unchanged"
		}
		return typedMap, unchanged, err
	}
	missingErr := err

	if p.build == nil || p.coordinator == nil {
		resultLabel = "not_found"
		captureSpanError(&spanErr, missingErr)
		return nil, false, missingErr
	}

	built := false
	if err := p.coordinator.WithMappedSplitBuildLease(ctx, lane, func(ctx context.Context, lease BuildLease) error {
		current, _, err := p.store.FetchMappedSplitMap(ctx, lane, 0, nil)
		if err == nil && current != nil {
			return nil
		}
		if err != nil && !errors.Is(err, snapshot.ErrNoSnapshot) {
			return err
		}

		buildStart := time.Now()
		buildResult := "success"
		buildCtx, buildSpan := startConfigOperationSpan(ctx, "orange.config.coldStartMappedSplitProvider.build",
			attribute.String("orange.lane", lane),
		)
		var buildSpanErr error
		defer func() {
			recordConfigOperation(buildCtx, "cold_start.build", buildResult, buildStart)
			finishConfigOperationSpan(buildSpan, buildResult, buildSpanErr)
		}()

		req, err := p.build(buildCtx, BuildRequest{Lane: lane})
		if err != nil {
			buildResult = "error"
			captureSpanError(&buildSpanErr, err)
			return err
		}
		if req.Lane == "" {
			req.Lane = lane
		}
		if req.Lane != lane {
			buildResult = "error"
			err := fmt.Errorf("on-demand build lane %q does not match requested lane %q", req.Lane, lane)
			captureSpanError(&buildSpanErr, err)
			return err
		}

		out, err := p.builder.Build(buildCtx, mappedsplit.BuildRequest(req))
		if err != nil {
			buildResult = "error"
			captureSpanError(&buildSpanErr, err)
			return err
		}

		result, err := p.publish(buildCtx, lease, out)
		if err != nil {
			buildResult = "error"
			captureSpanError(&buildSpanErr, err)
			return err
		}
		if result.Map != nil {
			built = true
			if err := p.coordinator.ClearMappedSplitDirty(buildCtx, lease, result.Map.Version); err != nil {
				buildResult = "error"
				captureSpanError(&buildSpanErr, err)
				return err
			}
			return nil
		}
		buildResult = "empty"
		return nil
	}); err != nil {
		if errors.Is(err, ErrBuildLeaseHeld) {
			resultLabel = "lease_held"
			typedMap, unchanged, fetchErr := p.store.FetchMappedSplitMap(ctx, lane, lastVersion, lastChecksum)
			if fetchErr == nil {
				return typedMap, unchanged, nil
			}
			if errors.Is(fetchErr, snapshot.ErrNoSnapshot) {
				captureSpanError(&spanErr, missingErr)
				return nil, false, missingErr
			}
			captureSpanError(&spanErr, fetchErr)
			return nil, false, fetchErr
		}
		resultLabel = storeErrorResult(err)
		captureSpanError(&spanErr, err)
		return nil, false, err
	}

	typedMap, unchanged, err = p.store.FetchMappedSplitMap(ctx, lane, lastVersion, lastChecksum)
	switch {
	case err != nil:
		resultLabel = "error"
		captureSpanError(&spanErr, err)
	case unchanged:
		resultLabel = "unchanged"
	case built:
		resultLabel = "built"
	default:
		resultLabel = "filled_by_other"
	}
	return typedMap, unchanged, err
}

func (p *coldStartMappedSplitProvider) publish(ctx context.Context, lease BuildLease, out MappedSplitPublication) (PublishResult, error) {
	if publisher, ok := p.store.(mappedSplitLeasePublisher); ok {
		return publisher.PublishMappedSplitWithLease(ctx, lease, out)
	}
	return p.store.PublishMappedSplit(ctx, out)
}
