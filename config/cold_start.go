package config

import (
	"context"
	"errors"
	"fmt"

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
	typedMap, unchanged, err := p.store.FetchMappedSplitMap(ctx, lane, lastVersion, lastChecksum)
	if err == nil || !errors.Is(err, snapshot.ErrNoSnapshot) {
		return typedMap, unchanged, err
	}
	missingErr := err

	if p.build == nil || p.coordinator == nil {
		return nil, false, missingErr
	}

	if err := p.coordinator.WithMappedSplitBuildLease(ctx, lane, func(ctx context.Context, lease BuildLease) error {
		current, _, err := p.store.FetchMappedSplitMap(ctx, lane, 0, nil)
		if err == nil && current != nil {
			return nil
		}
		if err != nil && !errors.Is(err, snapshot.ErrNoSnapshot) {
			return err
		}

		req, err := p.build(ctx, BuildRequest{Lane: lane})
		if err != nil {
			return err
		}
		if req.Lane == "" {
			req.Lane = lane
		}
		if req.Lane != lane {
			return fmt.Errorf("on-demand build lane %q does not match requested lane %q", req.Lane, lane)
		}

		out, err := p.builder.Build(ctx, mappedsplit.BuildRequest(req))
		if err != nil {
			return err
		}

		result, err := p.publish(ctx, lease, out)
		if err != nil {
			return err
		}
		if result.Map != nil {
			return p.coordinator.ClearMappedSplitDirty(ctx, lease, result.Map.Version)
		}
		return nil
	}); err != nil {
		if errors.Is(err, ErrBuildLeaseHeld) {
			typedMap, unchanged, fetchErr := p.store.FetchMappedSplitMap(ctx, lane, lastVersion, lastChecksum)
			if fetchErr == nil {
				return typedMap, unchanged, nil
			}
			if errors.Is(fetchErr, snapshot.ErrNoSnapshot) {
				return nil, false, missingErr
			}
			return nil, false, fetchErr
		}
		return nil, false, err
	}

	return p.store.FetchMappedSplitMap(ctx, lane, lastVersion, lastChecksum)
}

func (p *coldStartMappedSplitProvider) publish(ctx context.Context, lease BuildLease, out MappedSplitPublication) (PublishResult, error) {
	if publisher, ok := p.store.(mappedSplitLeasePublisher); ok {
		return publisher.PublishMappedSplitWithLease(ctx, lease, out)
	}
	return p.store.PublishMappedSplit(ctx, out)
}
