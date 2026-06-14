package config

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrBuildLeaseHeld is returned when another holder has a non-expired
	// mapped-split build lease for the lane.
	ErrBuildLeaseHeld = errors.New("mapped split build lease held")
	// ErrBuildLeaseLost is returned when a holder attempts to publish or clear
	// dirty state with a stale lease fencing token.
	ErrBuildLeaseLost = errors.New("mapped split build lease lost")
)

// BuildRequest is the coalesced request to rebuild one mapped-split lane.
type BuildRequest struct {
	Lane           string
	RequestedBy    string
	SourceRevision string
	ChangeHint     string
}

// Validate returns an error when req cannot be persisted.
func (r BuildRequest) Validate() error {
	if r.Lane == "" {
		return fmt.Errorf("build request lane is required")
	}
	return nil
}

// BuildLease is the acquired per-lane fencing token for one build attempt.
type BuildLease struct {
	Lane         string
	HolderID     string
	LeaseVersion int64
	LockedUntil  time.Time
}

// BuildCoordinator is the optional durable coordination surface for async and
// on-demand mapped-split builds.
type BuildCoordinator interface {
	MarkMappedSplitDirty(ctx context.Context, req BuildRequest) error
	GetMappedSplitBuildRequest(ctx context.Context, lane string) (*BuildRequest, error)
	ClearMappedSplitDirty(ctx context.Context, lease BuildLease, mapVersion uint64) error
	WithMappedSplitBuildLease(ctx context.Context, lane string, fn func(context.Context, BuildLease) error) error
}
