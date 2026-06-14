package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/dio/orange/mappedsplit"
)

const (
	DefaultPgQueMappedSplitQueue    = "orange_mapped_split_builds"
	DefaultPgQueMappedSplitConsumer = "orange_mapped_split_builder"
	PgQueMappedSplitBuildEventType  = "orange.mapped_split.build"
)

// PgQueSchedulerOption configures the optional PgQue mapped-split scheduler.
type PgQueSchedulerOption func(*PgQueScheduler)

// PgQueScheduler signals and processes mapped-split builds through PgQue.
// PgQue carries only wake-up events; PgStore remains authoritative for dirty
// state, leases, current maps, and component resources.
type PgQueScheduler struct {
	store                *PgStore
	builder              *mappedsplit.Builder
	build                OnDemandBuildFunc
	queue                string
	consumer             string
	maxMessages          int
	retryAfter           time.Duration
	pollInterval         time.Duration
	afterClearDirtyHook  func(context.Context, BuildLease, PublishResult) error
	resourceForComponent func(string) string
}

// NewPgQueScheduler creates a scheduler using store for authoritative state.
// Call config/pgque.Setup before using it.
func NewPgQueScheduler(store *PgStore, build OnDemandBuildFunc, opts ...PgQueSchedulerOption) (*PgQueScheduler, error) {
	if store == nil {
		return nil, fmt.Errorf("pgque scheduler: postgres store is required")
	}
	if build == nil {
		return nil, fmt.Errorf("pgque scheduler: build callback is required")
	}
	s := &PgQueScheduler{
		store:        store,
		build:        build,
		queue:        DefaultPgQueMappedSplitQueue,
		consumer:     DefaultPgQueMappedSplitConsumer,
		maxMessages:  math.MaxInt32,
		retryAfter:   time.Second,
		pollInterval: 100 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.queue == "" {
		return nil, fmt.Errorf("pgque scheduler: queue is required")
	}
	if s.consumer == "" {
		return nil, fmt.Errorf("pgque scheduler: consumer is required")
	}
	if s.maxMessages <= 0 {
		return nil, fmt.Errorf("pgque scheduler: max messages must be positive")
	}
	if s.retryAfter <= 0 {
		return nil, fmt.Errorf("pgque scheduler: retry after must be positive")
	}
	if s.pollInterval <= 0 {
		return nil, fmt.Errorf("pgque scheduler: poll interval must be positive")
	}
	if s.builder == nil {
		s.builder = mappedsplit.NewBuilder(mappedsplit.BuildOptions{
			Producer:             "orange-pgque-scheduler",
			ResourceForComponent: s.resourceForComponent,
		})
	}
	return s, nil
}

// WithPgQueSchedulerQueue sets the PgQue queue.
func WithPgQueSchedulerQueue(queue string) PgQueSchedulerOption {
	return func(s *PgQueScheduler) {
		s.queue = queue
	}
}

// WithPgQueSchedulerConsumer sets the logical PgQue consumer.
func WithPgQueSchedulerConsumer(consumer string) PgQueSchedulerOption {
	return func(s *PgQueScheduler) {
		s.consumer = consumer
	}
}

// WithPgQueSchedulerMaxMessages sets the maximum messages read per poll. PgQue
// acks at the batch level, so this should normally be high enough to drain a
// full batch; otherwise messages beyond the cap can be skipped when the batch
// is acked.
func WithPgQueSchedulerMaxMessages(maxMessages int) PgQueSchedulerOption {
	return func(s *PgQueScheduler) {
		s.maxMessages = maxMessages
	}
}

// WithPgQueSchedulerRetryAfter sets the PgQue retry delay for transient
// worker errors.
func WithPgQueSchedulerRetryAfter(retryAfter time.Duration) PgQueSchedulerOption {
	return func(s *PgQueScheduler) {
		s.retryAfter = retryAfter
	}
}

// WithPgQueSchedulerPollInterval sets the sleep between empty polls in Run.
func WithPgQueSchedulerPollInterval(interval time.Duration) PgQueSchedulerOption {
	return func(s *PgQueScheduler) {
		s.pollInterval = interval
	}
}

// WithPgQueSchedulerResourceForComponent configures component resource naming
// for build outputs.
func WithPgQueSchedulerResourceForComponent(fn func(string) string) PgQueSchedulerOption {
	return func(s *PgQueScheduler) {
		s.resourceForComponent = fn
	}
}

// ScheduleBuild marks the lane dirty and enqueues one PgQue build signal in a
// single Postgres transaction.
func (s *PgQueScheduler) ScheduleBuild(ctx context.Context, req BuildRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(pgQueBuildPayload{
		Lane:           req.Lane,
		RequestedBy:    req.RequestedBy,
		SourceRevision: req.SourceRevision,
		ChangeHint:     req.ChangeHint,
	})
	if err != nil {
		return fmt.Errorf("pgque scheduler: marshal build payload: %w", err)
	}

	tx, err := s.store.beginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := markMappedSplitDirtyTx(ctx, tx, req); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT pgque.send($1, $2, $3::jsonb)", s.queue, PgQueMappedSplitBuildEventType, string(payload)); err != nil {
		return fmt.Errorf("pgque scheduler: send build event lane %q: %w", req.Lane, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgque scheduler: schedule build lane %q commit: %w", req.Lane, err)
	}
	return nil
}

// Run polls PgQue until ctx is canceled.
func (s *PgQueScheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		processed, err := s.ProcessOnce(ctx)
		if err != nil {
			return err
		}
		if processed > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// ProcessOnce ticks the queue, receives one batch, and processes all returned
// mapped-split build events.
func (s *PgQueScheduler) ProcessOnce(ctx context.Context) (int, error) {
	if _, err := s.store.pool.Exec(ctx, "SELECT pgque.force_next_tick($1)", s.queue); err != nil {
		return 0, fmt.Errorf("pgque scheduler: force tick queue %q: %w", s.queue, err)
	}
	if _, err := s.store.pool.Exec(ctx, "SELECT pgque.ticker()"); err != nil {
		return 0, fmt.Errorf("pgque scheduler: tick queues: %w", err)
	}
	msgs, err := s.receive(ctx)
	if err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}
	for _, msg := range msgs {
		if err := s.processMessage(ctx, msg); err != nil {
			if errors.Is(err, ErrBuildLeaseHeld) {
				continue
			}
			if nackErr := s.nack(ctx, msg, err); nackErr != nil {
				return 0, errors.Join(err, nackErr)
			}
			return len(msgs), nil
		}
	}
	if err := s.ack(ctx, msgs[0].BatchID); err != nil {
		return 0, err
	}
	return len(msgs), nil
}

func (s *PgQueScheduler) receive(ctx context.Context) ([]pgQueMessage, error) {
	rows, err := s.store.pool.Query(ctx, "SELECT * FROM pgque.receive($1, $2, $3)", s.queue, s.consumer, s.maxMessages)
	if err != nil {
		return nil, fmt.Errorf("pgque scheduler: receive: %w", err)
	}
	defer rows.Close()

	var msgs []pgQueMessage
	for rows.Next() {
		var msg pgQueMessage
		if err := rows.Scan(
			&msg.MsgID,
			&msg.BatchID,
			&msg.Type,
			&msg.Payload,
			&msg.RetryCount,
			&msg.CreatedAt,
			&msg.Extra1,
			&msg.Extra2,
			&msg.Extra3,
			&msg.Extra4,
		); err != nil {
			return nil, fmt.Errorf("pgque scheduler: scan message: %w", err)
		}
		msgs = append(msgs, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgque scheduler: receive rows: %w", err)
	}
	return msgs, nil
}

func (s *PgQueScheduler) processMessage(ctx context.Context, msg pgQueMessage) error {
	if msg.Type != PgQueMappedSplitBuildEventType {
		return nil
	}
	var payload pgQueBuildPayload
	if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
		return fmt.Errorf("pgque scheduler: decode build payload: %w", err)
	}
	if payload.Lane == "" {
		return fmt.Errorf("pgque scheduler: build payload lane is required")
	}
	if err := s.store.WithMappedSplitBuildLease(ctx, payload.Lane, func(ctx context.Context, lease BuildLease) error {
		req, err := s.store.GetMappedSplitBuildRequest(ctx, payload.Lane)
		if err != nil {
			return err
		}
		if req == nil {
			return nil
		}
		mappedReq, err := s.build(ctx, *req)
		if err != nil {
			return err
		}
		if mappedReq.Lane == "" {
			mappedReq.Lane = req.Lane
		}
		if mappedReq.Lane != req.Lane {
			return fmt.Errorf("pgque scheduler: build callback returned lane %q for requested lane %q", mappedReq.Lane, req.Lane)
		}
		out, err := s.builder.Build(ctx, mappedsplit.BuildRequest(mappedReq))
		if err != nil {
			return err
		}
		result, err := s.store.PublishMappedSplitWithLease(ctx, lease, out)
		if err != nil {
			return err
		}
		if result.Map == nil {
			return nil
		}
		if err := s.store.ClearMappedSplitDirty(ctx, lease, result.Map.Version); err != nil {
			return err
		}
		if s.afterClearDirtyHook != nil {
			return s.afterClearDirtyHook(ctx, lease, result)
		}
		return nil
	}); err != nil {
		if errors.Is(err, ErrBuildLeaseHeld) {
			return nil
		}
		return err
	}
	return nil
}

func (s *PgQueScheduler) ack(ctx context.Context, batchID int64) error {
	if _, err := s.store.pool.Exec(ctx, "SELECT pgque.ack($1)", batchID); err != nil {
		return fmt.Errorf("pgque scheduler: ack batch %d: %w", batchID, err)
	}
	return nil
}

func (s *PgQueScheduler) nack(ctx context.Context, msg pgQueMessage, cause error) error {
	_ = cause
	reason := "mapped split build failed"
	if _, err := s.store.pool.Exec(ctx, `
		SELECT pgque.nack(
			$1,
			ROW($2,$3,$4,$5,$6,$7,$8,$9,$10,$11)::pgque.message,
			$12::interval,
			$13
		)
	`, msg.BatchID, msg.MsgID, msg.BatchID, msg.Type, msg.Payload, msg.RetryCount, msg.CreatedAt, msg.Extra1, msg.Extra2, msg.Extra3, msg.Extra4, pgInterval(s.retryAfter), reason); err != nil {
		return fmt.Errorf("pgque scheduler: nack message %d: %w", msg.MsgID, err)
	}
	return s.ack(ctx, msg.BatchID)
}

type pgQueBuildPayload struct {
	Lane           string `json:"lane"`
	RequestedBy    string `json:"requested_by,omitempty"`
	SourceRevision string `json:"source_revision,omitempty"`
	ChangeHint     string `json:"change_hint,omitempty"`
}

type pgQueMessage struct {
	MsgID      int64
	BatchID    int64
	Type       string
	Payload    string
	RetryCount *int32
	CreatedAt  time.Time
	Extra1     *string
	Extra2     *string
	Extra3     *string
	Extra4     *string
}
