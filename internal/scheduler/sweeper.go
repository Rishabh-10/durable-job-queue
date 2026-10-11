package scheduler

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Rishabh-10/durable-job-queue/internal/config"
	"github.com/Rishabh-10/durable-job-queue/internal/index"
	"github.com/Rishabh-10/durable-job-queue/internal/store"
)

// Sweeper backfills the Redis scheduling index from PostgreSQL.
//
// This is the loop that makes the whole design safe. Enqueue commits to
// PostgreSQL and then adds to Redis, two writes with no transaction spanning
// them, so the second one can be lost: the process can die between them, Redis
// can be unreachable, or Redis can restart with an empty dataset. In every one
// of those cases the job is still pending in PostgreSQL, so the sweeper finds
// it here and re-indexes it.
//
// The cost of a lost index write is therefore bounded by the sweep interval,
// not by anything permanent. Re-adding a job that is already indexed is free --
// ZADD on an existing member just updates its score -- so the sweeper needs no
// record of what it has already done.
type Sweeper struct {
	store *store.Store
	index *index.Index
	cfg   config.Sweeper
	log   *slog.Logger

	// onBackfilled reports how many entries were written, for metrics. A
	// persistently non-zero rate means enqueues are failing to reach Redis.
	onBackfilled func(queue string, n int)
}

// NewSweeper builds a sweeper.
func NewSweeper(st *store.Store, idx *index.Index, cfg config.Sweeper, log *slog.Logger) *Sweeper {
	return &Sweeper{
		store:        st,
		index:        idx,
		cfg:          cfg,
		log:          log.With("component", "sweeper"),
		onBackfilled: func(string, int) {},
	}
}

// OnBackfilled installs a callback invoked after each queue is backfilled.
func (s *Sweeper) OnBackfilled(fn func(queue string, n int)) { s.onBackfilled = fn }

// Run sweeps until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) {
	runLoop(ctx, "sweeper", s.cfg.Interval, s.log, s.Sweep)
}

// Sweep performs one pass: find pending jobs that are due or nearly due, and
// make sure every one of them is in the index.
//
// The scan covers all queues at once and groups by queue afterwards, so a
// backlog on one queue cannot starve another of its place in the batch.
func (s *Sweeper) Sweep(ctx context.Context) error {
	due, err := s.store.ListDueForIndexing(ctx, "", s.cfg.Lookahead, int32(s.cfg.BatchSize))
	if err != nil {
		return fmt.Errorf("scan for jobs to index: %w", err)
	}
	if len(due) == 0 {
		return nil
	}

	byQueue := make(map[string][]index.Entry)
	for _, job := range due {
		byQueue[job.Queue] = append(byQueue[job.Queue], index.Entry{ID: job.ID, RunAt: job.RunAt})
	}

	var firstErr error
	for queue, entries := range byQueue {
		if err := s.index.ScheduleBatch(ctx, queue, entries); err != nil {
			s.log.ErrorContext(ctx, "could not backfill index",
				"queue", queue, "count", len(entries), "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		s.onBackfilled(queue, len(entries))
		s.log.DebugContext(ctx, "backfilled scheduling index",
			"queue", queue, "count", len(entries))
	}
	return firstErr
}
