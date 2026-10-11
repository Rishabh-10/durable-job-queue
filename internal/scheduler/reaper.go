package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Rishabh-10/durable-job-queue/internal/backoff"
	"github.com/Rishabh-10/durable-job-queue/internal/config"
	"github.com/Rishabh-10/durable-job-queue/internal/index"
	"github.com/Rishabh-10/durable-job-queue/internal/store"
)

// reclaimReason is written to the job's last_error so an operator reading the
// row can tell a handler failure from a worker that vanished.
const reclaimReason = "lease expired: worker stopped heartbeating"

// Reaper returns jobs whose worker died mid-execution to the queue.
//
// This is what makes delivery at-least-once. A worker holds a lease while it
// runs a job and heartbeats to extend it; if the process dies, nothing renews
// the lease, it lapses, and the reaper puts the job back. Without this loop a
// crashed worker would strand its job in the running state forever.
//
// A job that has already used its whole attempt budget is dead-lettered
// instead, so a payload that reliably kills workers stops rather than cycling.
type Reaper struct {
	store   *store.Store
	index   *index.Index
	cfg     config.Reaper
	backoff backoff.Policy
	log     *slog.Logger

	now func() time.Time

	// onReclaimed reports what was recovered, for metrics.
	onReclaimed func(queue string, dead bool)
}

// NewReaper builds a reaper.
func NewReaper(st *store.Store, idx *index.Index, cfg config.Reaper, policy backoff.Policy, log *slog.Logger) *Reaper {
	return &Reaper{
		store:       st,
		index:       idx,
		cfg:         cfg,
		backoff:     policy,
		log:         log.With("component", "reaper"),
		now:         time.Now,
		onReclaimed: func(string, bool) {},
	}
}

// OnReclaimed installs a callback invoked for each recovered job.
func (r *Reaper) OnReclaimed(fn func(queue string, dead bool)) { r.onReclaimed = fn }

// Run reaps until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) {
	runLoop(ctx, "reaper", r.cfg.Interval, r.log, r.Reap)
}

// Reap performs one pass over expired leases.
func (r *Reaper) Reap(ctx context.Context) error {
	reclaimed, err := r.store.ReclaimExpiredLeases(ctx, int32(r.cfg.BatchSize), reclaimReason,
		func(lease store.ExpiredLease) time.Time {
			// Back off even though the failure was a dead worker rather than a
			// failing handler: a payload that crashes its worker would
			// otherwise be retried in a tight loop, killing workers as fast as
			// they pick it up.
			return r.backoff.RetryAt(r.now(), lease.Attempts)
		})
	if err != nil {
		return fmt.Errorf("reclaim expired leases: %w", err)
	}
	if len(reclaimed) == 0 {
		return nil
	}

	for _, job := range reclaimed {
		dead := job.State == store.StateDead
		r.onReclaimed(job.Queue, dead)

		if dead {
			r.log.WarnContext(ctx, "dead-lettered a job whose worker died with no attempts left",
				"job_id", job.ID, "queue", job.Queue, "attempts", job.Attempts)
			continue
		}

		r.log.InfoContext(ctx, "recovered a job from a dead worker",
			"job_id", job.ID, "queue", job.Queue,
			"attempts", job.Attempts, "retry_at", job.RunAt)

		// Put the job back in the index so it runs at its new time rather than
		// waiting for the sweeper to notice. The sweeper would get there
		// eventually, which is why this is logged and not returned as an error.
		if err := r.index.Schedule(ctx, job.Queue, index.Entry{ID: job.ID, RunAt: job.RunAt}); err != nil {
			r.log.WarnContext(ctx, "could not re-index a recovered job; the sweeper will backfill it",
				"job_id", job.ID, "queue", job.Queue, "error", err)
		}
	}
	return nil
}
