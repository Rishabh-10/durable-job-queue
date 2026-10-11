package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Rishabh-10/durable-job-queue/internal/config"
	"github.com/Rishabh-10/durable-job-queue/internal/index"
)

// Dispatcher moves job ids that have come due out of the scheduling sorted set
// and onto the ready list, where workers pick them up.
//
// It never touches PostgreSQL. Whether a job may actually run is decided by the
// claim, so the dispatcher's only job is to make due work visible quickly.
type Dispatcher struct {
	index *index.Index
	cfg   config.Dispatcher
	log   *slog.Logger

	now func() time.Time

	// onDispatched reports how many ids were moved, for metrics.
	onDispatched func(queue string, n int)
}

// NewDispatcher builds a dispatcher.
func NewDispatcher(idx *index.Index, cfg config.Dispatcher, log *slog.Logger) *Dispatcher {
	return &Dispatcher{
		index:        idx,
		cfg:          cfg,
		log:          log.With("component", "dispatcher"),
		now:          time.Now,
		onDispatched: func(string, int) {},
	}
}

// OnDispatched installs a callback invoked after each batch is moved.
func (d *Dispatcher) OnDispatched(fn func(queue string, n int)) { d.onDispatched = fn }

// Run polls until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	runLoop(ctx, "dispatcher", d.cfg.Interval, d.log, d.Dispatch)
}

// Dispatch performs one pass across every queue.
func (d *Dispatcher) Dispatch(ctx context.Context) error {
	queues, err := d.queues(ctx)
	if err != nil {
		return err
	}

	var firstErr error
	for _, queue := range queues {
		// One unhealthy queue must not stop the others from being served.
		if err := d.dispatchQueue(ctx, queue); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// dispatchQueue drains a queue's due jobs, in batches, until nothing is left.
//
// Draining rather than moving one batch per tick matters after an outage: a
// backlog of a million due jobs would otherwise take a million ticks to clear.
func (d *Dispatcher) dispatchQueue(ctx context.Context, queue string) error {
	for {
		ids, err := d.index.PopDue(ctx, queue, d.now(), d.cfg.BatchSize)
		if err != nil {
			return fmt.Errorf("dispatch %s: %w", queue, err)
		}
		if len(ids) == 0 {
			return nil
		}

		d.onDispatched(queue, len(ids))
		d.log.DebugContext(ctx, "dispatched due jobs", "queue", queue, "count", len(ids))

		// A partial batch means the queue is drained.
		if len(ids) < d.cfg.BatchSize {
			return nil
		}
		// Give shutdown a chance to interrupt a long drain.
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// queues returns the queues to poll: the configured list if there is one,
// otherwise whatever the index has seen.
func (d *Dispatcher) queues(ctx context.Context) ([]string, error) {
	if len(d.cfg.Queues) > 0 {
		return d.cfg.Queues, nil
	}
	queues, err := d.index.Queues(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover queues: %w", err)
	}
	return queues, nil
}
