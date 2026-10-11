// Package worker executes jobs.
//
// A worker pulls a job id from the Redis ready list, claims a lease on it in
// PostgreSQL, runs the registered handler, and records the outcome. Delivery is
// at-least-once: a worker can finish a job and die before its acknowledgement
// commits, in which case the reaper will hand the job to someone else. Handlers
// must therefore be idempotent.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Rishabh-10/durable-job-queue/internal/backoff"
	"github.com/Rishabh-10/durable-job-queue/internal/config"
	"github.com/Rishabh-10/durable-job-queue/internal/index"
	"github.com/Rishabh-10/durable-job-queue/internal/store"
)

// Handler runs one job. Returning nil marks the job succeeded; returning an
// error schedules a retry, unless the error is wrapped with Unretryable or the
// job has no attempts left.
//
// The context is cancelled when the job's timeout elapses, when the process is
// shutting down, or when the worker loses the job's lease. A handler that
// respects it stops work that nobody is waiting for any more.
type Handler func(ctx context.Context, job store.Job) error

// Registry maps job kinds to handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register binds a handler to a kind. Registering the same kind twice is a
// programming error and panics, because the second registration would silently
// take over jobs the first was written for.
func (r *Registry) Register(kind string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[kind]; exists {
		panic(fmt.Sprintf("worker: handler for kind %q is already registered", kind))
	}
	r.handlers[kind] = h
}

// Lookup returns the handler for a kind.
func (r *Registry) Lookup(kind string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	h, ok := r.handlers[kind]
	return h, ok
}

// Kinds lists the registered kinds.
func (r *Registry) Kinds() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	kinds := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		kinds = append(kinds, k)
	}
	return kinds
}

// errUnretryable marks a failure that retrying cannot fix.
type errUnretryable struct{ err error }

func (e errUnretryable) Error() string { return e.err.Error() }
func (e errUnretryable) Unwrap() error { return e.err }

// Unretryable wraps an error to send a job straight to the dead-letter queue.
//
// Use it for failures that are properties of the job rather than of the moment:
// a malformed payload, a record that no longer exists, a request the downstream
// will refuse every time. Retrying those burns the attempt budget to arrive at
// the same answer several minutes later.
func Unretryable(err error) error {
	if err == nil {
		return nil
	}
	return errUnretryable{err: err}
}

// IsUnretryable reports whether err was marked with Unretryable.
func IsUnretryable(err error) bool {
	var u errUnretryable
	return errors.As(err, &u)
}

// Outcome is how one execution ended, for metrics.
type Outcome string

const (
	OutcomeSucceeded  Outcome = "succeeded"
	OutcomeRetried    Outcome = "retried"
	OutcomeDead       Outcome = "dead"
	OutcomeLeaseLost  Outcome = "lease_lost"
	OutcomeNotClaimed Outcome = "not_claimed"
)

// Pool runs jobs concurrently.
type Pool struct {
	store    *store.Store
	index    *index.Index
	registry *Registry
	cfg      config.Worker
	backoff  backoff.Policy
	log      *slog.Logger
	id       string

	now func() time.Time

	// onFinished reports each execution's outcome, for metrics.
	onFinished func(queue, kind string, outcome Outcome, elapsed time.Duration)
	// onDispatchLag reports how late a job started relative to its run_at.
	onDispatchLag func(queue string, lag time.Duration)
}

// NewPool builds a worker pool.
func NewPool(
	st *store.Store,
	idx *index.Index,
	registry *Registry,
	cfg config.Worker,
	policy backoff.Policy,
	workerID string,
	log *slog.Logger,
) *Pool {
	return &Pool{
		store:         st,
		index:         idx,
		registry:      registry,
		cfg:           cfg,
		backoff:       policy,
		log:           log,
		id:            workerID,
		now:           time.Now,
		onFinished:    func(string, string, Outcome, time.Duration) {},
		onDispatchLag: func(string, time.Duration) {},
	}
}

// OnFinished installs a callback invoked after every execution.
func (p *Pool) OnFinished(fn func(queue, kind string, outcome Outcome, elapsed time.Duration)) {
	p.onFinished = fn
}

// OnDispatchLag installs a callback reporting how late each job started.
func (p *Pool) OnDispatchLag(fn func(queue string, lag time.Duration)) { p.onDispatchLag = fn }

// Run starts the pool and blocks until ctx is cancelled and every in-flight job
// has finished or the shutdown grace period elapses.
//
// Jobs still running when the deadline passes are simply abandoned: their
// leases lapse and the reaper hands them to another worker. That is the same
// path a crash takes, so shutdown needs no special recovery logic.
func (p *Pool) Run(ctx context.Context) {
	p.log.InfoContext(ctx, "worker pool started",
		"queues", p.cfg.Queues,
		"concurrency", p.cfg.Concurrency,
		"kinds", p.registry.Kinds(),
	)

	var wg sync.WaitGroup
	wg.Add(p.cfg.Concurrency)
	for n := range p.cfg.Concurrency {
		go func() {
			defer wg.Done()
			p.consume(ctx, n)
		}()
	}

	<-ctx.Done()
	p.log.InfoContext(ctx, "draining in-flight jobs", "grace_period", p.cfg.ShutdownTimeout)

	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		p.log.Info("worker pool stopped cleanly")
	case <-time.After(p.cfg.ShutdownTimeout):
		p.log.Warn("shutdown grace period elapsed; abandoning in-flight jobs to the reaper",
			"grace_period", p.cfg.ShutdownTimeout)
	}
}

// consume is one slot in the pool: poll for a job, run it, repeat.
func (p *Pool) consume(ctx context.Context, slot int) {
	log := p.log.With("slot", slot)

	for {
		if ctx.Err() != nil {
			return
		}

		queue, id, err := p.index.Next(ctx, p.cfg.Queues, p.cfg.PollTimeout)
		switch {
		case errors.Is(err, index.ErrNoJob):
			// An idle poll, which is the normal state of a quiet queue.
			continue
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return
		case err != nil:
			log.ErrorContext(ctx, "could not poll for work", "error", err)

			// Back off briefly so an unreachable Redis does not turn into a
			// tight error loop across every slot in the pool.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		p.execute(ctx, queue, id, log)
	}
}

// execute claims a job and runs it.
func (p *Pool) execute(ctx context.Context, queue string, id uuid.UUID, log *slog.Logger) {
	// Claiming uses a context detached from shutdown only for its own duration;
	// the job's execution context is derived below.
	job, claimed, err := p.store.ClaimJob(ctx, id, p.id, p.cfg.LeaseTTL)
	if err != nil {
		log.ErrorContext(ctx, "could not claim job", "job_id", id, "queue", queue, "error", err)
		return
	}
	if !claimed {
		// Another worker got there first, or the job was cancelled after being
		// dispatched. Either way it is not ours to run.
		log.DebugContext(ctx, "job was already claimed or is no longer runnable",
			"job_id", id, "queue", queue)
		p.onFinished(queue, "", OutcomeNotClaimed, 0)
		return
	}

	p.onDispatchLag(queue, p.now().Sub(job.RunAt))

	log = log.With("job_id", job.ID, "queue", job.Queue, "kind", job.Kind, "attempt", job.Attempts)
	started := p.now()

	outcome := p.runAndRecord(ctx, job, log)
	p.onFinished(job.Queue, job.Kind, outcome, p.now().Sub(started))
}

// runAndRecord runs the handler under a lease that is kept alive by heartbeats,
// then records the result.
func (p *Pool) runAndRecord(ctx context.Context, job store.Job, log *slog.Logger) Outcome {
	handler, ok := p.registry.Lookup(job.Kind)
	if !ok {
		// A job whose kind nothing handles will never succeed, however many
		// times it is retried, so it goes straight to the dead-letter queue
		// where an operator can see it.
		log.ErrorContext(ctx, "no handler registered for this kind")
		return p.recordFailure(ctx, job, fmt.Errorf("no handler registered for kind %q", job.Kind), true, log)
	}

	// The job's own context: independent of shutdown cancellation so a draining
	// pool lets handlers finish, but bounded by the job timeout.
	jobCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.JobTimeout)
	defer cancel()

	stopHeartbeat := p.heartbeat(jobCtx, cancel, job, log)
	defer stopHeartbeat()

	err := runHandler(jobCtx, handler, job)
	if err != nil {
		return p.recordFailure(ctx, job, err, IsUnretryable(err), log)
	}

	// Acknowledge on a fresh context: shutdown must not prevent a finished job
	// from being marked done, or it would be run a second time for nothing.
	ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer ackCancel()

	if _, err := p.store.CompleteJob(ackCtx, job.ID, p.id, job.Attempts); err != nil {
		if errors.Is(err, store.ErrLeaseLost) {
			// The reaper decided this worker was dead and gave the job away.
			// The work was done, but the new owner is the authority now.
			log.WarnContext(ctx, "lease was lost before the result could be recorded; the job will run again")
			return OutcomeLeaseLost
		}
		log.ErrorContext(ctx, "could not record success", "error", err)
		return OutcomeLeaseLost
	}

	log.InfoContext(ctx, "job succeeded")
	return OutcomeSucceeded
}

// runHandler isolates the handler call so a panic becomes an ordinary failed
// attempt instead of taking the whole worker process down with it.
func runHandler(ctx context.Context, handler Handler, job store.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return handler(ctx, job)
}

// recordFailure either schedules a retry or dead-letters the job.
func (p *Pool) recordFailure(ctx context.Context, job store.Job, cause error, unretryable bool, log *slog.Logger) Outcome {
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	exhausted := job.ExhaustedAfterThisAttempt()
	if unretryable || exhausted {
		reason := cause.Error()
		if _, err := p.store.DeadLetterJob(ackCtx, job.ID, p.id, job.Attempts, reason); err != nil {
			if errors.Is(err, store.ErrLeaseLost) {
				log.WarnContext(ctx, "lease was lost before the failure could be recorded")
				return OutcomeLeaseLost
			}
			log.ErrorContext(ctx, "could not dead-letter job", "error", err)
			return OutcomeLeaseLost
		}

		log.WarnContext(ctx, "job dead-lettered",
			"error", cause, "unretryable", unretryable, "attempts_used", job.Attempts)
		return OutcomeDead
	}

	retryAt := p.backoff.RetryAt(p.now(), job.Attempts)
	if _, err := p.store.RetryJob(ackCtx, job.ID, p.id, job.Attempts, cause.Error(), retryAt); err != nil {
		if errors.Is(err, store.ErrLeaseLost) {
			log.WarnContext(ctx, "lease was lost before the retry could be scheduled")
			return OutcomeLeaseLost
		}
		log.ErrorContext(ctx, "could not schedule retry", "error", err)
		return OutcomeLeaseLost
	}

	// Put the job back in the index at its new time. If this fails the job is
	// still pending in PostgreSQL, so the sweeper picks it up instead.
	if err := p.index.Schedule(ackCtx, job.Queue, index.Entry{ID: job.ID, RunAt: retryAt}); err != nil {
		log.WarnContext(ctx, "could not re-index a retrying job; the sweeper will backfill it", "error", err)
	}

	log.InfoContext(ctx, "job failed, retry scheduled",
		"error", cause, "retry_at", retryAt, "attempts_used", job.Attempts, "max_attempts", job.MaxAttempts)
	return OutcomeRetried
}

// heartbeat extends the job's lease while the handler runs, so a job that is
// slow is not mistaken for a job whose worker died.
//
// If the lease has been taken away, the job's context is cancelled: someone
// else owns the job now, and continuing would mean two workers doing the same
// work with only one of them able to record a result.
func (p *Pool) heartbeat(ctx context.Context, cancelJob context.CancelFunc, job store.Job, log *slog.Logger) (stop func()) {
	done := make(chan struct{})
	var once sync.Once

	go func() {
		ticker := time.NewTicker(p.cfg.HeartbeatInterval())
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				held, err := p.store.ExtendLease(ctx, job.ID, p.id, p.cfg.LeaseTTL)
				if err != nil {
					// A failed heartbeat is not yet fatal: the lease still has
					// time on it and the next tick may succeed.
					log.WarnContext(ctx, "could not extend lease", "error", err)
					continue
				}
				if !held {
					log.WarnContext(ctx, "lease was reclaimed while the job was running; cancelling it")
					cancelJob()
					return
				}
			}
		}
	}()

	return func() { once.Do(func() { close(done) }) }
}
