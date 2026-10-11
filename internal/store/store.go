package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Rishabh-10/durable-job-queue/internal/config"
	"github.com/Rishabh-10/durable-job-queue/internal/store/dbgen"
)

// Store is the PostgreSQL system of record.
type Store struct {
	pool *pgxpool.Pool
	q    *dbgen.Queries
}

// Open connects to PostgreSQL and verifies the connection before returning, so
// a misconfigured DSN fails at startup rather than on the first enqueue.
func Open(ctx context.Context, cfg config.Postgres) (*Store, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.ConnMaxLifetime

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return NewWithPool(pool), nil
}

// NewWithPool wraps an existing pool. Tests use it to share a container.
func NewWithPool(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, q: dbgen.New(pool)}
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

// Ping reports whether PostgreSQL is reachable, for the health check.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Pool exposes the underlying pool for migrations and tests.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// CreateJobParams describes a job to schedule.
type CreateJobParams struct {
	Queue          string
	Kind           string
	Payload        []byte
	RunAt          time.Time
	MaxAttempts    int32
	IdempotencyKey string
}

// CreateJob inserts a job and reports whether it was newly created.
//
// When an idempotency key collides, the insert writes nothing and the existing
// job is returned with created == false. A client that retries an Enqueue whose
// response it never saw therefore gets the original job back rather than a
// duplicate or an error.
func (s *Store) CreateJob(ctx context.Context, p CreateJobParams) (job Job, created bool, err error) {
	row, err := s.q.CreateJob(ctx, dbgen.CreateJobParams{
		ID:             uuid.New(),
		Queue:          p.Queue,
		Kind:           p.Kind,
		Payload:        p.Payload,
		RunAt:          timestamp(p.RunAt),
		MaxAttempts:    p.MaxAttempts,
		IdempotencyKey: ptr(p.IdempotencyKey),
	})
	if err == nil {
		return toJob(row), true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, fmt.Errorf("create job: %w", err)
	}

	// DO NOTHING fired, so an equal key already exists.
	existing, err := s.q.GetJobByIdempotencyKey(ctx, dbgen.GetJobByIdempotencyKeyParams{
		Queue:          p.Queue,
		IdempotencyKey: ptr(p.IdempotencyKey),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// The conflicting row disappeared between the two statements, which
			// nothing in this system does. Surface it rather than guess.
			return Job{}, false, fmt.Errorf("create job: conflict on %q but no row found", p.IdempotencyKey)
		}
		return Job{}, false, fmt.Errorf("look up existing job: %w", err)
	}
	return toJob(existing), false, nil
}

// GetJob returns a single job.
func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (Job, error) {
	row, err := s.q.GetJob(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrNotFound
		}
		return Job{}, fmt.Errorf("get job: %w", err)
	}
	return toJob(row), nil
}

// ListAttempts returns a job's execution history, oldest attempt first.
func (s *Store) ListAttempts(ctx context.Context, id uuid.UUID) ([]Attempt, error) {
	rows, err := s.q.ListAttempts(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	out := make([]Attempt, 0, len(rows))
	for _, r := range rows {
		out = append(out, toAttempt(r))
	}
	return out, nil
}

// CancelJob stops a job that has not started. It returns ErrNotFound if no such
// job exists, and ErrNotPending if the job has already started or finished.
func (s *Store) CancelJob(ctx context.Context, id uuid.UUID) (Job, error) {
	row, err := s.q.CancelJob(ctx, id)
	if err == nil {
		return toJob(row), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("cancel job: %w", err)
	}

	// The update matched nothing: either there is no such job, or it is no
	// longer pending. Distinguish the two so the API can answer precisely.
	current, getErr := s.GetJob(ctx, id)
	if getErr != nil {
		return Job{}, getErr
	}
	return current, fmt.Errorf("%w: job is %s", ErrNotPending, current.State)
}

// ErrNotPending is returned when an operation requires a job that has not yet
// started, and the job has already moved on.
var ErrNotPending = errors.New("job is not pending")

// ClaimJob takes an exclusive lease on a job and opens its attempt record.
//
// Both writes happen in one transaction: a lease without an attempt row would
// leave a running job with no history, and an attempt row without a lease would
// claim an execution that never started.
//
// claimed is false when the job was already taken, was cancelled after being
// dispatched, or is not yet due. That is an ordinary outcome, not an error --
// the caller simply drops the job.
func (s *Store) ClaimJob(ctx context.Context, id uuid.UUID, owner string, leaseTTL time.Duration) (job Job, claimed bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, false, fmt.Errorf("begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := s.q.WithTx(tx)

	row, err := qtx.ClaimJob(ctx, dbgen.ClaimJobParams{
		ID:         id,
		LeaseOwner: ptr(owner),
		LeaseTtl:   interval(leaseTTL),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, false, nil
		}
		return Job{}, false, fmt.Errorf("claim job: %w", err)
	}

	if _, err := qtx.RecordAttemptStart(ctx, dbgen.RecordAttemptStartParams{
		JobID:     row.ID,
		AttemptNo: row.Attempts,
		WorkerID:  owner,
	}); err != nil {
		return Job{}, false, fmt.Errorf("record attempt start: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, fmt.Errorf("commit claim: %w", err)
	}
	return toJob(row), true, nil
}

// ExtendLease pushes a running job's lease further out. It returns false when
// the lease is no longer held by owner, which means the reaper reclaimed the
// job and the caller must stop working on it.
func (s *Store) ExtendLease(ctx context.Context, id uuid.UUID, owner string, leaseTTL time.Duration) (bool, error) {
	_, err := s.q.ExtendLease(ctx, dbgen.ExtendLeaseParams{
		ID:         id,
		LeaseOwner: ptr(owner),
		LeaseTtl:   interval(leaseTTL),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("extend lease: %w", err)
	}
	return true, nil
}

// CompleteJob records a successful attempt and marks the job succeeded.
func (s *Store) CompleteJob(ctx context.Context, id uuid.UUID, owner string, attemptNo int32) (Job, error) {
	return s.finishAttempt(ctx, finishParams{
		id:        id,
		owner:     owner,
		attemptNo: attemptNo,
		apply: func(ctx context.Context, q *dbgen.Queries) (dbgen.Job, error) {
			return q.MarkJobSucceeded(ctx, dbgen.MarkJobSucceededParams{
				ID:         id,
				LeaseOwner: ptr(owner),
			})
		},
	})
}

// RetryJob records a failed attempt and returns the job to pending, due at
// runAt. The caller computes runAt so the backoff policy stays in one place.
func (s *Store) RetryJob(ctx context.Context, id uuid.UUID, owner string, attemptNo int32, cause string, runAt time.Time) (Job, error) {
	return s.finishAttempt(ctx, finishParams{
		id:        id,
		owner:     owner,
		attemptNo: attemptNo,
		cause:     cause,
		apply: func(ctx context.Context, q *dbgen.Queries) (dbgen.Job, error) {
			return q.MarkJobForRetry(ctx, dbgen.MarkJobForRetryParams{
				ID:         id,
				LeaseOwner: ptr(owner),
				RunAt:      timestamp(runAt),
				LastError:  ptr(cause),
			})
		},
	})
}

// DeadLetterJob records a final failed attempt and moves the job to the
// dead-letter queue.
func (s *Store) DeadLetterJob(ctx context.Context, id uuid.UUID, owner string, attemptNo int32, cause string) (Job, error) {
	return s.finishAttempt(ctx, finishParams{
		id:        id,
		owner:     owner,
		attemptNo: attemptNo,
		cause:     cause,
		apply: func(ctx context.Context, q *dbgen.Queries) (dbgen.Job, error) {
			return q.MarkJobDead(ctx, dbgen.MarkJobDeadParams{
				ID:         id,
				LeaseOwner: ptr(owner),
				LastError:  ptr(cause),
			})
		},
	})
}

type finishParams struct {
	id        uuid.UUID
	owner     string
	attemptNo int32
	cause     string
	apply     func(context.Context, *dbgen.Queries) (dbgen.Job, error)
}

// finishAttempt closes the attempt row and applies the job's new state in one
// transaction, so the history and the job can never disagree about how an
// execution ended.
//
// ErrLeaseLost means the lease moved on while the handler was running -- almost
// always because the reaper decided this worker was dead. The result is
// discarded rather than forced, because whoever holds the lease now is the
// authority on the job.
func (s *Store) finishAttempt(ctx context.Context, p finishParams) (Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, fmt.Errorf("begin finish: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := s.q.WithTx(tx)

	row, err := p.apply(ctx, qtx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrLeaseLost
		}
		return Job{}, fmt.Errorf("apply job outcome: %w", err)
	}

	if _, err := qtx.RecordAttemptFinish(ctx, dbgen.RecordAttemptFinishParams{
		JobID:     p.id,
		AttemptNo: p.attemptNo,
		Error:     ptr(p.cause),
	}); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("record attempt finish: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, fmt.Errorf("commit finish: %w", err)
	}
	return toJob(row), nil
}

// ErrLeaseLost means the job's lease is no longer held by the caller.
var ErrLeaseLost = errors.New("lease lost")

// RequeueDeadLetter returns a dead job to pending, granting it additional
// attempts on top of the ones it has already used.
//
// The budget is additive rather than reset so attempt numbering stays
// monotonic and the job's history remains append-only.
func (s *Store) RequeueDeadLetter(ctx context.Context, id uuid.UUID, additionalAttempts int32, runAt time.Time) (Job, error) {
	row, err := s.q.RequeueDeadLetter(ctx, dbgen.RequeueDeadLetterParams{
		ID:                 id,
		AdditionalAttempts: additionalAttempts,
		RunAt:              timestamp(runAt),
	})
	if err == nil {
		return toJob(row), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("requeue dead letter: %w", err)
	}

	current, getErr := s.GetJob(ctx, id)
	if getErr != nil {
		return Job{}, getErr
	}
	return Job{}, fmt.Errorf("%w: job is %s, not dead", ErrNotDead, current.State)
}

// ErrNotDead is returned when a requeue targets a job that is not dead-lettered.
var ErrNotDead = errors.New("job is not dead-lettered")

// ListDueForIndexing returns pending jobs that are due, or will be within the
// lookahead window. Passing an empty queue scans every queue.
//
// This is the sweeper's view of the world, and the reason a half-completed
// enqueue costs latency rather than a lost job.
func (s *Store) ListDueForIndexing(ctx context.Context, queue string, lookahead time.Duration, batchSize int32) ([]DueJob, error) {
	rows, err := s.q.ListDueForIndexing(ctx, dbgen.ListDueForIndexingParams{
		Queue:     queue,
		Lookahead: interval(lookahead),
		BatchSize: batchSize,
	})
	if err != nil {
		return nil, fmt.Errorf("list due for indexing: %w", err)
	}
	out := make([]DueJob, 0, len(rows))
	for _, r := range rows {
		out = append(out, DueJob{ID: r.ID, Queue: r.Queue, RunAt: r.RunAt.Time})
	}
	return out, nil
}

// ReclaimDecision is what the reaper decided to do with one expired lease.
type ReclaimDecision struct {
	Lease ExpiredLease
	// Dead is true when the job has no attempts left.
	Dead bool
	// RunAt is when the job should next run. Ignored when Dead.
	RunAt time.Time
}

// ReclaimExpiredLeases finds jobs whose workers stopped heartbeating and puts
// them back into circulation. decide computes the retry time for each, so the
// backoff policy lives with the rest of the retry logic rather than here.
//
// The scan and the updates share one transaction: the scan takes SKIP LOCKED
// row locks, so concurrent reaper replicas partition the work between
// themselves instead of fighting over it, and none of them has to be elected
// leader.
func (s *Store) ReclaimExpiredLeases(
	ctx context.Context,
	batchSize int32,
	reason string,
	decide func(ExpiredLease) time.Time,
) ([]Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin reclaim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := s.q.WithTx(tx)

	rows, err := qtx.ListExpiredLeases(ctx, batchSize)
	if err != nil {
		return nil, fmt.Errorf("list expired leases: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	reclaimed := make([]Job, 0, len(rows))
	for _, r := range rows {
		lease := ExpiredLease{
			ID:          r.ID,
			Queue:       r.Queue,
			Attempts:    r.Attempts,
			MaxAttempts: r.MaxAttempts,
			LeaseOwner:  deref(r.LeaseOwner),
		}
		dead := lease.Exhausted()

		var runAt time.Time
		if !dead {
			runAt = decide(lease)
		} else {
			// Unused by the query when dead, but the column is NOT NULL.
			runAt = time.Now()
		}

		updated, err := qtx.ReclaimExpiredLease(ctx, dbgen.ReclaimExpiredLeaseParams{
			ID:         lease.ID,
			LeaseOwner: ptr(lease.LeaseOwner),
			Dead:       dead,
			RunAt:      timestamp(runAt),
			Reason:     ptr(reason),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// The original worker acknowledged after the scan. Its result
				// stands; there is nothing to reclaim.
				continue
			}
			return nil, fmt.Errorf("reclaim lease for job %s: %w", lease.ID, err)
		}
		reclaimed = append(reclaimed, toJob(updated))
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit reclaim: %w", err)
	}
	return reclaimed, nil
}

// Cursor is an opaque position in a listing, used for keyset pagination.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// ListJobsParams filters and pages a job listing.
type ListJobsParams struct {
	Queue string
	// States is an optional filter; empty matches every state.
	States   []State
	PageSize int32
	// After positions the page. Nil starts from the newest job.
	After *Cursor
}

// ListJobs pages through a queue, newest first.
func (s *Store) ListJobs(ctx context.Context, p ListJobsParams) ([]Job, error) {
	filter, err := states(p.States)
	if err != nil {
		return nil, err
	}
	createdAt, id := cursorParams(p.After)

	rows, err := s.q.ListJobs(ctx, dbgen.ListJobsParams{
		Queue:           p.Queue,
		States:          filter,
		CursorCreatedAt: createdAt,
		CursorID:        id,
		PageSize:        p.PageSize,
	})
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	out := make([]Job, 0, len(rows))
	for _, r := range rows {
		out = append(out, toJob(r))
	}
	return out, nil
}

// ListDeadLetters pages through a queue's dead-lettered jobs, newest first.
func (s *Store) ListDeadLetters(ctx context.Context, queue string, pageSize int32, after *Cursor) ([]Job, error) {
	createdAt, id := cursorParams(after)

	rows, err := s.q.ListDeadLetters(ctx, dbgen.ListDeadLettersParams{
		Queue:           queue,
		CursorCreatedAt: createdAt,
		CursorID:        id,
		PageSize:        pageSize,
	})
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	out := make([]Job, 0, len(rows))
	for _, r := range rows {
		out = append(out, toJobFromDeadLetter(r))
	}
	return out, nil
}

func cursorParams(c *Cursor) (createdAt pgtype.Timestamptz, id *uuid.UUID) {
	if c == nil {
		return pgtype.Timestamptz{}, nil
	}
	cid := c.ID
	return timestamp(c.CreatedAt), &cid
}

// QueueStats summarises one queue.
func (s *Store) QueueStats(ctx context.Context, queue string) (QueueStats, error) {
	counts, err := s.q.CountJobsByState(ctx, queue)
	if err != nil {
		return QueueStats{}, fmt.Errorf("count jobs by state: %w", err)
	}
	lagSeconds, err := s.q.OldestDueJobAge(ctx, queue)
	if err != nil {
		return QueueStats{}, fmt.Errorf("oldest due job age: %w", err)
	}

	out := QueueStats{
		Queue:         queue,
		CountsByState: make(map[State]int64, len(counts)),
		OldestDueLag:  time.Duration(lagSeconds * float64(time.Second)),
	}
	for _, c := range counts {
		out.CountsByState[State(c.State)] = c.Total
	}
	return out, nil
}
