// Package store is the PostgreSQL system of record.
//
// It wraps the sqlc-generated queries in domain types that use ordinary Go
// values -- time.Time rather than pgtype.Timestamptz, string rather than
// *string -- so nothing above this package has to know how the rows are shaped.
package store

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/Rishabh-10/durable-job-queue/internal/store/dbgen"
)

// ErrNotFound is returned when a lookup matches no row.
var ErrNotFound = errors.New("job not found")

// State is the lifecycle position of a job.
type State string

// The job lifecycle. Succeeded, dead and cancelled are terminal; failed is
// transient bookkeeping between a failed attempt and the retry that follows.
const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateDead      State = "dead"
	StateCancelled State = "cancelled"
)

// Terminal reports whether a job in this state will never run again without
// explicit intervention.
func (s State) Terminal() bool {
	switch s {
	case StateSucceeded, StateDead, StateCancelled:
		return true
	default:
		return false
	}
}

// Valid reports whether s is a state the database will accept.
func (s State) Valid() bool {
	switch s {
	case StatePending, StateRunning, StateSucceeded, StateFailed, StateDead, StateCancelled:
		return true
	default:
		return false
	}
}

// Job is a unit of deferred work.
type Job struct {
	ID             uuid.UUID
	Queue          string
	Kind           string
	Payload        []byte
	State          State
	RunAt          time.Time
	Attempts       int32
	MaxAttempts    int32
	LeaseOwner     string
	LeaseExpiresAt time.Time
	IdempotencyKey string
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ExhaustedAfterThisAttempt reports whether the attempt currently in flight is
// the job's last. Attempts is incremented at claim time, so a job on its final
// attempt already has Attempts == MaxAttempts while it runs.
func (j Job) ExhaustedAfterThisAttempt() bool {
	return j.Attempts >= j.MaxAttempts
}

// Attempt is one execution of a job, kept after the job finishes so a dead job
// carries the evidence of why it died.
type Attempt struct {
	AttemptNo  int32
	WorkerID   string
	StartedAt  time.Time
	FinishedAt time.Time
	Error      string
}

// DueJob is the slim projection the sweeper scans: enough to rebuild the Redis
// index entry, and nothing else, so the scan stays cheap.
type DueJob struct {
	ID    uuid.UUID
	Queue string
	RunAt time.Time
}

// ExpiredLease identifies a job whose worker stopped heartbeating, along with
// the attempt budget needed to decide between retrying and dead-lettering.
type ExpiredLease struct {
	ID          uuid.UUID
	Queue       string
	Attempts    int32
	MaxAttempts int32
	LeaseOwner  string
}

// Exhausted reports whether this job has no attempts left.
func (e ExpiredLease) Exhausted() bool {
	return e.Attempts >= e.MaxAttempts
}

// QueueStats is a point-in-time summary of one queue.
type QueueStats struct {
	Queue         string
	CountsByState map[State]int64
	// OldestDueLag is how far behind the scheduler is: the age of the oldest
	// job that should have started and has not.
	OldestDueLag time.Duration
}

func toJob(r dbgen.Job) Job {
	return Job{
		ID:             r.ID,
		Queue:          r.Queue,
		Kind:           r.Kind,
		Payload:        r.Payload,
		State:          State(r.State),
		RunAt:          r.RunAt.Time,
		Attempts:       r.Attempts,
		MaxAttempts:    r.MaxAttempts,
		LeaseOwner:     deref(r.LeaseOwner),
		LeaseExpiresAt: r.LeaseExpiresAt.Time,
		IdempotencyKey: deref(r.IdempotencyKey),
		LastError:      deref(r.LastError),
		CreatedAt:      r.CreatedAt.Time,
		UpdatedAt:      r.UpdatedAt.Time,
	}
}

// toJobFromDeadLetter converts a row from the dead_letters view. The view
// selects jobs unchanged, so the columns line up one for one.
func toJobFromDeadLetter(r dbgen.DeadLetter) Job {
	return toJob(dbgen.Job(r))
}

func toAttempt(r dbgen.JobAttempt) Attempt {
	return Attempt{
		AttemptNo:  r.AttemptNo,
		WorkerID:   r.WorkerID,
		StartedAt:  r.StartedAt.Time,
		FinishedAt: r.FinishedAt.Time,
		Error:      deref(r.Error),
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ptr returns nil for the empty string so an unset value becomes SQL NULL
// rather than an empty string, keeping the partial idempotency index sparse.
func ptr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func timestamp(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t, Valid: true}
}

func interval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}

// states validates a filter before it reaches SQL. Without the check an unknown
// state would silently match nothing rather than telling the caller they asked
// for something that does not exist.
func states(in []State) ([]string, error) {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !s.Valid() {
			return nil, fmt.Errorf("unknown job state %q", s)
		}
		out = append(out, string(s))
	}
	return out, nil
}
