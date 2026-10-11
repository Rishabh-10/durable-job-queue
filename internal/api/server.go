// Package api implements the gRPC surface of the job queue.
//
// Every write follows the same order: commit to PostgreSQL, then update the
// Redis index. The second step is best effort. If it fails the job is still
// durable and the sweeper will index it within one sweep interval, so an
// indexing failure is logged and the call still succeeds -- reporting an error
// would tell the client to retry an enqueue that actually worked.
package api

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	jobqueuev1 "github.com/Rishabh-10/durable-job-queue/gen/jobqueue/v1"
	"github.com/Rishabh-10/durable-job-queue/internal/config"
	"github.com/Rishabh-10/durable-job-queue/internal/index"
	"github.com/Rishabh-10/durable-job-queue/internal/store"
)

const (
	defaultPageSize = 50
	maxPageSize     = 500
	maxPayloadBytes = 1 << 20 // 1 MiB
	maxQueueLen     = 128
	maxKindLen      = 128
	maxIdemKeyLen   = 256
)

// Server implements the JobQueue service.
type Server struct {
	jobqueuev1.UnimplementedJobQueueServer

	store *store.Store
	index *index.Index
	retry config.Retry
	log   *slog.Logger

	// now is injectable so tests can control scheduling time.
	now func() time.Time

	// onEnqueued and onIndexFailure report to metrics.
	onEnqueued     func(queue string, deduplicated bool)
	onIndexFailure func(queue string)
}

// Option customises a Server.
type Option func(*Server)

// WithClock replaces the time source, for tests.
func WithClock(now func() time.Time) Option {
	return func(s *Server) { s.now = now }
}

// WithEnqueueObserver reports accepted enqueues, and enqueues that committed
// but could not reach the Redis index.
func WithEnqueueObserver(onEnqueued func(queue string, deduplicated bool), onIndexFailure func(queue string)) Option {
	return func(s *Server) {
		s.onEnqueued = onEnqueued
		s.onIndexFailure = onIndexFailure
	}
}

// NewServer builds the service.
func NewServer(st *store.Store, idx *index.Index, retry config.Retry, log *slog.Logger, opts ...Option) *Server {
	s := &Server{
		store: st,
		index: idx,
		retry: retry,
		log:   log,
		now:   time.Now,

		onEnqueued:     func(string, bool) {},
		onIndexFailure: func(string) {},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Enqueue schedules a job.
func (s *Server) Enqueue(ctx context.Context, req *jobqueuev1.EnqueueRequest) (*jobqueuev1.EnqueueResponse, error) {
	if err := validateEnqueue(req); err != nil {
		return nil, err
	}

	runAt := s.runAtFor(req)
	maxAttempts := req.GetMaxAttempts()
	if maxAttempts == 0 {
		maxAttempts = int32(s.retry.DefaultMaxAttempts)
	}

	job, created, err := s.store.CreateJob(ctx, store.CreateJobParams{
		Queue:          req.GetQueue(),
		Kind:           req.GetKind(),
		Payload:        req.GetPayload(),
		RunAt:          runAt,
		MaxAttempts:    maxAttempts,
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, s.internal(ctx, "create job", err)
	}

	s.onEnqueued(job.Queue, !created)

	// Only a newly created job needs indexing. Re-indexing on an idempotent
	// replay would move a job that may already be running.
	if created {
		s.indexJob(ctx, job)
	}

	return &jobqueuev1.EnqueueResponse{
		Job:            toProtoJob(job),
		AlreadyExisted: !created,
	}, nil
}

// indexJob adds a job to the Redis scheduling index.
//
// A failure here is survivable by design: the job is already committed, and the
// sweeper backfills anything the index is missing. Logging it at warn level
// makes the degradation visible without turning it into a client-facing error.
func (s *Server) indexJob(ctx context.Context, job store.Job) {
	err := s.index.Schedule(ctx, job.Queue, index.Entry{ID: job.ID, RunAt: job.RunAt})
	if err == nil {
		return
	}
	s.onIndexFailure(job.Queue)
	s.log.WarnContext(ctx, "could not index job; the sweeper will backfill it",
		"job_id", job.ID,
		"queue", job.Queue,
		"run_at", job.RunAt,
		"error", err,
	)
}

func (s *Server) runAtFor(req *jobqueuev1.EnqueueRequest) time.Time {
	now := s.now()
	switch sched := req.GetSchedule().(type) {
	case *jobqueuev1.EnqueueRequest_RunAt:
		return sched.RunAt.AsTime()
	case *jobqueuev1.EnqueueRequest_Delay:
		return now.Add(sched.Delay.AsDuration())
	default:
		// Neither field set means run as soon as possible.
		return now
	}
}

// GetJob returns a job with its attempt history.
func (s *Server) GetJob(ctx context.Context, req *jobqueuev1.GetJobRequest) (*jobqueuev1.GetJobResponse, error) {
	id, err := parseJobID(req.GetId())
	if err != nil {
		return nil, err
	}

	job, err := s.store.GetJob(ctx, id)
	if err != nil {
		return nil, s.jobError(ctx, "get job", id, err)
	}

	attempts, err := s.store.ListAttempts(ctx, id)
	if err != nil {
		return nil, s.internal(ctx, "list attempts", err)
	}

	return &jobqueuev1.GetJobResponse{
		Job:      toProtoJob(job),
		Attempts: toProtoAttempts(attempts),
	}, nil
}

// CancelJob stops a job that has not started yet.
func (s *Server) CancelJob(ctx context.Context, req *jobqueuev1.CancelJobRequest) (*jobqueuev1.CancelJobResponse, error) {
	id, err := parseJobID(req.GetId())
	if err != nil {
		return nil, err
	}

	job, err := s.store.CancelJob(ctx, id)
	if err != nil {
		return nil, s.jobError(ctx, "cancel job", id, err)
	}

	// Removing the id from the index saves the dispatcher and a worker some
	// wasted work. It is not what makes cancellation correct -- the claim
	// refuses a job that is no longer pending, so a copy already in flight in
	// the ready list is dropped when the worker tries to claim it.
	if err := s.index.Unschedule(ctx, job.Queue, job.ID); err != nil {
		s.log.WarnContext(ctx, "could not remove cancelled job from the index",
			"job_id", job.ID, "queue", job.Queue, "error", err)
	}

	return &jobqueuev1.CancelJobResponse{Job: toProtoJob(job)}, nil
}

// ListJobs pages through a queue.
func (s *Server) ListJobs(ctx context.Context, req *jobqueuev1.ListJobsRequest) (*jobqueuev1.ListJobsResponse, error) {
	if req.GetQueue() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}

	states, err := fromProtoStates(req.GetStates())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "states: %v", err)
	}
	cursor, err := decodePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "page_token: %v", err)
	}
	pageSize := clampPageSize(req.GetPageSize())

	jobs, err := s.store.ListJobs(ctx, store.ListJobsParams{
		Queue:    req.GetQueue(),
		States:   states,
		PageSize: pageSize,
		After:    cursor,
	})
	if err != nil {
		return nil, s.internal(ctx, "list jobs", err)
	}

	return &jobqueuev1.ListJobsResponse{
		Jobs:          toProtoJobs(jobs),
		NextPageToken: nextPageToken(jobs, pageSize),
	}, nil
}

// ListDeadLetters pages through jobs that exhausted their attempts.
func (s *Server) ListDeadLetters(ctx context.Context, req *jobqueuev1.ListDeadLettersRequest) (*jobqueuev1.ListDeadLettersResponse, error) {
	if req.GetQueue() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}

	cursor, err := decodePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "page_token: %v", err)
	}
	pageSize := clampPageSize(req.GetPageSize())

	jobs, err := s.store.ListDeadLetters(ctx, req.GetQueue(), pageSize, cursor)
	if err != nil {
		return nil, s.internal(ctx, "list dead letters", err)
	}

	return &jobqueuev1.ListDeadLettersResponse{
		Jobs:          toProtoJobs(jobs),
		NextPageToken: nextPageToken(jobs, pageSize),
	}, nil
}

// RequeueDeadLetter returns a dead job to the queue with additional attempts.
func (s *Server) RequeueDeadLetter(ctx context.Context, req *jobqueuev1.RequeueDeadLetterRequest) (*jobqueuev1.RequeueDeadLetterResponse, error) {
	id, err := parseJobID(req.GetId())
	if err != nil {
		return nil, err
	}
	if req.GetAdditionalAttempts() < 0 {
		return nil, status.Error(codes.InvalidArgument, "additional_attempts must not be negative")
	}
	delay := durationOrZero(req.GetDelay())
	if delay < 0 {
		return nil, status.Error(codes.InvalidArgument, "delay must not be negative")
	}

	additional := req.GetAdditionalAttempts()
	if additional == 0 {
		additional = int32(s.retry.DefaultMaxAttempts)
	}

	job, err := s.store.RequeueDeadLetter(ctx, id, additional, s.now().Add(delay))
	if err != nil {
		return nil, s.jobError(ctx, "requeue dead letter", id, err)
	}

	s.indexJob(ctx, job)

	return &jobqueuev1.RequeueDeadLetterResponse{Job: toProtoJob(job)}, nil
}

// QueueStats summarises a queue, combining PostgreSQL's per-state counts with
// the depth of the Redis index.
func (s *Server) QueueStats(ctx context.Context, req *jobqueuev1.QueueStatsRequest) (*jobqueuev1.QueueStatsResponse, error) {
	if req.GetQueue() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}

	stats, err := s.store.QueueStats(ctx, req.GetQueue())
	if err != nil {
		return nil, s.internal(ctx, "queue stats", err)
	}

	counts := make(map[string]int64, len(stats.CountsByState))
	for state, total := range stats.CountsByState {
		counts[string(state)] = total
	}

	resp := &jobqueuev1.QueueStatsResponse{
		Queue:         req.GetQueue(),
		CountsByState: counts,
		OldestDueLag:  durationpb.New(stats.OldestDueLag),
	}

	// Index depth is a nice-to-have. If Redis is unreachable the counts from
	// PostgreSQL are still worth returning, so report what is available rather
	// than failing the whole call.
	scheduled, ready, err := s.index.Depth(ctx, req.GetQueue())
	if err != nil {
		s.log.WarnContext(ctx, "could not read index depth",
			"queue", req.GetQueue(), "error", err)
	} else {
		resp.ScheduledIndexSize = scheduled
		resp.ReadyDepth = ready
	}

	return resp, nil
}

func validateEnqueue(req *jobqueuev1.EnqueueRequest) error {
	switch {
	case req.GetQueue() == "":
		return status.Error(codes.InvalidArgument, "queue is required")
	case len(req.GetQueue()) > maxQueueLen:
		return status.Errorf(codes.InvalidArgument, "queue must be at most %d bytes", maxQueueLen)
	case req.GetKind() == "":
		return status.Error(codes.InvalidArgument, "kind is required")
	case len(req.GetKind()) > maxKindLen:
		return status.Errorf(codes.InvalidArgument, "kind must be at most %d bytes", maxKindLen)
	case len(req.GetPayload()) > maxPayloadBytes:
		return status.Errorf(codes.InvalidArgument, "payload must be at most %d bytes", maxPayloadBytes)
	case len(req.GetIdempotencyKey()) > maxIdemKeyLen:
		return status.Errorf(codes.InvalidArgument, "idempotency_key must be at most %d bytes", maxIdemKeyLen)
	case req.GetMaxAttempts() < 0:
		return status.Error(codes.InvalidArgument, "max_attempts must not be negative")
	}

	if d := req.GetDelay(); d != nil && d.AsDuration() < 0 {
		return status.Error(codes.InvalidArgument, "delay must not be negative")
	}
	return nil
}

func parseJobID(raw string) (uuid.UUID, error) {
	if raw == "" {
		return uuid.Nil, status.Error(codes.InvalidArgument, "id is required")
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, status.Errorf(codes.InvalidArgument, "id %q is not a uuid", raw)
	}
	return id, nil
}

func clampPageSize(requested int32) int32 {
	switch {
	case requested <= 0:
		return defaultPageSize
	case requested > maxPageSize:
		return maxPageSize
	default:
		return requested
	}
}

// jobError maps store errors to gRPC codes, so a client can tell a missing job
// from one that is simply in the wrong state.
func (s *Server) jobError(ctx context.Context, op string, id uuid.UUID, err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return status.Errorf(codes.NotFound, "job %s not found", id)
	case errors.Is(err, store.ErrNotPending):
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	case errors.Is(err, store.ErrNotDead):
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	default:
		return s.internal(ctx, op, err)
	}
}

// internal logs the cause and returns a generic error. The detail stays in the
// logs rather than going over the wire, where it would leak schema and
// topology to callers.
func (s *Server) internal(ctx context.Context, op string, err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request cancelled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "request timed out")
	}
	s.log.ErrorContext(ctx, "request failed", "op", op, "error", err)
	return status.Errorf(codes.Internal, "%s failed", op)
}
