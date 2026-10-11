//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/durationpb"

	jobqueuev1 "github.com/Rishabh-10/durable-job-queue/gen/jobqueue/v1"
	"github.com/Rishabh-10/durable-job-queue/internal/api"
	"github.com/Rishabh-10/durable-job-queue/internal/index"
	"github.com/Rishabh-10/durable-job-queue/internal/store"
	"github.com/Rishabh-10/durable-job-queue/internal/worker"
)

// The headline claim: a job runs end to end, at roughly the time it was asked
// to run.
func TestJobRunsAfterItsDelay(t *testing.T) {
	h := newHarness(t)

	ran := make(chan time.Time, 1)
	h.Registry.Register("noop", func(context.Context, store.Job) error {
		ran <- time.Now()
		return nil
	})
	h.Start()

	const delay = 750 * time.Millisecond
	enqueuedAt := time.Now()
	job := h.Enqueue(&jobqueuev1.EnqueueRequest{
		Queue:    "default",
		Kind:     "noop",
		Payload:  []byte("{}"),
		Schedule: &jobqueuev1.EnqueueRequest_Delay{Delay: durationpb.New(delay)},
	})

	select {
	case at := <-ran:
		elapsed := at.Sub(enqueuedAt)
		if elapsed < delay {
			t.Errorf("job ran %s after enqueue, before its %s delay had elapsed", elapsed, delay)
		}
		if elapsed > delay+3*time.Second {
			t.Errorf("job ran %s after enqueue, far later than its %s delay", elapsed, delay)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("job never ran")
	}

	waitFor(t, 10*time.Second, "the job to be marked succeeded", func() bool {
		return h.GetJob(job.GetId()).GetJob().GetState() == jobqueuev1.JobState_JOB_STATE_SUCCEEDED
	})
}

// Enqueue is two writes across two systems with no transaction spanning them:
// commit to PostgreSQL, then add to Redis. This test kills the second write
// outright and asserts the job still runs, which is the entire justification
// for the sweeper.
func TestHalfCompletedEnqueueIsRecovered(t *testing.T) {
	h := newHarness(t)

	ran := make(chan struct{}, 1)
	h.Registry.Register("noop", func(context.Context, store.Job) error {
		ran <- struct{}{}
		return nil
	})

	// An API server whose index writes always fail. Nothing else changes: the
	// job still commits to PostgreSQL exactly as it normally would.
	brokenIndex := index.New(redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1", // nothing listens here
		DialTimeout: 100 * time.Millisecond,
	}))
	defer func() { _ = brokenIndex.Close() }()

	failing := api.NewServer(h.Store, brokenIndex, h.cfg.Retry, h.log)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := failing.Enqueue(ctx, &jobqueuev1.EnqueueRequest{
		Queue:   "default",
		Kind:    "noop",
		Payload: []byte("{}"),
	})
	if err != nil {
		t.Fatalf("Enqueue must still succeed when the index write fails: %v", err)
	}
	job := resp.GetJob()

	// The job is durable but invisible to the scheduler: committed in
	// PostgreSQL, absent from Redis.
	if got := h.GetJob(job.GetId()).GetJob().GetState(); got != jobqueuev1.JobState_JOB_STATE_PENDING {
		t.Fatalf("job state = %s, want pending", got)
	}
	scheduled, ready, err := h.Index.Depth(ctx, "default")
	if err != nil {
		t.Fatalf("Depth: %v", err)
	}
	if scheduled != 0 || ready != 0 {
		t.Fatalf("index holds scheduled=%d ready=%d, want the job to be missing entirely", scheduled, ready)
	}

	// Now start the system. The sweeper is the only thing that can find this
	// job, because nothing ever told Redis about it.
	h.Start()

	select {
	case <-ran:
	case <-time.After(20 * time.Second):
		t.Fatal("the sweeper never backfilled a job whose index write failed")
	}
}

// Redis holds no durable state, so losing all of it must cost latency and
// nothing else.
func TestRedisDataLossLosesNoJobs(t *testing.T) {
	h := newHarness(t)

	const jobs = 100
	done := newCounter()
	h.Registry.Register("noop", func(_ context.Context, job store.Job) error {
		done.record(job.ID)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for range jobs {
		h.Enqueue(&jobqueuev1.EnqueueRequest{
			Queue:   "default",
			Kind:    "noop",
			Payload: []byte("{}"),
		})
	}

	// Everything is indexed and nothing has run yet.
	scheduled, _, err := h.Index.Depth(ctx, "default")
	if err != nil {
		t.Fatalf("Depth: %v", err)
	}
	if scheduled != jobs {
		t.Fatalf("scheduled = %d before the flush, want %d", scheduled, jobs)
	}

	// Simulate Redis restarting with an empty dataset, which is exactly what
	// the deployment's persistence-free configuration invites.
	if err := h.Redis.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	h.Start()

	waitFor(t, 60*time.Second, fmt.Sprintf("all %d jobs to run after Redis was wiped", jobs), func() bool {
		return done.distinct() == jobs
	})

	if got := done.total(); got != jobs {
		t.Errorf("handler ran %d times for %d jobs; the index rebuild duplicated work", got, jobs)
	}
}

// A worker that dies mid-job leaves a lease nobody will release. The reaper is
// what turns that into a retry rather than a job stuck running forever -- and
// it is why delivery is at-least-once.
func TestCrashedWorkerJobIsRecovered(t *testing.T) {
	h := newHarness(t)

	ran := make(chan struct{}, 4)
	h.Registry.Register("noop", func(context.Context, store.Job) error {
		ran <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	job := h.Enqueue(&jobqueuev1.EnqueueRequest{
		Queue:   "default",
		Kind:    "noop",
		Payload: []byte("{}"),
	})
	id := mustParseUUID(t, job.GetId())

	// Claim the job with a lease that has already lapsed. This is precisely the
	// state a worker leaves behind when its process dies: the row says running,
	// and nothing is ever going to heartbeat it again.
	claimed, ok, err := h.Store.ClaimJob(ctx, id, "worker-that-died", -time.Second)
	if err != nil || !ok {
		t.Fatalf("ClaimJob: ok=%v err=%v", ok, err)
	}
	if claimed.State != store.StateRunning {
		t.Fatalf("state = %s, want running", claimed.State)
	}

	h.Start()

	select {
	case <-ran:
	case <-time.After(30 * time.Second):
		t.Fatal("the reaper never recovered a job from a dead worker")
	}

	waitFor(t, 20*time.Second, "the recovered job to succeed", func() bool {
		return h.GetJob(job.GetId()).GetJob().GetState() == jobqueuev1.JobState_JOB_STATE_SUCCEEDED
	})

	// The attempt the dead worker started is still on the record, so the
	// history shows the job ran twice.
	attempts := h.GetJob(job.GetId()).GetAttempts()
	if len(attempts) < 2 {
		t.Errorf("attempt history has %d entries, want the dead worker's attempt kept alongside the retry", len(attempts))
	}
}

// A handler that always fails must exhaust its budget and stop, with the
// evidence of every attempt kept.
func TestFailingJobRetriesThenDeadLetters(t *testing.T) {
	h := newHarness(t)

	const maxAttempts = 3
	tries := newCounter()
	h.Registry.Register("always_fails", func(_ context.Context, job store.Job) error {
		tries.record(job.ID)
		return fmt.Errorf("attempt %d fails", job.Attempts)
	})
	h.Start()

	job := h.Enqueue(&jobqueuev1.EnqueueRequest{
		Queue:       "default",
		Kind:        "always_fails",
		Payload:     []byte("{}"),
		MaxAttempts: maxAttempts,
	})
	id := mustParseUUID(t, job.GetId())

	waitFor(t, 30*time.Second, "the job to be dead-lettered", func() bool {
		return h.GetJob(job.GetId()).GetJob().GetState() == jobqueuev1.JobState_JOB_STATE_DEAD
	})

	if got := tries.countFor(id); got != maxAttempts {
		t.Errorf("handler ran %d times, want exactly max_attempts (%d)", got, maxAttempts)
	}

	resp := h.GetJob(job.GetId())
	if got := len(resp.GetAttempts()); got != maxAttempts {
		t.Errorf("attempt history has %d entries, want %d", got, maxAttempts)
	}
	for _, a := range resp.GetAttempts() {
		if a.GetError() == "" {
			t.Errorf("attempt %d recorded no error", a.GetAttemptNo())
		}
	}
	if resp.GetJob().GetLastError() == "" {
		t.Error("dead job carries no last_error")
	}

	// The dead job is visible through the dead-letter listing.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	letters, err := h.API.ListDeadLetters(ctx, &jobqueuev1.ListDeadLettersRequest{Queue: "default"})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(letters.GetJobs()) != 1 || letters.GetJobs()[0].GetId() != job.GetId() {
		t.Errorf("dead letters = %v, want just the failed job", letters.GetJobs())
	}
}

// Some failures cannot be fixed by waiting. A handler must be able to say so
// and skip straight to the dead-letter queue instead of burning its budget.
func TestUnretryableFailureSkipsRetries(t *testing.T) {
	h := newHarness(t)

	tries := newCounter()
	h.Registry.Register("bad_payload", func(_ context.Context, job store.Job) error {
		tries.record(job.ID)
		return worker.Unretryable(fmt.Errorf("payload will never parse"))
	})
	h.Start()

	job := h.Enqueue(&jobqueuev1.EnqueueRequest{
		Queue:       "default",
		Kind:        "bad_payload",
		Payload:     []byte("not json"),
		MaxAttempts: 5,
	})
	id := mustParseUUID(t, job.GetId())

	waitFor(t, 30*time.Second, "the job to be dead-lettered", func() bool {
		return h.GetJob(job.GetId()).GetJob().GetState() == jobqueuev1.JobState_JOB_STATE_DEAD
	})

	if got := tries.countFor(id); got != 1 {
		t.Errorf("handler ran %d times, want 1 -- an unretryable failure must not be retried", got)
	}
}

// A job whose kind nothing handles can never succeed, so it must not consume a
// worker slot five times to find that out.
func TestUnknownKindIsDeadLetteredImmediately(t *testing.T) {
	h := newHarness(t)
	h.Start()

	job := h.Enqueue(&jobqueuev1.EnqueueRequest{
		Queue:       "default",
		Kind:        "nothing_handles_this",
		Payload:     []byte("{}"),
		MaxAttempts: 5,
	})

	waitFor(t, 30*time.Second, "the job to be dead-lettered", func() bool {
		return h.GetJob(job.GetId()).GetJob().GetState() == jobqueuev1.JobState_JOB_STATE_DEAD
	})

	resp := h.GetJob(job.GetId())
	if got := resp.GetJob().GetAttempts(); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
}

// Requeueing is the operator's undo button after the cause of a failure has
// been fixed.
func TestRequeuedDeadLetterRuns(t *testing.T) {
	h := newHarness(t)

	var failing atomic.Bool
	failing.Store(true)

	ran := make(chan struct{}, 8)
	h.Registry.Register("flaky", func(context.Context, store.Job) error {
		if failing.Load() {
			return fmt.Errorf("downstream is down")
		}
		ran <- struct{}{}
		return nil
	})
	h.Start()

	job := h.Enqueue(&jobqueuev1.EnqueueRequest{
		Queue:       "default",
		Kind:        "flaky",
		Payload:     []byte("{}"),
		MaxAttempts: 2,
	})

	waitFor(t, 30*time.Second, "the job to exhaust its attempts", func() bool {
		return h.GetJob(job.GetId()).GetJob().GetState() == jobqueuev1.JobState_JOB_STATE_DEAD
	})

	// The downstream recovers, and an operator requeues the job.
	failing.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := h.API.RequeueDeadLetter(ctx, &jobqueuev1.RequeueDeadLetterRequest{
		Id:                 job.GetId(),
		AdditionalAttempts: 3,
	}); err != nil {
		t.Fatalf("RequeueDeadLetter: %v", err)
	}

	select {
	case <-ran:
	case <-time.After(30 * time.Second):
		t.Fatal("a requeued job never ran")
	}

	waitFor(t, 20*time.Second, "the requeued job to succeed", func() bool {
		return h.GetJob(job.GetId()).GetJob().GetState() == jobqueuev1.JobState_JOB_STATE_SUCCEEDED
	})
}

// Under load, every job must run and none may be run twice concurrently.
func TestManyJobsAcrossManyWorkers(t *testing.T) {
	h := newHarness(t)

	const jobs = 300
	h.cfg.Worker.Concurrency = 10

	done := newCounter()
	h.Registry.Register("noop", func(_ context.Context, job store.Job) error {
		done.record(job.ID)
		return nil
	})
	h.Start()

	ids := make(map[string]bool, jobs)
	for range jobs {
		job := h.Enqueue(&jobqueuev1.EnqueueRequest{
			Queue:   "default",
			Kind:    "noop",
			Payload: []byte("{}"),
		})
		ids[job.GetId()] = true
	}

	waitFor(t, 90*time.Second, fmt.Sprintf("all %d jobs to run", jobs), func() bool {
		return done.distinct() == jobs
	})

	// Every job succeeded exactly once. Duplicates here would mean the claim is
	// not actually exclusive.
	if got := done.total(); got != jobs {
		t.Errorf("handler ran %d times for %d jobs, want one run each", got, jobs)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stats, err := h.Store.QueueStats(ctx, "default")
	if err != nil {
		t.Fatalf("QueueStats: %v", err)
	}
	if stats.CountsByState[store.StateSucceeded] != jobs {
		t.Errorf("succeeded = %d, want %d", stats.CountsByState[store.StateSucceeded], jobs)
	}
}

// Cancelling a job that has already been handed to the ready list has to stop
// it anyway: the index removal is best effort, so the claim is what enforces it.
func TestCancelledJobDoesNotRunEvenOnceDispatched(t *testing.T) {
	h := newHarness(t)

	ran := newCounter()
	h.Registry.Register("noop", func(_ context.Context, job store.Job) error {
		ran.record(job.ID)
		return nil
	})

	// Control plane only: the job reaches the ready list but nothing consumes it.
	h.StartWithoutWorkers()

	job := h.Enqueue(&jobqueuev1.EnqueueRequest{
		Queue:   "default",
		Kind:    "noop",
		Payload: []byte("{}"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	waitFor(t, 15*time.Second, "the job to reach the ready list", func() bool {
		_, ready, err := h.Index.Depth(ctx, "default")
		return err == nil && ready == 1
	})

	if _, err := h.API.CancelJob(ctx, &jobqueuev1.CancelJobRequest{Id: job.GetId()}); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	// The id is still sitting in the ready list, so a worker will pick it up and
	// be refused by the claim.
	h.Stop()
	h.Start()

	time.Sleep(3 * time.Second)

	if got := ran.total(); got != 0 {
		t.Errorf("a cancelled job ran %d times, want 0", got)
	}
	if got := h.GetJob(job.GetId()).GetJob().GetState(); got != jobqueuev1.JobState_JOB_STATE_CANCELLED {
		t.Errorf("state = %s, want cancelled", got)
	}
}
