//go:build integration

package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Rishabh-10/durable-job-queue/internal/store"
	"github.com/Rishabh-10/durable-job-queue/internal/testsupport"
)

const leaseTTL = 30 * time.Second

func TestCreateJobAndGet(t *testing.T) {
	s, ctx := setup(t)

	created, isNew, err := s.CreateJob(ctx, store.CreateJobParams{
		Queue:       "default",
		Kind:        "send_email",
		Payload:     []byte(`{"to":"a@example.com"}`),
		RunAt:       time.Now().Add(time.Hour),
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if !isNew {
		t.Error("created = false, want true for a job with no idempotency key")
	}
	if created.State != store.StatePending {
		t.Errorf("State = %s, want pending", created.State)
	}
	if created.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0", created.Attempts)
	}

	got, err := s.GetJob(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.ID != created.ID || got.Kind != "send_email" {
		t.Errorf("GetJob returned %+v, want the job just created", got)
	}
}

func TestGetJobMissing(t *testing.T) {
	s, ctx := setup(t)

	if _, err := s.GetJob(ctx, uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetJob error = %v, want ErrNotFound", err)
	}
}

// A client that retries an Enqueue it never got a response to must not create a
// second job.
func TestCreateJobIdempotency(t *testing.T) {
	s, ctx := setup(t)

	params := store.CreateJobParams{
		Queue:          "default",
		Kind:           "charge_card",
		Payload:        []byte("{}"),
		RunAt:          time.Now(),
		MaxAttempts:    3,
		IdempotencyKey: "order-1234",
	}

	first, isNew, err := s.CreateJob(ctx, params)
	if err != nil || !isNew {
		t.Fatalf("first CreateJob: job=%v new=%v err=%v", first.ID, isNew, err)
	}

	second, isNew, err := s.CreateJob(ctx, params)
	if err != nil {
		t.Fatalf("second CreateJob: %v", err)
	}
	if isNew {
		t.Error("created = true on a duplicate key, want false")
	}
	if second.ID != first.ID {
		t.Errorf("second job id = %s, want the original %s", second.ID, first.ID)
	}
}

// The uniqueness guarantee has to hold under concurrency, not just in sequence:
// it is enforced by a unique index, so every racing insert but one conflicts.
func TestCreateJobIdempotencyUnderConcurrency(t *testing.T) {
	s, ctx := setup(t)

	const racers = 50
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = make(map[uuid.UUID]struct{})
		newOnes int
		errs    []error
	)

	wg.Add(racers)
	for range racers {
		go func() {
			defer wg.Done()
			job, isNew, err := s.CreateJob(ctx, store.CreateJobParams{
				Queue:          "default",
				Kind:           "charge_card",
				Payload:        []byte("{}"),
				RunAt:          time.Now(),
				MaxAttempts:    3,
				IdempotencyKey: "order-race",
			})

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			ids[job.ID] = struct{}{}
			if isNew {
				newOnes++
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("%d of %d concurrent enqueues failed, first: %v", len(errs), racers, errs[0])
	}
	if len(ids) != 1 {
		t.Errorf("got %d distinct job ids, want exactly 1", len(ids))
	}
	if newOnes != 1 {
		t.Errorf("%d callers were told they created the job, want exactly 1", newOnes)
	}
}

// Idempotency is scoped per queue, so the same key on two queues is two jobs.
func TestIdempotencyIsScopedPerQueue(t *testing.T) {
	s, ctx := setup(t)

	mk := func(queue string) uuid.UUID {
		job, _, err := s.CreateJob(ctx, store.CreateJobParams{
			Queue: queue, Kind: "k", Payload: []byte("{}"),
			RunAt: time.Now(), MaxAttempts: 3, IdempotencyKey: "shared",
		})
		if err != nil {
			t.Fatalf("CreateJob on %s: %v", queue, err)
		}
		return job.ID
	}

	if a, b := mk("emails"), mk("webhooks"); a == b {
		t.Error("same key on different queues collapsed into one job")
	}
}

// An unset key must be SQL NULL, not the empty string, or the unique index
// would let only one keyless job exist per queue.
func TestManyJobsWithoutIdempotencyKey(t *testing.T) {
	s, ctx := setup(t)

	for i := range 5 {
		if _, isNew, err := s.CreateJob(ctx, store.CreateJobParams{
			Queue: "default", Kind: "k", Payload: []byte("{}"),
			RunAt: time.Now(), MaxAttempts: 3,
		}); err != nil || !isNew {
			t.Fatalf("job %d: new=%v err=%v", i, isNew, err)
		}
	}
}

func TestClaimJob(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(-time.Second))

	claimed, ok, err := s.ClaimJob(ctx, job.ID, "worker-a", leaseTTL)
	if err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
	if !ok {
		t.Fatal("claimed = false, want true for a due pending job")
	}
	if claimed.State != store.StateRunning {
		t.Errorf("State = %s, want running", claimed.State)
	}
	if claimed.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 after the first claim", claimed.Attempts)
	}
	if claimed.LeaseOwner != "worker-a" {
		t.Errorf("LeaseOwner = %q, want worker-a", claimed.LeaseOwner)
	}

	attempts, err := s.ListAttempts(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].AttemptNo != 1 || attempts[0].WorkerID != "worker-a" {
		t.Errorf("attempts = %+v, want one open attempt by worker-a", attempts)
	}
}

// The claim is the only thing standing between a dispatched job and two workers
// running it at once.
func TestClaimJobIsExclusive(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(-time.Second))

	const racers = 20
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		winners  int
		failures []error
	)

	wg.Add(racers)
	for i := range racers {
		go func() {
			defer wg.Done()
			_, ok, err := s.ClaimJob(ctx, job.ID, fmt.Sprintf("worker-%d", i), leaseTTL)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures = append(failures, err)
				return
			}
			if ok {
				winners++
			}
		}()
	}
	wg.Wait()

	if len(failures) > 0 {
		t.Fatalf("%d claims errored, first: %v", len(failures), failures[0])
	}
	if winners != 1 {
		t.Errorf("%d workers claimed the same job, want exactly 1", winners)
	}
}

func TestClaimJobRejectsFutureAndCancelled(t *testing.T) {
	s, ctx := setup(t)

	future := mustCreate(t, s, "default", time.Now().Add(time.Hour))
	if _, ok, err := s.ClaimJob(ctx, future.ID, "w", leaseTTL); err != nil || ok {
		t.Errorf("claiming a job that is not due: ok=%v err=%v, want ok=false", ok, err)
	}

	cancelled := mustCreate(t, s, "default", time.Now().Add(-time.Second))
	if _, err := s.CancelJob(ctx, cancelled.ID); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if _, ok, err := s.ClaimJob(ctx, cancelled.ID, "w", leaseTTL); err != nil || ok {
		t.Errorf("claiming a cancelled job: ok=%v err=%v, want ok=false", ok, err)
	}
}

func TestCancelJob(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(time.Hour))

	cancelled, err := s.CancelJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if cancelled.State != store.StateCancelled {
		t.Errorf("State = %s, want cancelled", cancelled.State)
	}

	// A second cancel has nothing left to stop.
	if _, err := s.CancelJob(ctx, job.ID); !errors.Is(err, store.ErrNotPending) {
		t.Errorf("second CancelJob error = %v, want ErrNotPending", err)
	}
}

func TestCancelRunningJobIsRefused(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(-time.Second))

	if _, _, err := s.ClaimJob(ctx, job.ID, "worker-a", leaseTTL); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
	if _, err := s.CancelJob(ctx, job.ID); !errors.Is(err, store.ErrNotPending) {
		t.Errorf("CancelJob on a running job = %v, want ErrNotPending", err)
	}
}

func TestCompleteJob(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(-time.Second))

	claimed, _, err := s.ClaimJob(ctx, job.ID, "worker-a", leaseTTL)
	if err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}

	done, err := s.CompleteJob(ctx, job.ID, "worker-a", claimed.Attempts)
	if err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}
	if done.State != store.StateSucceeded {
		t.Errorf("State = %s, want succeeded", done.State)
	}
	if done.LeaseOwner != "" {
		t.Errorf("LeaseOwner = %q, want it released", done.LeaseOwner)
	}

	attempts, _ := s.ListAttempts(ctx, job.ID)
	if len(attempts) != 1 || attempts[0].FinishedAt.IsZero() || attempts[0].Error != "" {
		t.Errorf("attempts = %+v, want one closed attempt with no error", attempts)
	}
}

func TestRetryJobRecordsError(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(-time.Second))

	claimed, _, _ := s.ClaimJob(ctx, job.ID, "worker-a", leaseTTL)
	nextRun := time.Now().Add(2 * time.Second).UTC().Truncate(time.Millisecond)

	retried, err := s.RetryJob(ctx, job.ID, "worker-a", claimed.Attempts, "smtp timeout", nextRun)
	if err != nil {
		t.Fatalf("RetryJob: %v", err)
	}
	if retried.State != store.StatePending {
		t.Errorf("State = %s, want pending", retried.State)
	}
	if retried.Attempts != 1 {
		t.Errorf("Attempts = %d, want the attempt to stay counted", retried.Attempts)
	}
	if retried.LastError != "smtp timeout" {
		t.Errorf("LastError = %q, want the failure cause", retried.LastError)
	}
	if !retried.RunAt.UTC().Truncate(time.Millisecond).Equal(nextRun) {
		t.Errorf("RunAt = %s, want %s", retried.RunAt, nextRun)
	}

	attempts, _ := s.ListAttempts(ctx, job.ID)
	if len(attempts) != 1 || attempts[0].Error != "smtp timeout" {
		t.Errorf("attempts = %+v, want the error recorded on the attempt", attempts)
	}
}

func TestDeadLetterJob(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(-time.Second))

	claimed, _, _ := s.ClaimJob(ctx, job.ID, "worker-a", leaseTTL)
	dead, err := s.DeadLetterJob(ctx, job.ID, "worker-a", claimed.Attempts, "gave up")
	if err != nil {
		t.Fatalf("DeadLetterJob: %v", err)
	}
	if dead.State != store.StateDead {
		t.Errorf("State = %s, want dead", dead.State)
	}

	letters, err := s.ListDeadLetters(ctx, "default", 10, nil)
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(letters) != 1 || letters[0].ID != job.ID {
		t.Errorf("dead letters = %+v, want the job just killed", letters)
	}
}

func TestRequeueDeadLetter(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(-time.Second))

	claimed, _, _ := s.ClaimJob(ctx, job.ID, "worker-a", leaseTTL)
	if _, err := s.DeadLetterJob(ctx, job.ID, "worker-a", claimed.Attempts, "gave up"); err != nil {
		t.Fatalf("DeadLetterJob: %v", err)
	}

	requeued, err := s.RequeueDeadLetter(ctx, job.ID, 3, time.Now())
	if err != nil {
		t.Fatalf("RequeueDeadLetter: %v", err)
	}
	if requeued.State != store.StatePending {
		t.Errorf("State = %s, want pending", requeued.State)
	}
	// Attempts keeps counting up so the history stays append-only; the budget
	// grows instead of the counter shrinking.
	if requeued.Attempts != 1 {
		t.Errorf("Attempts = %d, want the used attempt still counted", requeued.Attempts)
	}
	if requeued.MaxAttempts != 4 {
		t.Errorf("MaxAttempts = %d, want 1 used + 3 granted", requeued.MaxAttempts)
	}

	// A job that is not dead cannot be requeued.
	if _, err := s.RequeueDeadLetter(ctx, job.ID, 3, time.Now()); !errors.Is(err, store.ErrNotDead) {
		t.Errorf("requeueing a pending job = %v, want ErrNotDead", err)
	}
}

// A worker whose lease was reclaimed must not be able to write its result: the
// job now belongs to whoever holds the lease.
func TestFinishingWithALostLeaseIsRefused(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(-time.Second))

	claimed, _, _ := s.ClaimJob(ctx, job.ID, "worker-a", leaseTTL)

	if _, err := s.CompleteJob(ctx, job.ID, "worker-b", claimed.Attempts); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("CompleteJob by a non-owner = %v, want ErrLeaseLost", err)
	}
}

func TestExtendLease(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(-time.Second))

	claimed, _, _ := s.ClaimJob(ctx, job.ID, "worker-a", time.Second)

	ok, err := s.ExtendLease(ctx, job.ID, "worker-a", time.Hour)
	if err != nil || !ok {
		t.Fatalf("ExtendLease by the owner: ok=%v err=%v", ok, err)
	}

	after, _ := s.GetJob(ctx, job.ID)
	if !after.LeaseExpiresAt.After(claimed.LeaseExpiresAt) {
		t.Errorf("lease expiry did not move: %s -> %s", claimed.LeaseExpiresAt, after.LeaseExpiresAt)
	}

	// A worker that has lost the lease must not be able to take it back.
	if ok, err := s.ExtendLease(ctx, job.ID, "worker-b", time.Hour); err != nil || ok {
		t.Errorf("ExtendLease by a non-owner: ok=%v err=%v, want ok=false", ok, err)
	}
}

// The sweeper's scan is what makes a half-completed enqueue survivable, so it
// must see pending work and ignore everything else.
func TestListDueForIndexing(t *testing.T) {
	s, ctx := setup(t)

	due := mustCreate(t, s, "default", time.Now().Add(-time.Minute))
	soon := mustCreate(t, s, "default", time.Now().Add(10*time.Second))
	later := mustCreate(t, s, "default", time.Now().Add(time.Hour))

	running := mustCreate(t, s, "default", time.Now().Add(-time.Minute))
	if _, _, err := s.ClaimJob(ctx, running.ID, "worker-a", leaseTTL); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}

	found, err := s.ListDueForIndexing(ctx, "", 30*time.Second, 100)
	if err != nil {
		t.Fatalf("ListDueForIndexing: %v", err)
	}

	got := map[uuid.UUID]bool{}
	for _, j := range found {
		got[j.ID] = true
	}
	if !got[due.ID] {
		t.Error("an overdue pending job was not returned")
	}
	if !got[soon.ID] {
		t.Error("a job due inside the lookahead window was not returned")
	}
	if got[later.ID] {
		t.Error("a job well outside the lookahead window was returned")
	}
	if got[running.ID] {
		t.Error("a running job was returned; only pending jobs need indexing")
	}
}

func TestListDueForIndexingFiltersByQueue(t *testing.T) {
	s, ctx := setup(t)

	emails := mustCreate(t, s, "emails", time.Now().Add(-time.Minute))
	mustCreate(t, s, "webhooks", time.Now().Add(-time.Minute))

	found, err := s.ListDueForIndexing(ctx, "emails", time.Second, 100)
	if err != nil {
		t.Fatalf("ListDueForIndexing: %v", err)
	}
	if len(found) != 1 || found[0].ID != emails.ID {
		t.Errorf("found = %+v, want only the emails job", found)
	}
}

// A worker that dies mid-execution leaves a lease that nobody will ever
// release. The reaper is what turns that into a retry instead of a lost job.
func TestReclaimExpiredLeasesRetries(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(-time.Second))

	// A lease that has already lapsed stands in for a worker that died.
	if _, ok, err := s.ClaimJob(ctx, job.ID, "dead-worker", -time.Second); err != nil || !ok {
		t.Fatalf("ClaimJob: ok=%v err=%v", ok, err)
	}

	retryAt := time.Now().Add(5 * time.Second)
	reclaimed, err := s.ReclaimExpiredLeases(ctx, 10, "lease expired", func(store.ExpiredLease) time.Time {
		return retryAt
	})
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("reclaimed %d jobs, want 1", len(reclaimed))
	}
	if reclaimed[0].State != store.StatePending {
		t.Errorf("State = %s, want pending so the job runs again", reclaimed[0].State)
	}
	if reclaimed[0].LeaseOwner != "" {
		t.Errorf("LeaseOwner = %q, want the dead worker's lease released", reclaimed[0].LeaseOwner)
	}
	if reclaimed[0].Attempts != 1 {
		t.Errorf("Attempts = %d, want the failed attempt still counted", reclaimed[0].Attempts)
	}
}

// A job that keeps killing its workers must eventually stop, rather than
// cycling forever.
func TestReclaimExpiredLeasesDeadLettersWhenExhausted(t *testing.T) {
	s, ctx := setup(t)

	job, _, err := s.CreateJob(ctx, store.CreateJobParams{
		Queue: "default", Kind: "poison", Payload: []byte("{}"),
		RunAt: time.Now().Add(-time.Second), MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	if _, ok, err := s.ClaimJob(ctx, job.ID, "dead-worker", -time.Second); err != nil || !ok {
		t.Fatalf("ClaimJob: ok=%v err=%v", ok, err)
	}

	reclaimed, err := s.ReclaimExpiredLeases(ctx, 10, "lease expired", func(store.ExpiredLease) time.Time {
		t.Error("retry time was computed for a job with no attempts left")
		return time.Now()
	})
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].State != store.StateDead {
		t.Fatalf("reclaimed = %+v, want one dead job", reclaimed)
	}
}

// A live lease belongs to a worker that is still heartbeating, and must be left
// alone.
func TestReclaimExpiredLeasesIgnoresLiveLeases(t *testing.T) {
	s, ctx := setup(t)
	job := mustCreate(t, s, "default", time.Now().Add(-time.Second))

	if _, _, err := s.ClaimJob(ctx, job.ID, "healthy-worker", time.Hour); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}

	reclaimed, err := s.ReclaimExpiredLeases(ctx, 10, "lease expired", func(store.ExpiredLease) time.Time {
		return time.Now()
	})
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}
	if len(reclaimed) != 0 {
		t.Errorf("reclaimed %d live leases, want 0", len(reclaimed))
	}
}

func TestListJobsPagination(t *testing.T) {
	s, ctx := setup(t)

	const total = 7
	for range total {
		mustCreate(t, s, "default", time.Now())
	}

	var (
		seen   = map[uuid.UUID]bool{}
		cursor *store.Cursor
		pages  int
	)
	for {
		page, err := s.ListJobs(ctx, store.ListJobsParams{Queue: "default", PageSize: 3, After: cursor})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(page) == 0 {
			break
		}
		pages++
		if pages > total {
			t.Fatal("pagination did not terminate")
		}
		for _, j := range page {
			if seen[j.ID] {
				t.Errorf("job %s appeared on two pages", j.ID)
			}
			seen[j.ID] = true
		}
		last := page[len(page)-1]
		cursor = &store.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}

	if len(seen) != total {
		t.Errorf("paged through %d jobs, want %d", len(seen), total)
	}
}

func TestListJobsFiltersByState(t *testing.T) {
	s, ctx := setup(t)

	pending := mustCreate(t, s, "default", time.Now().Add(time.Hour))
	running := mustCreate(t, s, "default", time.Now().Add(-time.Second))
	if _, _, err := s.ClaimJob(ctx, running.ID, "worker-a", leaseTTL); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}

	got, err := s.ListJobs(ctx, store.ListJobsParams{
		Queue:    "default",
		States:   []store.State{store.StateRunning},
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(got) != 1 || got[0].ID != running.ID {
		t.Errorf("got = %+v, want only the running job", got)
	}

	all, err := s.ListJobs(ctx, store.ListJobsParams{Queue: "default", PageSize: 10})
	if err != nil {
		t.Fatalf("ListJobs unfiltered: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("unfiltered listing returned %d jobs, want 2 (%s, %s)", len(all), pending.ID, running.ID)
	}
}

func TestListJobsRejectsUnknownState(t *testing.T) {
	s, ctx := setup(t)

	_, err := s.ListJobs(ctx, store.ListJobsParams{
		Queue:    "default",
		States:   []store.State{"nonsense"},
		PageSize: 10,
	})
	if err == nil {
		t.Error("ListJobs accepted an unknown state, want an error")
	}
}

func TestQueueStats(t *testing.T) {
	s, ctx := setup(t)

	mustCreate(t, s, "default", time.Now().Add(time.Hour))
	overdue := mustCreate(t, s, "default", time.Now().Add(-time.Minute))

	running := mustCreate(t, s, "default", time.Now().Add(-time.Second))
	if _, _, err := s.ClaimJob(ctx, running.ID, "worker-a", leaseTTL); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}

	stats, err := s.QueueStats(ctx, "default")
	if err != nil {
		t.Fatalf("QueueStats: %v", err)
	}
	if stats.CountsByState[store.StatePending] != 2 {
		t.Errorf("pending = %d, want 2", stats.CountsByState[store.StatePending])
	}
	if stats.CountsByState[store.StateRunning] != 1 {
		t.Errorf("running = %d, want 1", stats.CountsByState[store.StateRunning])
	}
	// The overdue job has been waiting about a minute; allow slack for timing.
	if stats.OldestDueLag < 30*time.Second {
		t.Errorf("OldestDueLag = %s, want roughly the age of job %s (~1m)", stats.OldestDueLag, overdue.ID)
	}
}

func TestQueueStatsEmptyQueue(t *testing.T) {
	s, ctx := setup(t)

	stats, err := s.QueueStats(ctx, "nothing-here")
	if err != nil {
		t.Fatalf("QueueStats: %v", err)
	}
	if len(stats.CountsByState) != 0 || stats.OldestDueLag != 0 {
		t.Errorf("stats = %+v, want an empty summary", stats)
	}
}

func setup(t *testing.T) (*store.Store, context.Context) {
	t.Helper()
	s := testsupport.Store(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return s, ctx
}

func mustCreate(t *testing.T, s *store.Store, queue string, runAt time.Time) store.Job {
	t.Helper()
	job, _, err := s.CreateJob(context.Background(), store.CreateJobParams{
		Queue:       queue,
		Kind:        "test",
		Payload:     []byte("{}"),
		RunAt:       runAt,
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create job on %s: %v", queue, err)
	}
	return job
}
