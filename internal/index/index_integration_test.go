//go:build integration

package index_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Rishabh-10/durable-job-queue/internal/index"
	"github.com/Rishabh-10/durable-job-queue/internal/testsupport"
)

func TestScheduleAndPopDue(t *testing.T) {
	idx, ctx := setup(t)

	past := index.Entry{ID: uuid.New(), RunAt: time.Now().Add(-time.Minute)}
	future := index.Entry{ID: uuid.New(), RunAt: time.Now().Add(time.Hour)}

	for _, e := range []index.Entry{past, future} {
		if err := idx.Schedule(ctx, "default", e); err != nil {
			t.Fatalf("Schedule: %v", err)
		}
	}

	got, err := idx.PopDue(ctx, "default", time.Now(), 10)
	if err != nil {
		t.Fatalf("PopDue: %v", err)
	}
	if len(got) != 1 || got[0] != past.ID {
		t.Fatalf("PopDue returned %v, want only the overdue job %s", got, past.ID)
	}

	// The future job stays in the index; the popped one does not.
	scheduled, ready, err := idx.Depth(ctx, "default")
	if err != nil {
		t.Fatalf("Depth: %v", err)
	}
	if scheduled != 1 {
		t.Errorf("scheduled = %d, want 1 (the future job)", scheduled)
	}
	if ready != 1 {
		t.Errorf("ready = %d, want 1 (the popped job)", ready)
	}
}

func TestPopDueRespectsLimit(t *testing.T) {
	idx, ctx := setup(t)

	for range 10 {
		mustSchedule(t, idx, "default", time.Now().Add(-time.Minute))
	}

	got, err := idx.PopDue(ctx, "default", time.Now(), 4)
	if err != nil {
		t.Fatalf("PopDue: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("PopDue returned %d ids, want 4", len(got))
	}
}

func TestPopDueReturnsSoonestFirst(t *testing.T) {
	idx, ctx := setup(t)

	oldest := mustScheduleAt(t, idx, "default", time.Now().Add(-3*time.Hour))
	middle := mustScheduleAt(t, idx, "default", time.Now().Add(-2*time.Hour))
	newest := mustScheduleAt(t, idx, "default", time.Now().Add(-time.Hour))

	got, err := idx.PopDue(ctx, "default", time.Now(), 10)
	if err != nil {
		t.Fatalf("PopDue: %v", err)
	}
	want := []uuid.UUID{oldest, middle, newest}
	if len(got) != len(want) {
		t.Fatalf("PopDue returned %d ids, want %d", len(got), len(want))
	}
	for n := range want {
		if got[n] != want[n] {
			t.Errorf("position %d = %s, want %s (oldest due first)", n, got[n], want[n])
		}
	}
}

func TestPopDueEmpty(t *testing.T) {
	idx, ctx := setup(t)

	got, err := idx.PopDue(ctx, "nothing-here", time.Now(), 10)
	if err != nil {
		t.Fatalf("PopDue on an empty queue: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("PopDue returned %v, want nothing", got)
	}
}

// Two dispatchers racing must never hand the same id to two workers. The claim
// in PostgreSQL would catch the duplicate, but only after both workers had
// fetched the job, so the script has to be atomic.
func TestPopDueIsAtomicUnderConcurrency(t *testing.T) {
	idx, ctx := setup(t)

	const jobs = 200
	scheduled := make(map[uuid.UUID]bool, jobs)
	for range jobs {
		scheduled[mustSchedule(t, idx, "default", time.Now().Add(-time.Minute))] = true
	}

	const dispatchers = 8
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = map[uuid.UUID]int{}
		errs []error
	)

	wg.Add(dispatchers)
	for range dispatchers {
		go func() {
			defer wg.Done()
			for {
				got, err := idx.PopDue(ctx, "default", time.Now(), 16)
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, id := range got {
					seen[id]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("%d dispatcher calls failed, first: %v", len(errs), errs[0])
	}
	if len(seen) != jobs {
		t.Errorf("popped %d distinct ids, want %d", len(seen), jobs)
	}
	for id, count := range seen {
		if count > 1 {
			t.Errorf("job %s was popped %d times, want exactly 1", id, count)
		}
		if !scheduled[id] {
			t.Errorf("job %s was popped but never scheduled", id)
		}
	}
}

// The batch path has to move more ids than Lua's unpack limit allows in one
// call, which is why the script chunks.
func TestPopDueHandlesLargeBatches(t *testing.T) {
	idx, ctx := setup(t)

	const jobs = 2000
	entries := make([]index.Entry, 0, jobs)
	for range jobs {
		entries = append(entries, index.Entry{ID: uuid.New(), RunAt: time.Now().Add(-time.Minute)})
	}
	if err := idx.ScheduleBatch(ctx, "default", entries); err != nil {
		t.Fatalf("ScheduleBatch: %v", err)
	}

	got, err := idx.PopDue(ctx, "default", time.Now(), jobs)
	if err != nil {
		t.Fatalf("PopDue: %v", err)
	}
	if len(got) != jobs {
		t.Errorf("PopDue returned %d ids, want %d", len(got), jobs)
	}

	scheduled, ready, err := idx.Depth(ctx, "default")
	if err != nil {
		t.Fatalf("Depth: %v", err)
	}
	if scheduled != 0 || ready != jobs {
		t.Errorf("after popping everything: scheduled=%d ready=%d, want 0 and %d", scheduled, ready, jobs)
	}
}

// Re-indexing a job the sweeper has already seen must be free, because the
// sweeper re-adds the same jobs on every pass until they run.
func TestScheduleIsIdempotent(t *testing.T) {
	idx, ctx := setup(t)

	e := index.Entry{ID: uuid.New(), RunAt: time.Now().Add(time.Hour)}
	for range 5 {
		if err := idx.Schedule(ctx, "default", e); err != nil {
			t.Fatalf("Schedule: %v", err)
		}
	}

	scheduled, _, err := idx.Depth(ctx, "default")
	if err != nil {
		t.Fatalf("Depth: %v", err)
	}
	if scheduled != 1 {
		t.Errorf("scheduled = %d after 5 identical Schedules, want 1", scheduled)
	}
}

// Re-scheduling with a new time moves the job rather than duplicating it, which
// is how a retry's new run_at reaches the index.
func TestScheduleUpdatesRunAt(t *testing.T) {
	idx, ctx := setup(t)

	id := uuid.New()
	if err := idx.Schedule(ctx, "default", index.Entry{ID: id, RunAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if got, _ := idx.PopDue(ctx, "default", time.Now(), 10); len(got) != 0 {
		t.Fatalf("PopDue returned %v, want nothing while the job is in the future", got)
	}

	if err := idx.Schedule(ctx, "default", index.Entry{ID: id, RunAt: time.Now().Add(-time.Minute)}); err != nil {
		t.Fatalf("re-Schedule: %v", err)
	}
	got, err := idx.PopDue(ctx, "default", time.Now(), 10)
	if err != nil {
		t.Fatalf("PopDue: %v", err)
	}
	if len(got) != 1 || got[0] != id {
		t.Errorf("PopDue returned %v, want the rescheduled job %s", got, id)
	}
}

func TestUnschedule(t *testing.T) {
	idx, ctx := setup(t)

	id := mustSchedule(t, idx, "default", time.Now().Add(-time.Minute))
	if err := idx.Unschedule(ctx, "default", id); err != nil {
		t.Fatalf("Unschedule: %v", err)
	}

	got, err := idx.PopDue(ctx, "default", time.Now(), 10)
	if err != nil {
		t.Fatalf("PopDue: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("PopDue returned %v after unscheduling, want nothing", got)
	}
}

// Cancelling a job that is already in flight is an ordinary race, not an error.
func TestUnscheduleMissingJobSucceeds(t *testing.T) {
	idx, ctx := setup(t)

	if err := idx.Unschedule(ctx, "default", uuid.New()); err != nil {
		t.Errorf("Unschedule of an unknown job = %v, want nil", err)
	}
}

func TestNextReturnsReadyJob(t *testing.T) {
	idx, ctx := setup(t)

	id := mustSchedule(t, idx, "default", time.Now().Add(-time.Minute))
	if _, err := idx.PopDue(ctx, "default", time.Now(), 10); err != nil {
		t.Fatalf("PopDue: %v", err)
	}

	queue, got, err := idx.Next(ctx, []string{"default"}, time.Second)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if queue != "default" || got != id {
		t.Errorf("Next returned (%s, %s), want (default, %s)", queue, got, id)
	}
}

func TestNextTimesOutWhenIdle(t *testing.T) {
	idx, ctx := setup(t)

	_, _, err := idx.Next(ctx, []string{"default"}, 100*time.Millisecond)
	if !errors.Is(err, index.ErrNoJob) {
		t.Errorf("Next on an idle queue = %v, want ErrNoJob", err)
	}
}

// Queues are polled in order, so a job on an earlier queue is taken first.
func TestNextPrefersEarlierQueues(t *testing.T) {
	idx, ctx := setup(t)

	low := mustSchedule(t, idx, "low", time.Now().Add(-time.Minute))
	high := mustSchedule(t, idx, "high", time.Now().Add(-time.Minute))
	for _, q := range []string{"low", "high"} {
		if _, err := idx.PopDue(ctx, q, time.Now(), 10); err != nil {
			t.Fatalf("PopDue on %s: %v", q, err)
		}
	}

	queue, got, err := idx.Next(ctx, []string{"high", "low"}, time.Second)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if queue != "high" || got != high {
		t.Errorf("Next returned (%s, %s), want the high-priority job (high, %s)", queue, got, high)
	}

	queue, got, err = idx.Next(ctx, []string{"high", "low"}, time.Second)
	if err != nil {
		t.Fatalf("second Next: %v", err)
	}
	if queue != "low" || got != low {
		t.Errorf("second Next returned (%s, %s), want (low, %s)", queue, got, low)
	}
}

func TestQueuesTracksEveryQueueUsed(t *testing.T) {
	idx, ctx := setup(t)

	mustSchedule(t, idx, "emails", time.Now())
	mustSchedule(t, idx, "webhooks", time.Now())
	if err := idx.RegisterQueue(ctx, "reports"); err != nil {
		t.Fatalf("RegisterQueue: %v", err)
	}

	queues, err := idx.Queues(ctx)
	if err != nil {
		t.Fatalf("Queues: %v", err)
	}
	found := map[string]bool{}
	for _, q := range queues {
		found[q] = true
	}
	for _, want := range []string{"emails", "webhooks", "reports"} {
		if !found[want] {
			t.Errorf("queue %q missing from %v", want, queues)
		}
	}
}

// Queues must not share a sorted set, or one busy queue would starve another.
func TestQueuesAreIsolated(t *testing.T) {
	idx, ctx := setup(t)

	emails := mustSchedule(t, idx, "emails", time.Now().Add(-time.Minute))
	mustSchedule(t, idx, "webhooks", time.Now().Add(-time.Minute))

	got, err := idx.PopDue(ctx, "emails", time.Now(), 10)
	if err != nil {
		t.Fatalf("PopDue: %v", err)
	}
	if len(got) != 1 || got[0] != emails {
		t.Errorf("PopDue on emails returned %v, want only %s", got, emails)
	}
}

func setup(t *testing.T) (*index.Index, context.Context) {
	t.Helper()
	idx := index.New(testsupport.Redis(t))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return idx, ctx
}

func mustSchedule(t *testing.T, idx *index.Index, queue string, runAt time.Time) uuid.UUID {
	t.Helper()
	return mustScheduleAt(t, idx, queue, runAt)
}

func mustScheduleAt(t *testing.T, idx *index.Index, queue string, runAt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if err := idx.Schedule(context.Background(), queue, index.Entry{ID: id, RunAt: runAt}); err != nil {
		t.Fatalf("schedule on %s: %v", queue, err)
	}
	return id
}
