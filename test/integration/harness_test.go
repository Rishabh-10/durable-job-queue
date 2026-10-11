//go:build integration

// Package integration runs the whole system -- API, dispatcher, sweeper, reaper
// and worker pool -- against real PostgreSQL and Redis containers.
//
// The tests that matter most here are the fault-injection ones. Anyone can show
// a queue working; the claims worth proving are what happens when Redis loses a
// write, when Redis loses everything, and when a worker dies holding a job.
package integration

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	jobqueuev1 "github.com/Rishabh-10/durable-job-queue/gen/jobqueue/v1"
	"github.com/Rishabh-10/durable-job-queue/internal/api"
	"github.com/Rishabh-10/durable-job-queue/internal/backoff"
	"github.com/Rishabh-10/durable-job-queue/internal/config"
	"github.com/Rishabh-10/durable-job-queue/internal/index"
	"github.com/Rishabh-10/durable-job-queue/internal/scheduler"
	"github.com/Rishabh-10/durable-job-queue/internal/store"
	"github.com/Rishabh-10/durable-job-queue/internal/testsupport"
	"github.com/Rishabh-10/durable-job-queue/internal/worker"
)

// Intervals are compressed so a test that exercises a recovery path finishes in
// seconds rather than the minute the production defaults would take.
func testConfig() config.Config {
	return config.Config{
		Dispatcher: config.Dispatcher{
			Enabled:   true,
			Interval:  25 * time.Millisecond,
			BatchSize: 128,
		},
		Sweeper: config.Sweeper{
			Enabled:   true,
			Interval:  100 * time.Millisecond,
			Lookahead: 500 * time.Millisecond,
			BatchSize: 500,
		},
		Reaper: config.Reaper{
			Enabled:   true,
			Interval:  100 * time.Millisecond,
			BatchSize: 200,
		},
		Worker: config.Worker{
			Queues:          []string{"default"},
			Concurrency:     4,
			LeaseTTL:        3 * time.Second,
			JobTimeout:      10 * time.Second,
			PollTimeout:     100 * time.Millisecond,
			ShutdownTimeout: 5 * time.Second,
		},
		Retry: config.Retry{
			DefaultMaxAttempts: 3,
			BackoffBase:        50 * time.Millisecond,
			BackoffCap:         500 * time.Millisecond,
		},
	}
}

// harness wires the whole system against throwaway containers.
type harness struct {
	t   *testing.T
	cfg config.Config

	Store    *store.Store
	Index    *index.Index
	Redis    *redis.Client
	API      *api.Server
	Registry *worker.Registry

	log *slog.Logger

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	client := testsupport.Redis(t)
	st := testsupport.Store(t)
	idx := index.New(client)

	cfg := testConfig()
	log := testLogger(t)

	h := &harness{
		t:        t,
		cfg:      cfg,
		Store:    st,
		Index:    idx,
		Redis:    client,
		Registry: worker.NewRegistry(),
		log:      log,
	}
	h.API = api.NewServer(st, idx, cfg.Retry, log)

	t.Cleanup(h.Stop)
	return h
}

// testLogger keeps test output quiet unless something goes wrong. Warn and
// above still surface, because those are the lines that explain a failure.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	handler := slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelWarn})
	return slog.New(handler)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// Start runs the dispatcher, sweeper, reaper and worker pool.
func (h *harness) Start() {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.running {
		h.t.Fatal("harness is already started")
	}
	h.running = true

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	policy := backoff.New(h.cfg.Retry.BackoffBase, h.cfg.Retry.BackoffCap)

	loops := []func(context.Context){
		scheduler.NewDispatcher(h.Index, h.cfg.Dispatcher, h.log).Run,
		scheduler.NewSweeper(h.Store, h.Index, h.cfg.Sweeper, h.log).Run,
		scheduler.NewReaper(h.Store, h.Index, h.cfg.Reaper, policy, h.log).Run,
		worker.NewPool(h.Store, h.Index, h.Registry, h.cfg.Worker, policy, "test-worker", h.log).Run,
	}

	h.wg.Add(len(loops))
	for _, run := range loops {
		go func() {
			defer h.wg.Done()
			run(ctx)
		}()
	}
}

// StartWithoutWorkers runs only the control plane, so a test can observe jobs
// reaching the ready list without them being executed.
func (h *harness) StartWithoutWorkers() {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	h.running = true
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	policy := backoff.New(h.cfg.Retry.BackoffBase, h.cfg.Retry.BackoffCap)
	loops := []func(context.Context){
		scheduler.NewDispatcher(h.Index, h.cfg.Dispatcher, h.log).Run,
		scheduler.NewSweeper(h.Store, h.Index, h.cfg.Sweeper, h.log).Run,
		scheduler.NewReaper(h.Store, h.Index, h.cfg.Reaper, policy, h.log).Run,
	}

	h.wg.Add(len(loops))
	for _, run := range loops {
		go func() {
			defer h.wg.Done()
			run(ctx)
		}()
	}
}

// Stop shuts everything down and waits.
func (h *harness) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.running {
		return
	}
	h.running = false
	h.cancel()
	h.wg.Wait()
}

// Enqueue schedules a job through the real API surface.
func (h *harness) Enqueue(req *jobqueuev1.EnqueueRequest) *jobqueuev1.Job {
	h.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := h.API.Enqueue(ctx, req)
	if err != nil {
		h.t.Fatalf("Enqueue: %v", err)
	}
	return resp.GetJob()
}

// GetJob reads a job back through the API.
func (h *harness) GetJob(id string) *jobqueuev1.GetJobResponse {
	h.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := h.API.GetJob(ctx, &jobqueuev1.GetJobRequest{Id: id})
	if err != nil {
		h.t.Fatalf("GetJob %s: %v", id, err)
	}
	return resp
}

// counter records handler invocations across goroutines.
type counter struct {
	n      atomic.Int64
	mu     sync.Mutex
	perJob map[uuid.UUID]int
}

func newCounter() *counter {
	return &counter{perJob: make(map[uuid.UUID]int)}
}

func (c *counter) record(id uuid.UUID) {
	c.n.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.perJob[id]++
}

func (c *counter) total() int64 { return c.n.Load() }

func (c *counter) distinct() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.perJob)
}

func (c *counter) countFor(id uuid.UUID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.perJob[id]
}

// waitFor polls cond until it holds or the deadline passes. Polling beats a
// fixed sleep here: it keeps a passing test fast while still giving a slow
// container room to catch up.
func waitFor(t *testing.T, timeout time.Duration, describe string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, describe)
}

func mustParseUUID(t *testing.T, s string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(s)
	if err != nil {
		t.Fatalf("parse uuid %q: %v", s, err)
	}
	return id
}
