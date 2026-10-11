// Command worker runs the executor pool: it pulls due job ids off the Redis
// ready list, claims a lease in PostgreSQL, runs the registered handler, and
// acknowledges the result.
//
// Delivery is at-least-once, so handlers must be idempotent.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rishabh-10/durable-job-queue/internal/backoff"
	"github.com/Rishabh-10/durable-job-queue/internal/config"
	"github.com/Rishabh-10/durable-job-queue/internal/index"
	"github.com/Rishabh-10/durable-job-queue/internal/observability"
	"github.com/Rishabh-10/durable-job-queue/internal/store"
	"github.com/Rishabh-10/durable-job-queue/internal/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log, err := observability.NewLogger(cfg.Env, cfg.LogLevel, os.Stderr)
	if err != nil {
		return fmt.Errorf("build logger: %w", err)
	}

	workerID := cfg.Worker.ID
	if workerID == "" {
		workerID = generateWorkerID()
	}
	log = log.With("worker_id", workerID)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancelStartup := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStartup()

	st, err := store.Open(startupCtx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer st.Close()

	idx, err := index.Open(startupCtx, cfg.Redis)
	if err != nil {
		return err
	}
	defer func() { _ = idx.Close() }()

	// Register the queues up front so the dispatcher polls them even before the
	// first job is enqueued.
	for _, queue := range cfg.Worker.Queues {
		if err := idx.RegisterQueue(startupCtx, queue); err != nil {
			return fmt.Errorf("register queue %s: %w", queue, err)
		}
	}

	metrics := observability.NewMetrics()
	registry := worker.NewRegistry()
	registerHandlers(registry, log)

	pool := worker.NewPool(st, idx, registry, cfg.Worker,
		backoff.New(cfg.Retry.BackoffBase, cfg.Retry.BackoffCap), workerID, log)
	pool.OnFinished(func(queue, kind string, outcome worker.Outcome, elapsed time.Duration) {
		metrics.Executed.WithLabelValues(queue, kind, string(outcome)).Inc()
		if kind != "" {
			metrics.Duration.WithLabelValues(queue, kind).Observe(elapsed.Seconds())
		}
	})
	pool.OnDispatchLag(metrics.ObserveDispatchLag)

	admin := &http.Server{
		Addr:              cfg.Worker.MetricsAddr,
		Handler:           adminMux(metrics),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin server failed", "error", err)
		}
	}()

	pool.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := admin.Shutdown(shutdownCtx); err != nil {
		log.Warn("admin server did not shut down cleanly", "error", err)
	}

	log.Info("worker stopped")
	return nil
}

// registerHandlers wires the job kinds this worker can run.
//
// The handlers here are demonstrations: they exercise success, retryable
// failure, permanent failure and slow work, which is what the demo and the
// integration tests need. A real deployment replaces this function.
func registerHandlers(registry *worker.Registry, log *slog.Logger) {
	registry.Register("noop", func(context.Context, store.Job) error {
		return nil
	})

	registry.Register("echo", func(ctx context.Context, job store.Job) error {
		log.InfoContext(ctx, "echo", "job_id", job.ID, "payload", string(job.Payload))
		return nil
	})

	// sleep waits for the duration in its payload, to exercise heartbeats and
	// the shutdown drain.
	registry.Register("sleep", func(ctx context.Context, job store.Job) error {
		var payload struct {
			Seconds float64 `json:"seconds"`
		}
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			// A payload that will not parse cannot parse on a retry either.
			return worker.Unretryable(fmt.Errorf("parse payload: %w", err))
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(payload.Seconds * float64(time.Second))):
			return nil
		}
	})

	// always_fails drives a job through its whole retry budget and into the
	// dead-letter queue.
	registry.Register("always_fails", func(_ context.Context, job store.Job) error {
		return fmt.Errorf("this handler always fails (attempt %d of %d)", job.Attempts, job.MaxAttempts)
	})
}

func adminMux(metrics *observability.Metrics) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

// generateWorkerID identifies this process on leases and attempt rows. The
// hostname alone is not enough: several workers commonly share a host, and a
// restarted process must not be mistaken for its predecessor.
func generateWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	return fmt.Sprintf("%s-%s", host, hex.EncodeToString(suffix[:]))
}
