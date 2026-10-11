// Command jobqueued runs the job queue control plane: the gRPC API plus the
// dispatcher, sweeper and reaper loops.
//
// It is safe to run any number of replicas. Every background loop reconciles
// through conditional UPDATEs in PostgreSQL or idempotent writes to Redis, so
// none of them needs leader election.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	jobqueuev1 "github.com/Rishabh-10/durable-job-queue/gen/jobqueue/v1"
	"github.com/Rishabh-10/durable-job-queue/internal/api"
	"github.com/Rishabh-10/durable-job-queue/internal/backoff"
	"github.com/Rishabh-10/durable-job-queue/internal/config"
	"github.com/Rishabh-10/durable-job-queue/internal/index"
	"github.com/Rishabh-10/durable-job-queue/internal/observability"
	"github.com/Rishabh-10/durable-job-queue/internal/scheduler"
	"github.com/Rishabh-10/durable-job-queue/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "jobqueued: %v\n", err)
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
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Connecting before anything is served means a bad DSN or an unreachable
	// Redis fails the process at startup rather than on the first request.
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

	metrics := observability.NewMetrics()
	policy := backoff.New(cfg.Retry.BackoffBase, cfg.Retry.BackoffCap)

	log.Info("starting jobqueued",
		"env", cfg.Env,
		"grpc_addr", cfg.Server.GRPCAddr,
		"metrics_addr", cfg.Server.MetricsAddr,
		"dispatcher_enabled", cfg.Dispatcher.Enabled,
		"sweeper_enabled", cfg.Sweeper.Enabled,
		"reaper_enabled", cfg.Reaper.Enabled,
	)

	var wg sync.WaitGroup
	startBackgroundLoops(ctx, &wg, cfg, st, idx, policy, metrics, log)

	grpcServer, err := startGRPC(cfg, st, idx, metrics, log)
	if err != nil {
		return err
	}

	admin := &http.Server{
		Addr:              cfg.Server.MetricsAddr,
		Handler:           adminMux(st, idx, metrics),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 2)
	go func() {
		if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("admin server: %w", err)
		}
	}()
	go func() {
		listener, err := net.Listen("tcp", cfg.Server.GRPCAddr)
		if err != nil {
			serverErr <- fmt.Errorf("listen on %s: %w", cfg.Server.GRPCAddr, err)
			return
		}
		if err := grpcServer.Serve(listener); err != nil {
			serverErr <- fmt.Errorf("grpc server: %w", err)
		}
	}()

	select {
	case err := <-serverErr:
		stop()
		return err
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
	}

	// GracefulStop lets in-flight RPCs finish; the timer bounds how long a slow
	// client can hold shutdown open.
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(cfg.Server.ShutdownTimeout):
		log.Warn("grpc did not drain in time, forcing stop")
		grpcServer.Stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()
	if err := admin.Shutdown(shutdownCtx); err != nil {
		log.Warn("admin server did not shut down cleanly", "error", err)
	}

	wg.Wait()
	log.Info("jobqueued stopped")
	return nil
}

func startGRPC(
	cfg config.Config,
	st *store.Store,
	idx *index.Index,
	metrics *observability.Metrics,
	log *slog.Logger,
) (*grpc.Server, error) {
	svc := api.NewServer(st, idx, cfg.Retry, log,
		api.WithEnqueueObserver(
			func(queue string, deduplicated bool) {
				metrics.Enqueued.WithLabelValues(queue, boolLabel(deduplicated)).Inc()
			},
			func(queue string) {
				metrics.IndexWriteFailures.WithLabelValues(queue).Inc()
			},
		),
	)

	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			observability.RecoveryInterceptor(log),
			observability.LoggingInterceptor(log),
		),
	)
	jobqueuev1.RegisterJobQueueServer(server, svc)

	// A health service and reflection make the server usable from grpcurl and
	// from orchestrators without any client-side setup.
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("jobqueue.v1.JobQueue", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthSrv)
	reflection.Register(server)

	return server, nil
}

func startBackgroundLoops(
	ctx context.Context,
	wg *sync.WaitGroup,
	cfg config.Config,
	st *store.Store,
	idx *index.Index,
	policy backoff.Policy,
	metrics *observability.Metrics,
	log *slog.Logger,
) {
	run := func(fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(ctx)
		}()
	}

	if cfg.Dispatcher.Enabled {
		d := scheduler.NewDispatcher(idx, cfg.Dispatcher, log)
		d.OnDispatched(func(queue string, n int) {
			metrics.Dispatched.WithLabelValues(queue).Add(float64(n))
		})
		run(d.Run)
	}

	if cfg.Sweeper.Enabled {
		s := scheduler.NewSweeper(st, idx, cfg.Sweeper, log)
		s.OnBackfilled(func(queue string, n int) {
			metrics.SweeperBackfilled.WithLabelValues(queue).Add(float64(n))
		})
		run(s.Run)
	}

	if cfg.Reaper.Enabled {
		r := scheduler.NewReaper(st, idx, cfg.Reaper, policy, log)
		r.OnReclaimed(func(queue string, dead bool) {
			outcome := "retried"
			if dead {
				outcome = "dead_lettered"
			}
			metrics.ReaperReclaimed.WithLabelValues(queue, outcome).Inc()
		})
		run(r.Run)
	}
}

func adminMux(st *store.Store, idx *index.Index, metrics *observability.Metrics) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())

	// Liveness: the process is up. Deliberately does not touch a dependency --
	// restarting the process would not fix an unreachable database, and doing
	// so during an outage turns a degradation into an outage.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// Readiness: the process can actually serve. This one does check, so a
	// replica that has lost its database is taken out of rotation.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := st.Ping(ctx); err != nil {
			http.Error(w, fmt.Sprintf("postgres: %v", err), http.StatusServiceUnavailable)
			return
		}
		if err := idx.Ping(ctx); err != nil {
			http.Error(w, fmt.Sprintf("redis: %v", err), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})

	return mux
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
