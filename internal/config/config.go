// Package config loads process configuration from the environment.
//
// Every setting has a default that works against the docker-compose stack in
// deploy/, so a fresh checkout runs with no environment set at all.
package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the union of settings used by the server and worker binaries.
// Each binary reads the sections it needs.
type Config struct {
	Env        string // "dev" or "prod"; controls log formatting
	LogLevel   string // debug, info, warn, error
	Postgres   Postgres
	Redis      Redis
	Server     Server
	Dispatcher Dispatcher
	Sweeper    Sweeper
	Reaper     Reaper
	Worker     Worker
	Retry      Retry
}

// Postgres configures the system of record.
type Postgres struct {
	DSN             string
	MaxConns        int32
	MinConns        int32
	ConnMaxLifetime time.Duration
}

// Redis configures the scheduling index.
type Redis struct {
	Addr     string
	Password string
	DB       int
	PoolSize int
}

// Server configures the listeners owned by the jobqueued binary.
type Server struct {
	GRPCAddr        string
	MetricsAddr     string
	ShutdownTimeout time.Duration
}

// Dispatcher moves due job ids from the Redis sorted set to the ready list.
type Dispatcher struct {
	Enabled   bool
	Interval  time.Duration
	BatchSize int
	// Queues the dispatcher polls. Empty means every queue registered in Redis.
	Queues []string
}

// Sweeper backfills due jobs from PostgreSQL into Redis. It is the reason a
// half-completed enqueue, or a total loss of Redis, costs latency rather than
// data.
type Sweeper struct {
	Enabled bool
	// How often to scan. Also the worst-case added latency for a job whose
	// ZADD never landed.
	Interval time.Duration
	// How far past the present to look, so jobs reach the index slightly
	// before they come due rather than exactly when they do.
	Lookahead time.Duration
	BatchSize int
}

// Reaper returns jobs whose worker died mid-execution to the pending state.
// This is what makes delivery at-least-once rather than at-most-once.
type Reaper struct {
	Enabled   bool
	Interval  time.Duration
	BatchSize int
}

// Worker configures the executor pool.
type Worker struct {
	// Stable identity recorded on leases and attempt rows. Defaults to the
	// hostname plus a random suffix.
	ID string
	// Queues to consume. Defaults to "default".
	Queues []string
	// Concurrent in-flight jobs per process.
	Concurrency int
	// How long a claim stays valid before the reaper may reclaim the job.
	LeaseTTL time.Duration
	// How long a handler may run before its context is cancelled.
	JobTimeout time.Duration
	// Blocking read timeout against the ready list.
	PollTimeout time.Duration
	// Grace period for in-flight jobs on shutdown.
	ShutdownTimeout time.Duration
	// MetricsAddr is the worker's own admin listener. It defaults to a
	// different port from the server's so both can run on one host.
	MetricsAddr string
}

// HeartbeatInterval is how often a running job extends its lease. Renewing at
// a third of the TTL tolerates two consecutive missed heartbeats.
func (w Worker) HeartbeatInterval() time.Duration {
	return w.LeaseTTL / 3
}

// Retry controls the delay between attempts and the dead-letter threshold.
type Retry struct {
	DefaultMaxAttempts int
	BackoffBase        time.Duration
	BackoffCap         time.Duration
}

// Load reads configuration from the environment, applying defaults for every
// unset variable. It reports every problem at once rather than failing on the
// first, so a misconfigured deployment takes one restart to fix instead of ten.
func Load() (Config, error) {
	var errs []error
	fail := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	cfg := Config{
		Env:      envString("JQ_ENV", "dev"),
		LogLevel: envString("JQ_LOG_LEVEL", "info"),
		Postgres: Postgres{
			DSN: envString("JQ_POSTGRES_DSN",
				"postgres://jobqueue:jobqueue@localhost:5432/jobqueue?sslmode=disable"),
		},
		Redis: Redis{
			Addr:     envString("JQ_REDIS_ADDR", "localhost:6379"),
			Password: envString("JQ_REDIS_PASSWORD", ""),
		},
		Server: Server{
			GRPCAddr:    envString("JQ_GRPC_ADDR", ":8080"),
			MetricsAddr: envString("JQ_METRICS_ADDR", ":9090"),
		},
		Dispatcher: Dispatcher{
			Queues: envStringSlice("JQ_DISPATCHER_QUEUES", nil),
		},
		Worker: Worker{
			ID:          envString("JQ_WORKER_ID", ""),
			Queues:      envStringSlice("JQ_WORKER_QUEUES", []string{"default"}),
			MetricsAddr: envString("JQ_WORKER_METRICS_ADDR", ":9091"),
		},
	}

	fail(envInt32(&cfg.Postgres.MaxConns, "JQ_POSTGRES_MAX_CONNS", 16))
	fail(envInt32(&cfg.Postgres.MinConns, "JQ_POSTGRES_MIN_CONNS", 2))
	fail(envDuration(&cfg.Postgres.ConnMaxLifetime, "JQ_POSTGRES_CONN_MAX_LIFETIME", time.Hour))

	fail(envInt(&cfg.Redis.DB, "JQ_REDIS_DB", 0))
	fail(envInt(&cfg.Redis.PoolSize, "JQ_REDIS_POOL_SIZE", 16))

	fail(envDuration(&cfg.Server.ShutdownTimeout, "JQ_SHUTDOWN_TIMEOUT", 30*time.Second))

	fail(envBool(&cfg.Dispatcher.Enabled, "JQ_DISPATCHER_ENABLED", true))
	fail(envDuration(&cfg.Dispatcher.Interval, "JQ_DISPATCHER_INTERVAL", 250*time.Millisecond))
	fail(envInt(&cfg.Dispatcher.BatchSize, "JQ_DISPATCHER_BATCH_SIZE", 256))

	fail(envBool(&cfg.Sweeper.Enabled, "JQ_SWEEPER_ENABLED", true))
	fail(envDuration(&cfg.Sweeper.Interval, "JQ_SWEEPER_INTERVAL", 10*time.Second))
	fail(envDuration(&cfg.Sweeper.Lookahead, "JQ_SWEEPER_LOOKAHEAD", 30*time.Second))
	fail(envInt(&cfg.Sweeper.BatchSize, "JQ_SWEEPER_BATCH_SIZE", 500))

	fail(envBool(&cfg.Reaper.Enabled, "JQ_REAPER_ENABLED", true))
	fail(envDuration(&cfg.Reaper.Interval, "JQ_REAPER_INTERVAL", 15*time.Second))
	fail(envInt(&cfg.Reaper.BatchSize, "JQ_REAPER_BATCH_SIZE", 200))

	fail(envInt(&cfg.Worker.Concurrency, "JQ_WORKER_CONCURRENCY", 8))
	fail(envDuration(&cfg.Worker.LeaseTTL, "JQ_WORKER_LEASE_TTL", 30*time.Second))
	fail(envDuration(&cfg.Worker.JobTimeout, "JQ_WORKER_JOB_TIMEOUT", 5*time.Minute))
	fail(envDuration(&cfg.Worker.PollTimeout, "JQ_WORKER_POLL_TIMEOUT", 5*time.Second))
	fail(envDuration(&cfg.Worker.ShutdownTimeout, "JQ_WORKER_SHUTDOWN_TIMEOUT", 30*time.Second))

	fail(envInt(&cfg.Retry.DefaultMaxAttempts, "JQ_DEFAULT_MAX_ATTEMPTS", 5))
	fail(envDuration(&cfg.Retry.BackoffBase, "JQ_BACKOFF_BASE", time.Second))
	fail(envDuration(&cfg.Retry.BackoffCap, "JQ_BACKOFF_CAP", time.Hour))

	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	var errs []error
	require := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}

	require(c.Postgres.DSN != "", "JQ_POSTGRES_DSN must not be empty")
	require(c.Redis.Addr != "", "JQ_REDIS_ADDR must not be empty")
	require(c.Postgres.MaxConns >= c.Postgres.MinConns,
		"JQ_POSTGRES_MAX_CONNS (%d) must be >= JQ_POSTGRES_MIN_CONNS (%d)",
		c.Postgres.MaxConns, c.Postgres.MinConns)
	require(c.Dispatcher.BatchSize > 0, "JQ_DISPATCHER_BATCH_SIZE must be > 0")
	require(c.Sweeper.BatchSize > 0, "JQ_SWEEPER_BATCH_SIZE must be > 0")
	require(c.Reaper.BatchSize > 0, "JQ_REAPER_BATCH_SIZE must be > 0")
	require(c.Worker.Concurrency > 0, "JQ_WORKER_CONCURRENCY must be > 0")
	require(c.Worker.MetricsAddr != "", "JQ_WORKER_METRICS_ADDR must not be empty")
	require(c.Retry.DefaultMaxAttempts > 0, "JQ_DEFAULT_MAX_ATTEMPTS must be > 0")
	require(c.Retry.BackoffBase > 0, "JQ_BACKOFF_BASE must be > 0")
	require(c.Retry.BackoffCap >= c.Retry.BackoffBase,
		"JQ_BACKOFF_CAP (%s) must be >= JQ_BACKOFF_BASE (%s)",
		c.Retry.BackoffCap, c.Retry.BackoffBase)

	// A running job keeps its lease alive by heartbeat, so the TTL only has to
	// leave room for a few of them.
	require(c.Worker.LeaseTTL >= 3*time.Second,
		"JQ_WORKER_LEASE_TTL (%s) must be at least 3s so heartbeats have room",
		c.Worker.LeaseTTL)

	// The reaper must not run so rarely that a dead worker's jobs stall for far
	// longer than the lease promises.
	require(c.Reaper.Interval <= c.Worker.LeaseTTL,
		"JQ_REAPER_INTERVAL (%s) should not exceed JQ_WORKER_LEASE_TTL (%s)",
		c.Reaper.Interval, c.Worker.LeaseTTL)

	// The index must be populated before jobs come due, not after.
	require(c.Sweeper.Lookahead >= c.Sweeper.Interval,
		"JQ_SWEEPER_LOOKAHEAD (%s) must be >= JQ_SWEEPER_INTERVAL (%s)",
		c.Sweeper.Lookahead, c.Sweeper.Interval)

	return errors.Join(errs...)
}

func envString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envStringSlice(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func envInt(dst *int, key string, def int) error {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		*dst = def
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("%s: %q is not an integer", key, v)
	}
	*dst = n
	return nil
}

func envInt32(dst *int32, key string, def int32) error {
	var n int
	if err := envInt(&n, key, int(def)); err != nil {
		return err
	}
	if n > math.MaxInt32 || n < math.MinInt32 {
		return fmt.Errorf("%s: %d is out of range for an int32", key, n)
	}
	*dst = int32(n)
	return nil
}

func envBool(dst *bool, key string, def bool) error {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		*dst = def
		return nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fmt.Errorf("%s: %q is not a boolean", key, v)
	}
	*dst = b
	return nil
}

func envDuration(dst *time.Duration, key string, def time.Duration) error {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		*dst = def
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("%s: %q is not a duration (want e.g. 30s, 5m)", key, v)
	}
	*dst = d
	return nil
}
