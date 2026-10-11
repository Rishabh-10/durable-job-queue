//go:build integration

// Package testsupport starts throwaway PostgreSQL and Redis containers for
// integration tests.
//
// Containers are started once per test binary and shared. Each test gets a
// clean slate through truncation rather than a fresh container, because
// starting PostgreSQL costs seconds and truncating costs microseconds.
package testsupport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Rishabh-10/durable-job-queue/internal/store"
)

var (
	pgOnce sync.Once
	pgPool *pgxpool.Pool
	pgErr  error

	redisOnce sync.Once
	redisAddr string
	redisErr  error
)

// Store returns a store backed by a shared PostgreSQL container, with the
// schema migrated and every table emptied.
func Store(t *testing.T) *store.Store {
	t.Helper()
	pool := pgPoolFor(t)
	truncate(t, pool)
	return store.NewWithPool(pool)
}

// Pool returns the shared pool directly, for tests that need raw SQL.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return pgPoolFor(t)
}

// Redis returns a client for a shared Redis container, flushed clean.
func Redis(t *testing.T) *redis.Client {
	t.Helper()
	addr := redisAddrFor(t)

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	return client
}

// RedisAddr returns the shared container's address, for code that builds its
// own client.
func RedisAddr(t *testing.T) string {
	t.Helper()
	return redisAddrFor(t)
}

func pgPoolFor(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pgOnce.Do(func() { pgPool, pgErr = startPostgres() })
	if pgErr != nil {
		t.Fatalf("start postgres container: %v", pgErr)
	}
	return pgPool
}

func redisAddrFor(t *testing.T) string {
	t.Helper()
	redisOnce.Do(func() { redisAddr, redisErr = startRedis() })
	if redisErr != nil {
		t.Fatalf("start redis container: %v", redisErr)
	}
	return redisAddr
}

func startPostgres() (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("jobqueue"),
		tcpostgres.WithUsername("jobqueue"),
		tcpostgres.WithPassword("jobqueue"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("run container: %w", err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("connection string: %w", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	if err := migrate(ctx, pool); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return pool, nil
}

func startRedis() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		return "", fmt.Errorf("run container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("host: %w", err)
	}
	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		return "", fmt.Errorf("mapped port: %w", err)
	}
	return fmt.Sprintf("%s:%s", host, port.Port()), nil
}

// migrate applies every .up.sql file in order. Using the files directly rather
// than a migration library keeps the test path identical to what production
// runs, with no second source of schema truth.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	dir, err := migrationsDir()
	if err != nil {
		return err
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		return fmt.Errorf("glob migrations: %w", err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("no migrations found in %s", dir)
	}
	sort.Strings(paths)

	for _, p := range paths {
		sqlText, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("read %s: %w", p, err)
		}
		if _, err := pool.Exec(ctx, string(sqlText)); err != nil {
			return fmt.Errorf("apply %s: %w", filepath.Base(p), err)
		}
	}
	return nil
}

// migrationsDir locates migrations/ relative to this source file, so tests work
// regardless of which package directory they run from.
func migrationsDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("cannot determine caller location")
	}
	root := filepath.Join(filepath.Dir(thisFile), "..", "..")
	dir := filepath.Join(root, "migrations")
	if _, err := os.Stat(dir); err != nil {
		return "", fmt.Errorf("locate migrations dir: %w", err)
	}
	return dir, nil
}

// truncate empties every table between tests. RESTART IDENTITY and CASCADE keep
// it a single statement regardless of foreign keys.
func truncate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, `TRUNCATE jobs, job_attempts RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}
