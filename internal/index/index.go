// Package index is the Redis scheduling index.
//
// It holds one sorted set per queue, scored by execution time, plus a ready
// list of job ids that are due now. Nothing here is a source of truth: every
// entry is derived from a row in PostgreSQL, and the sweeper can rebuild the
// whole thing from scratch. That is deliberate -- it means a failed write to
// Redis costs latency rather than a lost job, and Redis needs no persistence.
package index

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/Rishabh-10/durable-job-queue/internal/config"
)

// popDueScript moves due job ids from the sorted set to the ready list.
//
//go:embed pop_due.lua
var popDueScript string

// Index is a handle on the Redis scheduling index.
type Index struct {
	client redis.UniversalClient
	popDue *redis.Script
}

// Open connects to Redis and verifies the connection.
func Open(ctx context.Context, cfg config.Redis) (*Index, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis at %s: %w", cfg.Addr, err)
	}
	return New(client), nil
}

// New wraps an existing client. Tests use it to share a container.
func New(client redis.UniversalClient) *Index {
	return &Index{client: client, popDue: redis.NewScript(popDueScript)}
}

// Close releases the connection pool.
func (i *Index) Close() error { return i.client.Close() }

// Ping reports whether Redis is reachable, for the health check.
func (i *Index) Ping(ctx context.Context) error { return i.client.Ping(ctx).Err() }

// Key layout. The queue name is hash-tagged so all of one queue's keys land on
// the same slot, which keeps the Lua script single-slot under Redis Cluster.
func schedKey(queue string) string { return fmt.Sprintf("jq:{%s}:sched", queue) }
func readyKey(queue string) string { return fmt.Sprintf("jq:{%s}:ready", queue) }

// queuesKey tracks which queues exist, so the dispatcher can find them without
// scanning the keyspace.
const queuesKey = "jq:queues"

// Entry is one job's position in the index.
type Entry struct {
	ID    uuid.UUID
	RunAt time.Time
}

// score converts an execution time to a sorted-set score. Milliseconds since
// the epoch keeps ordering exact within the resolution anything here cares
// about, and stays well inside the float64 mantissa for any realistic date.
func score(t time.Time) float64 { return float64(t.UnixMilli()) }

// Schedule adds or moves a job in the index.
//
// Callers must have committed the job to PostgreSQL first. If this fails, the
// job is still durable and the sweeper will index it on its next pass, so the
// error is worth logging but never worth failing an enqueue over.
func (i *Index) Schedule(ctx context.Context, queue string, e Entry) error {
	pipe := i.client.TxPipeline()
	pipe.SAdd(ctx, queuesKey, queue)
	pipe.ZAdd(ctx, schedKey(queue), redis.Z{Score: score(e.RunAt), Member: e.ID.String()})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("schedule job %s on %s: %w", e.ID, queue, err)
	}
	return nil
}

// ScheduleBatch indexes many jobs on one queue in a single round trip.
//
// ZADD on a member that is already present simply updates its score, so
// re-indexing a job the sweeper has already seen is free. That idempotency is
// what lets the sweeper run continuously without tracking what it has done.
func (i *Index) ScheduleBatch(ctx context.Context, queue string, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	members := make([]redis.Z, 0, len(entries))
	for _, e := range entries {
		members = append(members, redis.Z{Score: score(e.RunAt), Member: e.ID.String()})
	}

	pipe := i.client.TxPipeline()
	pipe.SAdd(ctx, queuesKey, queue)
	pipe.ZAdd(ctx, schedKey(queue), members...)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("schedule %d jobs on %s: %w", len(entries), queue, err)
	}
	return nil
}

// Unschedule removes a job from the index, for cancellation.
//
// Best effort by nature: the id may already have moved to the ready list, or be
// in a worker's hands. The claim in PostgreSQL is what actually stops a
// cancelled job from running; this only saves the dispatcher some work.
func (i *Index) Unschedule(ctx context.Context, queue string, id uuid.UUID) error {
	if err := i.client.ZRem(ctx, schedKey(queue), id.String()).Err(); err != nil {
		return fmt.Errorf("unschedule job %s from %s: %w", id, queue, err)
	}
	return nil
}

// PopDue atomically moves up to limit due job ids from the sorted set to the
// ready list and returns them.
//
// Atomicity is the whole point: a read-then-remove would let two dispatchers
// hand the same job to two workers. They would still be stopped by the claim in
// PostgreSQL, but only after both had done the work of fetching it.
func (i *Index) PopDue(ctx context.Context, queue string, now time.Time, limit int) ([]uuid.UUID, error) {
	raw, err := i.popDue.Run(ctx, i.client,
		[]string{schedKey(queue), readyKey(queue)},
		now.UnixMilli(), limit,
	).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("pop due jobs from %s: %w", queue, err)
	}

	values, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("pop due jobs from %s: unexpected reply type %T", queue, raw)
	}

	ids := make([]uuid.UUID, 0, len(values))
	for _, v := range values {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("pop due jobs from %s: unexpected member type %T", queue, v)
		}
		id, err := uuid.Parse(s)
		if err != nil {
			// A malformed member can never become a job. Dropping it is better
			// than wedging the dispatcher on every pass; the Lua script has
			// already removed it from the sorted set.
			return nil, fmt.Errorf("pop due jobs from %s: member %q is not a uuid: %w", queue, s, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ErrNoJob is returned by Next when the poll timed out with nothing ready.
var ErrNoJob = errors.New("no job ready")

// Next blocks until a job id is ready on one of the given queues, or timeout
// elapses. Queues are polled in the order given, so earlier queues have
// priority.
//
// The returned id may belong to a job that is cancelled, already claimed, or
// gone; the caller resolves that by claiming it in PostgreSQL.
func (i *Index) Next(ctx context.Context, queues []string, timeout time.Duration) (queue string, id uuid.UUID, err error) {
	if len(queues) == 0 {
		return "", uuid.Nil, fmt.Errorf("no queues to poll")
	}
	keys := make([]string, 0, len(queues))
	for _, q := range queues {
		keys = append(keys, readyKey(q))
	}

	res, err := i.client.BLPop(ctx, timeout, keys...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", uuid.Nil, ErrNoJob
		}
		return "", uuid.Nil, fmt.Errorf("poll ready queues: %w", err)
	}
	if len(res) != 2 {
		return "", uuid.Nil, fmt.Errorf("poll ready queues: unexpected reply %v", res)
	}

	// BLPOP answers with the key it popped from, which maps back to the queue.
	for n, key := range keys {
		if key == res[0] {
			queue = queues[n]
			break
		}
	}
	if queue == "" {
		return "", uuid.Nil, fmt.Errorf("poll ready queues: unknown key %q in reply", res[0])
	}

	id, err = uuid.Parse(res[1])
	if err != nil {
		return "", uuid.Nil, fmt.Errorf("poll ready queues: member %q is not a uuid: %w", res[1], err)
	}
	return queue, id, nil
}

// Queues lists the queues the index knows about.
func (i *Index) Queues(ctx context.Context) ([]string, error) {
	queues, err := i.client.SMembers(ctx, queuesKey).Result()
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	return queues, nil
}

// RegisterQueue records a queue so the dispatcher polls it even before anything
// has been enqueued to it.
func (i *Index) RegisterQueue(ctx context.Context, queue string) error {
	if err := i.client.SAdd(ctx, queuesKey, queue).Err(); err != nil {
		return fmt.Errorf("register queue %s: %w", queue, err)
	}
	return nil
}

// Depth reports how many jobs are waiting in the index and how many have been
// handed to the ready list but not yet claimed.
func (i *Index) Depth(ctx context.Context, queue string) (scheduled, ready int64, err error) {
	pipe := i.client.Pipeline()
	sched := pipe.ZCard(ctx, schedKey(queue))
	rdy := pipe.LLen(ctx, readyKey(queue))
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, 0, fmt.Errorf("measure depth of %s: %w", queue, err)
	}
	return sched.Val(), rdy.Val(), nil
}
