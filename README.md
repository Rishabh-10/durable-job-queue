# durable-job-queue

A durable delayed job queue in Go. Schedule work for later, retry it when it fails, and
don't lose it when a component dies.

PostgreSQL is the system of record. Redis holds a sorted set scored by execution time
that acts as the scheduling index — and it can be thrown away at any moment, because
everything in it is derived from PostgreSQL.

---

## Why it's built this way

Scheduling delayed work needs two things that no single store does well:

- **Durability.** A job accepted at 2pm and due at 8pm must survive anything that
  happens in between. That is a database's job.
- **Cheap "what is due now?" polling, several times a second.** That is a Redis sorted
  set's job — `ZRANGEBYSCORE ... LIMIT` is O(log N), where the equivalent database poll
  becomes a hot query on a hot index.

So the system uses both, with a strict rule about which one is allowed to be wrong:

> **PostgreSQL is the truth. Redis is a cache of "what is due soon."**
> Every write commits to PostgreSQL *before* Redis is touched, and Redis can be flushed
> entirely at any point without losing a job.

### The half-enqueue problem

Enqueueing is two writes across two systems with no transaction spanning them: an
`INSERT` into PostgreSQL, then a `ZADD` into Redis. If the process dies between them, the
job is durable but invisible to the scheduler.

Rather than reach for a distributed transaction, the `ZADD` is treated as an
**optimization rather than a requirement**, and a **sweeper** loop continuously
reconciles the difference:

```mermaid
sequenceDiagram
    autonumber
    actor C as client
    participant A as gRPC API
    participant P as PostgreSQL
    participant R as Redis
    participant S as sweeper
    participant W as worker

    C->>A: Enqueue, run in 6 hours
    A->>P: INSERT job, state = pending
    P-->>A: committed
    A-xR: ZADD fails, or the process dies right here
    A-->>C: accepted

    Note over P,R: The job is durable, but nothing is watching the clock for it

    loop every sweep interval
        S->>P: SELECT pending WHERE run_at <= now() + lookahead
        P-->>S: the orphaned job
        S->>R: ZADD, idempotent
    end

    R->>W: dispatched, claimed, run
    Note over C,W: Cost: one sweep interval of latency. Never a lost job.
```

`ZADD` on an existing member is an idempotent score update, so re-adding a job the
sweeper has already seen is free. That is what lets the sweeper run continuously without
tracking what it has done.

That single mechanism covers three failure modes at once:

| Failure | What happens |
|---|---|
| Redis unreachable during enqueue | Job still commits; sweeper indexes it within one sweep interval |
| Process dies between the two writes | Same — the job is late, never lost |
| Redis restarts empty, or is flushed | Sweeper rebuilds the entire index from PostgreSQL |

The cost is bounded latency (one sweep interval, 10s by default), never data loss.
Doing the writes in the other order — Redis first — would instead produce phantom jobs
that exist in the scheduler but not in the record, which is unrecoverable. Hence
PostgreSQL first, always.

---

## Architecture

```mermaid
flowchart TB
    client([client])
    api["gRPC API"]
    pg[("PostgreSQL<br/>system of record")]
    sched[("Redis ZSET<br/>scored by run_at")]
    disp["dispatcher"]
    ready[("Redis list<br/>due now")]
    pool["worker pool"]
    swp["sweeper"]
    rpr["reaper"]

    client -->|Enqueue| api
    api ==>|"1 · INSERT, commit"| pg
    api -.->|"2 · ZADD, best effort"| sched

    pg -->|"pending and due"| swp
    swp -.->|"ZADD backfill"| sched
    pg -->|"expired leases"| rpr
    rpr ==>|"retry or dead-letter"| pg
    rpr -.->|"re-index"| sched

    sched -.->|"Lua: pop due"| disp
    disp -.->|RPUSH| ready
    ready -.->|BLPOP| pool
    pool ==>|"claim, ack, nack"| pg
```

Read the edge styles: **thick** edges are writes to the system of record, plain edges are
reads from it, and **dotted** edges touch Redis. Every dotted edge is recoverable —
nothing in that half is a source of truth, which is why losing all of it costs latency
rather than jobs.

| Component | Responsibility |
|---|---|
| `api` | gRPC surface: enqueue, get, cancel, list, dead-letter inspect/requeue, stats |
| `dispatcher` | Atomically moves due job ids out of the sorted set into a ready list |
| `worker` | `BLPOP` ready list → claim a lease in PostgreSQL → run handler → ack/nack |
| `sweeper` | Reconciles `pending` jobs missing from Redis — the durability net |
| `reaper` | Reconciles `running` jobs whose lease expired — recovers crashed workers |

Two binaries: `jobqueued` runs the API and all three background loops; `worker` runs the
executor pool. Both scale horizontally with **no leader election** — every background
loop reconciles through conditional single-statement `UPDATE`s, so N replicas racing on
the same row is safe by construction and exactly one wins.

---

## Delivery semantics

**At-least-once. Handlers must be idempotent.**

```mermaid
stateDiagram-v2
    direction LR
    [*] --> pending: Enqueue

    pending --> running: claim
    pending --> cancelled: cancel

    running --> succeeded: ok
    running --> pending: failed, backoff
    running --> pending: lease expired
    running --> dead: attempts exhausted
    running --> dead: unretryable

    dead --> pending: requeue

    succeeded --> [*]
    cancelled --> [*]
```

Note the two arrows back to `pending`: a handler that failed and a worker that died are
handled by the same path, which is why a crash needs no recovery logic of its own.

A worker can finish a job and die before its acknowledgement commits. The reaper will
see the expired lease and run the job again. Exactly-once is not offered, because it
cannot be honestly delivered across a handler's arbitrary side effects — a handler that
charges a credit card and then loses its network connection has already charged the card.

Concurrent execution is guarded by making the claim a single atomic statement:

```sql
UPDATE jobs
   SET state='running', attempts=attempts+1,
       lease_owner=$2, lease_expires_at=now()+$3
 WHERE id=$1 AND state='pending' AND run_at<=now()
RETURNING *;
```

Zero rows back means someone else claimed it, or it was cancelled, and the worker drops
it silently. Long-running handlers heartbeat to extend `lease_expires_at`, so a slow job
is distinguishable from a dead one.

Failed jobs retry with exponential backoff and full jitter — `min(base · 2^(n-1), cap)`,
defaulting to 1s base and 1h cap. The jitter is not decoration: without it, a downstream
outage produces synchronized retry herds when it recovers. Jobs that exhaust
`max_attempts` move to `dead` and can be inspected and requeued over the API.

---

## Quick start

Requires Go 1.25+, Docker, and [`buf`](https://buf.build/docs/installation).

```bash
make up          # start PostgreSQL and Redis, wait for health
make migrate-up  # apply database migrations
make demo        # end-to-end walkthrough: delays, retries, the DLQ, 500 jobs
```

`make demo` schedules a delayed job, drives another through its retries into the
dead-letter queue, requeues it, cancels a third, and pushes 500 jobs through a single
worker, printing what happens at each step.

It empties the queue first, so the counts and the dead-letter listing describe that run
and nothing else. The reset refuses to touch a `JQ_POSTGRES_DSN` that is not local, and
`JQ_DEMO_KEEP_STATE=1` skips it entirely.

To drive it by hand instead, in three shells:

```bash
./bin/jobqueued                              # API + dispatcher + sweeper + reaper
./bin/worker                                 # executor pool
./bin/jobqctl enqueue -kind echo -delay 10s  # schedule something
```

Everything defaults to the docker-compose stack, so no environment variables are needed
for local development. `make help` lists every target.

If something already owns port 5432 or 6379 on your machine, override just the host
side: `JQ_REDIS_HOST_PORT=6380 make up`, then point the binaries at it with
`JQ_REDIS_ADDR=localhost:6380`.

### Working on it

```bash
make generate         # regenerate gRPC stubs and sqlc query code
make check            # format, vet, unit tests
make test-integration # everything above, plus container-backed tests
```

---

## Configuration

All settings come from the environment and have working defaults. Invalid configuration
fails at startup, reporting *every* problem at once rather than one per restart.

| Variable | Default | Purpose |
|---|---|---|
| `JQ_POSTGRES_DSN` | `postgres://jobqueue:jobqueue@localhost:5432/jobqueue?sslmode=disable` | System of record |
| `JQ_REDIS_ADDR` | `localhost:6379` | Scheduling index |
| `JQ_GRPC_ADDR` | `:8080` | gRPC listener |
| `JQ_METRICS_ADDR` | `:9090` | Metrics and health listener |
| `JQ_DISPATCHER_INTERVAL` | `250ms` | How often due ids are moved to the ready list |
| `JQ_SWEEPER_INTERVAL` | `10s` | Worst-case delay for a job whose `ZADD` never landed |
| `JQ_SWEEPER_LOOKAHEAD` | `30s` | How far ahead to index, so jobs are ready *before* they are due |
| `JQ_REAPER_INTERVAL` | `15s` | How often expired leases are reclaimed |
| `JQ_WORKER_QUEUES` | `default` | Comma-separated queues to consume |
| `JQ_WORKER_CONCURRENCY` | `8` | In-flight jobs per worker process |
| `JQ_WORKER_LEASE_TTL` | `30s` | How long a claim survives without a heartbeat |
| `JQ_WORKER_JOB_TIMEOUT` | `5m` | Handler deadline |
| `JQ_DEFAULT_MAX_ATTEMPTS` | `5` | Attempts before dead-lettering |
| `JQ_BACKOFF_BASE` / `JQ_BACKOFF_CAP` | `1s` / `1h` | Retry backoff curve |

Three of these are cross-validated at startup, because they are the ones that silently
break durability when they drift apart:

- `JQ_REAPER_INTERVAL` ≤ `JQ_WORKER_LEASE_TTL` — otherwise a dead worker's jobs stall far
  longer than the lease promised they would.
- `JQ_SWEEPER_LOOKAHEAD` ≥ `JQ_SWEEPER_INTERVAL` — otherwise the sweeper only ever
  indexes jobs that are already late.
- `JQ_BACKOFF_CAP` ≥ `JQ_BACKOFF_BASE` — otherwise the backoff curve is inverted.

---

## API

Defined in [`proto/jobqueue/v1/jobqueue.proto`](proto/jobqueue/v1/jobqueue.proto).
Reflection is enabled, so `grpcurl` works without the proto file on hand.

| RPC | Purpose |
|---|---|
| `Enqueue` | Schedule a job, absolutely (`run_at`) or relatively (`delay`) |
| `GetJob` | Fetch a job with its full attempt history |
| `CancelJob` | Stop a job that has not reached a terminal state |
| `ListJobs` | Page through a queue, optionally filtered by state |
| `ListDeadLetters` | Page through jobs that exhausted their attempts |
| `RequeueDeadLetter` | Return a dead job to `pending` with additional attempts |
| `QueueStats` | Per-state counts, index size, and scheduling lag |

`Enqueue` accepts an optional `idempotency_key`, unique per queue. Re-enqueueing with a
key that already exists returns the existing job with `already_existed = true` rather
than creating a duplicate or raising an error — so a client that retries a failed
`Enqueue` cannot accidentally schedule the same work twice.

---

## Command line

`jobqctl` speaks to the server over gRPC.

```bash
jobqctl enqueue -kind echo -payload '{"hi":true}' -delay 30s
jobqctl enqueue -kind charge -idempotency-key order-42   # safe to retry
jobqctl watch <job-id>                                   # follow to completion
jobqctl get <job-id>                                     # job plus attempt history
jobqctl ls -state pending,running
jobqctl cancel <job-id>
jobqctl dlq ls
jobqctl dlq requeue <job-id> -additional-attempts 3
jobqctl stats -watch 2s
```

`-addr` (or `$JQ_ADDR`) points it at a server other than `localhost:8080`.

---

## Observability

Both binaries expose `/metrics`, `/healthz` and, on the server, `/readyz` — the server
on `:9090`, workers on `:9091`.

Liveness deliberately does not touch PostgreSQL or Redis: restarting a process will not
fix an unreachable database, and killing every replica during a dependency outage turns
a degradation into an outage. Readiness does check, so a replica that has lost its
database is taken out of rotation without being killed.

The metrics worth alerting on:

| Metric | What it tells you |
|---|---|
| `jobqueue_dispatch_lag_seconds` | How late jobs actually start. The promise the queue makes. |
| `jobqueue_sweeper_backfilled_total` | **Near zero when healthy.** A sustained rate means enqueues are not reaching Redis and the durability net is load-bearing. |
| `jobqueue_reaper_reclaimed_total` | Workers dying mid-job. |
| `jobqueue_executed_total{outcome}` | Success, retry, dead-letter and lost-lease rates. |
| `jobqueue_index_write_failures_total` | Enqueues that committed but could not index. |

---

## Testing

```bash
make check             # unit tests: backoff curve, config validation
make test-integration  # plus real PostgreSQL and Redis, via testcontainers
```

The integration suite is where the durability claims are actually checked, not asserted.
Its three central tests inject the failures the design exists to survive:

- **`TestHalfCompletedEnqueueIsRecovered`** enqueues through an API server whose Redis
  client points at a dead port. The job commits, the index write fails, and the test
  asserts the index is genuinely empty — then starts the system and watches the sweeper
  find and run a job nothing ever told Redis about.
- **`TestRedisDataLossLosesNoJobs`** enqueues 100 jobs, confirms all 100 are indexed,
  `FLUSHALL`s Redis, and asserts all 100 still run exactly once.
- **`TestCrashedWorkerJobIsRecovered`** claims a job with an already-lapsed lease — the
  exact state a killed worker leaves behind — and asserts the reaper returns it to the
  queue, that it runs, and that the dead worker's attempt stays on the record.

Alongside those: exclusive claiming under 20 racing workers, idempotent enqueue under 50
concurrent callers, 300 jobs across 10 workers with no duplicates, retry-to-dead-letter
attempt accounting, and the gRPC surface over a real connection (status codes,
pagination, validation).

---

## Design notes

A few decisions worth calling out, and what they rule out:

**No Redis-side in-flight list.** The usual reliable-queue pattern moves ids through a
processing list with `BRPOPLPUSH`. That is redundant here: if a worker dies between
`BLPOP` and its claim, the job is still `pending` in PostgreSQL with `run_at` in the
past — precisely the sweeper's existing query. A processing list would add a second
reconciler and no extra safety.

**The dead-letter queue is a state, not a table.** `state = 'dead'` keeps a job's entire
history on one row, and requeueing is a single `UPDATE` rather than a cross-table move.

**Redis runs with persistence disabled** in [`deploy/docker-compose.yml`](deploy/docker-compose.yml)
— deliberately. The sweeper is supposed to be able to rebuild the index from scratch, and
running without an RDB or AOF locally keeps that recovery path exercised rather than
theoretical.

**Requeueing grants attempts rather than resetting them.** A dead job that is requeued
keeps its attempt counter and has its ceiling raised. Resetting to zero would make the
next attempt number 1 again, colliding with the attempt rows already on the job: the new
attempt would overwrite the old first one and strand the rest, so the history would claim
attempts happened after the requeue that actually happened before it. Counting on keeps
`job_attempts` append-only.

**`sqlc` over an ORM**, for SQL that is checked at compile time and visible at the call
site. The queries here are the interesting part of the system; hiding them behind a
query builder would obscure exactly the code a reader should be looking at.

---

## License

MIT — see [LICENSE](LICENSE).
