-- name: CreateJob :one
-- Insert a job. When an idempotency key collides, nothing is written and no
-- row comes back; the caller then reads the existing job. This is what lets a
-- client safely retry an Enqueue whose response it never saw.
INSERT INTO jobs (
    id, queue, kind, payload, state, run_at, max_attempts, idempotency_key
) VALUES (
    @id, @queue, @kind, @payload, 'pending', @run_at, @max_attempts, sqlc.narg('idempotency_key')
)
ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING *;

-- name: GetJob :one
SELECT * FROM jobs WHERE id = @id;

-- name: GetJobByIdempotencyKey :one
SELECT * FROM jobs WHERE queue = @queue AND idempotency_key = @idempotency_key;

-- name: CancelJob :one
-- Only a job that has not started can be cancelled. A running job would have to
-- cooperate, and a finished one has nothing left to stop.
UPDATE jobs
SET state = 'cancelled', updated_at = now()
WHERE id = @id AND state = 'pending'
RETURNING *;

-- name: ClaimJob :one
-- The single guard against two workers running the same job. Zero rows back
-- means someone else won the race, or the job was cancelled after it was
-- dispatched, and the caller must drop it.
UPDATE jobs
SET state = 'running',
    attempts = attempts + 1,
    lease_owner = @lease_owner,
    lease_expires_at = now() + sqlc.arg('lease_ttl')::interval,
    updated_at = now()
WHERE id = @id
  AND state = 'pending'
  AND run_at <= now()
RETURNING *;

-- name: ExtendLease :one
-- Heartbeat for a long-running handler. Scoped to the owner so a worker that
-- lost its lease to the reaper cannot take it back and produce a second runner.
UPDATE jobs
SET lease_expires_at = now() + sqlc.arg('lease_ttl')::interval,
    updated_at = now()
WHERE id = @id AND state = 'running' AND lease_owner = @lease_owner
RETURNING *;

-- name: MarkJobSucceeded :one
UPDATE jobs
SET state = 'succeeded',
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_error = NULL,
    updated_at = now()
WHERE id = @id AND state = 'running' AND lease_owner = @lease_owner
RETURNING *;

-- name: MarkJobForRetry :one
-- A failed attempt with budget left: back to pending, due after the backoff
-- the caller computed.
UPDATE jobs
SET state = 'pending',
    run_at = @run_at,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_error = @last_error,
    updated_at = now()
WHERE id = @id AND state = 'running' AND lease_owner = @lease_owner
RETURNING *;

-- name: MarkJobDead :one
-- Attempts exhausted. The row keeps its history and becomes visible through the
-- dead_letters view.
UPDATE jobs
SET state = 'dead',
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_error = @last_error,
    updated_at = now()
WHERE id = @id AND state = 'running' AND lease_owner = @lease_owner
RETURNING *;

-- name: RequeueDeadLetter :one
-- Give a dead job more attempts rather than resetting its counter to zero.
--
-- Resetting would make the next attempt number 1 again, colliding with the
-- attempt rows already on the job: the new attempt would overwrite the old
-- first attempt and leave the later ones stranded, so the history would claim
-- attempts happened after the requeue that actually happened before it.
-- Counting on keeps job_attempts append-only and the record honest.
UPDATE jobs
SET state = 'pending',
    run_at = @run_at,
    max_attempts = attempts + @additional_attempts,
    lease_owner = NULL,
    lease_expires_at = NULL,
    updated_at = now()
WHERE id = @id AND state = 'dead'
RETURNING *;

-- name: ListJobs :many
-- Keyset pagination over (created_at, id) so a page stays stable while jobs are
-- being inserted. An empty state filter matches every state.
--
-- The filter compares as text rather than job_state[] so the pool does not have
-- to register the enum's OID on every connection. The plan is unaffected: this
-- listing is driven by the (queue, created_at, id) index, and state is only ever
-- a filter on the rows that index already returned.
SELECT * FROM jobs
WHERE queue = @queue
  AND (cardinality(@states::text[]) = 0 OR state::text = ANY (@states::text[]))
  AND (
    sqlc.narg('cursor_created_at')::timestamptz IS NULL
    OR (created_at, id) < (sqlc.narg('cursor_created_at')::timestamptz, sqlc.narg('cursor_id')::uuid)
  )
ORDER BY created_at DESC, id DESC
LIMIT @page_size;

-- name: ListDeadLetters :many
SELECT * FROM dead_letters
WHERE queue = @queue
  AND (
    sqlc.narg('cursor_created_at')::timestamptz IS NULL
    OR (created_at, id) < (sqlc.narg('cursor_created_at')::timestamptz, sqlc.narg('cursor_id')::uuid)
  )
ORDER BY created_at DESC, id DESC
LIMIT @page_size;
