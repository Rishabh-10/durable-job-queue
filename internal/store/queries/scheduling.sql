-- name: ListDueForIndexing :many
-- The sweeper's scan. Returns pending jobs that are due, or will be within the
-- lookahead window, so the caller can re-add them to the Redis index.
--
-- This is the query that makes a half-completed enqueue survivable: a job whose
-- ZADD never landed is still pending in PostgreSQL, so it turns up here on the
-- next sweep. It is equally the path that rebuilds the entire index after Redis
-- restarts empty.
SELECT id, queue, run_at FROM jobs
WHERE state = 'pending'
  AND run_at <= now() + sqlc.arg('lookahead')::interval
  AND (@queue::text = '' OR queue = @queue::text)
ORDER BY run_at
LIMIT @batch_size;

-- name: ListExpiredLeases :many
-- The reaper's scan. A running job whose lease has lapsed means its worker died
-- without acknowledging, so the job must go back into circulation.
--
-- SKIP LOCKED lets several reaper replicas scan concurrently without blocking
-- on each other, and without any of them needing to be elected leader.
SELECT id, queue, attempts, max_attempts, lease_owner FROM jobs
WHERE state = 'running' AND lease_expires_at < now()
ORDER BY lease_expires_at
LIMIT @batch_size
FOR UPDATE SKIP LOCKED;

-- name: ReclaimExpiredLease :one
-- Apply the reaper's decision for one job: with budget left it goes back to
-- pending at the computed retry time, otherwise it is dead-lettered.
--
-- The lease_owner guard is what makes this safe to race. If the original worker
-- woke up and acknowledged between the scan and this update, the owner no
-- longer matches, no row comes back, and the worker's own result stands.
UPDATE jobs
SET state = CASE WHEN @dead::boolean THEN 'dead'::job_state ELSE 'pending'::job_state END,
    run_at = CASE WHEN @dead::boolean THEN run_at ELSE @run_at END,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_error = @reason,
    updated_at = now()
WHERE id = @id AND state = 'running' AND lease_owner = @lease_owner
RETURNING *;
