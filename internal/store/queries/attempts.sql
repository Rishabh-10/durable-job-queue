-- name: RecordAttemptStart :one
-- Opened when a worker claims a job. Re-claiming the same attempt number after
-- a crash-and-reclaim updates the existing row rather than colliding on the
-- primary key.
INSERT INTO job_attempts (job_id, attempt_no, worker_id, started_at)
VALUES (@job_id, @attempt_no, @worker_id, now())
ON CONFLICT (job_id, attempt_no) DO UPDATE
SET worker_id = EXCLUDED.worker_id,
    started_at = EXCLUDED.started_at,
    finished_at = NULL,
    error = NULL
RETURNING *;

-- name: RecordAttemptFinish :one
UPDATE job_attempts
SET finished_at = now(), error = sqlc.narg('error')
WHERE job_id = @job_id AND attempt_no = @attempt_no
RETURNING *;

-- name: ListAttempts :many
SELECT * FROM job_attempts
WHERE job_id = @job_id
ORDER BY attempt_no;

-- name: CountJobsByState :many
SELECT state, count(*) AS total
FROM jobs
WHERE queue = @queue
GROUP BY state;

-- name: OldestDueJobAge :one
-- How far behind the scheduler is: the age of the oldest job that should have
-- started and has not. Zero when nothing is overdue.
SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(run_at))), 0)::float8 AS lag_seconds
FROM jobs
WHERE queue = @queue AND state = 'pending' AND run_at <= now();
