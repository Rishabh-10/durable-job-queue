-- The system of record. Redis holds only a derived index of what is due soon;
-- everything durable lives here.

CREATE TYPE job_state AS ENUM (
    'pending',
    'running',
    'succeeded',
    'failed',
    'dead',
    'cancelled'
);

CREATE TABLE jobs (
    id               uuid        PRIMARY KEY,
    queue            text        NOT NULL,
    kind             text        NOT NULL,
    payload          bytea       NOT NULL,
    state            job_state   NOT NULL DEFAULT 'pending',
    run_at           timestamptz NOT NULL,
    attempts         int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL,
    lease_owner      text,
    lease_expires_at timestamptz,
    idempotency_key  text,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT jobs_queue_not_empty CHECK (queue <> ''),
    CONSTRAINT jobs_kind_not_empty CHECK (kind <> ''),
    CONSTRAINT jobs_max_attempts_positive CHECK (max_attempts > 0),
    CONSTRAINT jobs_attempts_not_negative CHECK (attempts >= 0),

    -- A lease is meaningful only while running, and running always has one.
    -- Enforcing the pairing here stops a half-written claim from creating a job
    -- that neither a worker owns nor the reaper will ever reclaim.
    CONSTRAINT jobs_lease_matches_state CHECK (
        (state = 'running') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

-- Makes Enqueue idempotent within a queue. Partial, so the common case of no
-- key costs nothing and unlimited jobs may omit it.
CREATE UNIQUE INDEX jobs_idempotency_key_idx
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- The sweeper's scan: due-or-soon pending jobs, oldest first. Partial on state
-- so the index holds only the working set rather than every job ever run.
CREATE INDEX jobs_pending_due_idx
    ON jobs (queue, run_at)
    WHERE state = 'pending';

-- The reaper's scan: leases that have expired.
CREATE INDEX jobs_expired_lease_idx
    ON jobs (lease_expires_at)
    WHERE state = 'running';

-- Paging through a queue in the API.
CREATE INDEX jobs_queue_created_idx ON jobs (queue, created_at DESC, id DESC);

-- One row per execution, kept after the job finishes so a dead job carries the
-- evidence of why it died rather than just its last error.
CREATE TABLE job_attempts (
    job_id      uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt_no  int         NOT NULL,
    worker_id   text        NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    error       text,

    PRIMARY KEY (job_id, attempt_no),
    CONSTRAINT job_attempts_no_positive CHECK (attempt_no > 0)
);

-- Dead-lettered jobs, for the DLQ read path. A view rather than a table: the
-- job keeps its full history on one row and requeueing is a state change.
CREATE VIEW dead_letters AS
SELECT *
FROM jobs
WHERE state = 'dead';
