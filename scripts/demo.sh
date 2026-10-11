#!/usr/bin/env bash
#
# End-to-end demonstration: starts the control plane and a worker, schedules a
# few jobs, and shows what happens to each of them.
#
# Expects the dependency stack to be up and migrated:
#
#   make up && make migrate-up && make demo
#
# Each run starts from an empty queue so the counts and the dead-letter listing
# describe this run and nothing else. Set JQ_DEMO_KEEP_STATE=1 to keep whatever
# is already there.
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

BIN=./bin
ADDR=${JQ_ADDR:-localhost:8080}
COMPOSE="docker compose -f deploy/docker-compose.yml"

# Short backoff so the retry demo finishes in seconds rather than minutes.
export JQ_BACKOFF_BASE=${JQ_BACKOFF_BASE:-300ms}
export JQ_BACKOFF_CAP=${JQ_BACKOFF_CAP:-2s}
export JQ_LOG_LEVEL=${JQ_LOG_LEVEL:-warn}

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  [[ -n "${WORKER_PID:-}" ]] && kill "$WORKER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# reset_state empties the queue so each run tells its own story rather than
# accumulating jobs from every previous one.
#
# It deletes every job, so it refuses to run against anything that is not
# obviously the local demo stack. Pointing JQ_POSTGRES_DSN at a real deployment
# and then running a demo should not be able to wipe it.
reset_state() {
  if [[ -n "${JQ_DEMO_KEEP_STATE:-}" ]]; then
    echo "keeping existing jobs (JQ_DEMO_KEEP_STATE is set)"
    return
  fi

  case "${JQ_POSTGRES_DSN:-localhost}" in
    *localhost*| *127.0.0.1*) ;;
    *)
      echo "refusing to reset: JQ_POSTGRES_DSN does not point at localhost." >&2
      echo "re-run with JQ_DEMO_KEEP_STATE=1 to skip the reset and keep going." >&2
      exit 1
      ;;
  esac

  # Truncating jobs cascades to job_attempts; both are named so the intent is
  # visible rather than implied by a foreign key.
  $COMPOSE exec -T postgres     psql -q -U jobqueue -d jobqueue     -c 'TRUNCATE jobs, job_attempts RESTART IDENTITY CASCADE'

  # The index is derived, so it has to go too. Left behind, its stale members
  # would make "scheduled in index" disagree with the job counts.
  $COMPOSE exec -T redis redis-cli FLUSHALL >/dev/null

  echo "queue emptied: no jobs, no index entries"
}

log "Building binaries"
go build -o "$BIN/" ./cmd/...

log "Starting from an empty queue"
reset_state

log "Starting the control plane and one worker"
"$BIN/jobqueued" >/tmp/jobqueue-demo-server.log 2>&1 &
SERVER_PID=$!
"$BIN/worker" >/tmp/jobqueue-demo-worker.log 2>&1 &
WORKER_PID=$!

# Wait for readiness rather than sleeping a guessed amount.
for _ in $(seq 1 50); do
  if curl -fsS "${JQ_DEMO_READYZ_URL:-http://localhost:9090/readyz}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
echo "control plane is ready on $ADDR"

ctl() { "$BIN/jobqctl" -addr "$ADDR" "$@"; }

log "1. A job scheduled for three seconds from now"
DELAYED=$(ctl enqueue -kind echo -payload '{"message":"scheduled work"}' -delay 3s | awk '{print $1}')
echo "   job $DELAYED"
ctl stats

log "   Waiting for it to come due"
ctl watch "$DELAYED" -interval 500ms

log "2. A job that always fails: retries with backoff, then dead-letters"
DOOMED=$(ctl enqueue -kind always_fails -max-attempts 3 | awk '{print $1}')
ctl watch "$DOOMED" -interval 400ms

log "3. The dead-letter queue"
ctl dlq ls

log "4. Requeueing it, now that an operator has 'fixed' the cause"
ctl dlq requeue "$DOOMED" -additional-attempts 1 >/dev/null
ctl watch "$DOOMED" -interval 400ms

log "5. Cancelling a job before it runs"
DOOMED_CANCEL=$(ctl enqueue -kind echo -delay 1h | awk '{print $1}')
ctl cancel "$DOOMED_CANCEL"

log "6. Throughput: 500 jobs through one worker"
ctl enqueue -kind noop -count 500
sleep 3
ctl stats

log "Done. Logs are in /tmp/jobqueue-demo-{server,worker}.log"
