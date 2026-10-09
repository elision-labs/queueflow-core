#!/usr/bin/env bash
#
# QueueFlow benchmark harness. Reproducible by anyone with docker + cargo:
#
#   ./scripts/bench.sh [TOTAL_JOBS] [WORKER_COUNTS]
#   ./scripts/bench.sh 20000 "4 8 16 32"
#
# Measures, against a dedicated dockerized Postgres 16 and a release build:
#   1. batch enqueue throughput  (1000-job batches over HTTP)
#   2. single enqueue rate       (sequential HTTP posts, 1 connection)
#   3. end-to-end drain rate     (claim -> execute `echo` -> terminal write,
#                                 at each worker count)
#
# The `echo` handler is a no-op, so drain numbers are the ENGINE's per-job
# overhead ceiling (claim query, lease, completion write, NOTIFY), not
# business-work throughput. Everything is durable: no fsync tricks, default
# Postgres configuration.
set -euo pipefail
cd "$(dirname "$0")/.."

TOTAL="${1:-20000}"
WORKERS="${2:-4 8 16 32}"
PG_PORT=55440
API_PORT=8077
PG_NAME=queueflow-bench-pg
DB_URL="postgres://bench:bench@localhost:${PG_PORT}/bench"
BASE="http://localhost:${API_PORT}"
AUTH="authorization: Bearer bench"

cleanup() {
  [ -n "${SERVER_PID:-}" ] && kill "${SERVER_PID}" 2>/dev/null || true
  docker rm -f "${PG_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

stat() { # stat jobs_completed -> number
  curl -sf "${BASE}/api/v1/stats" -H "${AUTH}" | python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"
}

start_server() { # start_server <mode> <workers>
  (target/release/queueflow serve --dev --mode "$1" --workers "$2" \
     --api-port "${API_PORT}" --metrics-port 0 --default-queue bench \
     >/tmp/queueflow-bench-server.log 2>&1) &
  SERVER_PID=$!
  until curl -sf "${BASE}/health" >/dev/null 2>&1; do sleep 0.1; done
}

stop_server() { kill "${SERVER_PID}" 2>/dev/null || true; wait "${SERVER_PID}" 2>/dev/null || true; SERVER_PID=""; }

echo "==> building release binary"
cargo build -q --release -p queueflow

echo "==> starting dedicated Postgres 16 (port ${PG_PORT})"
docker rm -f "${PG_NAME}" >/dev/null 2>&1 || true
docker run -d --name "${PG_NAME}" -e POSTGRES_USER=bench -e POSTGRES_PASSWORD=bench \
  -e POSTGRES_DB=bench -p "${PG_PORT}:5432" postgres:16-alpine >/dev/null
until docker exec "${PG_NAME}" pg_isready -U bench >/dev/null 2>&1; do sleep 0.3; done
export DATABASE_URL="${DB_URL}"

BATCH_BODY=$(python3 -c "import json; print(json.dumps({'jobs': [{'task_name':'echo','payload':{'i':i}} for i in range(1000)]}))")
BATCHES=$(( TOTAL / 1000 ))

echo "machine: $(uname -m) / $(sysctl -n hw.ncpu 2>/dev/null || nproc) cpus"
echo "jobs per run: ${TOTAL}  worker counts: ${WORKERS}"
echo

# --- 1+2: enqueue benchmarks (server with no local workers) -----------------
start_server api 1
echo "==> single enqueue (500 sequential posts)"
T0=$(now_ms)
for _ in $(seq 1 500); do
  curl -sf -o /dev/null -X POST "${BASE}/api/v1/jobs" -H "${AUTH}" \
    -H 'content-type: application/json' -d '{"task_name":"echo","payload":{}}'
done
T1=$(now_ms)
SINGLE_RATE=$(python3 -c "print(round(500 / (($T1-$T0)/1000)))")
echo "    ${SINGLE_RATE} enqueues/sec"

echo "==> batch enqueue (${BATCHES} x 1000-job batches)"
T0=$(now_ms)
for _ in $(seq 1 "${BATCHES}"); do
  curl -sf -o /dev/null -X POST "${BASE}/api/v1/jobs/batch" -H "${AUTH}" \
    -H 'content-type: application/json' -d "${BATCH_BODY}"
done
T1=$(now_ms)
BATCH_RATE=$(python3 -c "print(round(${TOTAL} / (($T1-$T0)/1000)))")
echo "    ${BATCH_RATE} jobs/sec enqueued"
stop_server

# --- 3: drain rate per worker count -----------------------------------------
echo "==> drain (claim -> echo -> complete), per worker count"
printf "%-10s %-14s\n" "workers" "jobs/sec"
for W in ${WORKERS}; do
  # fresh backlog for this run
  start_server api 1
  for _ in $(seq 1 "${BATCHES}"); do
    curl -sf -o /dev/null -X POST "${BASE}/api/v1/jobs/batch" -H "${AUTH}" \
      -H 'content-type: application/json' -d "${BATCH_BODY}"
  done
  stop_server

  start_server all "${W}"
  T0=$(now_ms)
  while :; do
    DONE=$(stat jobs_completed)
    [ "${DONE}" -ge "${TOTAL}" ] && break
    sleep 0.2
  done
  T1=$(now_ms)
  stop_server
  RATE=$(python3 -c "print(round(${TOTAL} / (($T1-$T0)/1000)))")
  printf "%-10s %-14s\n" "${W}" "${RATE}"
done
