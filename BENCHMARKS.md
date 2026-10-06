# Benchmarks

QueueFlow's pitch is that Postgres is enough: the jobs table is the queue,
claims are `FOR UPDATE SKIP LOCKED`, and every state transition is a durable
transactional write. These numbers quantify what that costs and what it
delivers on commodity hardware with a completely default Postgres.

Run them yourself with one command; the harness is in this repo:

```sh
./scripts/bench.sh                 # 20k jobs, worker counts 4 8 16 32
./scripts/bench.sh 50000 "8 32"    # customize total jobs and worker counts
```

Requirements: Docker (for the dedicated Postgres container), Rust toolchain,
`curl`, `python3`. The harness builds a release binary, starts a throwaway
`postgres:16-alpine` container on port 55440, serves the API on port 8077,
and tears both down on exit. It never touches an existing QueueFlow stack.

## What is measured

1. **Single enqueue**: 500 sequential `POST /api/v1/jobs` calls over one
   connection. This is dominated by HTTP round-trip latency, so it is a
   latency number expressed as a rate, not a throughput ceiling.
2. **Batch enqueue**: `POST /api/v1/jobs/batch` with 1000-job batches
   (the server's maximum) until the target count is inserted. One
   multi-row insert per batch.
3. **End-to-end drain**: with a backlog already enqueued, the server
   restarts in `--mode all --workers N` and the harness times how long the
   in-process worker pool takes to take every job from `pending` to
   `completed`, read off the engine's `/api/v1/stats` counters. Each job is
   the built-in `echo` handler (a no-op), so this measures the engine's
   per-job overhead: claim query, lease, handler dispatch, completion write,
   NOTIFY. It is a ceiling for the queue itself, not a prediction for jobs
   that do real work.

Durability is never relaxed: default Postgres configuration, synchronous
commits, no `UNLOGGED` tables, every claim and completion a real
transactional write.

## Results

Machine: Apple M3 Max (16 cores, 64 GB), macOS, Postgres 16 in Docker
(Docker 27.5.1). QueueFlow release build at commit `f839e4a`. 20,000 jobs
per drain run (10,000 for the 1- and 2-worker runs).

| Benchmark | Result |
|---|---|
| Single enqueue (sequential, 1 conn) | ~140 jobs/sec (~7 ms/job round trip) |
| Batch enqueue (1000-job batches) | ~30,000-36,000 jobs/sec |

| Workers | Drain throughput (jobs/sec) |
|---|---|
| 1 | ~1,100 |
| 2 | ~2,600 |
| 4 | ~3,000 |
| 8 | ~3,000 |
| 16 | ~3,000 |
| 32 | ~2,900 |

## Reading the numbers

- **Batch enqueue is effectively free.** At ~35k inserts/sec through the
  full HTTP + validation + multi-row-insert path, getting work into the
  queue will not be your bottleneck. If you are enqueuing one job at a
  time in a hot loop, use the batch endpoint.
- **Drain saturates at ~3k jobs/sec on this machine, by 4 workers.** Each
  worker claims one job per `FOR UPDATE SKIP LOCKED` query and writes its
  completion in a second transaction, so a no-op job costs roughly two
  Postgres round trips. Past a few workers the shared write path of a
  single Postgres becomes the limit; adding workers past that point adds
  claim contention instead of throughput (visible as the slight dip at 32).
- **Scale workers to your handlers, not to these numbers.** Real handlers
  spend their time doing work, not claiming. If a job takes 500 ms of real
  work, 32 workers sustain ~64 jobs/sec and the engine overhead (~1 ms) is
  noise. The plateau only matters when jobs are near-instant, and if you
  have sustained >3k near-instant jobs/sec, batch them.
- **At-least-once is included in the price.** These rates include lease
  tracking and the janitor's self-healing machinery; there is no faster
  mode that trades away crash safety.

## Caveats

- One laptop, Postgres in Docker on the same machine, client and server
  sharing cores. A dedicated Postgres with real storage and network
  latency between the pieces will shift every number; run the harness in
  your own topology before capacity planning.
- The drain timer starts when the restarted server passes `/health`,
  which is a few hundred milliseconds after its workers begin claiming.
  Over 20k jobs this inflates throughput by well under 5%.
- Single-enqueue rate is sequential on purpose (it is a latency probe).
  Concurrent producers will push aggregate single-enqueue throughput far
  higher.
- Results vary a few percent run to run; treat them as orders of
  magnitude, not contract numbers.
