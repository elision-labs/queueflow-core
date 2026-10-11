<div align="center">

# QueueFlow Core

**A high-performance, PostgreSQL-native distributed job queue and workflow engine, written in Rust.**

Durable background jobs and real DAG workflows on a database you already run — no Redis, no broker, no separate state store.

[![CI](https://github.com/elision-labs/queueflow-core/actions/workflows/ci.yml/badge.svg)](https://github.com/elision-labs/queueflow-core/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Rust 1.96+](https://img.shields.io/badge/rust-1.96%2B-orange.svg)](./rust-toolchain.toml)
[![OpenAPI 3.1](https://img.shields.io/badge/OpenAPI-3.1-6BA539.svg)](./spec/openapi.yaml)
[![Tests](https://img.shields.io/badge/tests-128%20passing-brightgreen.svg)](#testing)

[Quick start](#quick-start) · [Workflows](#workflows) · [How-to](#how-to-rest-api) · [Docs](#documentation) · [Roadmap](#roadmap)

</div>

---

QueueFlow Core runs durable background jobs and real DAG workflows directly on PostgreSQL — any
plain PostgreSQL 13+ (RDS, Cloud SQL, Azure, your laptop); no extensions. The jobs table *is* the
queue: workers claim rows with `FOR UPDATE SKIP LOCKED` and own them via lease tokens. Handlers can
live in the server binary (Rust) or in any language via the HTTP worker protocol; the OpenAPI spec
is generated from the code and ships with every release.

## Design highlights

- **Workflows as a real DAG orchestrator** — dependency gating, context propagation, per-step failure policies, cycle detection, a builder DSL, and a Mermaid diagram endpoint.
- **Thoroughly tested** — 128 tests; the entire engine, scheduler, HTTP API, and Rust client run against a deterministic in-memory adapter with **no database**, plus opt-in Postgres integration tests.
- **Durable retries & scheduling** — a retry (or a `run_at` job) is just a row whose `scheduled_at` lies in the future, so delays survive restarts.
- **Delivery safety** — lease tokens guard every outcome write (no stale worker can overwrite a finished or cancelled job), and a janitor reclaims expired leases through the normal retry policy, so a crashed worker consumes retry budget instead of crash-looping.
- **Ports & adapters** — the engine depends on the `JobStore` trait; Postgres and in-memory adapters implement it (the store doubles as the queue).
- **Code-generated OpenAPI** — the spec is generated **from the code** (utoipa), so it can't drift; emitted by `queueflow spec`.
- **Ergonomic API** — durations are plain integer seconds; statuses and backoff are exhaustive enums, so illegal states are unrepresentable.
- **Multi-tenancy** — tenant isolation enforced on every job/workflow endpoint, and the API fails closed: no credentials configured means no access, unless `--dev` is passed explicitly.
- **Polyglot workers** — a lease/heartbeat/complete/fail HTTP protocol lets handlers run in any language, with the same retry/DLQ/workflow semantics as in-process handlers.
- **Idempotent enqueue** — an `Idempotency-Key` header makes client retries safe (no duplicate jobs).
- **Clients** — a native Rust crate (`queueflow-client`), a hand-written TypeScript SDK, and self-serve generation for anything else from the released spec.

## Features

- 🐘 **PostgreSQL native** — durable, transactional, at-least-once delivery on any plain Postgres 13+; no extensions, no extra infrastructure.
- 🔀 **Workflows as DAGs** — declare steps and `depends_on`; the engine schedules each step once its dependencies complete, threads results through a shared context, and aggregates the final status.
- ♻️ **Durable retries** — typed backoff (fixed / linear / exponential, capped, with jitter) scheduled *in the row*, so retries outlive restarts.
- 🪦 **Dead-letter queue** — exhausted, non-retryable, and unhandled jobs land in an inspectable DLQ, with a replay API that re-runs them as fresh jobs.
- ⏰ **Cron schedules** — recurring enqueues from standard crontab expressions (UTC), deduplicated across servers, with pause/resume.
- 🧪 **Built to be tested** — a ports-and-adapters core means the whole system runs in memory, deterministically, with a controllable clock.
- 📜 **Code-generated OpenAPI** — the spec is derived from the handlers and attached to every release; generate a client for any language against it.
- 📈 **Observable** — Prometheus metrics (per-queue backlog gauges read from the database, handler and queue-wait latency histograms, engine counters), structured JSON logs (`tracing`), health/readiness probes, and `GET /api/v1/queues` for live per-queue backlog.
- 🛑 **Graceful shutdown** — drains in-flight work on `SIGINT`/`SIGTERM`.

## Architecture

```
queueflow-core/                    Cargo workspace
├── crates/
│   ├── queueflow-core/            library: domain, ports, adapters, engine, workflow
│   │   ├── domain.rs              Job / Workflow / config / status enums
│   │   ├── ports.rs               JobStore (storage + claim queue) + Clock traits
│   │   ├── adapters/
│   │   │   ├── memory.rs          in-memory store (tests / local dev)
│   │   │   ├── clock.rs           SystemClock + TestClock
│   │   │   └── postgres/          PostgreSQL adapter + LISTEN/NOTIFY hub (feature "postgres")
│   │   ├── engine/                enqueue, worker loop, durable retry, DLQ, janitor
│   │   ├── workflow/              DAG validation, builder DSL, scheduler
│   │   ├── api.rs                 object-safe JobApi facade
│   │   └── migrations/            sqlx migrations (plain-SQL claim-queue schema, shipped in the crate)
│   ├── queueflow-api/             axum router + utoipa OpenAPI
│   ├── queueflow-client/          native Rust client (REST + remote worker runtime)
│   └── queueflow/                 the `queueflow` binary (serve / spec / migrate / CLI)
├── sdk-configs/                   openapi-generator configs (on-demand python/go)
├── scripts/generate-sdks.sh       drives openapi-generator
└── Makefile
```

The key design choice is **ports & adapters**: `queueflow-core` is written against the `JobStore` trait
(which doubles as the claim queue), so the engine, the workflow scheduler, and the HTTP API can be
exercised end-to-end against a fast, deterministic in-memory adapter. In production the same code runs
on the PostgreSQL adapter.

## Quick start

### As a library (no database)

```rust
use std::sync::Arc;
use queueflow_core::*;
use queueflow_core::task::builtin;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), EngineError> {
    let clock = Arc::new(SystemClock);
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));

    let engine = Engine::builder(store, clock)
        .register("echo", builtin::echo())
        .register_fn("greet", |p: Map| async move {
            let name = p.get("name").and_then(|v| v.as_str()).unwrap_or("world");
            Ok(Map::from_iter([("greeting".into(), json!(format!("hello {name}")))]))
        })
        .build();

    let id = engine.enqueue("greet", Map::from_iter([("name".into(), json!("ada"))]), Default::default()).await?;
    engine.process_once("default").await?;          // drive one job (tests/demos)
    println!("{:?}", engine.get_job(&id).await?.result);
    Ok(())
}
```

Run the bundled examples:

```bash
cargo run --example in_memory_jobs -p queueflow-core
cargo run --example workflow       -p queueflow-core
```

### As a server (PostgreSQL)

```bash
# any plain Postgres 13+ works, e.g.:
docker run -d --name pg -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16-alpine

export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
cargo run -p queueflow -- serve --dev --mode all --workers 10 --api-port 8000
```

The server applies migrations on startup, exposes the REST API on `:8000`, Prometheus metrics on `:9090`,
and interactive docs at <http://localhost:8000/docs>.

`--dev` runs without credentials for local development (any non-empty bearer token is tenant
`tenant1`). Without it the server **refuses to start** until you configure tenant credentials
(`--api-keys` and/or `--jwt-secret`) and a `--worker-token`:

```bash
cargo run -p queueflow -- serve --mode all \
  --api-keys "$(openssl rand -hex 24):acme" \
  --worker-token "$(openssl rand -hex 24)"
```

### With Docker

```bash
docker run -p 8000:8000 -p 9090:9090 \
  -e DATABASE_URL=postgres://… \
  -e QUEUEFLOW_API_KEYS=my-token:acme \
  -e QUEUEFLOW_WORKER_TOKEN=my-worker-token \
  ghcr.io/elision-labs/queueflow:0.2 serve

make docker                       # builds ghcr.io/elision-labs/queueflow:dev locally
```

## Workflows

Build a DAG with the typed builder, or POST the same JSON to `/api/v1/workflows`:

```rust
use queueflow_core::workflow::{WorkflowBuilder, StepBuilder};
use queueflow_core::OnFailure;

let req = WorkflowBuilder::new("order_123")
    .step(StepBuilder::new("validate").task("validate_order"))
    .step(StepBuilder::new("pay").task("process_payment").after("validate"))
    .step(StepBuilder::new("ship").task("create_shipment").after("pay")
        .on_failure(OnFailure::Continue))
    .build()?;                  // validates the DAG (cycles -> Err)

let workflow_id = engine.create_workflow(req, None).await?;
```

- **Dependency gating** — a step is enqueued only once all its `depends_on` steps complete.
- **Context propagation** — each step's result is merged into the workflow context and injected into
  downstream payloads under `_context`.
- **Failure policies** — per step: `halt` (fail the workflow, cancel the rest), `skip` (skip it and its
  dependents, continue), or `continue` (leave it failed, keep going; the workflow ends `partially_failed`).
- **Cycle detection** — validated at creation; a cycle is a `400`.
- **Diagram** — `GET /api/v1/workflows/{id}/diagram` returns a Mermaid `graph TD`:

```mermaid
graph TD
    validate["validate"]
    pay["pay"]
    ship["ship"]
    validate --> pay
    pay --> ship
```

## How-to (REST API)

Every `/api/v1` route needs a bearer token. Configure tenant credentials with `--jwt-secret`
(HS256; the `sub` claim is the tenant) and/or `--api-keys "token:tenant,..."`. Worker-protocol
endpoints take the separate `--worker-token` credential. The server fails closed: without both a
tenant credential source and a worker token it refuses to start, unless `--dev` is passed, in which
case any non-empty token maps to the fixed tenant `tenant1` (the examples below assume `--dev`).

```bash
# Enqueue a job
curl -s -X POST http://localhost:8000/api/v1/jobs \
  -H 'Authorization: Bearer dev' -H 'Content-Type: application/json' \
  -d '{"task_name":"echo","payload":{"hello":"world"},"config":{"priority":5,"max_retries":3,"timeout":30}}'
# => {"job_id":"…"}

# Fetch it
curl -s http://localhost:8000/api/v1/jobs/<id> -H 'Authorization: Bearer dev'

# List pending jobs
curl -s 'http://localhost:8000/api/v1/jobs?status=pending&limit=20' -H 'Authorization: Bearer dev'

# Create a workflow
curl -s -X POST http://localhost:8000/api/v1/workflows \
  -H 'Authorization: Bearer dev' -H 'Content-Type: application/json' \
  -d '{"name":"etl","steps":[
        {"name":"extract","task_name":"echo"},
        {"name":"transform","task_name":"echo","depends_on":["extract"]},
        {"name":"load","task_name":"echo","depends_on":["transform"]}
      ]}'

# Health / metrics
curl -s http://localhost:8000/health
curl -s http://localhost:9090/metrics
```

| Method & path | Description |
| --- | --- |
| `POST /api/v1/jobs` | Enqueue a job (send `Idempotency-Key` to make retries safe) |
| `POST /api/v1/jobs/batch` | Enqueue up to 1000 jobs in one round trip |
| `GET /api/v1/jobs` | List jobs (filter by status/queue, paginate; `include_total=true` for exact counts) |
| `GET /api/v1/jobs/{id}` | Fetch a job |
| `GET /api/v1/jobs/{id}/events` | Server-Sent Events stream of status changes until terminal |
| `POST /api/v1/jobs/{id}/cancel` | Cancel a job (409 if already finished) |
| `POST /api/v1/queues/{queue}/lease` | Worker protocol: lease jobs (long-poll supported) |
| `POST /api/v1/jobs/{id}/heartbeat` · `/complete` · `/fail` | Worker protocol: extend lease · report outcome |
| `POST /api/v1/workflows` | Create a workflow |
| `GET /api/v1/workflows` · `/{id}` · `/{id}/cancel` · `/{id}/diagram` | List / fetch / cancel / diagram |
| `POST /api/v1/cron` · `GET /api/v1/cron` · `/{id}` | Create / list / fetch cron schedules |
| `DELETE /api/v1/cron/{id}` · `POST .../pause` · `POST .../resume` | Delete / pause / resume a schedule |
| `GET /api/v1/dlq` · `/{id}` | List / inspect dead-lettered jobs |
| `POST /api/v1/dlq/{id}/replay` | Replay a dead letter as a fresh job (once per entry) |
| `GET /api/v1/tasks` · `/stats` | Registered handlers · engine counters (process-local) |
| `GET /health` · `/ready` · `/docs` · `/openapi.json` | Probes · Swagger UI · spec |

### Remote workers (any language)

Handlers do not have to be compiled into the server. A worker in any language can drain a queue
over HTTP with at-least-once semantics, durable retries, and workflow advancement handled
server-side. Workers execute arbitrary tenants' jobs, so give them their own credential: set
`--worker-token` on the server and use that token below (only in `--dev` mode do the worker
endpoints accept any authenticated token):

```bash
# 1. Lease (long-polls up to wait_secs when the queue is empty)
curl -s -X POST http://localhost:8000/api/v1/queues/default/lease \
  -H 'Authorization: Bearer dev' -H 'Content-Type: application/json' \
  -d '{"max_jobs":1,"lease_secs":60,"wait_secs":20}'
# => {"jobs":[{"job":{...},"lease_token":"550e8400-…"}]}

# 2. (while working) heartbeat to keep the lease; the response carries the
#    job's live status, so a mid-run cancellation is observed here
curl -s -X POST http://localhost:8000/api/v1/jobs/<id>/heartbeat \
  -H 'Authorization: Bearer dev' -H 'Content-Type: application/json' \
  -d '{"lease_token":"550e8400-…","extend_secs":60}'
# => {"status":"running"}

# 3. Report the outcome (or /fail with {"error":"...","retryable":true})
curl -s -X POST http://localhost:8000/api/v1/jobs/<id>/complete \
  -H 'Authorization: Bearer dev' -H 'Content-Type: application/json' \
  -d '{"lease_token":"550e8400-…","result":{"ok":true}}'
```

In Rust, `queueflow-client` wraps this in a worker runtime with automatic heartbeating:

```rust
use queueflow_client::{Client, Map};
use queueflow_client::worker::{Worker, WorkerOptions};

let client = Client::new("http://localhost:8000", "dev");
Worker::new(client, "default", WorkerOptions::default())
    .register("resize-image", |job| async move {
        // ... do the work ...
        Ok(Map::new())
    })
    .run()
    .await;
```

### CLI

The `queueflow` binary doubles as a client for a running server (`--server-url` /
`QUEUEFLOW_SERVER_URL`, `--token` / `QUEUEFLOW_TOKEN`):

```bash
queueflow job create --task echo --payload '{"hello":"world"}' --wait
queueflow job list --status pending --limit 20
queueflow job watch <id>
queueflow workflow create --file etl.json
queueflow workflow diagram <id>
queueflow tasks
queueflow stats
```

## Configuration

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--mode` | `QUEUEFLOW_MODE` | `all` | `api`, `worker`, or `all` |
| `--database-url` | `DATABASE_URL` | — | PostgreSQL connection string |
| `--api-port` | `QUEUEFLOW_API_PORT` | `8000` | REST API port |
| `--workers` | `QUEUEFLOW_WORKERS` | `10` | Workers per queue |
| `--queues` | `QUEUEFLOW_QUEUES` | default queue | Comma-separated queues the in-process workers drain; each gets `--workers` workers |
| `--metrics-port` | `QUEUEFLOW_METRICS_PORT` | `9090` | Prometheus port |
| `--default-queue` | `QUEUEFLOW_DEFAULT_QUEUE` | `default` | Default queue name |
| `--worker-token` | `QUEUEFLOW_WORKER_TOKEN` | unset | Credential required by the worker-protocol endpoints (lease/heartbeat/complete/fail). Required in `api`/`all` mode unless `--dev` |
| `--jwt-secret` | `QUEUEFLOW_JWT_SECRET` | unset | HS256 secret for tenant JWTs (`sub` = tenant id, `exp` enforced) |
| `--api-keys` | `QUEUEFLOW_API_KEYS` | unset | Static tenant API keys, `token:tenant,...`. One of this or `--jwt-secret` is required in `api`/`all` mode unless `--dev` |
| `--dev` | `QUEUEFLOW_DEV` | off | Development mode: run without credentials. Any non-empty token is tenant `tenant1`; without a worker token the worker endpoints accept any authenticated caller. Never on a reachable server |
| `--cors-origins` | `QUEUEFLOW_CORS_ORIGINS` | unset | Comma-separated allowed origins. Unset = permissive CORS (warns) |
| `--max-db-connections` | `QUEUEFLOW_MAX_DB_CONNECTIONS` | `50` | Connection pool size |
| `--auto-migrate` | `QUEUEFLOW_AUTO_MIGRATE` | `true` | Apply migrations on startup; `--auto-migrate false` to run them separately with `queueflow migrate` |
| `--retention-hours` | `QUEUEFLOW_RETENTION_HOURS` | unset | Delete terminal jobs/workflows/dead letters older than this. Unset keeps history forever |
| `--retention-interval-secs` | `QUEUEFLOW_RETENTION_INTERVAL_SECS` | `3600` | Cadence of the retention sweep |
| `--lease-secs` | `QUEUEFLOW_LEASE_SECS` | `30` | Lease taken by in-process workers on claim (stretched to cover the job timeout plus grace) |
| `--lease-grace-secs` | `QUEUEFLOW_LEASE_GRACE_SECS` | `30` | Headroom over a job's timeout when a claim covers it |
| `--worker-poll-secs` | `QUEUEFLOW_WORKER_POLL_SECS` | `5` | Upper bound on an idle worker's wait when a NOTIFY is missed (e.g. behind a transaction-pooling pgbouncer) |
| `--janitor-interval-secs` | `QUEUEFLOW_JANITOR_INTERVAL_SECS` | `5` | Cadence of expired-lease reclaim and workflow self-heal |
| `--janitor-batch` | `QUEUEFLOW_JANITOR_BATCH` | `100` | Rows per janitor sweep |
| `--reclaim-lease-secs` | `QUEUEFLOW_RECLAIM_LEASE_SECS` | `60` | Lease on reclaimed jobs while they go through the failure policy |
| `--max-batch` | `QUEUEFLOW_MAX_BATCH` | `1000` | Maximum jobs per `POST /api/v1/jobs/batch` |

## Testing

```bash
make test        # unit + integration, no services required
make test-pg     # opt-in Postgres tests (needs TEST_DATABASE_URL; any plain Postgres 13+)
make clippy      # lint (warnings = errors)
make fmt-check   # formatting
```

Because the engine is generic over the ports, the durable-retry, backoff, timeout, dead-letter,
idempotency, and full workflow-orchestration behaviours are all verified deterministically with a
`TestClock` and in-memory adapters — no Docker, no Postgres. The Postgres adapter is then checked for
parity by the opt-in integration suite.

## SDKs

The OpenAPI spec is generated from the Rust handlers and types (`make spec`), so it always matches
the server, and it is attached to every GitHub release.

Every SDK except Rust uses the same two-layer shape: a **generated core** plus a thin
**hand-written facade**.

- **Generated core** (never hand-edited): models, per-tag API clients, and the HTTP transport,
  produced from the spec by [openapi-generator](https://openapi-generator.tech). Because it is
  regenerated, it cannot drift from the server.
- **Facade** (small, hand-written): the ergonomics codegen cannot express. A `QueueFlow` client with
  resource groups, `create()` (enqueue + fetch), `waitFor`/`wait_for` pollers, the `watch()` SSE
  stream, the remote-worker run loop, a workflow builder with local cycle detection, and a typed
  error hierarchy.

Per language:

- **Rust** [`queueflow-client`](./crates/queueflow-client) is the native crate in this workspace; it
  reuses the engine's own domain types (so it cannot drift) and is integration-tested against the
  real router on every CI run. No codegen can beat that, so Rust is not generated.
- **TypeScript/Node.js** [`queueflow-sdk-nodejs`](../queueflow-sdk-nodejs): generated core in `core/`
  (regenerate with `npm run generate-core`), hand-written facade in `src/`, bundled to dual ESM + CJS
  with tsup. `make check-ts-sdk` asserts the core covers every spec operation and the facade surfaces
  every tag group.
- **Python / Go**: `make sdks` regenerates both. The facade ships as an openapi-generator supporting
  file ([`sdk-templates/<lang>/facade.*`](./sdk-templates)) injected at generation time, so it lands
  inside the generated package (`queueflow/facade.py`, `facade.go`) and is regenerated alongside the
  core. CI compiles both facades against fresh codegen to catch drift.

```bash
make spec            # writes spec/openapi.{json,yaml} from the code
make validate-spec   # validate via openapi-generator (Docker)
make sdks            # regenerate the Python + Go SDKs (generated core + injected facade)
make check-ts-sdk    # assert the TS generated core + facade match the spec
```

## Documentation

- **API reference (rustdoc):** `cargo doc --open -p queueflow-core`
- **OpenAPI spec:** [`spec/openapi.yaml`](./spec/openapi.yaml) / [`spec/openapi.json`](./spec/openapi.json), or live at `/openapi.json` and `/docs`
- **Runnable examples:** [`crates/queueflow-core/examples/`](./crates/queueflow-core/examples)
- **Database schema:** [`crates/queueflow-core/migrations/0001_init.sql`](./crates/queueflow-core/migrations/0001_init.sql)
- **Multi-repo overview:** [`repository-structure.md`](../repository-structure.md)

## Roadmap

Contributions welcome — these are the planned next steps, roughly in priority order:

- [x] **Real authentication** — tenant JWTs (HS256, `--jwt-secret`) and static API keys
      (`--api-keys`), plus a dedicated worker credential (`--worker-token`) for the worker-protocol
      endpoints. Next: a database-backed API-key store with management endpoints, and asymmetric
      JWT (JWKS) support.
- [x] **Cron jobs** — recurring enqueues from crontab expressions (UTC), deduplicated across
      servers via per-firing idempotency keys, with pause/resume and catch-up-once semantics.
- [x] **DLQ admin API** — list, inspect, and replay dead-lettered jobs (each entry replays at
      most once, as a fresh detached job).
- [ ] **Conditional steps** — evaluate a per-step predicate before scheduling (skip when false).
- [ ] **Sub-workflows & fan-out** — a step that spawns a child workflow or a dynamic batch.
- [ ] **OpenTelemetry** — distributed tracing export alongside the Prometheus metrics.
- [ ] **Per-tenant rate limiting & quotas.**
- [x] **Publish** — crates.io, GHCR image, and GitHub release binaries ship from the tag pipeline
      (see [`PUBLISHING.md`](./PUBLISHING.md)).
- [ ] **Helm chart** and one-click deploy templates.
- [ ] **Web dashboard** — queues, jobs, workflow DAGs, DLQ, and cron in a browser.

## Contributing

```bash
git clone https://github.com/elision-labs/queueflow-core
cd queueflow-core
make test && make clippy && make fmt-check
```

PRs should keep `make test`, `make clippy`, and `make validate-spec` green. New behaviour belongs in a test
against the in-memory adapters; new endpoints/types are reflected in the OpenAPI spec automatically via their
`#[utoipa::path]` / `ToSchema` annotations.

## Requirements

- Rust 1.96+
- PostgreSQL 13+ (plain — no extensions; RDS, Cloud SQL, Azure all work)
- Docker (only for `make validate-spec` / `make sdks` / `make docker`)

## Star history

<a href="https://star-history.com/#elision-labs/queueflow-core&Date">
  <img src="https://api.star-history.com/svg?repos=elision-labs/queueflow-core&type=Date" alt="Star History Chart" width="600">
</a>

## License

[MIT](./LICENSE)
