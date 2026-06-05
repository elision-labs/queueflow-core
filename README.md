<div align="center">

# QueueFlow Core (Rust)

**A high-performance, PostgreSQL/PGMQ-native distributed job queue and workflow engine, written in Rust.**

Durable background jobs and real DAG workflows on a database you already run — no Redis, no broker, no separate state store.

[![CI](https://github.com/sjriddle/queueflow-core/actions/workflows/ci.yml/badge.svg)](https://github.com/sjriddle/queueflow-core/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Rust 1.83+](https://img.shields.io/badge/rust-1.83%2B-orange.svg)](./rust-toolchain.toml)
[![OpenAPI 3.1](https://img.shields.io/badge/OpenAPI-3.1-6BA539.svg)](./spec/openapi.yaml)
[![Tests](https://img.shields.io/badge/tests-56%20passing-brightgreen.svg)](#testing)

[Quick start](#quick-start) · [Workflows](#workflows) · [How-to](#how-to-rest-api) · [Docs](#documentation) · [Roadmap](#roadmap)

</div>

---

This is a ground-up rewrite of the Go [`queueflow-core`](../queueflow-core), keeping the same product idea
and the OpenAPI-driven SDK pipeline, while fixing the Go version's biggest gaps.

> The original Go module lives, untouched, in `../queueflow-core`. This crate is a parallel, improved
> implementation. See [`repository-structure.md`](../repository-structure.md) for how the whole QueueFlow
> multi-repo project fits together.

## What's different from the Go version

| Area | Go `queueflow-core` | This Rust rewrite |
| --- | --- | --- |
| **Workflows** | Every workflow endpoint returns `501 Not Implemented` | A real DAG orchestrator: dependency gating, context propagation, per-step failure policies, cycle detection, a builder DSL, and a Mermaid diagram endpoint |
| **Tests** | None (CI references a `tests/integration` dir that doesn't exist) | 56 tests; the entire engine, scheduler, and HTTP API run against deterministic in-memory adapters with **no database**, plus opt-in Postgres integration tests |
| **Retries** | A detached goroutine with a timer — lost on process restart | Queue-native delayed redelivery (PGMQ `send` with delay) — survives restarts |
| **Delivery safety** | Silently ignored ack failures; no idempotency guard | Logged acks, a terminal-state idempotency guard, and "advance-before-ack" so a failed workflow step self-heals on redelivery |
| **Architecture** | Engine hard-wired to `pgxpool` | Ports & adapters: the engine depends on `JobStore`/`MessageQueue` traits; Postgres and in-memory adapters implement them |
| **OpenAPI spec** | Hand-maintained YAML that can drift | Generated **from the code** (utoipa), so it can't drift; emitted by `queueflow spec` |
| **Durations in the API** | Nanoseconds (awkward for every SDK) | Plain integer seconds |
| **Types** | Stringly-typed statuses/backoff | Exhaustive enums; illegal states unrepresentable |
| **Multi-tenancy** | Ownership checks missing on cancel/diagram | Tenant isolation enforced on every job/workflow endpoint |
| **SDKs** | Python + TypeScript | Python, TypeScript, **Rust, Go, Java** |

## Features

- 🐘 **PostgreSQL + [PGMQ](https://github.com/tembo-io/pgmq) native** — durable, transactional, at-least-once delivery; no extra infrastructure.
- 🔀 **Workflows as DAGs** — declare steps and `depends_on`; the engine schedules each step once its dependencies complete, threads results through a shared context, and aggregates the final status.
- ♻️ **Durable retries** — typed backoff (fixed / linear / exponential, capped, with jitter) scheduled *in the queue*, so retries outlive restarts.
- 🪦 **Dead-letter queue** — exhausted, non-retryable, and unhandled jobs land somewhere you can inspect and replay.
- 🧪 **Built to be tested** — a ports-and-adapters core means the whole system runs in memory, deterministically, with a controllable clock.
- 📜 **Code-generated OpenAPI** — the spec is derived from the handlers, then drives client SDKs in five languages.
- 📈 **Observable** — Prometheus metrics, structured JSON logs (`tracing`), health/readiness probes.
- 🛑 **Graceful shutdown** — drains in-flight work on `SIGINT`/`SIGTERM`.

## Architecture

```
queueflow-core-rs/                 Cargo workspace
├── crates/
│   ├── queueflow-core/            library: domain, ports, adapters, engine, workflow
│   │   ├── domain.rs              Job / Workflow / config / status enums
│   │   ├── ports.rs               JobStore + MessageQueue + Clock traits
│   │   ├── adapters/
│   │   │   ├── memory.rs          in-memory store + queue (tests / local dev)
│   │   │   ├── clock.rs           SystemClock + TestClock
│   │   │   └── postgres/          PostgreSQL + PGMQ adapters (feature "postgres")
│   │   ├── engine/                enqueue, worker loop, durable retry, DLQ
│   │   ├── workflow/              DAG validation, builder DSL, scheduler
│   │   └── api.rs                 object-safe JobApi facade
│   ├── queueflow-api/             axum router + utoipa OpenAPI
│   └── queueflow-server/          the `queueflow` binary (serve / spec / migrate)
├── migrations/                    sqlx migrations (schema + PGMQ)
├── sdk-configs/                   per-language openapi-generator configs
├── scripts/generate-sdks.sh       drives openapi-generator
└── Makefile
```

The key design choice is **ports & adapters**: `queueflow-core` is written against the `JobStore` and
`MessageQueue` traits, so the engine, the workflow scheduler, and the HTTP API can be exercised end-to-end
against fast, deterministic in-memory adapters — which is why this rewrite ships with real tests where the
original had none. In production the same code runs on the PostgreSQL + PGMQ adapters.

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
    let store = Arc::new(InMemoryJobStore::new());
    let queue = Arc::new(InMemoryMessageQueue::new(clock.clone()));

    let engine = Engine::builder(store, queue, clock)
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

### As a server (PostgreSQL + PGMQ)

```bash
# needs a PGMQ-enabled Postgres, e.g.:
docker run -d --name pg -p 5432:5432 -e POSTGRES_PASSWORD=postgres quay.io/tembo/pg16-pgmq:latest

export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
cargo run -p queueflow-server -- serve --mode all --workers 10 --api-port 8000
```

The server applies migrations on startup, exposes the REST API on `:8000`, Prometheus metrics on `:9090`,
and interactive docs at <http://localhost:8000/docs>.

### With Docker

```bash
make docker                       # builds ghcr.io/queueflow/queueflow:dev
docker run -p 8000:8000 -p 9090:9090 \
  -e DATABASE_URL=postgres://… \
  ghcr.io/queueflow/queueflow:dev serve
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

Every `/api/v1` route needs a bearer token. (Token validation is currently a placeholder that maps any
non-empty token to a tenant — see [Roadmap](#roadmap).)

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
| `POST /api/v1/jobs` | Enqueue a job |
| `POST /api/v1/jobs/batch` | Enqueue up to 1000 jobs |
| `GET /api/v1/jobs` | List jobs (filter by status/queue, paginate) |
| `GET /api/v1/jobs/{id}` | Fetch a job |
| `POST /api/v1/jobs/{id}/cancel` | Cancel a job |
| `POST /api/v1/workflows` | Create a workflow |
| `GET /api/v1/workflows` · `/{id}` · `/{id}/cancel` · `/{id}/diagram` | List / fetch / cancel / diagram |
| `GET /api/v1/tasks` · `/stats` | Registered handlers · engine counters |
| `GET /health` · `/ready` · `/docs` · `/openapi.json` | Probes · Swagger UI · spec |

## Configuration

| Flag | Env | Default | Meaning |
| --- | --- | --- | --- |
| `--mode` | `QUEUEFLOW_MODE` | `all` | `api`, `worker`, or `all` |
| `--database-url` | `DATABASE_URL` | — | PostgreSQL connection string |
| `--api-port` | `QUEUEFLOW_API_PORT` | `8000` | REST API port |
| `--workers` | `QUEUEFLOW_WORKERS` | `10` | Workers per queue |
| `--metrics-port` | `QUEUEFLOW_METRICS_PORT` | `9090` | Prometheus port |
| `--default-queue` | `QUEUEFLOW_DEFAULT_QUEUE` | `default` | Default queue name |

## Testing

```bash
make test        # unit + integration, no services required
make test-pg     # opt-in Postgres tests (needs TEST_DATABASE_URL + pgmq)
make clippy      # lint (warnings = errors)
make fmt-check   # formatting
```

Because the engine is generic over the ports, the durable-retry, backoff, timeout, dead-letter,
idempotency, and full workflow-orchestration behaviours are all verified deterministically with a
`TestClock` and in-memory adapters — no Docker, no Postgres. The Postgres adapter is then checked for
parity by the opt-in integration suite.

## SDK generation

The OpenAPI spec is generated from the Rust handlers and types, so it always matches the server:

```bash
make spec            # writes spec/openapi.{json,yaml} from the code
make validate-spec   # validate via openapi-generator (Docker)
make sdks            # regenerate Python, TypeScript, Rust, Go, and Java SDKs
make sdks-python     # ...or one at a time
```

Generated SDKs land in the sibling repos:
[Python](../queueflow-sdk-python) · [Node.js/TS](../queueflow-sdk-nodejs) ·
[Rust](../queueflow-sdk-rust) · [Go](../queueflow-sdk-go) · [Java](../queueflow-sdk-java).

## Documentation

- **API reference (rustdoc):** `cargo doc --open -p queueflow-core`
- **OpenAPI spec:** [`spec/openapi.yaml`](./spec/openapi.yaml) / [`spec/openapi.json`](./spec/openapi.json), or live at `/openapi.json` and `/docs`
- **Runnable examples:** [`crates/queueflow-core/examples/`](./crates/queueflow-core/examples)
- **Database schema:** [`migrations/0001_init.sql`](./migrations/0001_init.sql)
- **Multi-repo overview:** [`repository-structure.md`](../repository-structure.md)
- **Go reference implementation:** [`../queueflow-core`](../queueflow-core)

## Roadmap

Contributions welcome — these are the planned next steps, roughly in priority order:

- [ ] **Real authentication** — replace the placeholder token check with JWT signature/claims validation
      and an API-key store (the seam is `auth::validate_token`).
- [ ] **Conditional steps** — evaluate a per-step predicate (the `condition` column is reserved but unread today).
- [ ] **Scheduled / cron jobs** — recurring enqueues with a `next_run_at` scheduler.
- [ ] **DLQ admin API** — list, inspect, and replay dead-lettered jobs.
- [ ] **Priority lanes** — dedicated queues per priority class (PGMQ itself is FIFO).
- [ ] **Sub-workflows & fan-out** — a step that spawns a child workflow or a dynamic batch.
- [ ] **OpenTelemetry** — distributed tracing export alongside the Prometheus metrics.
- [ ] **Per-tenant rate limiting & quotas.**
- [ ] **Publish** — crates.io release and a Helm chart.

## Contributing

```bash
git clone https://github.com/sjriddle/queueflow-core
cd queueflow-core-rs
make test && make clippy && make fmt-check
```

PRs should keep `make test`, `make clippy`, and `make validate-spec` green. New behaviour belongs in a test
against the in-memory adapters; new endpoints/types are reflected in the OpenAPI spec automatically via their
`#[utoipa::path]` / `ToSchema` annotations.

## Requirements

- Rust 1.83+
- PostgreSQL 15+ with the [PGMQ](https://github.com/tembo-io/pgmq) extension
- Docker (only for `make validate-spec` / `make sdks` / `make docker`)

## Star history

<a href="https://star-history.com/#sjriddle/queueflow-core&Date">
  <img src="https://api.star-history.com/svg?repos=sjriddle/queueflow-core&type=Date" alt="Star History Chart" width="600">
</a>

## License

[MIT](./LICENSE)
