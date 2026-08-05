# queueflow-core

The core engine of [QueueFlow](https://github.com/sjriddle/queueflow-core): a
PostgreSQL-native distributed job queue and workflow (DAG) engine.

The jobs table *is* the queue: workers claim due rows with
`FOR UPDATE SKIP LOCKED` and own them through lease tokens, so any plain
PostgreSQL 13+ works with no extensions and no extra infrastructure. Durable
retries with typed backoff, a dead-letter queue, DAG workflows with dependency
gating and context propagation, and a janitor that self-heals crashed workers
are all part of the engine.

## Design

Ports and adapters: the engine is written against the `JobStore` trait, with
two adapters included.

- `InMemoryJobStore` runs the entire engine (retries, timeouts, workflows,
  janitor sweeps) deterministically with a controllable `TestClock`, so your
  tests need no database.
- `PostgresJobStore` (feature `postgres`) is the production adapter; the
  bundled sqlx migrations create its schema.

## Quick start (no database)

```rust
use std::sync::Arc;
use queueflow_core::*;
use queueflow_core::task::builtin;

#[tokio::main]
async fn main() -> Result<(), EngineError> {
    let clock = Arc::new(SystemClock);
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));

    let engine = Engine::builder(store, clock)
        .register("echo", builtin::echo())
        .build();

    let id = engine.enqueue("echo", Map::new(), Default::default()).await?;
    engine.process_once("default").await?;
    assert_eq!(engine.get_job(&id).await?.status, JobStatus::Completed);
    Ok(())
}
```

## PostgreSQL

```rust,ignore
let pool = queueflow_core::connect(&database_url, 20).await?;
queueflow_core::migrate(&pool).await?;               // embedded migrations
let store = Arc::new(PostgresJobStore::new(pool));
let engine = Engine::builder(store, Arc::new(SystemClock)).build();
let workers = engine.run_workers("default");          // supervised worker pool
let janitor = engine.run_janitor();                   // lease recovery + self-heal
```

Enable with:

```toml
queueflow-core = { version = "0.1", features = ["postgres"] }
```

## Related crates

- [`queueflow`](https://crates.io/crates/queueflow): the ready-to-run server
  binary and CLI.
- [`queueflow-client`](https://crates.io/crates/queueflow-client): Rust client
  for the REST API, including the remote worker runtime.
- [`queueflow-api`](https://crates.io/crates/queueflow-api): the axum HTTP
  layer, embeddable in your own binary.

License: MIT.
