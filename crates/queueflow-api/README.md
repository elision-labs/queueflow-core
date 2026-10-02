# queueflow-api

The HTTP surface of [QueueFlow](https://github.com/elision-labs/queueflow-core):
an axum router plus the utoipa-generated OpenAPI document for the
PostgreSQL-native job queue and workflow engine.

This crate is the embeddable API layer. It depends only on the object-safe
`queueflow_core::JobApi` facade, so the same router serves a Postgres-backed
engine in production and the in-memory engine in tests. If you just want a
running server, use the [`queueflow`](https://crates.io/crates/queueflow)
binary instead.

```rust,ignore
use queueflow_api::{build_router, ApiState};

let state = ApiState::new(engine)                    // engine: Arc<dyn JobApi>
    .with_worker_token(std::env::var("QUEUEFLOW_WORKER_TOKEN").ok());
let app = build_router(state);
axum::serve(listener, app).await?;
```

The router provides:

- Job endpoints: create (idempotency-key aware), batch create, list, fetch,
  cancel, and a Server-Sent Events status stream.
- Workflow endpoints: create (DAG-validated), list, fetch, cancel, Mermaid
  diagram.
- The polyglot worker protocol: lease, heartbeat, complete, fail, gated by a
  dedicated worker credential.
- `/health`, `/ready`, Swagger UI at `/docs`, and the spec at
  `/openapi.json`.

The OpenAPI document is generated from the handler annotations, so it cannot
drift from the code.

License: MIT.
