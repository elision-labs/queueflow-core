# queueflow

The [QueueFlow](https://github.com/sjriddle/queueflow-core) server and CLI: a
high-performance, PostgreSQL-native distributed job queue and workflow engine.

Durable background jobs and real DAG workflows on a database you already run:
no Redis, no broker, no extensions. Any plain PostgreSQL 13+ (RDS, Cloud SQL,
Azure, your laptop) works.

## Install and run

```bash
cargo install queueflow

export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
queueflow serve --mode all --workers 10 --api-port 8000
```

The server applies migrations on startup, serves the REST API on `:8000`
(interactive docs at `/docs`), and Prometheus metrics on `:9090`. Handlers can
be compiled in (Rust) or run in any language via the HTTP worker protocol;
set `--worker-token` to give workers their own credential.

## CLI client

The same binary doubles as a client for a running server:

```bash
queueflow job create --task echo --payload '{"hello":"world"}' --wait
queueflow job list --status pending --limit 20
queueflow workflow create --file etl.json
queueflow workflow diagram <id>
queueflow spec               # emit the OpenAPI document
```

## Related crates

- [`queueflow-core`](https://crates.io/crates/queueflow-core): the engine as a
  library (embed jobs and workflows in your own binary, test in memory).
- [`queueflow-client`](https://crates.io/crates/queueflow-client): Rust client
  and remote worker runtime.
- [`queueflow-api`](https://crates.io/crates/queueflow-api): the embeddable
  axum HTTP layer.

License: MIT.
