# QueueFlow Rust facade

This crate ships an ergonomic facade (`src/facade.rs`) layered over the generated client. It is the
recommended entry point. The generated `apis::*` modules and `models::*` types remain available for
anything the helpers do not cover.

## Quick start

```rust
use std::time::Duration;

use queueflow::{QueueFlow, WorkflowBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let qf = QueueFlow::new("http://localhost:8000", "dev");

    // Enqueue a job and wait for the result.
    let payload = std::collections::HashMap::from([
        ("hello".to_owned(), serde_json::json!("world")),
    ]);
    let job = qf.create_job("echo", Some(payload)).await?;
    let done = qf
        .wait_for_job(&job.id, Duration::from_secs(60), Duration::from_millis(500))
        .await?;
    println!("job {} finished as {}", done.id, done.status);

    // Define and run a DAG. `build()` validates it locally (duplicate names,
    // dangling deps, cycles) before anything goes over the wire.
    let builder = WorkflowBuilder::new("etl")
        .step("extract", "extract_task", &[])
        .step("transform", "transform_task", &["extract"])
        .step("load", "load_task", &["transform"]);
    let wf = qf.create_workflow(&builder).await?;
    let finished = qf
        .wait_for_workflow(&wf.id, Duration::from_secs(300), Duration::from_secs(1))
        .await?;
    println!("workflow {} finished as {}", finished.id, finished.status);
    Ok(())
}
```

## What the facade adds

- `QueueFlow::new(base_url, token)`: one-line client setup with bearer auth.
- `create_job` / `create_workflow`: enqueue, then fetch and return the full record.
- `wait_for_job` / `wait_for_workflow`: poll until a terminal status or time out.
- `WorkflowBuilder`: a typed DAG builder that rejects structurally invalid workflows before any
  network round-trip.

Everything else (leasing, heartbeats, batch enqueue, stats, diagrams) is available on the generated
modules, e.g. `queueflow::apis::worker_api::lease_jobs(&qf.config, ...)`.

## Worker protocol: use `queueflow-client` instead

If you are writing a Rust worker, prefer the first-party `queueflow-client` crate (in the
`queueflow-core` workspace): it shares the server's own domain types and ships a worker runtime
with automatic heartbeating, lease-loss detection, and concurrent batch processing. This crate
exposes the raw worker endpoints only.

If you do call `worker_api` directly, three rules keep the at-least-once contract honest:

1. Worker routes authenticate with the **worker token**, not a tenant token. Build a second
   `Configuration` for it:

   ```rust
   let mut worker_config = qf.config.clone();
   worker_config.bearer_access_token = Some(worker_token.to_owned());
   ```

2. Heartbeat every in-flight job at roughly half its lease interval. A heartbeat whose `status`
   is anything other than `running` (or an HTTP 409) means the server owns the outcome: abandon
   the handler and report nothing.
3. Delivery is at-least-once, so handlers must be idempotent. Report permanent failures with
   `retryable: false` so they dead-letter immediately instead of burning retries.

## Known limitation: `stream_job_events`

The generated `jobs_api::stream_job_events` cannot consume the server's SSE stream: it buffers
the whole response until the stream closes and then fails to decode it. Use
`QueueFlow::wait_for_job` (polling) instead, or a hand-rolled SSE consumer over
`GET /api/v1/jobs/{id}/events`.
