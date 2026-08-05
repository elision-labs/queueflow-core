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
