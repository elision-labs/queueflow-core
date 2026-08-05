# queueflow-client

Rust client for the [QueueFlow](https://github.com/sjriddle/queueflow-core)
REST API, including a remote worker runtime with automatic heartbeating.

QueueFlow is a PostgreSQL-native job queue and workflow engine. This crate
reuses the engine's own domain types (`Job`, `Workflow`, statuses, configs),
so requests and responses cannot drift from the server.

## Producer

```rust,ignore
use queueflow_client::Client;

let client = Client::new("http://localhost:8000", "my-token");
let job = client.create_job("resize-image", payload, Default::default()).await?;
let done = client.wait_for_job(&job.id, std::time::Duration::from_secs(60)).await?;
```

## Remote worker (any queue, at-least-once)

```rust,ignore
use queueflow_client::{Client, Map};
use queueflow_client::worker::{Worker, WorkerOptions};

// Use the deployment's worker token, not a tenant token.
let client = Client::new("http://localhost:8000", worker_token);
Worker::new(client, "default", WorkerOptions::default())
    .register("resize-image", |job| async move {
        // ... do the work ...
        Ok(Map::new())
    })
    .run()
    .await;
```

The worker leases jobs over HTTP, heartbeats while handlers run (observing
mid-run cancellation), reports outcomes, and gets the same retry, dead-letter,
and workflow semantics as handlers compiled into the server.

License: MIT.
