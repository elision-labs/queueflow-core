//! Enqueue and process jobs entirely in memory — no database required.
//!
//! ```text
//! cargo run --example in_memory_jobs -p queueflow-core
//! ```

use std::sync::Arc;

use queueflow_core::task::builtin;
use queueflow_core::*;
use serde_json::json;

fn to_map(v: serde_json::Value) -> Map {
    v.as_object()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), EngineError> {
    let clock = Arc::new(SystemClock);
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));

    let engine = Engine::builder(store, clock)
        .register("echo", builtin::echo())
        .register_fn("sum", |p: Map| async move {
            let a = p.get("a").and_then(|v| v.as_i64()).unwrap_or(0);
            let b = p.get("b").and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(Map::from_iter([("sum".to_string(), json!(a + b))]))
        })
        .build();

    let id = engine
        .enqueue("sum", to_map(json!({"a": 2, "b": 3})), Default::default())
        .await?;

    // Drive one job synchronously (in a server this is the worker loop).
    engine.process_once("default").await?;

    let job = engine.get_job(&id).await?;
    println!(
        "job {} -> {} result={:?}",
        job.id,
        job.status.as_str(),
        job.result
    );
    println!("stats: {:?}", engine.stats().snapshot());
    Ok(())
}
