//! Build and run a DAG workflow in memory, then print its Mermaid diagram.
//!
//! ```text
//! cargo run --example workflow -p queueflow-core
//! ```

use std::sync::Arc;

use queueflow_core::task::builtin;
use queueflow_core::workflow::{StepBuilder, WorkflowBuilder};
use queueflow_core::*;

#[tokio::main]
async fn main() -> Result<(), EngineError> {
    let clock = Arc::new(SystemClock);
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));

    let engine = Engine::builder(store, clock)
        // One generic handler stands in for every step's work.
        .register("step", builtin::echo())
        .build();

    // extract -> transform -> load, with a parallel "notify" branch off extract.
    let req = WorkflowBuilder::new("etl")
        .step(StepBuilder::new("extract").task("step"))
        .step(StepBuilder::new("transform").task("step").after("extract"))
        .step(StepBuilder::new("load").task("step").after("transform"))
        .step(StepBuilder::new("notify").task("step").after("extract"))
        .build()?; // validates the DAG (a cycle would be an Err here)

    let id = engine.create_workflow(req, None).await?;

    // Completing a step enqueues its now-ready dependents, so draining the
    // queue runs the whole workflow.
    while engine.process_once("default").await? {}

    let wf = engine.get_workflow(&id).await?;
    println!("workflow {} -> {}", wf.id, wf.status.as_str());
    println!(
        "\nMermaid diagram:\n{}",
        engine.workflow_diagram(&id).await?
    );
    Ok(())
}
