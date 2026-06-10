//! Workflow DAG orchestration tests on the in-memory adapters.
//!
//! Covers dependency-gated scheduling, fan-out/fan-in, the three failure
//! policies (halt / skip / continue), status aggregation, context propagation,
//! and creation-time cycle rejection.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use queueflow_core::*;
use serde_json::json;

type Mem = Engine<InMemoryJobStore>;

fn map(v: serde_json::Value) -> Map {
    v.as_object()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect()
}

/// Process jobs until the queue is drained. Because completing a step enqueues
/// its now-ready dependents synchronously, this runs an entire workflow.
async fn drain(engine: &Arc<Mem>) {
    while engine.process_once("default").await.unwrap() {}
}

struct Ctx {
    engine: Arc<Mem>,
    store: Arc<InMemoryJobStore>,
    order: Arc<Mutex<Vec<String>>>,
}

/// Build an engine whose handlers record execution order and implement a few
/// behaviours selected by payload.
fn ctx(ctx_seen: Arc<AtomicBool>) -> Ctx {
    let clock = Arc::new(TestClock::epoch());
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));
    let order = Arc::new(Mutex::new(Vec::<String>::new()));

    let o1 = order.clone();
    let o2 = order.clone();

    let engine = Engine::builder(store.clone(), clock)
        // Records its `who`, returns `{"who": who}`.
        .register_fn("ok", move |p: Map| {
            let o = o1.clone();
            async move {
                let who = p
                    .get("who")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
                    .to_string();
                o.lock().unwrap().push(who.clone());
                Ok(map(json!({ "who": who })))
            }
        })
        // Always fails permanently (no retries), records that it ran.
        .register_fn("fail", move |p: Map| {
            let o = o2.clone();
            async move {
                let who = p
                    .get("who")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
                    .to_string();
                o.lock().unwrap().push(who);
                Err(HandlerError::permanent("intentional"))
            }
        })
        // Emits a value into the workflow context.
        .register_fn("emit", |_p| async { Ok(map(json!({ "value": 42 }))) })
        // Asserts it received upstream context, sets a flag.
        .register_fn("check", move |p: Map| {
            let seen = ctx_seen.clone();
            async move {
                let v = p
                    .get(CONTEXT_KEY)
                    .and_then(|c| c.get("emitter"))
                    .and_then(|e| e.get("value"))
                    .and_then(|v| v.as_i64());
                if v == Some(42) {
                    seen.store(true, Ordering::SeqCst);
                }
                Ok(Map::new())
            }
        })
        .build();

    Ctx {
        engine,
        store,
        order,
    }
}

async fn step_status(store: &Arc<InMemoryJobStore>, wf: &str, name: &str) -> StepStatus {
    store
        .workflow_step_statuses(wf)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.name == name)
        .unwrap_or_else(|| panic!("step {name} not found"))
        .status
}

fn step(name: &str, task: &str, deps: &[&str]) -> StepBuilder {
    let mut b = StepBuilder::new(name)
        .task(task)
        .payload_value("who", json!(name));
    for d in deps {
        b = b.after(*d);
    }
    b
}

#[tokio::test]
async fn linear_workflow_runs_in_order_and_completes() {
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("order")
        .step(step("validate", "ok", &[]))
        .step(step("pay", "ok", &["validate"]))
        .step(step("ship", "ok", &["pay"]))
        .build()
        .unwrap();

    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c.engine).await;

    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );
    assert_eq!(*c.order.lock().unwrap(), vec!["validate", "pay", "ship"]);
}

#[tokio::test]
async fn diamond_fan_out_fan_in_respects_dependencies() {
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("diamond")
        .step(step("a", "ok", &[]))
        .step(step("b", "ok", &["a"]))
        .step(step("c", "ok", &["a"]))
        .step(step("d", "ok", &["b", "c"]))
        .build()
        .unwrap();

    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c.engine).await;

    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );
    let order = c.order.lock().unwrap().clone();
    let pos = |n: &str| order.iter().position(|x| x == n).unwrap();
    assert!(pos("a") < pos("b") && pos("a") < pos("c"));
    assert!(pos("b") < pos("d") && pos("c") < pos("d"));
}

#[tokio::test]
async fn halt_policy_fails_workflow_and_cancels_downstream() {
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("halt")
        .step(step("a", "ok", &[]))
        .step(step("b", "fail", &["a"])) // default on_failure = Halt
        .step(step("c", "ok", &["b"]))
        .build()
        .unwrap();

    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c.engine).await;

    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Failed
    );
    assert_eq!(step_status(&c.store, &id, "a").await, StepStatus::Completed);
    assert_eq!(step_status(&c.store, &id, "b").await, StepStatus::Failed);
    assert_eq!(step_status(&c.store, &id, "c").await, StepStatus::Cancelled);
}

#[tokio::test]
async fn continue_policy_yields_partially_failed() {
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("continue")
        .step(step("a", "ok", &[]))
        .step(step("b", "fail", &["a"]).on_failure(OnFailure::Continue))
        .step(step("c", "ok", &["a"]))
        .step(step("d", "ok", &["b"]))
        .build()
        .unwrap();

    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c.engine).await;

    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::PartiallyFailed
    );
    assert_eq!(step_status(&c.store, &id, "a").await, StepStatus::Completed);
    assert_eq!(step_status(&c.store, &id, "b").await, StepStatus::Failed);
    assert_eq!(step_status(&c.store, &id, "c").await, StepStatus::Completed);
    assert_eq!(step_status(&c.store, &id, "d").await, StepStatus::Skipped);
}

#[tokio::test]
async fn skip_policy_skips_failed_step_and_dependents() {
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("skip")
        .step(step("a", "ok", &[]))
        .step(step("b", "fail", &["a"]).on_failure(OnFailure::Skip))
        .step(step("c", "ok", &["a"]))
        .step(step("d", "ok", &["b"]))
        .build()
        .unwrap();

    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c.engine).await;

    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::PartiallyFailed
    );
    assert_eq!(step_status(&c.store, &id, "b").await, StepStatus::Skipped);
    assert_eq!(step_status(&c.store, &id, "c").await, StepStatus::Completed);
    assert_eq!(step_status(&c.store, &id, "d").await, StepStatus::Skipped);
}

#[tokio::test]
async fn context_propagates_from_upstream_step() {
    let seen = Arc::new(AtomicBool::new(false));
    let c = ctx(seen.clone());
    let req = WorkflowBuilder::new("ctx")
        .step(StepBuilder::new("emitter").task("emit"))
        .step(StepBuilder::new("checker").task("check").after("emitter"))
        .build()
        .unwrap();

    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c.engine).await;

    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );
    assert!(
        seen.load(Ordering::SeqCst),
        "downstream step should see upstream context"
    );
}

#[tokio::test]
async fn cyclic_workflow_is_rejected_before_persisting() {
    let c = ctx(Arc::new(AtomicBool::new(false)));
    // Bypass the builder (which would also reject) to ensure the engine itself
    // validates and persists nothing.
    let req = CreateWorkflowRequest {
        name: "cyclic".into(),
        steps: vec![
            WorkflowStep {
                name: "a".into(),
                task_name: "ok".into(),
                depends_on: vec!["b".into()],
                ..Default::default()
            },
            WorkflowStep {
                name: "b".into(),
                task_name: "ok".into(),
                depends_on: vec!["a".into()],
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let err = c.engine.create_workflow(req, None).await.unwrap_err();
    assert!(matches!(err, EngineError::Workflow(_)));
    let page = c
        .engine
        .list_workflows(&ListFilter {
            include_total: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.total, Some(0));
    assert!(page.items.is_empty());
}

#[tokio::test]
async fn fan_in_step_runs_exactly_once() {
    // a and b fan in to c. Each completion re-runs `advance`, and only the
    // claim (link_step_job) guards c from being enqueued twice.
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = CreateWorkflowRequest {
        name: "fan-in".into(),
        steps: vec![
            WorkflowStep {
                name: "a".into(),
                task_name: "ok".into(),
                payload: map(json!({"who": "a"})),
                ..Default::default()
            },
            WorkflowStep {
                name: "b".into(),
                task_name: "ok".into(),
                payload: map(json!({"who": "b"})),
                ..Default::default()
            },
            WorkflowStep {
                name: "c".into(),
                task_name: "ok".into(),
                payload: map(json!({"who": "c"})),
                depends_on: vec!["a".into(), "b".into()],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c.engine).await;

    let runs = c
        .order
        .lock()
        .unwrap()
        .iter()
        .filter(|w| w.as_str() == "c")
        .count();
    assert_eq!(runs, 1, "join step must execute exactly once");
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );
}

#[tokio::test]
async fn step_claim_is_atomic() {
    // The storage-level guard behind the fan-in property: only the first
    // link wins; the loser must not publish.
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = CreateWorkflowRequest {
        name: "claim".into(),
        steps: vec![WorkflowStep {
            name: "only".into(),
            task_name: "ok".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let id = c.engine.create_workflow(req, None).await.unwrap();

    // The scheduler already claimed "only" while starting the workflow.
    let claimed = c
        .store
        .link_step_job(&id, "only", "intruder")
        .await
        .unwrap();
    assert!(!claimed, "a second claim on a linked step must lose");
}

#[tokio::test]
async fn janitor_heals_a_completed_job_with_unfinished_step() {
    // Simulate a worker that crashed after the job's terminal write but
    // before the workflow advance: finish the step job directly through the
    // store, bypassing the scheduler entirely.
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("stall")
        .step(StepBuilder::new("a").task("ok"))
        .step(StepBuilder::new("b").task("ok").after("a"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();

    let claimed = c.store.claim_jobs("default", 1, 30).await.unwrap();
    let lease = &claimed.jobs[0];
    assert!(c
        .store
        .finish_if_leased(
            &lease.job.id,
            &lease.lease_token,
            JobStatus::Completed,
            None,
            None
        )
        .await
        .unwrap());
    assert_eq!(step_status(&c.store, &id, "a").await, StepStatus::Pending);

    // Job terminal + step non-terminal = exactly what the janitor heals.
    let report = c.engine.janitor_sweep().await;
    assert!(report.healed_steps >= 1, "got: {report:?}");
    assert_eq!(step_status(&c.store, &id, "a").await, StepStatus::Completed);

    // The heal re-drove the advance, so b is claimable; drain to the end.
    drain(&c.engine).await;
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );
}

#[tokio::test]
async fn janitor_advances_a_stalled_workflow() {
    // Simulate a crash *after* the step-status write but before dependents
    // were enqueued: the step is terminal, the workflow has no live jobs, and
    // nothing will ever advance it without the janitor.
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("stall2")
        .step(StepBuilder::new("a").task("ok"))
        .step(StepBuilder::new("b").task("ok").after("a"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();

    let claimed = c.store.claim_jobs("default", 1, 30).await.unwrap();
    let lease = &claimed.jobs[0];
    assert!(c
        .store
        .finish_if_leased(
            &lease.job.id,
            &lease.lease_token,
            JobStatus::Completed,
            None,
            None
        )
        .await
        .unwrap());
    c.store
        .set_step_status(&id, "a", StepStatus::Completed, None)
        .await
        .unwrap();

    let report = c.engine.janitor_sweep().await;
    assert!(report.advanced_workflows >= 1, "got: {report:?}");

    drain(&c.engine).await;
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );
}

#[tokio::test]
async fn cancelling_a_workflow_cancels_its_inflight_jobs() {
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("cancel-live")
        .step(step("a", "ok", &[]))
        .step(step("b", "ok", &["a"]))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();

    // a's job is pending; cancel the workflow before any worker claims it.
    c.engine.cancel_workflow(&id).await.unwrap();

    // The step job was cancelled with its workflow, so it is not claimable
    // and no handler ever runs for the dead workflow.
    assert!(!c.engine.process_once("default").await.unwrap());
    assert!(c.order.lock().unwrap().is_empty());

    let job_id = c
        .store
        .workflow_step_statuses(&id)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.name == "a")
        .unwrap()
        .job_id
        .unwrap();
    assert_eq!(
        c.engine.get_job(&job_id).await.unwrap().status,
        JobStatus::Cancelled
    );
}

#[tokio::test]
async fn cancelled_step_job_settles_the_workflow_via_janitor() {
    // Cancelling an individual step *job* (not the whole workflow) leaves a
    // terminal job under a non-terminal step; the janitor routes it through
    // the step's failure policy so the workflow still reaches a terminal
    // state instead of hanging forever.
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("cancel-step-job")
        .step(step("a", "ok", &[]))
        .step(step("b", "ok", &["a"]))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();

    let job_id = c
        .store
        .workflow_step_statuses(&id)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.name == "a")
        .unwrap()
        .job_id
        .unwrap();
    c.engine.cancel_job(&job_id).await.unwrap();

    let report = c.engine.janitor_sweep().await;
    assert!(report.healed_steps >= 1, "got: {report:?}");

    // Default policy is halt: a is failed ("job was cancelled"), b cancelled.
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Failed
    );
    assert_eq!(step_status(&c.store, &id, "a").await, StepStatus::Failed);
    assert_eq!(step_status(&c.store, &id, "b").await, StepStatus::Cancelled);
}

#[tokio::test]
async fn cancelling_a_finished_workflow_is_a_conflict() {
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = CreateWorkflowRequest {
        name: "done".into(),
        steps: vec![WorkflowStep {
            name: "a".into(),
            task_name: "ok".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c.engine).await;
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );

    let err = c.engine.cancel_workflow(&id).await.unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)));
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed,
        "terminal workflow status must never be rewritten"
    );
}
