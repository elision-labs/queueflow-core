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
    /// Payload received by the last `grab` handler invocation.
    grabbed: Arc<Mutex<Option<Map>>>,
}

/// Build an engine whose handlers record execution order and implement a few
/// behaviours selected by payload.
fn ctx(ctx_seen: Arc<AtomicBool>) -> Ctx {
    let clock = Arc::new(TestClock::epoch());
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));
    let order = Arc::new(Mutex::new(Vec::<String>::new()));
    let grabbed = Arc::new(Mutex::new(None::<Map>));

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
        // Stores the payload it received, for asserting on injected context.
        .register_fn("grab", {
            let grabbed = grabbed.clone();
            move |p: Map| {
                let g = grabbed.clone();
                async move {
                    *g.lock().unwrap() = Some(p);
                    Ok(Map::new())
                }
            }
        })
        .build();

    Ctx {
        engine,
        store,
        order,
        grabbed,
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

    let claimed = c.store.claim_jobs("default", 1, 30, false).await.unwrap();
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
        .unwrap()
        .is_some());
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

    let claimed = c.store.claim_jobs("default", 1, 30, false).await.unwrap();
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
        .unwrap()
        .is_some());
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

#[tokio::test]
async fn workflow_with_absurd_step_config_is_rejected_before_persisting() {
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("bad-config")
        .step(step("a", "ok", &[]).config(JobConfig {
            retry_max_delay_secs: u64::MAX,
            ..JobConfig::default()
        }))
        .build()
        .unwrap();

    let err = c.engine.create_workflow(req, None).await.unwrap_err();
    assert!(matches!(err, EngineError::Validation(_)), "got: {err:?}");

    let page = c
        .engine
        .list_workflows(&ListFilter {
            include_total: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.total, Some(0), "nothing may be persisted");
}

#[tokio::test]
async fn halt_cancels_already_scheduled_sibling_jobs() {
    let c = ctx(Arc::new(AtomicBool::new(false)));
    // Both roots are enqueued when the workflow starts; priority makes the
    // failing step claimable first while its sibling's job is still pending.
    let req = WorkflowBuilder::new("halt-siblings")
        .step(step("boom", "fail", &[]).config(JobConfig {
            priority: 10,
            ..JobConfig::default()
        }))
        .step(step("slow", "ok", &[]))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();

    // One tick claims and permanently fails "boom", halting the workflow.
    assert!(c.engine.process_once("default").await.unwrap());

    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Failed
    );
    assert_eq!(
        step_status(&c.store, &id, "slow").await,
        StepStatus::Cancelled
    );

    // The sibling's already-enqueued job must not stay claimable: no worker
    // should start fresh work for a halted workflow.
    let slow_job = c
        .store
        .workflow_step_statuses(&id)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.name == "slow")
        .unwrap()
        .job_id
        .expect("slow was scheduled at workflow start");
    assert_eq!(
        c.engine.get_job(&slow_job).await.unwrap().status,
        JobStatus::Cancelled
    );
    assert!(!c.engine.process_once("default").await.unwrap());
    assert!(!c.order.lock().unwrap().iter().any(|w| w == "slow"));
}

#[tokio::test]
async fn fan_in_join_step_receives_context_from_all_dependencies() {
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("fan-in-ctx")
        .step(step("x", "ok", &[]))
        .step(step("y", "ok", &[]))
        .step(step("z", "grab", &["x", "y"]))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c.engine).await;

    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );
    let payload = c.grabbed.lock().unwrap().clone().expect("z ran");
    let ctx_val = payload.get(CONTEXT_KEY).expect("z received _context");
    assert_eq!(
        ctx_val.get("x").and_then(|v| v.get("who")),
        Some(&json!("x"))
    );
    assert_eq!(
        ctx_val.get("y").and_then(|v| v.get("who")),
        Some(&json!("y"))
    );
}

#[tokio::test]
async fn janitor_heals_a_crash_between_context_merge_and_status_write() {
    // `on_step_completed` merges the context first and marks the step
    // Completed second. Simulate a worker that crashed between the two
    // writes: the job is terminal, the context already holds its result, but
    // the step is still pending. The stalled-step sweep must re-drive the
    // completion so the join step is scheduled with a complete context.
    let c = ctx(Arc::new(AtomicBool::new(false)));
    let req = WorkflowBuilder::new("crash-window")
        .step(step("x", "ok", &[]))
        .step(step("y", "ok", &[]))
        .step(step("z", "grab", &["x", "y"]))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();

    // Claim both root jobs; drive x through the engine normally.
    let claimed = c
        .store
        .claim_jobs("default", 2, 30, false)
        .await
        .unwrap()
        .jobs;
    let by_step = |name: &str| {
        claimed
            .iter()
            .find(|l| l.job.workflow_step_id.as_deref() == Some(name))
            .expect("both roots were claimed")
            .clone()
    };
    c.engine.process_job(by_step("x")).await;

    // y: terminal write and context merge land, then the "crash" (no status
    // write, no advance).
    let y = by_step("y");
    let result = json!({"who": "y"});
    assert!(c
        .store
        .finish_if_leased(
            &y.job.id,
            &y.lease_token,
            JobStatus::Completed,
            None,
            Some(&result)
        )
        .await
        .unwrap()
        .is_some());
    c.store
        .merge_workflow_context(&id, "y", &result)
        .await
        .unwrap();

    let report = c.engine.janitor_sweep().await;
    assert!(report.healed_steps >= 1, "got: {report:?}");

    drain(&c.engine).await;
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );
    let payload = c.grabbed.lock().unwrap().clone().expect("z ran");
    let ctx_val = payload.get(CONTEXT_KEY).expect("z received _context");
    assert_eq!(
        ctx_val.get("x").and_then(|v| v.get("who")),
        Some(&json!("x")),
        "context from x: {ctx_val:?}"
    );
    assert_eq!(
        ctx_val.get("y").and_then(|v| v.get("who")),
        Some(&json!("y")),
        "y's result must survive the crash window: {ctx_val:?}"
    );
}

// ---- Write-order probe -------------------------------------------------------

/// A [`JobStore`] wrapper that records the relative order of workflow context
/// merges and step-status writes, delegating everything else to the in-memory
/// store. Guards the invariant that makes fan-in context propagation safe
/// under concurrency: a step's result is merged into the context BEFORE the
/// step is marked Completed, so an observer that sees a terminal dependency
/// can trust that its context contribution is already visible.
struct OrderProbe {
    inner: InMemoryJobStore,
    events: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl JobStore for OrderProbe {
    async fn create_job(&self, job: &Job) -> Result<bool, StorageError> {
        self.inner.create_job(job).await
    }
    async fn batch_create_jobs(&self, jobs: &[Job]) -> Result<Vec<String>, StorageError> {
        self.inner.batch_create_jobs(jobs).await
    }
    async fn get_job(&self, id: &str) -> Result<Job, StorageError> {
        self.inner.get_job(id).await
    }
    async fn list_jobs(&self, filter: &ListFilter) -> Result<Page<Job>, StorageError> {
        self.inner.list_jobs(filter).await
    }
    async fn find_job_by_idempotency_key(
        &self,
        tenant_id: Option<&str>,
        key: &str,
    ) -> Result<Option<Job>, StorageError> {
        self.inner.find_job_by_idempotency_key(tenant_id, key).await
    }
    async fn claim_jobs(
        &self,
        queue: &str,
        count: usize,
        lease_secs: u32,
        cover_timeout: bool,
    ) -> Result<Claimed, StorageError> {
        self.inner
            .claim_jobs(queue, count, lease_secs, cover_timeout)
            .await
    }
    async fn extend_lease(
        &self,
        job_id: &str,
        token: &str,
        lease_secs: u32,
    ) -> Result<Option<JobStatus>, StorageError> {
        self.inner.extend_lease(job_id, token, lease_secs).await
    }
    async fn finish_if_leased(
        &self,
        job_id: &str,
        token: &str,
        status: JobStatus,
        error: Option<&str>,
        result: Option<&Json>,
    ) -> Result<Option<FinishedJob>, StorageError> {
        self.inner
            .finish_if_leased(job_id, token, status, error, result)
            .await
    }
    async fn await_work(
        &self,
        queue: &str,
        since_epoch: u64,
        max_wait: std::time::Duration,
    ) -> Result<(), StorageError> {
        self.inner.await_work(queue, since_epoch, max_wait).await
    }
    async fn cancel_job_if_active(&self, id: &str, reason: &str) -> Result<bool, StorageError> {
        self.inner.cancel_job_if_active(id, reason).await
    }
    async fn mark_retrying(
        &self,
        id: &str,
        token: &str,
        retry_count: u32,
        next_retry_at: chrono::DateTime<chrono::Utc>,
        error: &str,
    ) -> Result<bool, StorageError> {
        self.inner
            .mark_retrying(id, token, retry_count, next_retry_at, error)
            .await
    }
    async fn move_to_dlq(&self, id: &str, reason: &str, error: &str) -> Result<(), StorageError> {
        self.inner.move_to_dlq(id, reason, error).await
    }
    async fn count_dead_letters(&self) -> Result<i64, StorageError> {
        self.inner.count_dead_letters().await
    }
    async fn list_dead_letters(
        &self,
        filter: &ListFilter,
    ) -> Result<Page<DeadLetter>, StorageError> {
        self.inner.list_dead_letters(filter).await
    }
    async fn get_dead_letter(&self, id: i64) -> Result<DeadLetter, StorageError> {
        self.inner.get_dead_letter(id).await
    }
    async fn replay_dead_letter(&self, id: i64, replacement: &Job) -> Result<bool, StorageError> {
        self.inner.replay_dead_letter(id, replacement).await
    }
    async fn create_cron(&self, cron: &CronSchedule) -> Result<bool, StorageError> {
        self.inner.create_cron(cron).await
    }
    async fn get_cron(&self, id: &str) -> Result<CronSchedule, StorageError> {
        self.inner.get_cron(id).await
    }
    async fn list_crons(&self, filter: &ListFilter) -> Result<Page<CronSchedule>, StorageError> {
        self.inner.list_crons(filter).await
    }
    async fn delete_cron(&self, id: &str) -> Result<bool, StorageError> {
        self.inner.delete_cron(id).await
    }
    async fn set_cron_enabled(
        &self,
        id: &str,
        enabled: bool,
        next_run_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        self.inner.set_cron_enabled(id, enabled, next_run_at).await
    }
    async fn due_crons(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        limit: usize,
    ) -> Result<Vec<CronSchedule>, StorageError> {
        self.inner.due_crons(now, limit).await
    }
    async fn advance_cron(
        &self,
        id: &str,
        fired_at: chrono::DateTime<chrono::Utc>,
        next_run_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        self.inner.advance_cron(id, fired_at, next_run_at).await
    }
    async fn ping(&self) -> Result<(), StorageError> {
        self.inner.ping().await
    }
    async fn claim_expired_leases(
        &self,
        limit: usize,
        lease_secs: u32,
    ) -> Result<Vec<LeasedJob>, StorageError> {
        self.inner.claim_expired_leases(limit, lease_secs).await
    }
    async fn stalled_step_jobs(&self, limit: usize) -> Result<Vec<Job>, StorageError> {
        self.inner.stalled_step_jobs(limit).await
    }
    async fn stalled_workflow_ids(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        self.inner.stalled_workflow_ids(limit).await
    }
    async fn purge_terminal(
        &self,
        older_than: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, StorageError> {
        self.inner.purge_terminal(older_than).await
    }
    async fn create_workflow(&self, wf: &Workflow) -> Result<(), StorageError> {
        self.inner.create_workflow(wf).await
    }
    async fn get_workflow(&self, id: &str) -> Result<Workflow, StorageError> {
        self.inner.get_workflow(id).await
    }
    async fn list_workflows(&self, filter: &ListFilter) -> Result<Page<Workflow>, StorageError> {
        self.inner.list_workflows(filter).await
    }
    async fn workflow_step_statuses(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<StepRecord>, StorageError> {
        self.inner.workflow_step_statuses(workflow_id).await
    }
    async fn link_step_job(
        &self,
        workflow_id: &str,
        step_name: &str,
        job_id: &str,
    ) -> Result<bool, StorageError> {
        self.inner
            .link_step_job(workflow_id, step_name, job_id)
            .await
    }
    async fn create_step_job(&self, job: &Job) -> Result<bool, StorageError> {
        self.inner.create_step_job(job).await
    }
    async fn set_step_status(
        &self,
        workflow_id: &str,
        step_name: &str,
        status: StepStatus,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("status:{step_name}:{}", status.as_str()));
        self.inner
            .set_step_status(workflow_id, step_name, status, error)
            .await
    }
    async fn set_workflow_status(
        &self,
        workflow_id: &str,
        status: WorkflowStatus,
    ) -> Result<bool, StorageError> {
        self.inner.set_workflow_status(workflow_id, status).await
    }
    async fn merge_workflow_context(
        &self,
        workflow_id: &str,
        key: &str,
        value: &Json,
    ) -> Result<(), StorageError> {
        self.events.lock().unwrap().push(format!("merge:{key}"));
        self.inner
            .merge_workflow_context(workflow_id, key, value)
            .await
    }
}

#[tokio::test]
async fn step_result_merge_precedes_the_completed_status_write() {
    let clock = Arc::new(TestClock::epoch());
    let probe = Arc::new(OrderProbe {
        inner: InMemoryJobStore::new(clock.clone()),
        events: Mutex::new(Vec::new()),
    });
    let engine = Engine::builder(probe.clone(), clock)
        .register_fn("emit", |_p: Map| async { Ok(map(json!({"value": 1}))) })
        .build();

    let req = WorkflowBuilder::new("ordering")
        .step(StepBuilder::new("a").task("emit"))
        .step(StepBuilder::new("b").task("emit").after("a"))
        .build()
        .unwrap();
    engine.create_workflow(req, None).await.unwrap();
    while engine.process_once("default").await.unwrap() {}

    let events = probe.events.lock().unwrap().clone();
    let merge = events
        .iter()
        .position(|e| e == "merge:a")
        .expect("a's result was merged");
    let done = events
        .iter()
        .position(|e| e == "status:a:completed")
        .expect("a was completed");
    assert!(
        merge < done,
        "a step's context merge must land before its Completed status write \
         (observers treat Completed as proof the context is complete); got {events:?}"
    );
}
