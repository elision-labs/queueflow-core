//! Workflows v2 on the in-memory adapters: conditional steps, dynamic
//! fan-out, inline sub-workflows, per-step queues, step timestamps, the
//! workflow deadline, and retry from failure.

use std::sync::atomic::{AtomicU32, Ordering};
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

struct Ctx {
    engine: Arc<Mem>,
    store: Arc<InMemoryJobStore>,
    clock: Arc<TestClock>,
    /// Every payload the `grab` handler received, in order.
    grabbed: Arc<Mutex<Vec<Map>>>,
    /// How many times `flaky` ran.
    flaky_runs: Arc<AtomicU32>,
}

fn ctx() -> Ctx {
    let clock = Arc::new(TestClock::epoch());
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));
    let grabbed = Arc::new(Mutex::new(Vec::<Map>::new()));
    let flaky_runs = Arc::new(AtomicU32::new(0));
    let g = grabbed.clone();
    let f = flaky_runs.clone();
    let engine = Engine::builder(store.clone(), clock.clone())
        .register_fn("ok", |_p| async { Ok(map(json!({"ok": true}))) })
        .register_fn("emit", |_p| async { Ok(map(json!({"value": 42}))) })
        .register_fn("fail", |_p| async {
            Err(HandlerError::permanent("intentional"))
        })
        .register_fn("items", |_p| async {
            Ok(map(json!({"items": [10, 20, 30]})))
        })
        .register_fn("double", |p: Map| async move {
            let item = p.get("item").and_then(|v| v.as_i64()).unwrap_or(0);
            let index = p
                .get(FAN_OUT_INDEX_KEY)
                .and_then(|v| v.as_i64())
                .unwrap_or(-1);
            Ok(map(json!({"doubled": item * 2, "index": index})))
        })
        .register_fn("fail_on_twenty", |p: Map| async move {
            let item = p.get("item").and_then(|v| v.as_i64()).unwrap_or(0);
            if item == 20 {
                Err(HandlerError::permanent("twenty is bad"))
            } else {
                Ok(map(json!({"doubled": item * 2})))
            }
        })
        .register_fn("flaky", move |_p| {
            let f = f.clone();
            async move {
                if f.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(HandlerError::permanent("first run fails"))
                } else {
                    Ok(map(json!({"ran": true})))
                }
            }
        })
        .register_fn("grab", move |p: Map| {
            let g = g.clone();
            async move {
                g.lock().unwrap().push(p);
                Ok(Map::new())
            }
        })
        .build();
    Ctx {
        engine,
        store,
        clock,
        grabbed,
        flaky_runs,
    }
}

/// Process jobs on every queue until nothing is left.
async fn drain(c: &Ctx) {
    loop {
        let mut any = false;
        for q in ["default", "images"] {
            while c.engine.process_once(q).await.unwrap() {
                any = true;
            }
        }
        if !any {
            break;
        }
    }
}

async fn record(c: &Ctx, wf: &str, name: &str) -> StepRecord {
    c.store
        .workflow_step_statuses(wf)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.name == name)
        .unwrap_or_else(|| panic!("step {name} not found"))
}

fn leaf(path: &str, op: ConditionOp, value: serde_json::Value) -> StepCondition {
    StepCondition {
        path: Some(path.into()),
        op: Some(op),
        value: Some(value),
        ..Default::default()
    }
}

// ---- Conditions --------------------------------------------------------------

#[tokio::test]
async fn false_condition_skips_the_step_and_its_dependents_still_run() {
    let c = ctx();
    let req = WorkflowBuilder::new("cond")
        .step(StepBuilder::new("emitter").task("emit"))
        .step(
            StepBuilder::new("big")
                .task("grab")
                .after("emitter")
                .when(leaf("emitter.value", ConditionOp::Gt, json!(100))),
        )
        .step(
            StepBuilder::new("small")
                .task("grab")
                .after("emitter")
                .when(leaf("emitter.value", ConditionOp::Lte, json!(100))),
        )
        .step(
            StepBuilder::new("after")
                .task("ok")
                .after("big")
                .after("small"),
        )
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c).await;

    let big = record(&c, &id, "big").await;
    assert_eq!(big.status, StepStatus::Skipped);
    assert_eq!(big.error.as_deref(), Some(CONDITION_NOT_MET));
    assert!(
        big.job_id.is_none(),
        "a condition-skipped step never gets a job"
    );
    assert_eq!(record(&c, &id, "small").await.status, StepStatus::Completed);
    assert_eq!(record(&c, &id, "after").await.status, StepStatus::Completed);
    let wf = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(wf.status, WorkflowStatus::Completed, "{wf:?}");
    assert_eq!(c.grabbed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn policy_skip_and_condition_skip_differ_for_dependents() {
    let c = ctx();
    let req = WorkflowBuilder::new("skips")
        .step(
            StepBuilder::new("broken")
                .task("fail")
                .on_failure(OnFailure::Skip),
        )
        .step(StepBuilder::new("after_broken").task("ok").after("broken"))
        .step(
            StepBuilder::new("gated")
                .task("ok")
                .when(leaf("missing", ConditionOp::Exists, json!(null)).without_value()),
        )
        .step(StepBuilder::new("after_gated").task("ok").after("gated"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c).await;

    assert_eq!(record(&c, &id, "broken").await.status, StepStatus::Skipped);
    assert_eq!(
        record(&c, &id, "after_broken").await.status,
        StepStatus::Skipped,
        "a policy skip propagates to dependents"
    );
    assert_eq!(record(&c, &id, "gated").await.status, StepStatus::Skipped);
    assert_eq!(
        record(&c, &id, "after_gated").await.status,
        StepStatus::Completed,
        "a condition skip does not"
    );
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::PartiallyFailed
    );
}

trait WithoutValue {
    fn without_value(self) -> Self;
}
impl WithoutValue for StepCondition {
    fn without_value(mut self) -> Self {
        self.value = None;
        self
    }
}

#[tokio::test]
async fn invalid_definitions_are_rejected_before_persisting() {
    let c = ctx();
    let bad_condition = CreateWorkflowRequest {
        name: "bad".into(),
        steps: vec![WorkflowStep {
            name: "a".into(),
            task_name: "ok".into(),
            when: Some(StepCondition::default()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let no_task = CreateWorkflowRequest {
        name: "bad".into(),
        steps: vec![WorkflowStep {
            name: "a".into(),
            fan_out: Some(FanOut {
                path: "x".into(),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let bad_timeout = CreateWorkflowRequest {
        name: "bad".into(),
        steps: vec![WorkflowStep {
            name: "a".into(),
            task_name: "ok".into(),
            ..Default::default()
        }],
        timeout_secs: Some(0),
        ..Default::default()
    };
    let bad_queue = CreateWorkflowRequest {
        name: "bad".into(),
        steps: vec![WorkflowStep {
            name: "a".into(),
            task_name: "ok".into(),
            queue: Some("has space".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    // A child that is itself invalid is reported with its step name.
    let bad_child = CreateWorkflowRequest {
        name: "bad".into(),
        steps: vec![WorkflowStep {
            name: "sub".into(),
            workflow: Some(Box::new(CreateWorkflowRequest {
                name: "child".into(),
                steps: vec![],
                ..Default::default()
            })),
            ..Default::default()
        }],
        ..Default::default()
    };
    for (label, req) in [
        ("condition", bad_condition),
        ("task", no_task),
        ("timeout", bad_timeout),
        ("queue", bad_queue),
        ("child", bad_child),
    ] {
        let err = c.engine.create_workflow(req, None).await.unwrap_err();
        assert!(
            matches!(err, EngineError::Validation(_)),
            "{label}: {err:?}"
        );
    }
    assert!(c
        .engine
        .list_workflows(&ListFilter::default())
        .await
        .unwrap()
        .items
        .is_empty());
}

// ---- Fan-out ------------------------------------------------------------------

#[tokio::test]
async fn fan_out_runs_one_child_per_item_and_collects_results_in_order() {
    let c = ctx();
    let req = WorkflowBuilder::new("fan")
        .step(StepBuilder::new("load").task("items"))
        .step(
            StepBuilder::new("work")
                .task("double")
                .after("load")
                .fan_out("load.items"),
        )
        .step(StepBuilder::new("collect").task("grab").after("work"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c).await;

    let wf = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(wf.status, WorkflowStatus::Completed, "{wf:?}");
    let work = record(&c, &id, "work").await;
    assert_eq!(work.status, StepStatus::Completed);
    let progress = work.fan_out.expect("fan-out progress");
    assert_eq!(
        (progress.total, progress.completed, progress.failed),
        (3, 3, 0)
    );
    assert!(work.started_at.is_some() && work.completed_at.is_some());

    // The step's result is every child's result, in item order.
    let results = wf.context["work"]["results"].as_array().unwrap().clone();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0]["doubled"], 20);
    assert_eq!(results[1]["doubled"], 40);
    assert_eq!(results[2]["doubled"], 60);
    assert_eq!(results[2]["index"], 2);
    // The collector saw it through _context.
    let seen = c.grabbed.lock().unwrap()[0].clone();
    assert_eq!(seen[CONTEXT_KEY]["work"]["results"][1]["doubled"], 40);
    // Children carry their position in metadata and the step linkage.
    let children = c
        .engine
        .list_jobs(&ListFilter {
            task_name: Some("double".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .items;
    assert_eq!(children.len(), 3);
    assert!(children
        .iter()
        .all(|j| j.workflow_step_id.as_deref() == Some("work")
            && j.metadata[FAN_OUT_TOTAL_META] == 3));
}

#[tokio::test]
async fn fan_out_over_an_empty_array_completes_at_once() {
    let c = ctx();
    let req = WorkflowBuilder::new("fan")
        .context("items", json!([]))
        .step(StepBuilder::new("work").task("double").fan_out("items"))
        .step(StepBuilder::new("after").task("ok").after("work"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c).await;
    let wf = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(wf.status, WorkflowStatus::Completed);
    assert_eq!(wf.context["work"]["results"], json!([]));
    assert!(record(&c, &id, "work").await.job_id.is_none());
}

#[tokio::test]
async fn fan_out_with_a_missing_or_wrong_path_fails_the_step_under_its_policy() {
    let c = ctx();
    let req = WorkflowBuilder::new("fan")
        .context("scalar", json!(5))
        .step(
            StepBuilder::new("missing")
                .task("double")
                .fan_out("nowhere")
                .on_failure(OnFailure::Continue),
        )
        .step(
            StepBuilder::new("scalar")
                .task("double")
                .fan_out("scalar")
                .on_failure(OnFailure::Continue),
        )
        .step(StepBuilder::new("alone").task("ok"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c).await;
    let missing = record(&c, &id, "missing").await;
    assert_eq!(missing.status, StepStatus::Failed);
    assert!(missing
        .error
        .as_deref()
        .unwrap()
        .contains("not in the workflow context"));
    let scalar = record(&c, &id, "scalar").await;
    assert_eq!(scalar.status, StepStatus::Failed);
    assert!(scalar
        .error
        .as_deref()
        .unwrap()
        .contains("a number rather than an array"));
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::PartiallyFailed
    );
}

#[tokio::test]
async fn fan_out_child_failure_under_halt_cancels_the_siblings() {
    let c = ctx();
    let req = WorkflowBuilder::new("fan")
        .step(StepBuilder::new("load").task("items"))
        .step(
            StepBuilder::new("work")
                .task("fail_on_twenty")
                .after("load")
                .fan_out("load.items"),
        )
        .step(StepBuilder::new("after").task("ok").after("work"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    // Children with equal timestamps claim in id order, so which of 10 and
    // 30 run before 20 fails is not fixed; what is fixed is that 20's
    // failure halts the rest.
    drain(&c).await;

    let wf = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(wf.status, WorkflowStatus::Failed);
    assert!(
        wf.error_message
            .as_deref()
            .unwrap()
            .contains("twenty is bad"),
        "{wf:?}"
    );
    let work = record(&c, &id, "work").await;
    assert_eq!(work.status, StepStatus::Failed);
    assert!(work
        .error
        .as_deref()
        .unwrap()
        .starts_with("1 of 3 items failed"));
    let progress = work.fan_out.unwrap();
    assert_eq!(progress.total, 3);
    assert!(
        progress.failed >= 1 && progress.completed + progress.failed == 3,
        "{progress:?}"
    );
    let children = c.store.step_job_outcomes(&id, "work").await.unwrap();
    assert!(children.iter().all(|o| o.status.is_terminal()));
    assert!(children.iter().any(|o| o.status == JobStatus::Failed));
    assert_eq!(record(&c, &id, "after").await.status, StepStatus::Cancelled);
}

#[tokio::test]
async fn fan_out_child_failure_under_continue_lets_the_siblings_finish() {
    let c = ctx();
    let req = WorkflowBuilder::new("fan")
        .step(StepBuilder::new("load").task("items"))
        .step(
            StepBuilder::new("work")
                .task("fail_on_twenty")
                .after("load")
                .fan_out("load.items")
                .on_failure(OnFailure::Continue),
        )
        .step(StepBuilder::new("alone").task("ok"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c).await;
    let work = record(&c, &id, "work").await;
    assert_eq!(work.status, StepStatus::Failed);
    let progress = work.fan_out.unwrap();
    assert_eq!(
        (progress.total, progress.completed, progress.failed),
        (3, 2, 1)
    );
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::PartiallyFailed
    );
}

// ---- Sub-workflows ---------------------------------------------------------

fn child_req() -> CreateWorkflowRequest {
    WorkflowBuilder::new("child")
        .step(StepBuilder::new("x").task("emit"))
        .step(StepBuilder::new("y").task("grab").after("x"))
        .build()
        .unwrap()
}

#[tokio::test]
async fn sub_workflow_runs_as_a_child_and_its_context_becomes_the_step_result() {
    let c = ctx();
    let req = WorkflowBuilder::new("parent")
        .step(StepBuilder::new("prep").task("emit"))
        .step(
            StepBuilder::new("sub")
                .sub_workflow(child_req())
                .after("prep")
                .payload_value("mode", json!("fast")),
        )
        .step(StepBuilder::new("after").task("grab").after("sub"))
        .build()
        .unwrap();
    let id = c
        .engine
        .create_workflow(req, Some("acme".into()))
        .await
        .unwrap();
    drain(&c).await;

    let parent = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(parent.status, WorkflowStatus::Completed, "{parent:?}");
    let sub = record(&c, &id, "sub").await;
    assert_eq!(sub.status, StepStatus::Completed);
    let child_id = sub.child_workflow_id.clone().expect("child linked");
    assert!(sub.job_id.is_none());

    let child = c.engine.get_workflow(&child_id).await.unwrap();
    assert_eq!(child.status, WorkflowStatus::Completed);
    assert_eq!(child.parent_workflow_id.as_deref(), Some(id.as_str()));
    assert_eq!(child.parent_step_name.as_deref(), Some("sub"));
    assert_eq!(child.tenant_id.as_deref(), Some("acme"));
    // The child saw the step payload and the parent context.
    assert_eq!(child.context["mode"], "fast");
    assert_eq!(child.context[PARENT_CONTEXT_KEY]["prep"]["value"], 42);
    // The step result is the child's final context.
    assert_eq!(parent.context["sub"]["x"]["value"], 42);
    assert_eq!(parent.context["sub"]["mode"], "fast");
    // The child's own step saw its context; the parent's "after" step saw
    // the sub result.
    let grabbed = c.grabbed.lock().unwrap();
    assert_eq!(grabbed.len(), 2);
    assert_eq!(grabbed[0][CONTEXT_KEY]["x"]["value"], 42);
    assert_eq!(grabbed[1][CONTEXT_KEY]["sub"]["x"]["value"], 42);
}

#[tokio::test]
async fn sub_workflow_failure_fails_the_parent_step_under_its_policy() {
    let c = ctx();
    let failing_child = WorkflowBuilder::new("child")
        .step(StepBuilder::new("boom").task("fail"))
        .build()
        .unwrap();
    let req = WorkflowBuilder::new("parent")
        .step(StepBuilder::new("sub").sub_workflow(failing_child))
        .step(StepBuilder::new("after").task("ok").after("sub"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c).await;

    let parent = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(parent.status, WorkflowStatus::Failed);
    let sub = record(&c, &id, "sub").await;
    assert_eq!(sub.status, StepStatus::Failed);
    assert!(
        sub.error.as_deref().unwrap().starts_with("child workflow "),
        "{sub:?}"
    );
    assert!(sub.error.as_deref().unwrap().contains("intentional"));
    assert_eq!(record(&c, &id, "after").await.status, StepStatus::Cancelled);
}

#[tokio::test]
async fn cancelling_the_parent_cancels_a_running_child() {
    let c = ctx();
    let req = WorkflowBuilder::new("parent")
        .step(StepBuilder::new("sub").sub_workflow(child_req()))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    // The child's first job is pending; cancel the parent now.
    c.engine.cancel_workflow(&id).await.unwrap();
    let child_id = record(&c, &id, "sub").await.child_workflow_id.unwrap();
    let child = c.engine.get_workflow(&child_id).await.unwrap();
    assert_eq!(child.status, WorkflowStatus::Cancelled);
    assert_eq!(
        child.error_message.as_deref(),
        Some("parent workflow ended")
    );
    assert_eq!(
        record(&c, &child_id, "x").await.status,
        StepStatus::Cancelled
    );
    assert!(!c.engine.process_once("default").await.unwrap());
    let parent = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(parent.status, WorkflowStatus::Cancelled);
    assert_eq!(parent.error_message.as_deref(), Some("cancelled by user"));
}

#[tokio::test]
async fn janitor_settles_a_child_that_finished_without_its_parent_noticing() {
    let c = ctx();
    let req = WorkflowBuilder::new("parent")
        .step(StepBuilder::new("sub").sub_workflow(child_req()))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    let child_id = record(&c, &id, "sub").await.child_workflow_id.unwrap();
    // Simulate a crash between the child's final write and the parent's
    // settlement: finish the child directly in the store.
    assert!(c.engine.process_once("default").await.unwrap()); // x
    c.store
        .cancel_step_jobs(&child_id, "y", "simulated crash")
        .await
        .unwrap();
    c.store
        .set_step_status(&child_id, "y", StepStatus::Completed, None)
        .await
        .unwrap();
    assert!(c
        .store
        .set_workflow_status(&child_id, WorkflowStatus::Completed, None)
        .await
        .unwrap());
    assert_eq!(record(&c, &id, "sub").await.status, StepStatus::Pending);

    let report = c.engine.janitor_sweep().await;
    assert_eq!(report.settled_children, 1, "{report:?}");
    assert_eq!(record(&c, &id, "sub").await.status, StepStatus::Completed);
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );
    // Idempotent: nothing left to settle.
    assert_eq!(c.engine.janitor_sweep().await.settled_children, 0);
}

// ---- Queues, timestamps -----------------------------------------------------

#[tokio::test]
async fn a_step_can_name_its_queue_and_records_timestamps() {
    let c = ctx();
    let req = WorkflowBuilder::new("q")
        .step(StepBuilder::new("resize").task("ok").queue("images"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    let before = record(&c, &id, "resize").await;
    assert!(before.started_at.is_some(), "scheduled: started_at set");
    assert!(before.completed_at.is_none());
    assert!(!c.engine.process_once("default").await.unwrap());
    c.clock.advance_secs(5);
    assert!(c.engine.process_once("images").await.unwrap());
    let after = record(&c, &id, "resize").await;
    assert_eq!(after.status, StepStatus::Completed);
    assert_eq!(
        (after.completed_at.unwrap() - after.started_at.unwrap()).num_seconds(),
        5
    );
    let job = c.engine.get_job(&after.job_id.unwrap()).await.unwrap();
    assert_eq!(job.queue_name, "images");
}

// ---- Timeout ------------------------------------------------------------------

#[tokio::test]
async fn a_workflow_past_its_deadline_is_failed_by_the_janitor() {
    let c = ctx();
    let req = WorkflowBuilder::new("slow")
        .timeout_secs(60)
        .step(StepBuilder::new("a").task("ok"))
        .step(StepBuilder::new("b").task("ok").after("a"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    let wf = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(wf.timeout_secs, Some(60));
    assert_eq!(
        wf.deadline_at.unwrap(),
        wf.created_at + chrono::Duration::seconds(60)
    );

    c.clock.advance_secs(59);
    assert_eq!(c.engine.janitor_sweep().await.timed_out_workflows, 0);
    c.clock.advance_secs(2);
    let report = c.engine.janitor_sweep().await;
    assert_eq!(report.timed_out_workflows, 1, "{report:?}");

    let wf = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(wf.status, WorkflowStatus::Failed);
    assert_eq!(
        wf.error_message.as_deref(),
        Some("workflow timed out after 60s")
    );
    let a = record(&c, &id, "a").await;
    assert_eq!(a.status, StepStatus::Cancelled);
    assert_eq!(a.error.as_deref(), Some("workflow timed out"));
    assert_eq!(record(&c, &id, "b").await.status, StepStatus::Cancelled);
    assert_eq!(
        c.engine.get_job(&a.job_id.unwrap()).await.unwrap().status,
        JobStatus::Cancelled
    );
    // Terminal now: the sweep leaves it alone.
    assert_eq!(c.engine.janitor_sweep().await.timed_out_workflows, 0);
}

// ---- Retry --------------------------------------------------------------------

#[tokio::test]
async fn retry_reruns_only_the_unfinished_steps_and_rearms_the_deadline() {
    let c = ctx();
    let req = WorkflowBuilder::new("retry")
        .timeout_secs(600)
        .step(StepBuilder::new("a").task("grab"))
        .step(
            StepBuilder::new("b")
                .task("flaky")
                .after("a")
                .on_failure(OnFailure::Continue),
        )
        .step(StepBuilder::new("c").task("ok").after("b"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    drain(&c).await;
    let wf = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(wf.status, WorkflowStatus::PartiallyFailed);
    assert!(wf
        .error_message
        .as_deref()
        .unwrap()
        .contains("first run fails"));
    assert_eq!(record(&c, &id, "c").await.status, StepStatus::Skipped);
    let first_deadline = wf.deadline_at.unwrap();

    c.clock.advance_secs(100);
    c.engine.retry_workflow(&id).await.unwrap();
    let wf = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(wf.status, WorkflowStatus::Running);
    assert!(wf.error_message.is_none());
    assert_eq!(wf.retry_count, 1);
    assert_eq!(
        wf.deadline_at.unwrap(),
        first_deadline + chrono::Duration::seconds(100)
    );
    assert_eq!(
        record(&c, &id, "a").await.status,
        StepStatus::Completed,
        "kept"
    );
    let b = record(&c, &id, "b").await;
    assert_eq!(b.status, StepStatus::Pending);
    assert!(b.job_id.is_some(), "re-scheduled at once");

    drain(&c).await;
    let wf = c.engine.get_workflow(&id).await.unwrap();
    assert_eq!(wf.status, WorkflowStatus::Completed, "{wf:?}");
    assert_eq!(wf.context["b"]["ran"], true);
    assert_eq!(c.flaky_runs.load(Ordering::SeqCst), 2);
    assert_eq!(c.grabbed.lock().unwrap().len(), 1, "a did not run again");
    assert_eq!(record(&c, &id, "c").await.status, StepStatus::Completed);
}

#[tokio::test]
async fn retry_of_a_running_or_completed_workflow_is_a_conflict() {
    let c = ctx();
    let req = WorkflowBuilder::new("r")
        .step(StepBuilder::new("a").task("ok"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    let err = c.engine.retry_workflow(&id).await.unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)), "{err:?}");
    drain(&c).await;
    let err = c.engine.retry_workflow(&id).await.unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)), "{err:?}");
    assert!(matches!(
        c.engine.retry_workflow("nope").await.unwrap_err(),
        EngineError::Storage(StorageError::WorkflowNotFound(_))
    ));
}

#[tokio::test]
async fn retry_after_a_cancel_resumes_the_cancelled_steps() {
    let c = ctx();
    let req = WorkflowBuilder::new("r")
        .step(StepBuilder::new("a").task("ok"))
        .step(StepBuilder::new("b").task("ok").after("a"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    assert!(c.engine.process_once("default").await.unwrap());
    c.engine.cancel_workflow(&id).await.unwrap();
    assert_eq!(record(&c, &id, "b").await.status, StepStatus::Cancelled);
    c.engine.retry_workflow(&id).await.unwrap();
    drain(&c).await;
    assert_eq!(
        c.engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );
}

// ---- Change notifications ---------------------------------------------------

#[tokio::test]
async fn await_workflow_change_wakes_on_a_step_transition() {
    let c = ctx();
    let req = WorkflowBuilder::new("w")
        .step(StepBuilder::new("a").task("ok"))
        .build()
        .unwrap();
    let id = c.engine.create_workflow(req, None).await.unwrap();
    let engine = c.engine.clone();
    let wid = id.clone();
    let waiter = tokio::spawn(async move {
        let started = std::time::Instant::now();
        engine
            .await_workflow_change(&wid, std::time::Duration::from_secs(10))
            .await
            .unwrap();
        started.elapsed()
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(c.engine.process_once("default").await.unwrap());
    let waited = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
        .await
        .expect("woken")
        .unwrap();
    assert!(
        waited < std::time::Duration::from_secs(5),
        "woke by notification, not timeout"
    );
}
