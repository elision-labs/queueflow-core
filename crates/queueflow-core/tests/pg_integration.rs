//! Opt-in PostgreSQL integration tests.
//!
//! These prove that the Postgres adapter is behaviourally equivalent to the
//! in-memory one — the same engine and workflow assertions, but against a real
//! database: the `FOR UPDATE SKIP LOCKED` claim query, lease tokens, and the
//! LISTEN/NOTIFY wakeup. They run only when `TEST_DATABASE_URL` points at a
//! PostgreSQL (any plain 13+; no extensions); otherwise each test logs a skip
//! and returns, so the default offline `cargo test` stays green. The whole
//! file is compiled out unless the `postgres` feature is enabled.
#![cfg(feature = "postgres")]

use std::sync::Arc;
use std::time::Duration;

use queueflow_core::task::builtin;
use queueflow_core::*;
use serde_json::json;

fn map(v: serde_json::Value) -> Map {
    v.as_object()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect()
}

/// Connect, migrate, and isolate this test on its own queue (cleared of any
/// leftovers from previous runs). Returns `None` (with a logged skip) when no
/// database is configured.
async fn setup(queue: &str) -> Option<Arc<PostgresJobStore>> {
    let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!("skipping: TEST_DATABASE_URL not set");
        return None;
    };
    let pool = connect(&url, 5).await.expect("connect to postgres");
    migrate(&pool).await.expect("run migrations");
    let store = Arc::new(PostgresJobStore::new(pool));
    store.purge_queue(queue).await.expect("clear test queue");
    Some(store)
}

#[tokio::test]
async fn pg_job_runs_to_completion() {
    let queue = "test_job_lifecycle";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store, Arc::new(SystemClock))
        .default_queue(queue)
        .register("echo", builtin::echo())
        .build();

    let id = engine
        .enqueue("echo", map(json!({"x": 1})), Default::default())
        .await
        .unwrap();
    assert!(engine.process_once(queue).await.unwrap());
    let job = engine.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Completed);
    assert_eq!(job.delivery_count, 1);
}

#[tokio::test]
async fn pg_unknown_task_is_dead_lettered() {
    let queue = "test_dlq";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store.clone(), Arc::new(SystemClock))
        .default_queue(queue)
        .build();

    let before = store.count_dead_letters().await.unwrap();
    let id = engine
        .enqueue("ghost", Map::new(), Default::default())
        .await
        .unwrap();
    assert!(engine.process_once(queue).await.unwrap());
    assert_eq!(engine.get_job(&id).await.unwrap().status, JobStatus::Failed);
    assert_eq!(store.count_dead_letters().await.unwrap(), before + 1);
}

#[tokio::test]
async fn pg_claim_is_exclusive_and_lease_guarded() {
    let queue = "test_claim_lease";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store.clone(), Arc::new(SystemClock))
        .default_queue(queue)
        .build();

    let id = engine
        .enqueue("external", Map::new(), Default::default())
        .await
        .unwrap();

    // First claim wins the job; a second claim sees nothing.
    let first = store.claim_jobs(queue, 5, 30).await.unwrap();
    assert_eq!(first.jobs.len(), 1);
    assert_eq!(first.jobs[0].job.id, id);
    assert!(store
        .claim_jobs(queue, 5, 30)
        .await
        .unwrap()
        .jobs
        .is_empty());

    // A bogus token cannot finish or extend the job; the real one can.
    let token = &first.jobs[0].lease_token;
    let bogus = "00000000-0000-0000-0000-000000000000";
    assert!(!store
        .finish_if_leased(&id, bogus, JobStatus::Completed, None, None)
        .await
        .unwrap());
    assert_eq!(store.extend_lease(&id, bogus, 60).await.unwrap(), None);
    assert_eq!(
        store.extend_lease(&id, token, 60).await.unwrap(),
        Some(JobStatus::Running)
    );
    assert!(store
        .finish_if_leased(&id, token, JobStatus::Completed, None, None)
        .await
        .unwrap());
    assert_eq!(
        engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );
}

#[tokio::test]
async fn pg_run_at_and_priority_order_claims() {
    let queue = "test_ordering";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store.clone(), Arc::new(SystemClock))
        .default_queue(queue)
        .build();

    // A future-dated job is invisible and reported via next_due.
    let run_at = chrono::Utc::now() + chrono::Duration::seconds(3600);
    engine
        .enqueue(
            "later",
            Map::new(),
            EnqueueOptions {
                run_at: Some(run_at),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let claimed = store.claim_jobs(queue, 1, 30).await.unwrap();
    assert!(claimed.jobs.is_empty());
    let next_due = claimed.next_due.expect("future job must report next_due");
    assert!((next_due - run_at).num_seconds().abs() < 2);

    // Higher priority wins among due jobs.
    for (who, priority) in [("low", 0), ("high", 9)] {
        engine
            .enqueue(
                "now",
                map(json!({ "who": who })),
                EnqueueOptions {
                    config: Some(JobConfig {
                        priority,
                        ..JobConfig::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let claimed = store.claim_jobs(queue, 1, 30).await.unwrap();
    assert_eq!(claimed.jobs[0].job.payload["who"], json!("high"));
}

#[tokio::test]
async fn pg_notify_wakes_an_idle_waiter() {
    let queue = "test_notify";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store.clone(), Arc::new(SystemClock))
        .default_queue(queue)
        .build();

    // Prime the LISTEN connection, then enqueue from a parallel task while
    // this one is parked in await_work. The NOTIFY (not the 10s timeout)
    // must wake it.
    store
        .await_work(queue, Duration::from_millis(50))
        .await
        .unwrap();
    let eng = engine.clone();
    let q = queue.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        eng.enqueue("late", Map::new(), Default::default())
            .await
            .unwrap();
    });
    let start = std::time::Instant::now();
    store
        .await_work(queue, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(8),
        "await_work should be woken by NOTIFY, took {:?}",
        start.elapsed()
    );
    let _ = q;
    assert_eq!(store.claim_jobs(queue, 1, 30).await.unwrap().jobs.len(), 1);
}

#[tokio::test]
async fn pg_expired_lease_is_reaped_into_a_retry() {
    let queue = "test_reaper";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store.clone(), Arc::new(SystemClock))
        .default_queue(queue)
        .build();

    let id = engine
        .enqueue("crashy", Map::new(), Default::default())
        .await
        .unwrap();
    // Claim with a 1-second lease and "crash" (never report).
    let claimed = store.claim_jobs(queue, 1, 1).await.unwrap();
    assert_eq!(claimed.jobs.len(), 1);
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let report = engine.janitor_sweep().await;
    assert!(report.expired_leases >= 1, "got: {report:?}");
    let job = engine.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Retrying);
    assert_eq!(job.retry_count, 1);
}

#[tokio::test]
async fn pg_workflow_dag_completes() {
    let queue = "test_workflow";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store, Arc::new(SystemClock))
        .default_queue(queue)
        .register("echo", builtin::echo())
        .build();

    let req = WorkflowBuilder::new("pg-wf")
        .step(StepBuilder::new("a").task("echo"))
        .step(StepBuilder::new("b").task("echo").after("a"))
        .step(StepBuilder::new("c").task("echo").after("b"))
        .build()
        .unwrap();
    let id = engine.create_workflow(req, None).await.unwrap();

    // Drain: completing a step enqueues its dependents synchronously.
    while engine.process_once(queue).await.unwrap() {}

    assert_eq!(
        engine.get_workflow(&id).await.unwrap().status,
        WorkflowStatus::Completed
    );
}

#[tokio::test]
async fn pg_workflow_halt_policy() {
    let queue = "test_workflow_halt";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store.clone(), Arc::new(SystemClock))
        .default_queue(queue)
        .register("echo", builtin::echo())
        .register("fail", builtin::fail())
        .build();

    let req = WorkflowBuilder::new("pg-halt")
        .step(StepBuilder::new("a").task("echo"))
        .step(
            StepBuilder::new("b")
                .task("fail")
                .payload_value("permanent", json!(true))
                .after("a"),
        )
        .step(StepBuilder::new("c").task("echo").after("b"))
        .build()
        .unwrap();
    let id = engine.create_workflow(req, None).await.unwrap();
    while engine.process_once(queue).await.unwrap() {}

    let wf = engine.get_workflow(&id).await.unwrap();
    assert_eq!(wf.status, WorkflowStatus::Failed);
    let statuses = store.workflow_step_statuses(&id).await.unwrap();
    let status_of = |n: &str| statuses.iter().find(|r| r.name == n).unwrap().status;
    assert_eq!(status_of("a"), StepStatus::Completed);
    assert_eq!(status_of("b"), StepStatus::Failed);
    assert_eq!(status_of("c"), StepStatus::Cancelled);
}
