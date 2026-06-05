//! Opt-in PostgreSQL + PGMQ integration tests.
//!
//! These prove that the Postgres adapter is behaviourally equivalent to the
//! in-memory one — the same engine and workflow assertions, but against a real
//! database. They run only when `TEST_DATABASE_URL` points at a pgmq-enabled
//! PostgreSQL; otherwise each test logs a skip and returns, so the default
//! offline `cargo test` stays green. The whole file is compiled out unless the
//! `postgres` feature is enabled.
#![cfg(feature = "postgres")]

use std::sync::Arc;

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

/// Connect, migrate, and isolate this test on its own queue. Returns `None`
/// (with a logged skip) when no database is configured.
async fn setup(queue: &str) -> Option<(Arc<PostgresJobStore>, Arc<PostgresMessageQueue>)> {
    let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!("skipping: TEST_DATABASE_URL not set");
        return None;
    };
    let pool = connect(&url, 5).await.expect("connect to postgres");
    migrate(&pool).await.expect("run migrations");
    let store = Arc::new(PostgresJobStore::new(pool.clone()));
    let mq = Arc::new(PostgresMessageQueue::new(pool));
    mq.ensure_queue(queue).await.ok();
    mq.purge(queue).await.ok();
    Some((store, mq))
}

#[tokio::test]
async fn pg_job_runs_to_completion() {
    let queue = "test_job_lifecycle";
    let Some((store, mq)) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store, mq, Arc::new(SystemClock))
        .default_queue(queue)
        .register("echo", builtin::echo())
        .build();

    let id = engine
        .enqueue("echo", map(json!({"x": 1})), Default::default())
        .await
        .unwrap();
    assert!(engine.process_once(queue).await.unwrap());
    assert_eq!(
        engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );
}

#[tokio::test]
async fn pg_unknown_task_is_dead_lettered() {
    let queue = "test_dlq";
    let Some((store, mq)) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store.clone(), mq, Arc::new(SystemClock))
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
async fn pg_workflow_dag_completes() {
    let queue = "test_workflow";
    let Some((store, mq)) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store, mq, Arc::new(SystemClock))
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
    let Some((store, mq)) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store.clone(), mq, Arc::new(SystemClock))
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
