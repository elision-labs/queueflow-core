//! Opt-in PostgreSQL integration tests.
//!
//! These prove that the Postgres adapter is behaviourally equivalent to the
//! in-memory one — the same engine and workflow assertions, but against a real
//! database: the `FOR UPDATE SKIP LOCKED` claim query, lease tokens, and the
//! LISTEN/NOTIFY wakeup. They run only when `TEST_DATABASE_URL` points at a
//! PostgreSQL (any plain 13+; no extensions); otherwise each test logs a skip
//! and returns, so the default offline `cargo test` stays green. Set
//! `REQUIRE_PG=1` (as CI does) to turn that skip into a failure, so a database
//! that never came up cannot masquerade as a passing run. The whole file is
//! compiled out unless the `postgres` feature is enabled.
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
/// database is configured, unless `REQUIRE_PG` is set, in which case a missing
/// `TEST_DATABASE_URL` is a hard failure rather than a silent pass.
async fn setup(queue: &str) -> Option<Arc<PostgresJobStore>> {
    let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
        // Skipping is a convenience for offline development, never for CI: a
        // green run there must mean the assertions actually hit a database, so
        // any environment that intends to test Postgres sets REQUIRE_PG and
        // turns a lost database into a red build instead of 8 vacuous passes.
        assert!(
            std::env::var_os("REQUIRE_PG").is_none(),
            "REQUIRE_PG is set but TEST_DATABASE_URL is not: refusing to skip \
             the Postgres integration tests and report a false pass",
        );
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

    let id = engine
        .enqueue("ghost", Map::new(), Default::default())
        .await
        .unwrap();
    assert!(engine.process_once(queue).await.unwrap());
    assert_eq!(engine.get_job(&id).await.unwrap().status, JobStatus::Failed);
    // Look the entry up by job id rather than diffing the global count:
    // the pg tests share one database and run concurrently.
    let dl = store
        .list_dead_letters(&ListFilter {
            queue: Some(queue.into()),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap()
        .items
        .into_iter()
        .find(|d| d.job_id == id);
    assert!(dl.is_some(), "dead letter recorded for {id}");
    assert_eq!(dl.unwrap().reason, "handler_not_found");
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
    let first = store.claim_jobs(queue, 5, 30, false).await.unwrap();
    assert_eq!(first.jobs.len(), 1);
    assert_eq!(first.jobs[0].job.id, id);
    assert!(store
        .claim_jobs(queue, 5, 30, false)
        .await
        .unwrap()
        .jobs
        .is_empty());

    // A bogus token cannot finish or extend the job; the real one can.
    let token = &first.jobs[0].lease_token;
    let bogus = "00000000-0000-0000-0000-000000000000";
    assert!(store
        .finish_if_leased(&id, bogus, JobStatus::Completed, None, None)
        .await
        .unwrap()
        .is_none());
    assert_eq!(store.extend_lease(&id, bogus, 60).await.unwrap(), None);
    assert_eq!(
        store.extend_lease(&id, token, 60).await.unwrap(),
        Some(JobStatus::Running)
    );
    assert!(store
        .finish_if_leased(&id, token, JobStatus::Completed, None, None)
        .await
        .unwrap()
        .is_some());
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
    let claimed = store.claim_jobs(queue, 1, 30, false).await.unwrap();
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
    let claimed = store.claim_jobs(queue, 1, 30, false).await.unwrap();
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
    let prime_epoch = store.claim_jobs(queue, 1, 30, false).await.unwrap().epoch;
    store
        .await_work(queue, prime_epoch, Duration::from_millis(50))
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
    let epoch = store.claim_jobs(queue, 1, 30, false).await.unwrap().epoch;
    let start = std::time::Instant::now();
    store
        .await_work(queue, epoch, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(8),
        "await_work should be woken by NOTIFY, took {:?}",
        start.elapsed()
    );
    let _ = q;
    assert_eq!(
        store
            .claim_jobs(queue, 1, 30, false)
            .await
            .unwrap()
            .jobs
            .len(),
        1
    );
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
    let claimed = store.claim_jobs(queue, 1, 1, false).await.unwrap();
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

#[tokio::test]
async fn pg_dead_letter_replay_claim_is_atomic_and_single_shot() {
    let queue = "test_dlq_replay";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store.clone(), Arc::new(SystemClock))
        .default_queue(queue)
        .build();

    // No handler registered: the job dead-letters on first delivery.
    let id = engine
        .enqueue("ghost", map(json!({"k": 1})), Default::default())
        .await
        .unwrap();
    assert!(engine.process_once(queue).await.unwrap());

    let dl = store
        .list_dead_letters(&ListFilter {
            queue: Some(queue.into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .items
        .into_iter()
        .find(|d| d.job_id == id)
        .expect("dead letter recorded");
    assert!(dl.replayed_at.is_none());

    // Replay: the claim and the fresh job commit together.
    let new_id = engine.replay_dead_letter(dl.id).await.unwrap();
    let fresh = engine.get_job(&new_id).await.unwrap();
    assert_eq!(fresh.status, JobStatus::Pending);
    assert_eq!(fresh.queue_name, queue);
    assert_eq!(fresh.retry_count, 0);

    let dl = store.get_dead_letter(dl.id).await.unwrap();
    assert_eq!(dl.replay_job_id.as_deref(), Some(new_id.as_str()));

    // The claim is single-shot: a second replay conflicts and persists nothing.
    let err = engine.replay_dead_letter(dl.id).await.unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)), "got: {err:?}");
}

#[tokio::test]
async fn pg_cron_schedule_fires_exactly_once_per_due_instant() {
    let queue = "test_cron";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store.clone(), Arc::new(SystemClock))
        .default_queue(queue)
        .register("echo", builtin::echo())
        .build();

    // The schedule table persists across runs and names are unique per
    // tenant; clear leftovers from previous runs.
    for c in store
        .list_crons(&ListFilter {
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap()
        .items
    {
        if c.name == "pg-tick" {
            store.delete_cron(&c.id).await.unwrap();
        }
    }

    let id = engine
        .create_cron(
            CreateCronRequest {
                name: "pg-tick".into(),
                cron_expr: "*/5 * * * *".into(),
                task_name: "echo".into(),
                queue: Some(queue.into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    // Force the schedule due, then pump twice: exactly one job (the firing
    // key is bound to the due instant).
    let now = chrono::Utc::now();
    store
        .advance_cron(&id, now, now - chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(engine.cron_tick().await.unwrap(), 1);
    assert_eq!(engine.cron_tick().await.unwrap(), 0);
    let page = engine
        .list_jobs(&ListFilter {
            queue: Some(queue.into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(
        page.items[0].metadata.get("cron_name"),
        Some(&serde_json::json!("pg-tick"))
    );

    engine.delete_cron(&id).await.unwrap();
}

#[tokio::test]
async fn pg_status_change_wakes_a_job_watcher() {
    let queue = "test_job_events";
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
    // Prime the shared LISTEN connection.
    store
        .await_job_change(&id, Duration::from_millis(300))
        .await
        .unwrap();

    let store2 = store.clone();
    let id2 = id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        store2.cancel_job_if_active(&id2, "watch me").await.unwrap();
    });
    let start = std::time::Instant::now();
    store
        .await_job_change(&id, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(8),
        "the status trigger (not the timeout) should wake the watcher, took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn pg_keyset_cursor_pages_without_offset() {
    let queue = "test_cursor";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store.clone(), Arc::new(SystemClock))
        .default_queue(queue)
        .build();
    for i in 0..5 {
        engine
            .enqueue("noop", map(json!({ "i": i })), Default::default())
            .await
            .unwrap();
    }

    let all: Vec<String> = store
        .list_jobs(&ListFilter {
            queue: Some(queue.into()),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap()
        .items
        .into_iter()
        .map(|j| j.id)
        .collect();
    assert_eq!(all.len(), 5);

    // Walk the same set in pages of 2 via the cursor. The absurd offset on
    // cursor pages proves the cursor makes OFFSET irrelevant.
    let mut walked = Vec::new();
    let mut after: Option<PageCursor> = None;
    loop {
        let page = store
            .list_jobs(&ListFilter {
                queue: Some(queue.into()),
                limit: 2,
                offset: if after.is_some() { 9999 } else { 0 },
                after: after.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        walked.extend(page.items.iter().map(|j| j.id.clone()));
        if !page.has_more {
            break;
        }
        let last = page.items.last().expect("has_more implies items");
        after = Some(PageCursor {
            created_at: last.created_at,
            id: last.id.clone(),
        });
    }
    assert_eq!(walked, all, "cursor paging must reproduce the full listing");
}

#[tokio::test]
async fn pg_count_stats_are_scoped_to_the_tenant() {
    let queue = "test_count_stats";
    let Some(store) = setup(queue).await else {
        return;
    };
    let engine = Engine::builder(store, Arc::new(SystemClock))
        .default_queue(queue)
        .register("echo", builtin::echo())
        .build();

    // Tenants are database-wide (not per queue), so pick ids no other test or
    // earlier run can have used.
    let mine = format!("stats-mine-{}", uuid::Uuid::new_v4());
    let theirs = format!("stats-theirs-{}", uuid::Uuid::new_v4());
    for tenant in [&mine, &mine, &theirs] {
        engine
            .enqueue(
                "echo",
                Map::new(),
                EnqueueOptions {
                    tenant_id: Some(tenant.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    // An unknown task for the other tenant lands in the DLQ immediately.
    engine
        .enqueue(
            "no_such_task",
            Map::new(),
            EnqueueOptions {
                tenant_id: Some(theirs.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    for _ in 0..4 {
        engine.process_once(queue).await.unwrap();
    }

    let snap = engine.tenant_stats(Some(&mine)).await.unwrap();
    assert_eq!(snap.jobs_created, 2);
    assert_eq!(snap.jobs_completed, 2);
    assert_eq!(snap.jobs_failed, 0);
    assert_eq!(snap.jobs_dead_lettered, 0);
    assert_eq!(snap.workflows_created, 0);

    let snap = engine.tenant_stats(Some(&theirs)).await.unwrap();
    assert_eq!(snap.jobs_created, 2);
    assert_eq!(snap.jobs_completed, 1);
    assert_eq!(snap.jobs_dead_lettered, 1);

    let nobody = engine
        .tenant_stats(Some("stats-nobody-ever"))
        .await
        .unwrap();
    assert_eq!(nobody, StatsSnapshot::default());
}
