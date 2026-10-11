//! End-to-end engine tests on the in-memory adapter — no database required.
//!
//! These cover the core engine behaviours: the success path, durable
//! exponential-backoff retries, dead-lettering, per-attempt timeouts,
//! cancellation, priority ordering, scheduled (`run_at`) jobs, lease
//! ownership, and the janitor's crash recovery.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use queueflow_core::task::builtin;
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

fn batch_item(task: &str, payload: Map, config: Option<JobConfig>) -> BatchItem {
    BatchItem {
        task_name: task.into(),
        payload,
        config,
        queue: None,
        run_at: None,
    }
}

struct Harness {
    engine: Arc<Mem>,
    store: Arc<InMemoryJobStore>,
    clock: Arc<TestClock>,
}

fn harness(
    build: impl FnOnce(EngineBuilder<InMemoryJobStore>) -> EngineBuilder<InMemoryJobStore>,
) -> Harness {
    let clock = Arc::new(TestClock::epoch());
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));
    let engine = build(Engine::builder(store.clone(), clock.clone())).build();
    Harness {
        engine,
        store,
        clock,
    }
}

/// A config with deterministic (jitter-free) backoff for reproducible tests.
fn deterministic_cfg(max_retries: u32) -> JobConfig {
    JobConfig {
        max_retries,
        retry_delay_secs: 60,
        retry_backoff: BackoffStrategy::Exponential,
        retry_max_delay_secs: 3600,
        jitter_factor: None,
        timeout_secs: 300,
        priority: 0,
    }
}

#[tokio::test]
async fn job_runs_to_completion() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    let id = h
        .engine
        .enqueue("echo", map(json!({"hello": "world"})), Default::default())
        .await
        .unwrap();

    assert!(h.engine.process_once("default").await.unwrap());

    let job = h.engine.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Completed);
    assert_eq!(job.result.unwrap()["echoed"], json!(true));
    assert_eq!(job.delivery_count, 1);
    assert_eq!(h.engine.stats().snapshot().jobs_completed, 1);
}

#[tokio::test]
async fn retries_use_durable_exponential_backoff_then_succeed() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let h = harness(move |b| {
        b.register_fn("flaky", move |_p| {
            let a = a.clone();
            async move {
                let n = a.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(HandlerError::retryable("transient"))
                } else {
                    Ok(Map::new())
                }
            }
        })
    });

    let id = h
        .engine
        .enqueue(
            "flaky",
            Map::new(),
            EnqueueOptions {
                config: Some(deterministic_cfg(5)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Attempt 1 fails → rescheduled 60s out.
    assert!(h.engine.process_once("default").await.unwrap());
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Retrying
    );
    // The retry is invisible until the backoff delay elapses (durable, in-row).
    assert!(!h.engine.process_once("default").await.unwrap());

    // After 60s, attempt 2 fails → next delay doubles to 120s.
    h.clock.advance_secs(61);
    assert!(h.engine.process_once("default").await.unwrap());
    assert!(!h.engine.process_once("default").await.unwrap());

    // After 120s, attempt 3 succeeds.
    h.clock.advance_secs(121);
    assert!(h.engine.process_once("default").await.unwrap());

    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(h.engine.stats().snapshot().jobs_retried, 2);
}

#[tokio::test]
async fn exhausted_retries_go_to_dead_letter_queue() {
    let h = harness(|b| b.register("boom", builtin::fail()));
    let id = h
        .engine
        .enqueue(
            "boom",
            Map::new(),
            EnqueueOptions {
                config: Some(deterministic_cfg(2)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // 3 attempts total (initial + 2 retries).
    assert!(h.engine.process_once("default").await.unwrap()); // attempt 1
    h.clock.advance_secs(61);
    assert!(h.engine.process_once("default").await.unwrap()); // attempt 2
    h.clock.advance_secs(121);
    assert!(h.engine.process_once("default").await.unwrap()); // attempt 3 → DLQ

    let job = h.engine.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Failed);
    assert_eq!(h.store.count_dead_letters().await.unwrap(), 1);
    assert_eq!(h.engine.stats().snapshot().jobs_dead_lettered, 1);
}

#[tokio::test]
async fn permanent_failure_skips_retries() {
    let h = harness(|b| b.register("boom", builtin::fail()));
    let id = h
        .engine
        .enqueue(
            "boom",
            map(json!({"permanent": true})),
            EnqueueOptions {
                config: Some(deterministic_cfg(5)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(h.engine.process_once("default").await.unwrap());
    // No retry was scheduled.
    assert!(!h.engine.process_once("default").await.unwrap());
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Failed
    );
    assert_eq!(h.engine.stats().snapshot().jobs_retried, 0);
    assert_eq!(h.store.count_dead_letters().await.unwrap(), 1);
}

#[tokio::test(start_paused = true)]
async fn per_attempt_timeout_fails_the_job() {
    let h = harness(|b| {
        b.register_fn("slow", |_p| async {
            // Far longer than the 1s timeout below; under a paused clock the
            // timeout fires in virtual time with no real waiting.
            tokio::time::sleep(std::time::Duration::from_secs(100)).await;
            Ok(Map::new())
        })
    });

    let cfg = JobConfig {
        timeout_secs: 1,
        ..deterministic_cfg(0)
    };
    let id = h
        .engine
        .enqueue(
            "slow",
            Map::new(),
            EnqueueOptions {
                config: Some(cfg),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(h.engine.process_once("default").await.unwrap());
    let job = h.engine.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Failed);
    assert!(job.error_message.unwrap().contains("timeout"));
}

#[tokio::test]
async fn unknown_task_is_dead_lettered_not_dropped() {
    let h = harness(|b| b);
    let id = h
        .engine
        .enqueue("nonexistent", Map::new(), Default::default())
        .await
        .unwrap();

    assert!(h.engine.process_once("default").await.unwrap());
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Failed
    );
    assert_eq!(h.store.count_dead_letters().await.unwrap(), 1);
}

#[tokio::test]
async fn cancelled_job_is_never_claimed() {
    let ran = Arc::new(AtomicU32::new(0));
    let r = ran.clone();
    let h = harness(move |b| {
        b.register_fn("count", move |_p| {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                Ok(Map::new())
            }
        })
    });

    let id = h
        .engine
        .enqueue("count", Map::new(), Default::default())
        .await
        .unwrap();
    h.engine.cancel_job(&id).await.unwrap();

    // A cancelled job is simply not claimable: there is no shadow message to
    // drain, so process_once finds nothing at all.
    assert!(!h.engine.process_once("default").await.unwrap());
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Cancelled
    );
    assert_eq!(ran.load(Ordering::SeqCst), 0, "cancelled job must not run");
}

#[tokio::test]
async fn higher_priority_jobs_run_first() {
    let order = Arc::new(Mutex::new(Vec::<String>::new()));
    let o = order.clone();
    let h = harness(move |b| {
        b.register_fn("rec", move |p: Map| {
            let o = o.clone();
            async move {
                let who = p
                    .get("who")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
                    .to_string();
                o.lock().unwrap().push(who);
                Ok(Map::new())
            }
        })
    });

    h.engine
        .enqueue(
            "rec",
            map(json!({"who": "low"})),
            EnqueueOptions {
                config: Some(JobConfig {
                    priority: 0,
                    ..deterministic_cfg(0)
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    h.engine
        .enqueue(
            "rec",
            map(json!({"who": "high"})),
            EnqueueOptions {
                config: Some(JobConfig {
                    priority: 10,
                    ..deterministic_cfg(0)
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    h.engine.process_once("default").await.unwrap();
    h.engine.process_once("default").await.unwrap();

    assert_eq!(
        *order.lock().unwrap(),
        vec!["high".to_string(), "low".to_string()]
    );
}

#[tokio::test]
async fn run_at_job_is_invisible_until_due() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    let run_at = h.clock.now() + chrono::Duration::seconds(300);
    let id = h
        .engine
        .enqueue(
            "echo",
            Map::new(),
            EnqueueOptions {
                run_at: Some(run_at),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(h.engine.get_job(&id).await.unwrap().scheduled_at, run_at);
    assert!(!h.engine.process_once("default").await.unwrap());

    h.clock.advance_secs(301);
    assert!(h.engine.process_once("default").await.unwrap());
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );
}

#[tokio::test]
async fn completed_job_is_never_claimable_again() {
    // At-least-once delivery no longer rides on a shadow message that could
    // be redelivered: a terminal row simply never matches the claim filter.
    let runs = Arc::new(AtomicU32::new(0));
    let r = runs.clone();
    let h = harness(move |b| {
        b.register_fn("once", move |_p| {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                Ok(Map::new())
            }
        })
    });

    let id = h
        .engine
        .enqueue("once", Map::new(), Default::default())
        .await
        .unwrap();
    assert!(h.engine.process_once("default").await.unwrap());
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    // Nothing left to claim, even after time passes and the janitor runs.
    h.clock.advance_secs(3600);
    assert!(!h.engine.process_once("default").await.unwrap());
    let report = h.engine.janitor_sweep().await;
    assert!(report.is_empty(), "got: {report:?}");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );
}

#[tokio::test]
async fn long_running_job_is_not_reaped_mid_flight() {
    // The default lease (30s) is shorter than this job's timeout. The engine
    // must extend the lease when it starts the job, otherwise the janitor
    // would reap and retry the job while it is still running.
    let gate = Arc::new(tokio::sync::Notify::new());
    let g = gate.clone();
    let h = harness(move |b| {
        b.register_fn("slow", move |_p| {
            let g = g.clone();
            async move {
                g.notified().await;
                Ok(Map::new())
            }
        })
    });

    let id = h
        .engine
        .enqueue(
            "slow",
            Map::new(),
            EnqueueOptions {
                config: Some(JobConfig {
                    timeout_secs: 600,
                    ..deterministic_cfg(0)
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let eng = h.engine.clone();
    let worker = tokio::spawn(async move { eng.process_once("default").await.unwrap() });
    // Let the worker claim the job and enter the handler.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 31 virtual seconds later the original 30s lease would have expired; the
    // extended lease (timeout + grace) keeps the job off the janitor's sweep.
    h.clock.advance_secs(31);
    assert!(
        h.store
            .claim_expired_leases(10, 30)
            .await
            .unwrap()
            .is_empty(),
        "in-flight job must not be reapable"
    );

    gate.notify_one();
    assert!(worker.await.unwrap());
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );
}

#[tokio::test]
async fn expired_lease_consumes_retry_budget_then_dead_letters() {
    // A worker that crashes mid-run never reports. The janitor must reap the
    // expired lease, consume retry budget, and eventually dead-letter — no
    // infinite crash-loop.
    let h = harness(|b| b.register("echo", builtin::echo()));
    let id = h
        .engine
        .enqueue(
            "echo",
            Map::new(),
            EnqueueOptions {
                config: Some(deterministic_cfg(1)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // "Crash" 1: claim directly (simulating a worker that died mid-run).
    let claimed = h.store.claim_jobs("default", 1, 30, false).await.unwrap();
    assert_eq!(claimed.jobs.len(), 1);
    h.clock.advance_secs(31);
    let report = h.engine.janitor_sweep().await;
    assert_eq!(report.expired_leases, 1);

    let job = h.engine.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Retrying);
    assert_eq!(job.retry_count, 1, "a crash must consume retry budget");

    // "Crash" 2 after the backoff: budget exhausted → DLQ.
    h.clock.advance_secs(61);
    let claimed = h.store.claim_jobs("default", 1, 30, false).await.unwrap();
    assert_eq!(claimed.jobs.len(), 1);
    h.clock.advance_secs(31);
    let report = h.engine.janitor_sweep().await;
    assert_eq!(report.expired_leases, 1);

    let job = h.engine.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Failed);
    assert!(job.error_message.unwrap().contains("lease expired"));
    assert_eq!(h.store.count_dead_letters().await.unwrap(), 1);
}

#[tokio::test]
async fn stale_lease_token_cannot_complete_or_heartbeat_a_reclaimed_job() {
    let h = harness(|b| b);
    let id = h
        .engine
        .enqueue(
            "external-task",
            Map::new(),
            EnqueueOptions {
                config: Some(deterministic_cfg(3)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let leases = h.engine.lease_jobs("default", 1, 30, 0).await.unwrap();
    let stale = leases[0].lease_token.clone();

    // The lease expires and the janitor reclaims the job into a retry...
    h.clock.advance_secs(31);
    assert_eq!(h.engine.janitor_sweep().await.expired_leases, 1);
    h.clock.advance_secs(61);
    // ...and another worker claims the retry under a fresh token.
    let fresh = h.engine.lease_jobs("default", 1, 30, 0).await.unwrap()[0]
        .lease_token
        .clone();
    assert_ne!(stale, fresh);

    // The original worker comes back from the dead: every write is refused.
    let err = h.engine.heartbeat_lease(&id, &stale, 30).await.unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)), "got: {err}");
    let err = h
        .engine
        .complete_leased(&id, &stale, Map::new())
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)), "got: {err}");
    let err = h
        .engine
        .fail_leased(&id, &stale, "boom", true)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)), "got: {err}");

    // The fresh token still works.
    assert_eq!(
        h.engine.heartbeat_lease(&id, &fresh, 60).await.unwrap(),
        JobStatus::Running
    );
    h.engine
        .complete_leased(&id, &fresh, Map::new())
        .await
        .unwrap();
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );
}

#[tokio::test]
async fn heartbeat_reports_mid_run_cancellation() {
    let h = harness(|b| b);
    let id = h
        .engine
        .enqueue("external-task", Map::new(), Default::default())
        .await
        .unwrap();
    let lease = h.engine.lease_jobs("default", 1, 30, 0).await.unwrap();
    let token = lease[0].lease_token.clone();

    h.engine.cancel_job(&id).await.unwrap();

    // The worker's next heartbeat learns the job is cancelled...
    assert_eq!(
        h.engine.heartbeat_lease(&id, &token, 30).await.unwrap(),
        JobStatus::Cancelled
    );
    // ...and a late completion replay is an idempotent no-op that does not
    // rewrite the cancellation.
    h.engine
        .complete_leased(&id, &token, Map::new())
        .await
        .unwrap();
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Cancelled
    );
}

#[tokio::test]
async fn cancelling_a_finished_job_is_a_conflict_and_keeps_history() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    let id = h
        .engine
        .enqueue("echo", Map::new(), Default::default())
        .await
        .unwrap();
    assert!(h.engine.process_once("default").await.unwrap());
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );

    let err = h.engine.cancel_job(&id).await.unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)), "got: {err}");
    // The terminal status must be untouched.
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );
}

#[tokio::test]
async fn idempotency_key_dedupes_enqueue() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    let opts = || EnqueueOptions {
        idempotency_key: Some("once-please".into()),
        tenant_id: Some("t1".into()),
        ..Default::default()
    };
    let first = h.engine.enqueue("echo", Map::new(), opts()).await.unwrap();
    let second = h.engine.enqueue("echo", Map::new(), opts()).await.unwrap();
    assert_eq!(first, second);

    // Exactly one job exists to claim.
    assert!(h.engine.process_once("default").await.unwrap());
    assert!(!h.engine.process_once("default").await.unwrap());
    assert_eq!(h.engine.stats().snapshot().jobs_created, 1);
}

#[tokio::test]
async fn remote_lease_complete_and_fail_roundtrip() {
    // The remote worker protocol must share the local loop's semantics:
    // lease marks running, complete records the result, and a replayed
    // complete is a no-op.
    let h = harness(|b| b);
    let id = h
        .engine
        .enqueue("external-task", Map::new(), Default::default())
        .await
        .unwrap();

    let leases = h.engine.lease_jobs("default", 5, 30, 0).await.unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].job.id, id);
    assert_eq!(leases[0].job.status, JobStatus::Running);
    let token = leases[0].lease_token.clone();

    let mut result = Map::new();
    result.insert("answer".into(), json!(42));
    h.engine
        .complete_leased(&id, &token, result.clone())
        .await
        .unwrap();
    let job = h.engine.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Completed);
    assert_eq!(job.result.unwrap()["answer"], json!(42));

    // Replaying the completion (client retry after a network blip) is fine.
    h.engine.complete_leased(&id, &token, result).await.unwrap();
    assert_eq!(h.engine.stats().snapshot().jobs_completed, 1);
}

#[tokio::test]
async fn batch_enqueue_creates_all_jobs() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    let ids = h
        .engine
        .enqueue_batch(
            vec![
                batch_item("echo", map(json!({"i": 1})), None),
                batch_item("echo", map(json!({"i": 2})), None),
                batch_item("echo", map(json!({"i": 3})), None),
            ],
            Some("tenant-a".into()),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);

    let page = h
        .engine
        .list_jobs(&ListFilter {
            include_total: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.total, Some(3));
    assert!(!page.has_more);
    assert!(page
        .items
        .iter()
        .all(|j| j.tenant_id.as_deref() == Some("tenant-a")));
}

#[tokio::test]
async fn retention_purges_old_terminal_jobs_only() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    let done = h
        .engine
        .enqueue("echo", Map::new(), Default::default())
        .await
        .unwrap();
    assert!(h.engine.process_once("default").await.unwrap());
    let pending = h
        .engine
        .enqueue(
            "echo",
            Map::new(),
            EnqueueOptions {
                run_at: Some(h.clock.now() + chrono::Duration::days(30)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    h.clock.advance_secs(8 * 24 * 3600); // 8 days
    let purged = h
        .store
        .purge_terminal(h.clock.now() - chrono::Duration::days(7))
        .await
        .unwrap();
    assert_eq!(purged, 1);
    assert!(
        h.engine.get_job(&done).await.is_err(),
        "terminal job purged"
    );
    assert!(
        h.engine.get_job(&pending).await.is_ok(),
        "non-terminal job must survive retention"
    );
}

#[tokio::test]
async fn panicking_handler_fails_the_job_without_killing_the_worker() {
    let h = harness(|b| {
        b.register_fn("explode", |_p: Map| async { panic!("boom") })
            .register("echo", builtin::echo())
    });

    let id = h
        .engine
        .enqueue(
            "explode",
            Map::new(),
            EnqueueOptions {
                config: Some(deterministic_cfg(0)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // The panic is contained: process_once returns normally and the job goes
    // through the standard failure policy (0 retries here, so straight to
    // failed + dead-letter).
    assert!(h.engine.process_once("default").await.unwrap());
    let job = h.engine.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Failed);
    assert!(
        job.error_message
            .as_deref()
            .unwrap_or_default()
            .contains("handler panicked: boom"),
        "got: {:?}",
        job.error_message
    );
    assert_eq!(h.store.count_dead_letters().await.unwrap(), 1);

    // The same worker path keeps processing afterwards.
    h.engine
        .enqueue("echo", Map::new(), Default::default())
        .await
        .unwrap();
    assert!(h.engine.process_once("default").await.unwrap());
    assert_eq!(h.engine.stats().snapshot().jobs_completed, 1);
}

#[tokio::test]
async fn absurd_job_config_is_rejected_at_enqueue() {
    let h = harness(|b| b.register("echo", builtin::echo()));

    for bad in [
        JobConfig {
            retry_delay_secs: u64::MAX,
            ..JobConfig::default()
        },
        JobConfig {
            retry_max_delay_secs: u64::MAX,
            ..JobConfig::default()
        },
        JobConfig {
            timeout_secs: 0,
            ..JobConfig::default()
        },
        JobConfig {
            max_retries: u32::MAX,
            ..JobConfig::default()
        },
        JobConfig {
            jitter_factor: Some(7.0),
            ..JobConfig::default()
        },
    ] {
        let err = h
            .engine
            .enqueue(
                "echo",
                Map::new(),
                EnqueueOptions {
                    config: Some(bad.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, EngineError::Validation(_)),
            "config {bad:?} should be rejected, got: {err:?}"
        );
    }

    // Batches are validated per entry, with the index in the error.
    let err = h
        .engine
        .enqueue_batch(
            vec![batch_item(
                "echo",
                Map::new(),
                Some(JobConfig {
                    timeout_secs: 0,
                    ..JobConfig::default()
                }),
            )],
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Validation(_)), "got: {err:?}");
}

#[tokio::test]
async fn dead_letter_replay_creates_a_fresh_detached_job() {
    let h = harness(|b| {
        b.register_fn("boom", |_p: Map| async {
            Err::<Map, _>(HandlerError::permanent("nope"))
        })
    });
    let id = h
        .engine
        .enqueue(
            "boom",
            map(json!({"k": 1})),
            EnqueueOptions {
                config: Some(deterministic_cfg(0)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(h.engine.process_once("default").await.unwrap());
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Failed
    );

    let page = h
        .engine
        .list_dead_letters(&ListFilter::default())
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    let dl = page.items[0].clone();
    assert_eq!(dl.job_id, id);
    assert_eq!(dl.reason, "non_retryable");
    assert!(dl.replayed_at.is_none());

    let new_id = h.engine.replay_dead_letter(dl.id).await.unwrap();
    assert_ne!(new_id, id);
    let fresh = h.engine.get_job(&new_id).await.unwrap();
    assert_eq!(fresh.status, JobStatus::Pending);
    assert_eq!(fresh.payload, map(json!({"k": 1})));
    assert_eq!(fresh.retry_count, 0);
    assert_eq!(fresh.delivery_count, 0);
    assert!(fresh.workflow_id.is_none(), "replays are detached jobs");
    assert_eq!(fresh.metadata.get("replayed_from_job"), Some(&json!(id)));

    // The entry records the replay, and a second replay is refused.
    let dl = h.engine.get_dead_letter(dl.id).await.unwrap();
    assert_eq!(dl.replay_job_id.as_deref(), Some(new_id.as_str()));
    let err = h.engine.replay_dead_letter(dl.id).await.unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)), "got: {err:?}");

    // The replayed job is genuinely claimable.
    assert!(h.engine.process_once("default").await.unwrap());
}

#[tokio::test]
async fn cron_schedules_fire_on_time_dedupe_and_skip_missed_runs() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    // TestClock epoch is 2021-01-01T00:00:00Z; every 5 minutes fires at :05.
    let cron_id = h
        .engine
        .create_cron(
            CreateCronRequest {
                name: "tick".into(),
                cron_expr: "*/5 * * * *".into(),
                task_name: "echo".into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    // Not due yet.
    assert_eq!(h.engine.cron_tick().await.unwrap(), 0);

    // 00:05 passes: one firing; a second tick at the same instant is a no-op
    // (the idempotency key is bound to the due instant).
    h.clock.advance_secs(5 * 60 + 1);
    assert_eq!(h.engine.cron_tick().await.unwrap(), 1);
    assert_eq!(h.engine.cron_tick().await.unwrap(), 0);
    let page = h.engine.list_jobs(&ListFilter::default()).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].metadata.get("cron_id"), Some(&json!(cron_id)));

    // An hour "offline": exactly one catch-up firing, then the schedule
    // jumps to its next future slot.
    h.clock.advance_secs(3600);
    assert_eq!(h.engine.cron_tick().await.unwrap(), 1);
    assert_eq!(h.engine.cron_tick().await.unwrap(), 0);
    assert_eq!(
        h.engine
            .list_jobs(&ListFilter::default())
            .await
            .unwrap()
            .items
            .len(),
        2
    );

    // Pause stops firings; resume does not catch up on missed runs.
    h.engine.set_cron_enabled(&cron_id, false).await.unwrap();
    h.clock.advance_secs(3600);
    assert_eq!(h.engine.cron_tick().await.unwrap(), 0);
    h.engine.set_cron_enabled(&cron_id, true).await.unwrap();
    assert_eq!(
        h.engine.cron_tick().await.unwrap(),
        0,
        "resume must not fire for runs missed while paused"
    );
    h.clock.advance_secs(5 * 60);
    assert_eq!(h.engine.cron_tick().await.unwrap(), 1);

    // Names are unique; deletion is final.
    let err = h
        .engine
        .create_cron(
            CreateCronRequest {
                name: "tick".into(),
                cron_expr: "*/5 * * * *".into(),
                task_name: "echo".into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Conflict(_)), "got: {err:?}");
    h.engine.delete_cron(&cron_id).await.unwrap();
    assert!(h.engine.get_cron(&cron_id).await.is_err());
}

// ---- Queue registry and pause ---------------------------------------------------

fn tenant_opts(tenant: &str, queue: &str) -> EnqueueOptions {
    EnqueueOptions {
        tenant_id: Some(tenant.into()),
        queue: Some(queue.into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn paused_queue_is_skipped_until_resumed() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    for _ in 0..2 {
        h.engine
            .enqueue("echo", Map::new(), tenant_opts("acme", "orders"))
            .await
            .unwrap();
    }
    h.engine.pause_queue(Some("acme"), "orders").await.unwrap();

    // Nothing is claimable while paused; the backlog is still reported.
    assert!(!h.engine.process_once("orders").await.unwrap());
    let stats = h.engine.queue_stats(Some("acme")).await.unwrap();
    let orders = stats.iter().find(|q| q.queue == "orders").unwrap();
    assert!(orders.paused);
    assert_eq!(orders.pending, 2);

    h.engine.resume_queue(Some("acme"), "orders").await.unwrap();
    assert!(h.engine.process_once("orders").await.unwrap());
    assert!(h.engine.process_once("orders").await.unwrap());
    let stats = h.engine.queue_stats(Some("acme")).await.unwrap();
    let orders = stats.iter().find(|q| q.queue == "orders").unwrap();
    assert!(!orders.paused);
    assert_eq!(orders.pending, 0);
}

#[tokio::test]
async fn pause_is_scoped_to_one_tenant() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    let acme = h
        .engine
        .enqueue("echo", Map::new(), tenant_opts("acme", "orders"))
        .await
        .unwrap();
    let globex = h
        .engine
        .enqueue("echo", Map::new(), tenant_opts("globex", "orders"))
        .await
        .unwrap();
    let nobody = h
        .engine
        .enqueue(
            "echo",
            Map::new(),
            EnqueueOptions {
                queue: Some("orders".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    h.engine.pause_queue(Some("acme"), "orders").await.unwrap();

    // The other tenant's job and the untenanted job still run; acme's waits.
    assert!(h.engine.process_once("orders").await.unwrap());
    assert!(h.engine.process_once("orders").await.unwrap());
    assert!(!h.engine.process_once("orders").await.unwrap());
    assert_eq!(
        h.engine.get_job(&globex).await.unwrap().status,
        JobStatus::Completed
    );
    assert_eq!(
        h.engine.get_job(&nobody).await.unwrap().status,
        JobStatus::Completed
    );
    assert_eq!(
        h.engine.get_job(&acme).await.unwrap().status,
        JobStatus::Pending
    );

    // The all-tenants view only reports paused when every tenant is.
    let all = h.engine.queue_stats(None).await.unwrap();
    assert!(!all.iter().find(|q| q.queue == "orders").unwrap().paused);
    let acme_view = h.engine.queue_stats(Some("acme")).await.unwrap();
    assert!(
        acme_view
            .iter()
            .find(|q| q.queue == "orders")
            .unwrap()
            .paused
    );
    let globex_view = h.engine.queue_stats(Some("globex")).await.unwrap();
    assert!(
        !globex_view
            .iter()
            .find(|q| q.queue == "orders")
            .unwrap()
            .paused
    );
}

#[tokio::test]
async fn registry_lists_queues_that_are_currently_empty() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    h.engine
        .enqueue("echo", Map::new(), tenant_opts("acme", "emails"))
        .await
        .unwrap();
    h.engine
        .enqueue_batch(
            vec![BatchItem {
                task_name: "echo".into(),
                payload: Map::new(),
                config: None,
                queue: Some("reports".into()),
                run_at: None,
            }],
            Some("acme".into()),
        )
        .await
        .unwrap();
    assert!(h.engine.process_once("emails").await.unwrap());
    assert!(h.engine.process_once("reports").await.unwrap());

    // Both queues are drained, yet both are listed, with zero counts.
    let stats = h.engine.queue_stats(Some("acme")).await.unwrap();
    let names: Vec<&str> = stats.iter().map(|q| q.queue.as_str()).collect();
    assert_eq!(names, vec!["emails", "reports"]);
    assert!(stats
        .iter()
        .all(|q| q.pending + q.running + q.scheduled == 0));
    assert!(stats.iter().all(|q| !q.paused));
    // Another tenant sees nothing: registration is per tenant.
    assert!(h
        .engine
        .queue_stats(Some("globex"))
        .await
        .unwrap()
        .is_empty());
    // Registration is cached: a second enqueue does not need the store, and
    // the registry still holds exactly one row per pair.
    h.engine
        .enqueue("echo", Map::new(), tenant_opts("acme", "emails"))
        .await
        .unwrap();
    let rows = h.store.list_registered_queues(Some("acme")).await.unwrap();
    assert_eq!(rows.len(), 2, "{rows:?}");
}

#[tokio::test]
async fn invalid_queue_names_are_rejected_by_pause() {
    let h = harness(|b| b);
    for bad in [
        "",
        "has space",
        "tab\tbed",
        &"x".repeat(MAX_QUEUE_NAME_LEN + 1),
    ] {
        let err = h.engine.pause_queue(Some("acme"), bad).await.unwrap_err();
        assert!(
            matches!(err, EngineError::Validation(_)),
            "{bad:?}: {err:?}"
        );
    }
    assert!(h
        .engine
        .pause_queue(Some("acme"), "ok-queue.v2")
        .await
        .is_ok());
}

// ---- Bulk operations --------------------------------------------------------

#[tokio::test]
async fn bulk_cancel_honours_filter_and_limit() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    for _ in 0..5 {
        h.engine
            .enqueue("a", Map::new(), tenant_opts("acme", "orders"))
            .await
            .unwrap();
    }
    for _ in 0..2 {
        h.engine
            .enqueue("b", Map::new(), tenant_opts("acme", "orders"))
            .await
            .unwrap();
    }
    h.engine
        .enqueue("a", Map::new(), tenant_opts("globex", "orders"))
        .await
        .unwrap();

    let filter = |task: &str, limit: i64| ListFilter {
        tenant_id: Some("acme".into()),
        task_name: Some(task.into()),
        limit,
        ..Default::default()
    };
    assert_eq!(h.engine.cancel_jobs(&filter("a", 3)).await.unwrap(), 3);
    assert_eq!(h.engine.cancel_jobs(&filter("a", 3)).await.unwrap(), 2);
    assert_eq!(h.engine.cancel_jobs(&filter("a", 3)).await.unwrap(), 0);
    // Task b and the other tenant are untouched.
    let remaining = h
        .engine
        .list_jobs(&ListFilter {
            status: Some("pending".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .items;
    assert_eq!(remaining.len(), 3, "{remaining:?}");
    assert!(remaining
        .iter()
        .all(|j| j.task_name == "b" || j.tenant_id.as_deref() == Some("globex")));

    // Terminal and unknown statuses are rejected: history is never rewritten.
    for bad in ["completed", "failed", "cancelled", "bogus"] {
        let err = h
            .engine
            .cancel_jobs(&ListFilter {
                status: Some(bad.into()),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::Validation(_)), "{bad}: {err:?}");
    }
}

#[tokio::test]
async fn bulk_replay_replays_each_dead_letter_once() {
    // No handler for "ghost": every delivery dead-letters.
    let h = harness(|b| b);
    for _ in 0..3 {
        h.engine
            .enqueue("ghost", Map::new(), tenant_opts("acme", "default"))
            .await
            .unwrap();
    }
    for _ in 0..3 {
        assert!(h.engine.process_once("default").await.unwrap());
    }
    let filter = ListFilter {
        tenant_id: Some("acme".into()),
        limit: 2,
        ..Default::default()
    };
    let first = h.engine.replay_dead_letters(&filter).await.unwrap();
    assert_eq!(first.len(), 2);
    assert!(first
        .iter()
        .all(|o| o.job_id.is_some() && o.error.is_none()));
    let second = h.engine.replay_dead_letters(&filter).await.unwrap();
    assert_eq!(second.len(), 1, "{second:?}");
    let third = h.engine.replay_dead_letters(&filter).await.unwrap();
    assert!(third.is_empty(), "{third:?}");
    // Three fresh pending jobs now exist alongside the three failed ones.
    let pending = h
        .engine
        .list_jobs(&ListFilter {
            status: Some("pending".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(pending.items.len(), 3);
}

#[tokio::test]
async fn jobs_can_be_listed_by_task_name() {
    let h = harness(|b| b);
    h.engine
        .enqueue("a", Map::new(), Default::default())
        .await
        .unwrap();
    h.engine
        .enqueue("b", Map::new(), Default::default())
        .await
        .unwrap();
    let page = h
        .engine
        .list_jobs(&ListFilter {
            task_name: Some("b".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].task_name, "b");
}

// ---- Worker pool ----------------------------------------------------------------

#[tokio::test]
async fn worker_pool_drains_a_backlog_concurrently_and_stops_cleanly() {
    let peak = Arc::new(AtomicU32::new(0));
    let active = Arc::new(AtomicU32::new(0));
    let (p, a) = (peak.clone(), active.clone());
    let h = harness(move |b| {
        b.worker_count(4).register_fn("slow", move |_p| {
            let (p, a) = (p.clone(), a.clone());
            async move {
                let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                p.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                a.fetch_sub(1, Ordering::SeqCst);
                Ok(Map::new())
            }
        })
    });
    let mut ids = Vec::new();
    for _ in 0..12 {
        ids.push(
            h.engine
                .enqueue("slow", Map::new(), Default::default())
                .await
                .unwrap(),
        );
    }
    let mut pool = h.engine.run_workers("default");
    let all_done = async {
        loop {
            let mut done = 0;
            for id in &ids {
                if h.engine.get_job(id).await.unwrap().status == JobStatus::Completed {
                    done += 1;
                }
            }
            if done == ids.len() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), all_done)
        .await
        .expect("backlog drained");
    assert!(
        peak.load(Ordering::SeqCst) > 1 && peak.load(Ordering::SeqCst) <= 4,
        "peak concurrency {} should be bounded by the pool size",
        peak.load(Ordering::SeqCst)
    );

    h.engine.shutdown();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while pool.join_next().await.is_some() {}
    })
    .await
    .expect("pool stops after shutdown");
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_jobs() {
    let finished = Arc::new(AtomicU32::new(0));
    let f = finished.clone();
    let h = harness(move |b| {
        b.worker_count(2).register_fn("slow", move |_p| {
            let f = f.clone();
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                f.fetch_add(1, Ordering::SeqCst);
                Ok(Map::new())
            }
        })
    });
    let id = h
        .engine
        .enqueue("slow", Map::new(), Default::default())
        .await
        .unwrap();
    let mut pool = h.engine.run_workers("default");
    // Let the dispatcher claim it, then stop while the handler is running.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Running
    );
    h.engine.shutdown();
    while pool.join_next().await.is_some() {}
    assert_eq!(
        finished.load(Ordering::SeqCst),
        1,
        "the in-flight job ran to completion"
    );
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );
}
