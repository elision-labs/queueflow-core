//! End-to-end engine tests on the in-memory adapters — no database required.
//!
//! These cover the core engine behaviours:
//! the success path, durable exponential-backoff retries, dead-lettering,
//! per-attempt timeouts, cancellation, and priority ordering.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use queueflow_core::task::builtin;
use queueflow_core::*;
use serde_json::json;

type Mem = Engine<InMemoryJobStore, InMemoryMessageQueue>;

fn map(v: serde_json::Value) -> Map {
    v.as_object()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect()
}

struct Harness {
    engine: Arc<Mem>,
    store: Arc<InMemoryJobStore>,
    queue: Arc<InMemoryMessageQueue>,
    clock: Arc<TestClock>,
}

fn harness(
    build: impl FnOnce(
        EngineBuilder<InMemoryJobStore, InMemoryMessageQueue>,
    ) -> EngineBuilder<InMemoryJobStore, InMemoryMessageQueue>,
) -> Harness {
    let clock = Arc::new(TestClock::epoch());
    let store = Arc::new(InMemoryJobStore::new());
    let queue = Arc::new(InMemoryMessageQueue::new(clock.clone()));
    let engine = build(Engine::builder(store.clone(), queue.clone(), clock.clone())).build();
    Harness {
        engine,
        store,
        queue,
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

    // Attempt 1 fails → scheduled with a 60s delay.
    assert!(h.engine.process_once("default").await.unwrap());
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Retrying
    );
    // The retry is invisible until the backoff delay elapses (durable, in-queue).
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
async fn cancelled_job_is_not_executed() {
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
    assert!(h.engine.process_once("default").await.unwrap());

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
async fn redelivered_terminal_job_is_not_reprocessed() {
    // At-least-once delivery means a completed job's message can reappear (e.g.
    // an ack failed). The handler must not run twice.
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
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    // Simulate a redelivery of the same job's message.
    let job = h.engine.get_job(&id).await.unwrap();
    h.queue
        .send("default", &QueueMessage::for_job(&job), 0)
        .await
        .unwrap();
    assert!(h.engine.process_once("default").await.unwrap());

    // The terminal-state guard makes the redelivery a no-op.
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "handler must not re-run for a terminal job"
    );
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
    );
}

#[tokio::test]
async fn long_running_job_is_not_redelivered_mid_flight() {
    // The default visibility timeout (30s) is shorter than this job's timeout.
    // The engine must extend the lease when it starts the job, otherwise a
    // second worker would lease and run the same job concurrently.
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
    // Let the worker lease the message and enter the handler.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 31 virtual seconds later, the original 30s lease would have expired; the
    // extended lease (timeout + grace) keeps the message invisible.
    h.clock.advance_secs(31);
    assert!(
        h.queue.read("default", 1, 1).await.unwrap().is_empty(),
        "in-flight job must not be redeliverable"
    );

    gate.notify_one();
    assert!(worker.await.unwrap());
    assert_eq!(
        h.engine.get_job(&id).await.unwrap().status,
        JobStatus::Completed
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

    // Exactly one message was published.
    assert!(h.engine.process_once("default").await.unwrap());
    assert!(!h.engine.process_once("default").await.unwrap());
    assert_eq!(h.engine.stats().snapshot().jobs_created, 1);
}

#[tokio::test]
async fn remote_lease_complete_and_fail_roundtrip() {
    // The remote worker protocol must share the local loop's semantics:
    // lease marks running, complete records the result, fail applies retry
    // policy, and a replayed complete is a no-op.
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

    let mut result = Map::new();
    result.insert("answer".into(), json!(42));
    h.engine
        .complete_leased("default", leases[0].lease_id, &id, result.clone())
        .await
        .unwrap();
    let job = h.engine.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Completed);
    assert_eq!(job.result.unwrap()["answer"], json!(42));

    // Replaying the completion (client retry after a network blip) is fine.
    h.engine
        .complete_leased("default", leases[0].lease_id, &id, result)
        .await
        .unwrap();
    assert_eq!(h.engine.stats().snapshot().jobs_completed, 1);
}

#[tokio::test]
async fn batch_enqueue_creates_all_jobs() {
    let h = harness(|b| b.register("echo", builtin::echo()));
    let ids = h
        .engine
        .enqueue_batch(
            vec![
                ("echo".into(), map(json!({"i": 1})), None),
                ("echo".into(), map(json!({"i": 2})), None),
                ("echo".into(), map(json!({"i": 3})), None),
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
