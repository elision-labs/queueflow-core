//! End-to-end client tests: a real axum server (in-memory engine) on an
//! ephemeral port, driven through `queueflow-client` over actual HTTP. This
//! exercises the full producer surface plus the remote worker protocol:
//! lease, complete, fail-with-retry-policy, and idempotent job creation.

use std::sync::Arc;
use std::time::Duration;

use queueflow_api::{build_router, ApiState};
use queueflow_client::{Client, CreateJobOptions, ListQuery};
use queueflow_core::{Engine, InMemoryJobStore, JobStatus, Map, SystemClock};

/// Start an API server on an ephemeral port; returns a connected client.
/// No local workers are spawned: these tests drive execution through the
/// remote worker protocol, exactly like an out-of-process worker would.
async fn start_server() -> Client {
    let clock = Arc::new(SystemClock);
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));
    let engine = Engine::builder(store, clock).build();
    engine.mark_running();

    let app = build_router(ApiState::new(engine));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Client::new(format!("http://{addr}"), "test-token")
}

#[tokio::test]
async fn remote_worker_leases_completes_and_propagates_result() {
    let client = start_server().await;

    let id = client
        .create_job("remote-task", Map::new(), CreateJobOptions::default())
        .await
        .unwrap();

    // Lease it like a remote worker.
    let leases = client.lease_jobs("default", 1, 30, 0).await.unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].job.id, id);
    assert_eq!(leases[0].job.status, JobStatus::Running);

    // Heartbeat (which now reports the live status), then complete.
    assert_eq!(
        client.heartbeat_job(&leases[0], 60).await.unwrap(),
        JobStatus::Running
    );
    let mut result = Map::new();
    result.insert("ok".into(), serde_json::json!(true));
    client.complete_job(&leases[0], result).await.unwrap();

    let job = client.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Completed);
    assert_eq!(job.result.unwrap()["ok"], serde_json::json!(true));

    // The queue is drained: nothing left to lease.
    assert!(client
        .lease_jobs("default", 1, 30, 0)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn remote_failure_is_retried_then_replayable() {
    let client = start_server().await;
    let id = client
        .create_job(
            "flaky-remote",
            Map::new(),
            CreateJobOptions {
                max_retries: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let leases = client.lease_jobs("default", 1, 30, 0).await.unwrap();
    client
        .fail_job(&leases[0], "transient failure", true)
        .await
        .unwrap();

    // Retry is rescheduled in the row (future scheduled_at); status reflects it.
    let job = client.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Retrying);
    assert_eq!(job.retry_count, 1);
}

#[tokio::test]
async fn permanent_remote_failure_dead_letters() {
    let client = start_server().await;
    let id = client
        .create_job("bad-input", Map::new(), CreateJobOptions::default())
        .await
        .unwrap();

    let leases = client.lease_jobs("default", 1, 30, 0).await.unwrap();
    client
        .fail_job(&leases[0], "unparseable input", false)
        .await
        .unwrap();

    let job = client.get_job(&id).await.unwrap();
    assert_eq!(job.status, JobStatus::Failed);
}

#[tokio::test]
async fn idempotency_key_replays_the_original_job() {
    let client = start_server().await;
    let opts = CreateJobOptions {
        idempotency_key: Some("order-42".into()),
        ..Default::default()
    };
    let first = client
        .create_job("charge", Map::new(), opts.clone())
        .await
        .unwrap();
    let second = client.create_job("charge", Map::new(), opts).await.unwrap();
    assert_eq!(first, second, "same key must return the original job id");

    // Exactly one message was enqueued.
    let leases = client.lease_jobs("default", 10, 30, 0).await.unwrap();
    assert_eq!(leases.len(), 1);
}

#[tokio::test]
async fn cancel_then_cancel_again_conflicts() {
    let client = start_server().await;
    let id = client
        .create_job("never-runs", Map::new(), CreateJobOptions::default())
        .await
        .unwrap();

    client.cancel_job(&id).await.unwrap();
    let err = client.cancel_job(&id).await.unwrap_err();
    assert_eq!(err.status(), Some(409), "double cancel must be a conflict");
}

#[tokio::test]
async fn list_pagination_reports_has_more_and_optional_total() {
    let client = start_server().await;
    let jobs: Vec<(String, Map)> = (0..5).map(|_| ("t".to_string(), Map::new())).collect();
    client.create_batch_jobs(jobs).await.unwrap();

    let page = client
        .list_jobs(&ListQuery {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.jobs.len(), 2);
    assert!(page.has_more);
    assert_eq!(page.total, None, "total must be omitted unless requested");

    let page = client
        .list_jobs(&ListQuery {
            limit: Some(2),
            include_total: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.total, Some(5));
}

#[tokio::test]
async fn health_ready_and_watch_job_stream() {
    let client = start_server().await;
    assert_eq!(client.health().await.unwrap().status, "healthy");
    assert_eq!(client.ready().await.unwrap().status, "ready");

    let id = client
        .create_job("watched", Map::new(), Default::default())
        .await
        .unwrap();

    // The stream opens on a pending job and emits the current status first.
    let mut events = client.watch_job(&id).await.unwrap();
    let first = events.next().await.unwrap().expect("first event");
    assert_eq!(first.status, JobStatus::Pending);

    // A "worker" completes the job; the stream must deliver the terminal
    // transition and then end.
    let leases = client.lease_jobs("default", 1, 30, 0).await.unwrap();
    client.complete_job(&leases[0], Map::new()).await.unwrap();
    loop {
        match events.next().await.unwrap() {
            Some(job) if job.status.is_terminal() => {
                assert_eq!(job.status, JobStatus::Completed);
                break;
            }
            Some(_) => continue, // intermediate transition (e.g. running)
            None => panic!("stream ended before a terminal event"),
        }
    }
    assert!(events.next().await.unwrap().is_none(), "stream must close");
}

#[tokio::test]
async fn wait_for_job_resolves_when_a_worker_completes_it() {
    let client = start_server().await;
    let id = client
        .create_job("slowish", Map::new(), CreateJobOptions::default())
        .await
        .unwrap();

    // A "worker" completes the job after a short delay, while we wait.
    let worker = client.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let leases = worker.lease_jobs("default", 1, 30, 0).await.unwrap();
        worker.complete_job(&leases[0], Map::new()).await.unwrap();
    });

    let job = client
        .wait_for_job(&id, Duration::from_millis(100), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(job.status, JobStatus::Completed);
}
