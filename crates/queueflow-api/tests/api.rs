//! In-process HTTP tests for the API: routing, auth, serialization, and error
//! mapping — all driven through the real axum router against the in-memory
//! engine, with no socket and no database.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use queueflow_api::{build_router, ApiState};
use queueflow_core::task::builtin;
use queueflow_core::*;
use serde_json::{json, Value};
use tower::ServiceExt;

fn app() -> axum::Router {
    let clock = Arc::new(SystemClock);
    let store = Arc::new(InMemoryJobStore::new());
    let queue = Arc::new(InMemoryMessageQueue::new(clock.clone()));
    let engine = Engine::builder(store, queue, clock)
        .register("echo", builtin::echo())
        .build();
    // Pretend workers are running so /ready reports ready.
    let _ = engine.run_workers("default");
    engine.shutdown(); // we only needed the running flag flipped; stop the tasks
    build_router(ApiState::new(engine))
}

fn req(method: &str, uri: &str, token: Option<&str>, body: Option<Value>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    match body {
        Some(v) => b
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&v).unwrap()))
            .unwrap(),
        None => b.body(Body::empty()).unwrap(),
    }
}

async fn json_body(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

#[tokio::test]
async fn health_is_public_and_ok() {
    let resp = app()
        .oneshot(req("GET", "/health", None, None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(json_body(resp).await["status"], "healthy");
}

#[tokio::test]
async fn create_job_requires_auth() {
    let resp = app()
        .oneshot(req(
            "POST",
            "/api/v1/jobs",
            None,
            Some(json!({"task_name": "echo"})),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn create_and_fetch_job() {
    let app = app();
    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/jobs",
            Some("key"),
            Some(json!({"task_name": "echo", "payload": {"x": 1}})),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let id = json_body(resp).await["job_id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = app
        .oneshot(req("GET", &format!("/api/v1/jobs/{id}"), Some("key"), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["task_name"], "echo");
    assert_eq!(body["status"], "pending");
}

#[tokio::test]
async fn missing_task_name_is_400() {
    let resp = app()
        .oneshot(req(
            "POST",
            "/api/v1/jobs",
            Some("key"),
            Some(json!({"task_name": "  "})),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn unknown_job_is_404() {
    let resp = app()
        .oneshot(req("GET", "/api/v1/jobs/does-not-exist", Some("key"), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn create_workflow_and_get_it() {
    let app = app();
    let body = json!({
        "name": "wf",
        "steps": [
            {"name": "a", "task_name": "echo"},
            {"name": "b", "task_name": "echo", "depends_on": ["a"]}
        ]
    });
    let resp = app
        .clone()
        .oneshot(req("POST", "/api/v1/workflows", Some("key"), Some(body)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let id = json_body(resp).await["workflow_id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = app
        .oneshot(req(
            "GET",
            &format!("/api/v1/workflows/{id}"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(json_body(resp).await["name"], "wf");
}

#[tokio::test]
async fn cyclic_workflow_is_400() {
    let body = json!({
        "name": "cyclic",
        "steps": [
            {"name": "a", "task_name": "echo", "depends_on": ["b"]},
            {"name": "b", "task_name": "echo", "depends_on": ["a"]}
        ]
    });
    let resp = app()
        .oneshot(req("POST", "/api/v1/workflows", Some("key"), Some(body)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn workflow_diagram_is_mermaid() {
    let app = app();
    let body = json!({
        "name": "wf",
        "steps": [
            {"name": "a", "task_name": "echo"},
            {"name": "b", "task_name": "echo", "depends_on": ["a"]}
        ]
    });
    let resp = app
        .clone()
        .oneshot(req("POST", "/api/v1/workflows", Some("key"), Some(body)))
        .await
        .unwrap();
    let id = json_body(resp).await["workflow_id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = app
        .oneshot(req(
            "GET",
            &format!("/api/v1/workflows/{id}/diagram"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["format"], "mermaid");
    assert!(body["diagram"].as_str().unwrap().contains("a --> b"));
}

#[tokio::test]
async fn cannot_cancel_another_tenants_job() {
    // Seed a job owned by a different tenant directly through the engine.
    let clock = Arc::new(SystemClock);
    let store = Arc::new(InMemoryJobStore::new());
    let queue = Arc::new(InMemoryMessageQueue::new(clock.clone()));
    let engine = Engine::builder(store, queue, clock)
        .register("echo", builtin::echo())
        .build();
    let id = engine
        .enqueue(
            "echo",
            Map::new(),
            EnqueueOptions {
                tenant_id: Some("other-tenant".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let app = build_router(ApiState::new(engine));

    // The API authenticates every token as "tenant1", so this is cross-tenant.
    let resp = app
        .oneshot(req(
            "POST",
            &format!("/api/v1/jobs/{id}/cancel"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn openapi_json_is_served() {
    let resp = app()
        .oneshot(req("GET", "/openapi.json", None, None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let spec = json_body(resp).await;
    assert_eq!(spec["openapi"].as_str().unwrap().chars().next(), Some('3'));
    assert!(spec["paths"]["/api/v1/jobs"].is_object());
    assert!(spec["paths"]["/api/v1/workflows"].is_object());
}
