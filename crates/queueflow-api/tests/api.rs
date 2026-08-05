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

fn test_state() -> ApiState {
    let clock = Arc::new(SystemClock);
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));
    let engine = Engine::builder(store, clock)
        .register("echo", builtin::echo())
        .build();
    // Pretend workers are running so /ready reports ready.
    let _ = engine.run_workers("default");
    engine.shutdown(); // we only needed the running flag flipped; stop the tasks
    ApiState::new(engine)
}

fn app() -> axum::Router {
    build_router(test_state())
}

fn app_with_worker_token(token: &str) -> axum::Router {
    build_router(test_state().with_worker_token(Some(token.to_string())))
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
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));
    let engine = Engine::builder(store, clock)
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
async fn job_events_stream_emits_status_and_closes_on_terminal() {
    // Build an engine we can drive directly: complete the job first, then the
    // SSE stream must emit one terminal `status` event and close.
    let clock = Arc::new(SystemClock);
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));
    let engine = Engine::builder(store, clock)
        .register("echo", builtin::echo())
        .build();
    let id = engine
        .enqueue("echo", Map::new(), Default::default())
        .await
        .unwrap();
    engine.process_once("default").await.unwrap();
    let app = build_router(ApiState::new(engine));

    let resp = app
        .oneshot(req(
            "GET",
            &format!("/api/v1/jobs/{id}/events"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()["content-type"].to_str().unwrap(),
        "text/event-stream"
    );
    // The stream closes after the terminal event, so the body is collectable.
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("event: status"), "got: {text}");
    assert!(text.contains("\"completed\""), "got: {text}");
}

#[tokio::test]
async fn list_jobs_has_more_and_opt_in_total() {
    let app = app();
    for i in 0..3 {
        let resp = app
            .clone()
            .oneshot(req(
                "POST",
                "/api/v1/jobs",
                Some("key"),
                Some(json!({"task_name": "echo", "payload": {"i": i}})),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // Default: no total, has_more derived from limit+1.
    let resp = app
        .clone()
        .oneshot(req("GET", "/api/v1/jobs?limit=2", Some("key"), None))
        .await
        .unwrap();
    let body = json_body(resp).await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 2);
    assert_eq!(body["has_more"], json!(true));
    assert!(body.get("total").is_none(), "total must be opt-in");

    // Opt-in exact total.
    let resp = app
        .oneshot(req(
            "GET",
            "/api/v1/jobs?limit=2&include_total=true",
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    let body = json_body(resp).await;
    assert_eq!(body["total"], json!(3));
}

#[tokio::test]
async fn idempotency_key_header_replays_job() {
    let app = app();
    let send = |app: axum::Router| async move {
        let mut r = req(
            "POST",
            "/api/v1/jobs",
            Some("key"),
            Some(json!({"task_name": "echo"})),
        );
        r.headers_mut()
            .insert("idempotency-key", "abc-123".parse().unwrap());
        let resp = app.oneshot(r).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        json_body(resp).await["job_id"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let first = send(app.clone()).await;
    let second = send(app).await;
    assert_eq!(first, second);
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
    // Worker protocol + SSE are part of the published contract.
    assert!(spec["paths"]["/api/v1/queues/{queue}/lease"].is_object());
    assert!(spec["paths"]["/api/v1/jobs/{id}/complete"].is_object());
    assert!(spec["paths"]["/api/v1/jobs/{id}/fail"].is_object());
    assert!(spec["paths"]["/api/v1/jobs/{id}/heartbeat"].is_object());
    assert!(spec["paths"]["/api/v1/jobs/{id}/events"].is_object());
}

#[tokio::test]
async fn worker_endpoints_require_the_worker_token_when_configured() {
    let app = app_with_worker_token("wt-secret");

    // A tenant token is authenticated but is not the worker credential: 403.
    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/queues/default/lease",
            Some("dev"),
            Some(json!({"max_jobs": 1})),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/jobs/some-id/heartbeat",
            Some("dev"),
            Some(json!({"lease_token": "x", "extend_secs": 30})),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // The worker token itself works.
    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/queues/default/lease",
            Some("wt-secret"),
            Some(json!({"max_jobs": 1})),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // No token at all: 401.
    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/queues/default/lease",
            None,
            Some(json!({"max_jobs": 1})),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // And the worker credential grants no tenant surface.
    let resp = app
        .oneshot(req(
            "POST",
            "/api/v1/jobs",
            Some("wt-secret"),
            Some(json!({"task_name": "echo"})),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn worker_endpoints_accept_any_authenticated_token_in_development_mode() {
    // Without a configured worker token (the default), the worker protocol
    // keeps working with a plain tenant token, preserving the quick start.
    let resp = app()
        .oneshot(req(
            "POST",
            "/api/v1/queues/default/lease",
            Some("dev"),
            Some(json!({"max_jobs": 1})),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn dlq_endpoints_are_tenant_scoped_and_replay_once() {
    // Seed one dead letter for the calling tenant ("tenant1" under the
    // placeholder auth) and one for another tenant, directly via the engine.
    let clock = Arc::new(SystemClock);
    let store = Arc::new(InMemoryJobStore::new(clock.clone()));
    let engine = Engine::builder(store.clone(), clock)
        .register_fn("boom", |_p: Map| async {
            Err::<Map, _>(HandlerError::permanent("nope"))
        })
        .build();
    for tenant in ["tenant1", "someone-else"] {
        engine
            .enqueue(
                "boom",
                Map::new(),
                EnqueueOptions {
                    tenant_id: Some(tenant.into()),
                    config: Some(JobConfig {
                        max_retries: 0,
                        ..JobConfig::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(engine.process_once("default").await.unwrap());
    }
    let all = store
        .list_dead_letters(&ListFilter::default())
        .await
        .unwrap()
        .items;
    let own = all
        .iter()
        .find(|d| d.tenant_id.as_deref() == Some("tenant1"))
        .unwrap()
        .id;
    let foreign = all
        .iter()
        .find(|d| d.tenant_id.as_deref() == Some("someone-else"))
        .unwrap()
        .id;
    let app = build_router(ApiState::new(engine));

    // The list is scoped to the caller's tenant.
    let resp = app
        .clone()
        .oneshot(req(
            "GET",
            "/api/v1/dlq?include_total=true",
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["total"], json!(1));
    assert_eq!(body["dead_letters"][0]["id"], json!(own));
    assert_eq!(body["dead_letters"][0]["reason"], "non_retryable");

    // Foreign entries are forbidden; unknown ids are 404.
    let resp = app
        .clone()
        .oneshot(req(
            "GET",
            &format!("/api/v1/dlq/{foreign}"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            &format!("/api/v1/dlq/{foreign}/replay"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let resp = app
        .clone()
        .oneshot(req("GET", "/api/v1/dlq/999999", Some("key"), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Replay creates a fresh job; a second replay conflicts.
    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            &format!("/api/v1/dlq/{own}/replay"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let job_id = json_body(resp).await["job_id"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = app
        .clone()
        .oneshot(req(
            "GET",
            &format!("/api/v1/jobs/{job_id}"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(json_body(resp).await["status"], "pending");

    let resp = app
        .oneshot(req(
            "POST",
            &format!("/api/v1/dlq/{own}/replay"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn strict_auth_maps_credentials_to_tenants_and_rejects_the_rest() {
    use queueflow_api::auth::AuthConfig;
    let app = build_router(test_state().with_auth(AuthConfig {
        jwt_secret: Some("s3cret".into()),
        api_keys: vec![("k-acme".into(), "acme".into())],
        worker_token: Some("wt".into()),
    }));

    // Placeholder tokens no longer authenticate once real auth is configured.
    let resp = app
        .clone()
        .oneshot(req("GET", "/api/v1/jobs", Some("dev"), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // An API key authenticates and scopes to its tenant.
    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/jobs",
            Some("k-acme"),
            Some(json!({"task_name": "echo"})),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let id = json_body(resp).await["job_id"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = app
        .clone()
        .oneshot(req(
            "GET",
            &format!("/api/v1/jobs/{id}"),
            Some("k-acme"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(json_body(resp).await["tenant_id"], "acme");

    // A JWT is its own tenant (sub claim): it cannot touch acme's job but has
    // its own scope.
    let exp = (chrono::Utc::now().timestamp() + 3600) as usize;
    let jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &json!({"sub": "globex", "exp": exp}),
        &jsonwebtoken::EncodingKey::from_secret(b"s3cret"),
    )
    .unwrap();
    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            &format!("/api/v1/jobs/{id}/cancel"),
            Some(&jwt),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let resp = app
        .clone()
        .oneshot(req(
            "GET",
            "/api/v1/jobs?include_total=true",
            Some(&jwt),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(json_body(resp).await["total"], json!(0));

    // An expired JWT is rejected.
    let expired = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &json!({"sub": "globex", "exp": (chrono::Utc::now().timestamp() - 3600) as usize}),
        &jsonwebtoken::EncodingKey::from_secret(b"s3cret"),
    )
    .unwrap();
    let resp = app
        .oneshot(req("GET", "/api/v1/jobs", Some(&expired), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn cron_endpoints_validate_conflict_and_pause_resume() {
    let app = app();

    // An invalid expression is a 400 before anything persists.
    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            "/api/v1/cron",
            Some("key"),
            Some(json!({"name": "nightly", "cron_expr": "not a cron", "task_name": "echo"})),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = json!({"name": "nightly", "cron_expr": "0 3 * * *", "task_name": "echo"});
    let resp = app
        .clone()
        .oneshot(req("POST", "/api/v1/cron", Some("key"), Some(body.clone())))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let id = json_body(resp).await["cron_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Names are unique per tenant.
    let resp = app
        .clone()
        .oneshot(req("POST", "/api/v1/cron", Some("key"), Some(body)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);

    let resp = app
        .clone()
        .oneshot(req(
            "GET",
            "/api/v1/cron?include_total=true",
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    let listed = json_body(resp).await;
    assert_eq!(listed["total"], json!(1));
    assert_eq!(listed["crons"][0]["enabled"], json!(true));

    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            &format!("/api/v1/cron/{id}/pause"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = app
        .clone()
        .oneshot(req("GET", &format!("/api/v1/cron/{id}"), Some("key"), None))
        .await
        .unwrap();
    assert_eq!(json_body(resp).await["enabled"], json!(false));

    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            &format!("/api/v1/cron/{id}/resume"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = app
        .clone()
        .oneshot(req(
            "DELETE",
            &format!("/api/v1/cron/{id}"),
            Some("key"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = app
        .oneshot(req("GET", &format!("/api/v1/cron/{id}"), Some("key"), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
