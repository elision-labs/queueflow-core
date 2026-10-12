# Changelog

All notable changes to the QueueFlow server and its Rust crates
(`queueflow-core`, `queueflow-api`, `queueflow-client`, `queueflow`). The
language SDKs keep their own changelogs in their repositories.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/); before 1.0, minor
versions may contain breaking changes, which are called out explicitly.

## [Unreleased]

## [0.6.0] - 2026-10-12

### Added

- **Workflows v2.**
  - Conditional steps: `when` on a step is a JSON predicate over the workflow
    context (`path` + `op` + `value` leaves, `all` / `any` / `not` nodes). A
    false condition skips the step with the reason `condition not met`; its
    dependents still run.
  - Dynamic fan-out: `fan_out: {path, item_key?, max_items?}` runs the task
    once per element of an array in the context as concurrent child jobs
    (each with the element under `item_key`, its position under `_index`, and
    `fan_out_index` / `fan_out_total` metadata), created in one atomic claim.
    The step completes with `{"results": [...]}` in order; a failed child
    follows the step's `on_failure` policy (halt cancels the siblings).
  - Sub-workflows: `workflow` on a step runs an inline child workflow in the
    same tenant, linked through `parent_workflow_id` / `parent_step_name` and
    the step's `child_workflow_id`. The child sees the step payload and the
    parent's context under `_parent`; its final context is the step's result.
    Cancelling or timing out the parent cancels the child.
  - Workflow deadlines: `timeout_secs` on the request sets `deadline_at`; the
    janitor fails an overdue workflow (`workflow timed out after Ns`),
    cancelling its unfinished steps.
  - Retry: `POST /api/v1/workflows/{id}/retry` re-runs a failed, partially
    failed or cancelled workflow from its unfinished steps, keeping completed
    results, incrementing `retry_count` and re-arming the deadline.
  - Per-step `queue`; step `started_at` / `completed_at` / `error`;
    `error_message` on the workflow; `GET /api/v1/workflows/{id}/events`
    (Server-Sent Events, driven by a new Postgres notification channel);
    richer `GET /workflows/{id}/steps` (error, timestamps, child workflow,
    fan-out progress); fan-out and sub-workflow shapes in the diagram.
  - Dashboard: the workflow page shows all of the above, offers retry, and
    follows the event stream. CLI: `workflow retry`, `workflow steps`.
    Rust client: `retry_workflow`, `workflow_steps`. Builder:
    `StepBuilder::{when, fan_out, sub_workflow, queue}`,
    `WorkflowBuilder::timeout_secs`.
  - Janitor sweeps for overdue workflows and for finished child workflows
    whose parent step was not settled.

### Changed

- **Migration 0007** adds the columns above to `workflows` and
  `workflow_steps`, two small partial indexes (`idx_workflows_deadline`,
  `idx_workflows_parent`), and the `queueflow_workflow_events` triggers.
  Transactional and quick: no index touches the jobs table.
- The halt policy now records the failing step's error as the workflow's
  `error_message`. (The docs said a step under the `skip` policy ends
  `failed`; it has always ended `skipped`, and the docs now say so.)

### Breaking

- Library users: `JobStore::set_workflow_status` takes an `error` argument;
  `JobStore` gains `create_step_jobs`, `create_child_workflow`,
  `step_job_outcomes`, `cancel_step_jobs`, `reset_workflow_for_retry`,
  `expired_workflows`, `stalled_child_workflows` and (with a default)
  `await_workflow_change`; `StepRecord`, `Workflow`, `WorkflowStep` and
  `CreateWorkflowRequest` gained fields. `JanitorSweepReport` gained
  `timed_out_workflows` and `settled_children`.

## [0.5.0] - 2026-10-11

### Added

- **Queue registry and per-tenant pause.** The engine records every
  (tenant, queue) it enqueues to in `queueflow.queues`, so
  `GET /api/v1/queues` now lists a queue that is currently empty, each
  entry carrying a `paused` flag. `POST /api/v1/queues/{queue}/pause` stops
  claims of the caller's jobs on that queue (pending jobs wait, running
  jobs finish; the response reports `running`, so a queue is drained once
  it reaches zero) and `.../resume` restarts them and wakes parked workers.
  Pausing is per tenant: other tenants' jobs on the same queue are
  unaffected. Metrics gain `queueflow_queue_paused{queue}`.
- **Bulk operations.** `POST /api/v1/jobs/cancel` cancels every
  non-terminal job matching a filter (queue, task, non-terminal status,
  creation window; oldest first, up to `limit`, capped by `--max-batch`)
  in one statement on Postgres. `POST /api/v1/dlq/replay` replays every
  unreplayed dead letter matching a filter and reports the outcome per
  entry.
- **`GET /api/v1/whoami`** returns the tenant and access level
  (`read_write` or `read_only`) of the bearer token.
- **Filters.** `GET /api/v1/jobs?task_name=` and, on `GET /api/v1/dlq`,
  `task_name=` and `replayed=true|false`.
- **Dashboard actions.** With a read-write token the dashboard offers
  cancel (job, workflow, and every job matching the list's filter), replay
  (dead letters), and pause/resume (queues and cron schedules), each
  behind a confirmation. Read-only tokens see the same pages without the
  buttons; the header shows the tenant and access level. The jobs list
  gains a task filter.
- **Batch claiming.** The in-process worker pool is now one dispatcher per
  queue that claims for every idle worker in a single query, instead of
  one claim query per worker. `--claim-batch` (`QUEUEFLOW_CLAIM_BATCH`)
  caps how many jobs one claim takes; `0`, the default, means up to
  `--workers`. Library users: `EngineBuilder::claim_batch`.
- Library users: `JobStore` gains `cancel_jobs_matching`,
  `register_queue`, `set_queue_paused` and `list_registered_queues`;
  `ListFilter` gains `task_name` and `replayed`; `QueueStats` gains
  `paused`; `Engine` gains `pause_queue`, `resume_queue`, `cancel_jobs`
  and `replay_dead_letters`; `JobApi` mirrors them.

### Changed

- **Migration 0006** adds the queue registry, `CHECK` constraints on every
  status column (and `on_failure`), per-tenant listing indexes
  (`idx_jobs_tenant_created`, `idx_workflows_tenant_created`, replacing
  the bare tenant indexes), expression indexes for the retention purge
  (`idx_jobs_purge`, `idx_workflows_purge`), and drops the redundant
  `idx_jobs_status` and `idx_jobs_queue`. The index builds are plain
  (transactional) and briefly block writes on large tables; run
  `queueflow migrate` from one place before rolling the fleet, or build
  the new indexes by hand with `CONCURRENTLY` first (the migration's
  `IF NOT EXISTS` statements then no-op). The file explains why
  `CONCURRENTLY` cannot be used from auto-migrate.
- utoipa 6 generates the OpenAPI document. The only change to the
  published spec is the member order inside nullable `oneOf` schemas,
  which generators treat identically.

## [0.4.0] - 2026-10-11

### Added

- **Read-only tenant credentials.** `--api-keys` accepts `token:tenant:ro`
  entries and JWTs may carry a `scope` claim (`"read"` makes the token
  read-only). Read-only credentials pass every `GET` route, the SSE streams,
  and the dashboard, and get `403` on writes, enforced in the auth
  middleware before any handler runs. The request extensions now carry an
  `auth::Access` value next to `Tenant`.

### Changed

- Dependency updates (grouped minor and patch bumps, tower-http 0.7) and
  GitHub Actions on the Node 24 runtime.
- The dashboard's top navigation never shows a scrollbar.

### Breaking

- Library users: `AuthConfig::api_keys` is `Vec<auth::ApiKey>` instead of
  `Vec<(String, String)>`; `parse_api_keys` returns the same. Use
  `ApiKey::new(token, tenant)` or `ApiKey::read_only(token, tenant)`.

## [0.3.0] - 2026-10-10

### Added

- A read-only web dashboard served by the API at `/ui/` (embedded in the
  binary, no build step): per-queue backlog with auto-refresh, jobs with
  filters, keyset paging and live SSE updates, workflows with the dependency
  graph coloured by step state, dead letters, cron schedules, and registered
  tasks. It authenticates with a tenant token kept in session storage. The
  graph is drawn with Mermaid loaded from `cdn.jsdelivr.net` on demand, with
  the Mermaid source shown when the CDN is unreachable.
- `GET /api/v1/queues`: live per-queue backlog for the caller's tenant
  (claimable `pending`, future `scheduled`, `running`, and the age of the
  oldest claimable job). `JobStore::queue_stats` / `JobApi::queue_stats`.
- In-process workers can drain several queues: `--queues a,b,c`
  (`QUEUEFLOW_QUEUES`), each with `--workers` workers.
- Tuning flags for values that were hard-coded: `--lease-secs`,
  `--lease-grace-secs`, `--worker-poll-secs`, `--janitor-interval-secs`,
  `--janitor-batch`, `--reclaim-lease-secs`, `--retention-interval-secs`,
  `--max-batch` (and matching `QUEUEFLOW_*` env vars). Library users:
  `EngineBuilder::worker_poll_secs`, `PostgresJobStore::with_lease_grace_secs`,
  `InMemoryJobStore::with_lease_grace_secs`, `ApiState::with_max_batch`.
- Operator metrics on the Prometheus port: per-queue gauges read from the
  store on each scrape (`queueflow_queue_pending_jobs`,
  `queueflow_queue_scheduled_jobs`, `queueflow_queue_running_jobs`,
  `queueflow_queue_oldest_pending_age_seconds`, plus `queueflow_store_up`),
  and process-local histograms `queueflow_handler_duration_seconds` and
  `queueflow_job_queue_wait_seconds`. `JobApi::latency`,
  `EngineStats::latency`.
- Release binaries for x86_64 macOS, x86_64 Linux (musl, static), aarch64
  Linux, and x86_64 Windows, alongside the existing targets.

### Changed

- The multi-arch container image is built on native amd64 and arm64 runners
  and merged into one manifest, instead of emulating arm64 under QEMU.

## [0.2.0] - 2026-10-09

### Breaking

- **The API fails closed.** `queueflow serve` in `api` or `all` mode now
  refuses to start unless tenant credentials (`--jwt-secret` and/or
  `--api-keys`) and a `--worker-token` are configured. The previous
  development placeholder (any non-empty token authenticates as tenant
  `tenant1`; worker endpoints open to any authenticated caller) is still
  available, but only when requested explicitly with `--dev`
  (`QUEUEFLOW_DEV=1`). Library users: `AuthConfig::default()` now rejects
  every request; use `AuthConfig::development()` for the old behaviour.
- **`GET /api/v1/stats` is tenant-scoped.** It now returns durable counts for
  the caller's tenant read from the store (jobs in the store, completed,
  failed, retries, dead letters, and the same for workflows) instead of the
  process-local engine counters, which leaked cross-tenant activity and reset
  on restart. The response shape is unchanged. The process counters remain
  available on the Prometheus metrics port.
- `JobStore` gained a required method, `count_stats`, and `JobApi` gained
  `tenant_stats`. Custom store implementations must implement it.

### Added

- `--dev` / `QUEUEFLOW_DEV` flag for local development without credentials.
- `--auto-migrate false` (and `QUEUEFLOW_AUTO_MIGRATE=false`) can now turn
  startup migrations off; previously the flag could only be set, not unset.
- `Engine::tenant_stats` and `JobStore::count_stats`.
- Benchmark harness (`scripts/bench.sh`) and `BENCHMARKS.md`.
- `SECURITY.md`, this changelog, and Dependabot configuration.

### Changed

- The OpenAPI document's `info.version` now follows the crate version instead
  of a hard-coded `1.0.0`.
- `make docker` and the README now use the `ghcr.io/elision-labs/queueflow`
  image name that the release workflow actually publishes.
- Python SDK generator templates ship a worker runtime; the Go SDK is
  generated with its real module path.
- Dependency updates for RUSTSEC-2026-0190 (anyhow), RUSTSEC-2026-0221
  (event-listener), RUSTSEC-2026-0258 (h2), and RUSTSEC-2026-0285 (rustls).
  RUSTSEC-2023-0071 (`rsa`, via jsonwebtoken) is documented as not applicable
  in `.cargo/audit.toml`.

## [0.1.0] - 2026-10-02

Initial public release: PostgreSQL-native job queue with `FOR UPDATE SKIP
LOCKED` claims and lease tokens, durable retries with typed backoff, a
dead-letter queue with replay, cron schedules, DAG workflows with per-step
failure policies, multi-tenant bearer authentication (HS256 JWT and static API
keys) with a separate worker credential, idempotent enqueue, an HTTP worker
protocol for handlers in any language, Prometheus metrics, health and
readiness probes, graceful shutdown, and a code-generated OpenAPI 3.1 spec.

[Unreleased]: https://github.com/elision-labs/queueflow-core/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/elision-labs/queueflow-core/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/elision-labs/queueflow-core/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/elision-labs/queueflow-core/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/elision-labs/queueflow-core/releases/tag/v0.1.0
