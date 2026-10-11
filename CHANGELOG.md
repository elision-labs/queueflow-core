# Changelog

All notable changes to the QueueFlow server and its Rust crates
(`queueflow-core`, `queueflow-api`, `queueflow-client`, `queueflow`). The
language SDKs keep their own changelogs in their repositories.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/); before 1.0, minor
versions may contain breaking changes, which are called out explicitly.

## [Unreleased]

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

[Unreleased]: https://github.com/elision-labs/queueflow-core/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/elision-labs/queueflow-core/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/elision-labs/queueflow-core/releases/tag/v0.1.0
