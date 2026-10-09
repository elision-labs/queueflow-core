# Changelog

All notable changes to the QueueFlow server and its Rust crates
(`queueflow-core`, `queueflow-api`, `queueflow-client`, `queueflow`). The
language SDKs keep their own changelogs in their repositories.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/); before 1.0, minor
versions may contain breaking changes, which are called out explicitly.

## [Unreleased]

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
