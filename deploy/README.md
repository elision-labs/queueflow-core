# Deploying QueueFlow

QueueFlow is one stateless binary (`queueflow`) plus PostgreSQL 13+. Every
option below runs the same container image,
`ghcr.io/elision-labs/queueflow` (tags `0.2.0`, `0.2`, `latest`; linux/amd64
and linux/arm64), or the same release binary. Pick the one that matches where
you run things.

| Target | Files | Notes |
| --- | --- | --- |
| Docker Compose | [`docker-compose.yml`](../docker-compose.yml), [`.env.example`](../.env.example) | Postgres 16 + QueueFlow in `--mode all`. Credentials come from `.env`, never from the compose file. |
| Kubernetes (Helm) | [`helm/queueflow/`](helm/queueflow/) | Single `all` Deployment or split api/worker; Secret or `existingSecret`; HPA, PDB, Ingress, ServiceMonitor, migration hook Job. |
| Fly.io | [`fly/`](fly/) | `fly.toml` on the GHCR image, Managed Postgres, release-command migrations, health checks. |
| Render | [`render.yaml`](../render.yaml), [`render/`](render/) | Blueprint: Render Postgres + image-based web service, secrets via `sync: false` / `generateValue`. |
| Railway | [`railway/`](railway/) | Manual steps: Docker image service, `${{Postgres.DATABASE_URL}}`, health check, domain. |
| Homebrew | [`homebrew/`](homebrew/) | Formula for the `elision-labs/homebrew-tap` tap installing the prebuilt release tarballs, plus the per-release update script. |

Everything the server reads is a flag or environment variable; the full
table is in the repository [README](../README.md#configuration). The parts
that matter for every deployment:

- **`DATABASE_URL`** (required): any plain PostgreSQL 13+, no extensions.
- **Credentials** (required in `api`/`all` mode, or the server refuses to
  start): `QUEUEFLOW_API_KEYS` (`token:tenant,...`) and/or
  `QUEUEFLOW_JWT_SECRET`, plus `QUEUEFLOW_WORKER_TOKEN`. `QUEUEFLOW_DEV=1`
  bypasses this for local development only.
- **Ports**: 8000 REST API (`/health`, `/ready`, `/docs`), 9090 Prometheus
  `/metrics` (unauthenticated; never expose publicly).
- **Probes**: liveness `GET /health`, readiness `GET /ready`, no token.
- **Migrations**: applied at startup by default; set
  `QUEUEFLOW_AUTO_MIGRATE=false` and run `queueflow migrate` separately to
  decouple them (the Helm chart and Fly config show how).
- **Shutdown**: SIGTERM drains in-flight handlers (bounded to 30s); give the
  process a grace period longer than your longest handler.
- **Image user**: uid 10001, non-root, nothing written to disk.

Operational guidance (topologies, Postgres sizing, metrics, security
checklist) lives in the docs: <https://docs.queueflow.dev/deployment>.

## CI

[`.github/workflows/deploy-assets.yml`](../.github/workflows/deploy-assets.yml)
runs on changes to these files: `helm lint` + `helm template` for both chart
topologies, `docker compose config` with placeholder credentials, YAML
validation of `render.yaml`, and `ruby -c` on the Homebrew formula.

## Grafana

[`grafana/`](./grafana/) holds an importable overview dashboard for the
Prometheus metrics (per-queue backlog gauges, outcome rates, handler and
queue-wait latency percentiles) and suggested alert expressions.
