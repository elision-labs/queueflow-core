# queueflow Helm chart

Deploys [QueueFlow](https://queueflow.dev), a PostgreSQL-native job queue and
workflow engine, from the image `ghcr.io/elision-labs/queueflow`. The chart
does not run PostgreSQL; bring any plain PostgreSQL 13+ (RDS, Cloud SQL,
CloudNativePG, Neon, ...).

Chart version 0.1.0, appVersion 0.2.0. Requires Kubernetes 1.25+.

## Install

```bash
# 1. the database connection string, as a Secret
kubectl create namespace queueflow
kubectl -n queueflow create secret generic queueflow-db \
  --from-literal=DATABASE_URL='postgres://queueflow:secret@postgres.example.internal:5432/queueflow'

# 2. install; credentials are rendered into a Secret by the chart
helm install queueflow deploy/helm/queueflow -n queueflow \
  --set database.existingSecret=queueflow-db \
  --set auth.apiKeys="$(openssl rand -hex 24):acme" \
  --set auth.workerToken="$(openssl rand -hex 24)"

# 3. check
kubectl -n queueflow rollout status deploy/queueflow
kubectl -n queueflow port-forward svc/queueflow 8000:8000 &
curl -s localhost:8000/health && curl -s localhost:8000/ready
```

The server refuses to start in `api`/`all` mode without a tenant credential
source (`auth.apiKeys` and/or `auth.jwtSecret`) and `auth.workerToken`; the
chart checks the same conditions at render time so `helm install` fails with a
readable message instead of a crash loop.

Prefer to manage credentials yourself (sealed-secrets, external-secrets,
Vault)? Point the chart at a Secret:

```bash
kubectl -n queueflow create secret generic queueflow-auth \
  --from-literal=QUEUEFLOW_API_KEYS='k1:acme' \
  --from-literal=QUEUEFLOW_WORKER_TOKEN='...'          # QUEUEFLOW_JWT_SECRET optional
helm install queueflow deploy/helm/queueflow -n queueflow \
  --set database.existingSecret=queueflow-db \
  --set auth.existingSecret=queueflow-auth
```

Key names inside that Secret are configurable under `auth.existingSecretKeys`.

## Topologies

`mode: all` (default) runs one Deployment of `serve --mode all`: API,
in-process workers, janitor and cron in each pod. Scale with `replicaCount`;
many replicas sharing one database is safe (claims use
`FOR UPDATE SKIP LOCKED`, cron firings are deduplicated, the janitor is
idempotent).

`mode: split` renders two Deployments:

- `<release>-api` (`serve --mode api`), behind the Service, Ingress, HPA and
  PDB; `api.replicaCount`, `api.resources`.
- `<release>-worker` (`serve --mode worker`), no HTTP API, so no credentials
  are injected; `worker.replicaCount`, `worker.workers`, `worker.resources`.
  Liveness uses the metrics server's `/health` on port 9090. A
  `<release>-worker` Service exposes only its metrics port so the
  ServiceMonitor can scrape it.

```bash
helm upgrade --install queueflow deploy/helm/queueflow -n queueflow \
  -f my-values.yaml --set mode=split --set api.replicaCount=3 --set worker.replicaCount=2
```

Remote workers (other languages, via the SDKs) talk to the Service on port
8000 with the worker token and need nothing from this chart.

## Migrations

By default the server applies schema migrations at startup (idempotent), so a
rolling upgrade is enough. To separate schema changes from deploys:

```yaml
migration:
  enabled: true
```

renders a `pre-install,pre-upgrade` hook Job running `queueflow migrate`
against `database.existingSecret`, and starts every server container with
`--auto-migrate false`. A failed migration fails the release before any pod
is replaced.

## Values

Only the ones you are likely to touch; see `values.yaml` for all of them with
comments.

| Key | Default | Description |
| --- | --- | --- |
| `image.repository` / `image.tag` | `ghcr.io/elision-labs/queueflow` / appVersion | Published tags: `0.2.0`, `0.2`, `latest` (amd64 + arm64). |
| `mode` | `all` | `all` or `split`. |
| `replicaCount` | `1` | Replicas for `mode=all`. |
| `api.replicaCount`, `worker.replicaCount` | `2`, `1` | Replicas for `mode=split`. |
| `database.existingSecret` | required | Secret holding the connection string. |
| `database.existingSecretKey` | `DATABASE_URL` | Key within it. |
| `auth.apiKeys` | `""` | `token:tenant[,token2:tenant2]`. |
| `auth.jwtSecret` | `""` | HS256 secret for tenant JWTs (`sub` = tenant). |
| `auth.workerToken` | `""` | Worker-protocol credential. |
| `auth.existingSecret` | `""` | Use this Secret instead of rendering one. |
| `auth.dev` | `false` | `--dev`: no credentials, tenant `tenant1`. Never on a reachable cluster. |
| `config.workers` / `worker.workers` | `10` | In-process workers per queue. |
| `config.corsOrigins` | `""` | Comma-separated browser origins; unset is permissive (server warns). |
| `config.maxDbConnections` | `50` | Pool size per pod. Budget pods x pool against Postgres `max_connections`. |
| `config.retentionHours` | `""` | Delete terminal history older than N hours; unset keeps forever. |
| `config.extraArgs` / `extraEnv` / `extraEnvFrom` | `[]` | Pass any other `serve` flag or env var. |
| `migration.enabled` | `false` | Hook Job + `--auto-migrate false`. |
| `service.type` / `service.port` / `service.metricsPort` | `ClusterIP` / `8000` / `9090` | |
| `ingress.enabled` | `false` | Routes only the API port. |
| `autoscaling.enabled` | `false` | HPA (autoscaling/v2) on the API Deployment. |
| `podDisruptionBudget.enabled` | `false` | PDB on the API Deployment. |
| `serviceMonitor.enabled` | `false` | Prometheus Operator ServiceMonitor on port `metrics`. |
| `resources`, `api.resources`, `worker.resources` | `{}` | Set requests/limits for real deployments. |
| `podSecurityContext` / `securityContext` | non-root uid/gid 10001, read-only root FS, no capabilities | Matches the image's user. |
| `terminationGracePeriodSeconds` | `60` | Must exceed your longest handler; the binary drains for up to 30s. |

## Probes and metrics

Liveness `GET /health` (200 when the process can reach Postgres) and
readiness `GET /ready` (200 when the engine accepts work and Postgres
answers), both on port 8000, unauthenticated. Prometheus metrics on
`:9090/metrics`, unauthenticated: keep that port off any Ingress.

## Upgrading

```bash
helm upgrade queueflow deploy/helm/queueflow -n queueflow --reuse-values --set image.tag=0.2.1
```

Within a minor version, API and worker pods of different patch versions can
share one database during the rollout.

## Development

```bash
helm lint deploy/helm/queueflow -f deploy/helm/queueflow/ci/lint-values.yaml
helm template qf deploy/helm/queueflow -f deploy/helm/queueflow/ci/split-values.yaml
```

`ci/` holds values files that exercise both topologies and every optional
resource; the `deploy-assets` GitHub workflow runs them.
