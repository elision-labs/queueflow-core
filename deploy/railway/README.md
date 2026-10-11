# QueueFlow on Railway

Manual steps to run the prebuilt image `ghcr.io/elision-labs/queueflow:0.2`
next to a Railway Postgres in one project. Nothing here creates resources for
you; it is a checklist for the Dashboard (or the `railway` CLI).

References used:

- Services from a Docker image: <https://docs.railway.com/guides/services>
- PostgreSQL: <https://docs.railway.com/guides/postgresql>
- Reference variables: <https://docs.railway.com/reference/variables>
- Health checks: <https://docs.railway.com/reference/healthchecks>
- Public networking and target ports: <https://docs.railway.com/networking/domains/working-with-domains>

## 1. Project and database

1. Create a project (or open one).
2. **+ New** (or `Cmd/Ctrl+K`) -> **Database** -> **PostgreSQL**. Keep the
   service name `Postgres`; the reference variable below uses it.
   Railway exposes `DATABASE_URL`, `DATABASE_PUBLIC_URL` (only once public
   access is enabled), `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`
   on that service.

## 2. QueueFlow service

1. **+ New** -> **Docker Image**, enter `ghcr.io/elision-labs/queueflow:0.2`
   (pin `0.2.0` for an exact version). The image is public; no registry
   credentials are needed.
2. Rename the service to `queueflow` (or `engine`).
3. Leave the start command empty: the image's default command is `serve`, and
   everything below is configured through environment variables. If you want
   an explicit command, use `serve --mode all`.

## 3. Variables

Service -> **Variables** -> **Raw Editor**, paste:

```text
DATABASE_URL=${{Postgres.DATABASE_URL}}
QUEUEFLOW_MODE=all
QUEUEFLOW_API_PORT=8000
PORT=8000
QUEUEFLOW_METRICS_PORT=9090
QUEUEFLOW_WORKERS=10
RUST_LOG=info
QUEUEFLOW_API_KEYS=<token>:<tenant>
QUEUEFLOW_WORKER_TOKEN=<random>
```

- `${{Postgres.DATABASE_URL}}` is a reference variable: `Postgres` is the
  database service's name, so rename it here if you renamed the service. It
  resolves to the private-network connection string.
- `PORT=8000` matters twice: Railway probes the health check on the port in
  `PORT`, and uses it as the default target port for a public domain.
- Generate the two credentials locally, e.g.
  `openssl rand -hex 24`. The server refuses to start in `all`/`api` mode
  without both `QUEUEFLOW_API_KEYS` (or `QUEUEFLOW_JWT_SECRET`) and
  `QUEUEFLOW_WORKER_TOKEN`.
- Optional: `QUEUEFLOW_CORS_ORIGINS=https://app.example.com`,
  `QUEUEFLOW_RETENTION_HOURS=168`, `QUEUEFLOW_JWT_SECRET=...`.

Deploy the staged changes.

## 4. Health check

Service -> **Settings** -> **Deploy** -> **Healthcheck Path**: `/health`.
Railway waits for a 2xx on `PORT` before switching traffic to a new
deployment; the default timeout is 300s (`RAILWAY_HEALTHCHECK_TIMEOUT_SEC`
changes it). `/health` returns 200 only once Postgres is reachable, which also
guards against deploying ahead of the database.

## 5. Networking

- Public API: **Settings** -> **Networking** -> **Public Networking** ->
  **Generate Domain**. Railway detects the target port when the app listens on
  one port; if it offers a list, pick `8000`. The port can be changed later
  with the edit icon next to the domain. Do not expose 9090 publicly; metrics
  are unauthenticated.
- Private: other services in the project reach the API at
  `http://queueflow.railway.internal:8000` (replace `queueflow` with the
  service name). Remote workers deployed in the same project should use this
  address with `QUEUEFLOW_WORKER_TOKEN`.

```bash
curl -s https://<domain>/health
curl -s https://<domain>/ready
```

## Split API and workers

Deploy the image twice: `api` with `QUEUEFLOW_MODE=api` (public domain,
health check, both credentials) and `worker` with `QUEUEFLOW_MODE=worker`
(no domain, no health check path needed, only `DATABASE_URL` plus tuning such
as `QUEUEFLOW_WORKERS`). Scale replicas of each independently under
**Settings** -> **Deploy**.

## Migrations

By default the server applies migrations at startup (idempotent). To
decouple schema changes from deploys, set `QUEUEFLOW_AUTO_MIGRATE=false` on
the service(s) and run `queueflow migrate` with the same `DATABASE_URL`, for
example as a one-off service or from the CLI on a machine that can reach the
database (`DATABASE_PUBLIC_URL` after enabling public access on Postgres).

## Updating

Pinned tags are not auto-updated. Edit the image tag in the service settings
and deploy the staged change; within a minor version, old and new processes
can coexist against one database during the rollout.
