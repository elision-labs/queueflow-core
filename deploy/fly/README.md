# QueueFlow on Fly.io

Deploys the prebuilt image `ghcr.io/elision-labs/queueflow:0.2` as one Fly app
in `--mode all` (API + in-process workers + janitor + cron) with Fly Managed
Postgres. No Dockerfile or build step.

References used for this config:

- fly.toml reference: <https://docs.fly.io/reference/configuration>
- Secrets: <https://docs.fly.io/apps/secrets/>
- Process groups: <https://docs.fly.io/launch/processes/>
- Managed Postgres CLI: <https://docs.fly.io/flyctl/mpg-create/>, <https://docs.fly.io/flyctl/mpg-attach/>

## 1. Create the app

Edit `app` (and `primary_region`) in `fly.toml`, then from this directory:

```bash
fly launch --copy-config --no-deploy      # creates the app from fly.toml, deploys nothing yet
```

(`fly apps create <name>` works too if you prefer not to use `launch`.)

## 2. Database

Any PostgreSQL 13+ works. With Fly Managed Postgres:

```bash
fly mpg create --name queueflow-db --region ord --pg-major-version 16
fly mpg attach <CLUSTER ID> --app <your-app>
```

`fly mpg attach` stores the connection string as the app secret `DATABASE_URL`
(the name QueueFlow reads; change it only with `--variable-name`). For an
external database, set the secret yourself in the next step.

## 3. Secrets

The server refuses to start in `all`/`api` mode without a tenant credential
source and a worker token. Set them as secrets (they become environment
variables at runtime and are never in `fly.toml`):

```bash
fly secrets set \
  QUEUEFLOW_API_KEYS="$(openssl rand -hex 24):acme" \
  QUEUEFLOW_WORKER_TOKEN="$(openssl rand -hex 24)" \
  --app <your-app> --stage
# external Postgres only (skip if you ran `fly mpg attach`):
fly secrets set DATABASE_URL="postgres://user:pass@host:5432/db" --app <your-app> --stage
```

`--stage` defers the restart until the deploy below. Optional secrets:
`QUEUEFLOW_JWT_SECRET` (HS256 tenant JWTs; may be combined with API keys).
Non-secret settings (`QUEUEFLOW_WORKERS`, `QUEUEFLOW_CORS_ORIGINS`,
`QUEUEFLOW_RETENTION_HOURS`, `RUST_LOG`) live in `[env]`.

## 4. Deploy

```bash
fly deploy
fly status
curl -s https://<your-app>.fly.dev/health
curl -s https://<your-app>.fly.dev/ready
```

What the config does:

- `[deploy] release_command = "migrate"` runs `queueflow migrate` in a
  throwaway Machine before each rollout (release commands replace CMD but keep
  the image ENTRYPOINT). The server process therefore runs with
  `--auto-migrate false`. To let the server migrate itself instead, delete the
  `release_command` line and the `--auto-migrate false` argument.
- `auto_stop_machines = "off"` and `min_machines_running = 1`: a queue
  server must stay up to run workers, cron and the janitor.
- Health checks: `/ready` every 15s and `/health` every 30s on port 8000.
  Neither needs a token.
- `[metrics]` lets Fly's built-in Prometheus scrape `:9090/metrics`. Port 9090
  is not published publicly.
- `kill_timeout = 60` gives in-flight handlers time to drain after SIGTERM.

## Scaling

```bash
fly scale count 2                  # more `all` replicas; safe, they share the database
fly scale vm shared-cpu-2x --memory 1024
```

Running several `all` Machines is fine: claims use `FOR UPDATE SKIP LOCKED`,
cron firings are deduplicated, and the janitor is idempotent.

### Split API and workers

To scale HTTP and job execution independently, replace `[processes]` with two
groups and point the HTTP service at the API one:

```toml
[processes]
  api = "serve --mode api --auto-migrate false"
  worker = "serve --mode worker --auto-migrate false"

[http_service]
  internal_port = 8000
  processes = ["api"]
  # ... checks as before

[[vm]]
  processes = ["api", "worker"]
```

```bash
fly scale count api=2 worker=1
```

The `worker` group never serves HTTP, so it does not need the API-key or
worker-token secrets (they are harmless if present).

## Remote workers

Workers in other languages run anywhere and reach the API over the public
hostname or the private network (`http://<your-app>.internal:8000`) using
`QUEUEFLOW_WORKER_TOKEN`. See the SDK docs at <https://docs.queueflow.dev>.

## Rotating credentials

```bash
fly secrets set QUEUEFLOW_WORKER_TOKEN="$(openssl rand -hex 24)"   # restarts the app
fly secrets list
```

Multiple API keys can be active at once (`k1:acme,k2:acme`), so rotate a tenant
key by adding the new one, moving clients over, then removing the old one.
