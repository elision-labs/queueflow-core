# Security policy

## Supported versions

QueueFlow is pre-1.0. Security fixes land on `main` and ship in the next
release; only the latest minor release line receives fixes.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Report it
privately through GitHub's security advisory form for this repository
(Security tab, "Report a vulnerability"), or email security@queueflow.dev.

Include the version (`queueflow --version` or the image tag), a description of
the issue, and reproduction steps if you have them. You should receive an
acknowledgement within three working days. We will coordinate a fix and a
disclosure date with you and credit you in the release notes unless you prefer
otherwise.

## Deployment guidance

- Never run a reachable server with `--dev` (`QUEUEFLOW_DEV=1`). Without it
  the server refuses to start unless tenant credentials (`--jwt-secret` and/or
  `--api-keys`) and a `--worker-token` are configured.
- Workers execute every tenant's jobs and see their payloads. Give them the
  worker token only, never a tenant credential, and keep the worker token out
  of client code.
- Restrict CORS with `--cors-origins` before exposing the API to browsers.
- Put TLS termination (a reverse proxy or your platform's edge) in front of
  the API; the server itself speaks plain HTTP.

## Dependency advisories

CI runs `cargo audit` weekly and on every dependency change. Advisories that
are confirmed not to affect QueueFlow are listed with a justification in
`.cargo/audit.toml`.
