# Publishing QueueFlow

Runbook for moving the repo to its org and shipping the crates to crates.io.
Order matters: transfer the repository first, so `Cargo.toml` metadata,
badges, and crates.io Trusted Publishing all bind to the final home.

The publishable crates, in dependency order:

| crates.io name | path | contents |
| --- | --- | --- |
| `queueflow-core` | `crates/queueflow-core` | engine, adapters, embedded migrations |
| `queueflow-api` | `crates/queueflow-api` | axum router + OpenAPI |
| `queueflow-client` | `crates/queueflow-client` | Rust client + remote worker runtime |
| `queueflow` | `crates/queueflow` | the server/CLI binary (`cargo install queueflow`) |

Crate names are permanent on crates.io (they cannot be renamed or freed), so
settle any naming debate before step 4.

## 1. Move the repository to the org

1. Create the GitHub org (or pick the existing one). You need admin on it.
2. GitHub -> repo Settings -> Danger Zone -> Transfer ownership -> the org.
   Stars, issues, releases, and redirects from the old URL are preserved.
   Transfer the SDK repos too (`queueflow-sdk-nodejs`, `-python`, `-go`) so
   CI's cross-repo checkouts (`ts-sdk-drift`) keep working; they resolve via
   `github.repository_owner`, so no workflow edits are needed.
3. Update your local remote: `git remote set-url origin git@github.com:ORG/queueflow-core.git`

## 2. Update repo metadata to the new home

- `Cargo.toml` (workspace): `repository`, and `homepage` if queueflow.dev is
  not (or not yet) yours.
- `README.md`: the CI badge URL, the clone URL under Contributing, and the
  star-history chart repo.
- Per-crate `README.md` files: the repository links in the first paragraph.
- `scripts/publish-crates.sh`: the User-Agent comment URL.

`grep -rn "sjriddle" --include="*.md" --include="*.toml" --include="*.sh"`
finds every remaining reference.

## 3. Verify locally

```bash
make test clippy fmt-check    # all green
make spec && git diff --exit-code -- spec/   # spec in sync
make check-package            # the packaged core crate builds standalone with --features postgres
cargo publish -p queueflow-core --dry-run    # metadata sanity
```

Push a branch and let the full CI matrix pass on the org repo (it was
re-enabled together with this file; the pg job needs nothing but the workflow
itself).

## 4. First publish (0.1.0)

1. Create a crates.io account (log in with the GitHub account that owns the
   org), verify the email address.
2. Create a scoped API token: crates.io -> Account Settings -> API Tokens ->
   New Token, scope `publish-new` + `publish-update`.
3. Either publish locally:

   ```bash
   cargo login            # paste the token
   ./scripts/publish-crates.sh
   ```

   or add the token as the `CARGO_REGISTRY_TOKEN` repository secret and push
   the tag; the release workflow publishes in order, then builds binaries,
   the spec artifact, and the container image:

   ```bash
   git tag v0.1.0 && git push origin v0.1.0
   ```

   The script is idempotent: already-published versions are skipped, so a
   half-finished release can be re-run.

## 5. Ownership and hardening (right after the first publish)

1. Add the org team as owners so publishing is not tied to one person:

   ```bash
   for c in queueflow-core queueflow-api queueflow-client queueflow; do
     cargo owner --add github:ORG:TEAM "$c"
   done
   ```

2. Switch CI to crates.io **Trusted Publishing** (GitHub OIDC, no long-lived
   token): each crate's crates.io Settings -> Trusted Publishing -> add the
   org repo + `release.yml` workflow. Then, in `release.yml`, mint the token
   with `rust-lang/crates-io-auth-action` instead of reading
   `CARGO_REGISTRY_TOKEN`, and delete the repository secret.

## 6. Releasing after 0.1.0

1. Bump `workspace.package.version` and the three internal dependency
   versions in the root `Cargo.toml` (they move in lockstep).
2. Update CHANGELOG/README as needed; `make spec` if the API changed.
3. Tag `vX.Y.Z` and push the tag.
