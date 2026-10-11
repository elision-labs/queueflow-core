# Homebrew tap

`queueflow.rb` is a Homebrew formula that installs the prebuilt `queueflow`
binary from the GitHub release tarballs (no Rust toolchain needed). It is
meant to be published from the tap repository **`elision-labs/homebrew-tap`**
so users can run:

```bash
brew install elision-labs/tap/queueflow
# or
brew tap elision-labs/tap
brew install queueflow
```

Reference: <https://docs.brew.sh/How-to-Create-and-Maintain-a-Tap>. A GitHub
repository named `homebrew-<name>` is addressed as `user/<name>`, formulae go
in a `Formula/` directory, and Homebrew adds the tap automatically on
`brew install user/name/formula`.

## One-time setup (maintainer)

The tap repository does not exist yet. Create it manually:

1. Create the public GitHub repository `elision-labs/homebrew-tap` (empty, or
   `brew tap-new elision-labs/tap` locally and push the result; `tap-new`
   also drops in GitHub Actions workflows for bottling, which this formula
   does not need since it ships prebuilt binaries).
2. Copy `deploy/homebrew/queueflow.rb` to `Formula/queueflow.rb` in that
   repository and push.
3. Smoke test from a clean machine:

   ```bash
   brew install elision-labs/tap/queueflow
   queueflow --version
   brew test queueflow
   ```

   Homebrew asks third-party taps to be trusted explicitly on first use
   (`brew trust --tap elision-labs/tap`) on versions that enforce it.

## Platforms

| Platform | Release asset | Status |
| --- | --- | --- |
| macOS arm64 (Apple silicon) | `queueflow-aarch64-apple-darwin.tar.gz` | active |
| Linux x86_64 (glibc) | `queueflow-x86_64-unknown-linux-gnu.tar.gz` | active |
| macOS x86_64 (Intel) | `queueflow-x86_64-apple-darwin.tar.gz` | commented stanza; enabled automatically by `update-formula.sh` once the release pipeline publishes it |
| Linux arm64 (glibc) | `queueflow-aarch64-unknown-linux-gnu.tar.gz` | same |

Asset names follow `.github/workflows/release.yml`
(`queueflow-${{ matrix.target }}.tar.gz`, one `queueflow` binary inside). The
musl and Windows builds being added to that workflow are not consumed by this
formula (Homebrew on Linux targets glibc; Windows is out of scope).

## Per release

1. Wait for the `v*` tag's Release workflow to finish uploading assets.
2. In this repository:

   ```bash
   deploy/homebrew/update-formula.sh v0.2.1
   ```

   This downloads each platform tarball for the tag, rewrites `version` and
   every `sha256` in `queueflow.rb`, uncomments a platform stanza the first
   time its asset exists, and fails if an already-active platform has no
   asset on the release. `--check` makes it a dry run that exits 1 when the
   file is stale (useful in CI after a release).
3. Commit the updated `queueflow.rb` here, then copy it to
   `Formula/queueflow.rb` in `elision-labs/homebrew-tap` and push. Homebrew
   users pick it up on their next `brew update && brew upgrade queueflow`.

Checks to run on the formula before pushing to the tap:

```bash
ruby -c deploy/homebrew/queueflow.rb
brew style deploy/homebrew/queueflow.rb
brew install --formula ./deploy/homebrew/queueflow.rb && brew test queueflow
```

## Notes

- The formula pins `version` explicitly because the asset file names carry
  no version; the URLs interpolate `v#{version}`.
- `livecheck` uses the `github_latest` strategy against the release page, so
  `brew livecheck queueflow` reports when a newer tag exists.
- Users who prefer Cargo can `cargo install queueflow` (crates.io) instead;
  the Docker image is `ghcr.io/elision-labs/queueflow`.
