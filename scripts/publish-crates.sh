#!/usr/bin/env bash
# Publish the workspace crates to crates.io in dependency order:
#   queueflow-core -> queueflow-api -> queueflow-client -> queueflow
#
# Idempotent: a crate version that is already on crates.io is skipped, so a
# release interrupted halfway can simply be re-run.
#
# Auth: CARGO_REGISTRY_TOKEN in the environment (CI), a token minted by
# crates.io Trusted Publishing, or a prior `cargo login` (local).
set -euo pipefail
cd "$(dirname "$0")/.."

CRATES=(queueflow-core queueflow-api queueflow-client queueflow)

version() {
  # `cargo pkgid` prints `...#name@x.y.z` (or `...#x.y.z` when the package
  # name matches the directory); strip everything up to the last # or @.
  cargo pkgid -p "$1" | sed 's/.*[#@]//'
}

published() {
  local crate="$1" ver="$2"
  curl -fsSL --max-time 30 \
    -H "User-Agent: queueflow-release (github.com/elision-labs/queueflow-core)" \
    "https://crates.io/api/v1/crates/${crate}/${ver}" >/dev/null 2>&1
}

for crate in "${CRATES[@]}"; do
  ver="$(version "${crate}")"
  if published "${crate}" "${ver}"; then
    echo "==> ${crate} ${ver} is already on crates.io; skipping"
    continue
  fi
  echo "==> publishing ${crate} ${ver}"
  # cargo waits for index propagation after each publish, but a freshly
  # published dependency can still lag; retry a few times before giving up.
  ok=""
  for attempt in 1 2 3 4 5; do
    if cargo publish -p "${crate}"; then
      ok=1
      break
    fi
    echo "   publish failed (attempt ${attempt}); retrying in 20s" >&2
    sleep 20
  done
  [ -n "${ok}" ] || {
    echo "!! giving up on ${crate}" >&2
    exit 1
  }
done

echo "all crates published"
