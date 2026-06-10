#!/usr/bin/env bash
#
# Regenerate the QueueFlow client SDKs from the OpenAPI spec.
#
# The spec is generated FROM the Rust code (`make spec`), so it can never drift
# from the running server. This script then drives openapi-generator to produce
# each language SDK. Generated files are canonical output — do not edit by hand.
#
# Requires: docker, and a generated spec at spec/openapi.json (run `make spec`).
#
# NOTE: Two SDKs are NOT generated here:
#   - TypeScript (../queueflow-sdk-nodejs) is HAND-WRITTEN for a nicer developer
#     experience; `scripts/check-ts-sdk.mjs` guards it against spec drift.
#   - Rust ships as the native `queueflow-client` crate in this workspace,
#     which reuses the engine's own domain types (no generation possible to
#     beat that).
# Both are no-ops if requested explicitly.
#
# Python and Go remain available as on-demand targets, but are NOT published or
# maintained by default: each release attaches spec/openapi.json, and consumers
# can self-generate against their own toolchain with:
#   openapi-generator-cli generate -i openapi.json -g python -o ./queueflow-python
#
# Usage:
#   scripts/generate-sdks.sh            # all generated SDKs (python, go)
#   scripts/generate-sdks.sh python     # one

set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
SPEC="${ROOT}/spec/openapi.json"
IMAGE="openapitools/openapi-generator-cli:v7.10.0"

# language -> sibling output directory (relative to the repo root's parent).
# A function (not an associative array) so this runs on the bash 3.2 that ships
# with macOS as well as bash 4+.
out_dir_for() {
  case "$1" in
    python)     echo "${ROOT}/../queueflow-sdk-python" ;;
    typescript) echo "${ROOT}/../queueflow-sdk-nodejs" ;;
    go)         echo "${ROOT}/../queueflow-sdk-go" ;;
    *)          echo "" ;;
  esac
}

if [[ ! -f "${SPEC}" ]]; then
  echo "spec not found: ${SPEC}" >&2
  echo "Run 'make spec' first." >&2
  exit 1
fi

generate() {
  local name="$1"

  # The TypeScript SDK is hand-written; never regenerate (and never wipe) it.
  if [[ "${name}" == "typescript" ]]; then
    echo "==> Skipping typescript: ../queueflow-sdk-nodejs is hand-written, not generated."
    return 0
  fi
  # The Rust SDK is the native queueflow-client crate in this workspace.
  if [[ "${name}" == "rust" ]]; then
    echo "==> Skipping rust: use the native crates/queueflow-client instead of a generated client."
    return 0
  fi

  local out
  out="$(out_dir_for "${name}")"
  local config="${ROOT}/sdk-configs/${name}.yaml"

  if [[ -z "${out}" ]]; then
    echo "unknown SDK target: ${name}" >&2
    exit 2
  fi
  if [[ ! -f "${config}" ]]; then
    echo "missing config: ${config}" >&2
    exit 2
  fi
  if [[ ! -d "${out}" ]]; then
    echo "SDK output directory missing: ${out}" >&2
    echo "Create it (e.g. 'git init \"${out}\"') before regenerating." >&2
    exit 1
  fi

  # Wipe everything except .git so stale generated files do not linger.
  find "${out}" -mindepth 1 -maxdepth 1 -not -name '.git' -exec rm -rf {} +

  echo "==> Generating ${name} SDK into ${out}"
  # Mount the spec, config, and output dir individually so a generation can
  # never write outside its target.
  docker run --rm \
    -v "${SPEC}:/spec/openapi.json:ro" \
    -v "${config}:/config.yaml:ro" \
    -v "${out}:/out" \
    "${IMAGE}" generate \
      -i /spec/openapi.json \
      -c /config.yaml \
      -o /out
}

targets=("$@")
if [[ ${#targets[@]} -eq 0 || "${targets[0]}" == "all" ]]; then
  # typescript (hand-written) and rust (native crate) intentionally omitted.
  targets=(python go)
fi

for t in "${targets[@]}"; do
  generate "${t}"
done

echo "==> Done."
