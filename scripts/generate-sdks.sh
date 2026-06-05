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
# Usage:
#   scripts/generate-sdks.sh            # all SDKs
#   scripts/generate-sdks.sh python     # one
#   scripts/generate-sdks.sh python typescript rust

set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
SPEC="${ROOT}/spec/openapi.json"
IMAGE="openapitools/openapi-generator-cli:v7.10.0"

# language -> sibling output directory (relative to the repo root's parent).
declare -A OUT_DIRS=(
  [python]="${ROOT}/../queueflow-sdk-python"
  [typescript]="${ROOT}/../queueflow-sdk-nodejs"
  [rust]="${ROOT}/../queueflow-sdk-rust"
  [go]="${ROOT}/../queueflow-sdk-go"
  [java]="${ROOT}/../queueflow-sdk-java"
)

if [[ ! -f "${SPEC}" ]]; then
  echo "spec not found: ${SPEC}" >&2
  echo "Run 'make spec' first." >&2
  exit 1
fi

generate() {
  local name="$1"
  local out="${OUT_DIRS[$name]:-}"
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
  targets=(python typescript rust go java)
fi

for t in "${targets[@]}"; do
  generate "${t}"
done

echo "==> Done."
