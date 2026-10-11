#!/usr/bin/env bash
# Rewrite deploy/homebrew/queueflow.rb for a release tag: set `version` and
# the sha256 of every platform tarball that exists on the GitHub release.
#
#   ./update-formula.sh v0.2.1              # updates queueflow.rb in place
#   ./update-formula.sh v0.2.1 --check      # exit 1 if the file would change
#
# Assets are named queueflow-<rust-target>.tar.gz by
# .github/workflows/release.yml. Targets whose asset is missing from the
# release are left as they are (active stanzas keep their old sha, so the
# script fails loudly in that case; commented stanzas stay commented).
# A commented stanza wrapped in "# BEGIN <target>" / "# END <target>" markers
# is uncommented the first time its asset appears.
set -euo pipefail

REPO="elision-labs/queueflow-core"
TARGETS=(
  aarch64-apple-darwin
  x86_64-apple-darwin
  x86_64-unknown-linux-gnu
  aarch64-unknown-linux-gnu
)

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
formula="$here/queueflow.rb"

tag="${1:-}"
check=0
[[ "${2:-}" == "--check" ]] && check=1
if [[ -z "$tag" ]]; then
  echo "usage: $0 vX.Y.Z [--check]" >&2
  exit 2
fi
version="${tag#v}"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

sha_for() {
  # Download an asset (untrusted data: hashed, never extracted or executed).
  local target="$1" url out
  url="https://github.com/$REPO/releases/download/$tag/queueflow-$target.tar.gz"
  out="$workdir/queueflow-$target.tar.gz"
  if curl -fsSL --retry 3 -o "$out" "$url" 2>/dev/null; then
    shasum -a 256 "$out" | awk '{print $1}'
  else
    return 1
  fi
}

new="$workdir/queueflow.rb"
cp "$formula" "$new"

# 1. version
sed -i.bak -E "s/^(  version \")[^\"]+(\")/\1$version\2/" "$new"

# 2. per-target sha256 (and uncomment the stanza if it was a placeholder)
updated=()
missing=()
for target in "${TARGETS[@]}"; do
  if sha="$(sha_for "$target")"; then
    # Uncomment "# BEGIN target" ... "# END target" blocks (strip the leading "# "
    # from the stanza lines, keep the marker and explanatory lines intact).
    awk -v t="$target" '
      $0 == "    # BEGIN " t { inblock = 1; print; next }
      $0 == "    # END " t   { inblock = 0; print; next }
      inblock && /^    # (on_|  url|  sha256|end)/ { sub(/^    # /, "    "); print; next }
      { print }
    ' "$new" > "$new.tmp" && mv "$new.tmp" "$new"
    # Set the sha256 on the line following this target's url.
    awk -v t="$target" -v s="$sha" '
      found && /sha256 "/ { sub(/sha256 "[0-9a-f]*"/, "sha256 \"" s "\""); found = 0 }
      index($0, "queueflow-" t ".tar.gz") { found = 1 }
      { print }
    ' "$new" > "$new.tmp" && mv "$new.tmp" "$new"
    updated+=("$target")
  else
    missing+=("$target")
  fi
done
rm -f "$new.bak"

# 3. Every active (uncommented) url must have been refreshed for this tag.
for target in "${missing[@]}"; do
  if grep -qE "^\s+url .*queueflow-$target\.tar\.gz" "$new"; then
    echo "error: $tag has no asset for active target $target" >&2
    exit 1
  fi
done

if (( check )); then
  if diff -u "$formula" "$new"; then
    echo "queueflow.rb is up to date for $tag"
  else
    echo "queueflow.rb is out of date for $tag" >&2
    exit 1
  fi
else
  cp "$new" "$formula"
  echo "updated $formula to $version"
  printf '  refreshed: %s\n' "${updated[@]}"
  (( ${#missing[@]} )) && printf '  no asset (left unchanged): %s\n' "${missing[@]}"
  ruby -c "$formula" >/dev/null && echo "  ruby -c: ok"
fi
