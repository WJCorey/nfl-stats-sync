#!/usr/bin/env bash
# Publish staged canonical artifacts to this repository's GitHub Release
# "artifacts" so every ledger durableUri resolves before a live synchronize.
#
# Idempotent: existing assets are left untouched (content-addressed names make
# re-upload unnecessary); missing assets are uploaded from artifacts/staged/.
# Requires: gh CLI authenticated with push access to this repository.
#
# NOTE: the durable-locator host decision (public GitHub Releases on this
# repository) is PENDING operator approval -- see project/SPEC.md.
set -euo pipefail

REPO="${NFLSTATS_RELEASE_REPO:-warmautomation/nfl-stats-sync}"
TAG="${NFLSTATS_RELEASE_TAG:-artifacts}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STAGED="$ROOT/artifacts/staged"

if [ ! -d "$STAGED" ]; then
  echo "nothing staged under $STAGED"
  exit 0
fi

if ! gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1; then
  gh release create "$TAG" --repo "$REPO" --title "Canonical semantic artifacts" \
    --notes "Content-addressed canonical semantic artifacts (agentgm-nflverse-canonical-jsonl/v1). Named <stream-slug>-<sha16>.jsonl; each file's SHA-256 is recorded in artifacts/ledger.json." >/dev/null
  echo "created release $TAG"
fi

existing="$(gh release view "$TAG" --repo "$REPO" --json assets --jq '.assets[].name')"

uploaded=0
skipped=0
for file in "$STAGED"/*/*.jsonl; do
  [ -e "$file" ] || continue
  slug="$(basename "$(dirname "$file")")"
  sha="$(basename "$file" .jsonl)"
  asset="${slug}-${sha:0:16}.jsonl"
  if printf '%s\n' "$existing" | grep -qx "$asset"; then
    skipped=$((skipped + 1))
    continue
  fi
  cp "$file" "/tmp/$asset"
  gh release upload "$TAG" "/tmp/$asset" --repo "$REPO" >/dev/null
  rm -f "/tmp/$asset"
  echo "uploaded $asset"
  uploaded=$((uploaded + 1))
done

echo "done: uploaded=$uploaded skipped=$skipped"
