#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

mode=--write
case "${1:-}" in
  --check)
    mode=--check
    shift
    ;;
  -h|--help)
    echo "usage: scripts/reflow_markdown.sh [--check] [FILE ...]"
    exit
    ;;
esac

if (( $# )); then
  files=("$@")
else
  mapfile -d '' files < <(git ls-files -z '*.md')
fi

exec npx --yes prettier@3.8.4 --prose-wrap never "$mode" "${files[@]}"
