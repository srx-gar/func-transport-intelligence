#!/usr/bin/env bash
set -euo pipefail

log() { echo "[derive_names] $*"; }

repo_name=""
if [ -n "${GITHUB_REPOSITORY:-}" ]; then
  repo_name="${GITHUB_REPOSITORY##*/}"
fi

if [ -z "$repo_name" ]; then
  if repo_url=$(git config --get remote.origin.url 2>/dev/null); then
    repo_basename=$(basename "$repo_url")
    repo_name="${repo_basename%%.git}"
  else
    repo_name=$(basename "$(pwd)")
  fi
fi

norm=$(printf '%s' "$repo_name" \
  | tr '[:upper:]' '[:lower:]' \
  | sed -E 's/[^a-z0-9-]+/-/g; s/-+/-/g; s/^-+//; s/-+$//')

if [ -z "$norm" ]; then
  log "Repo name could not be determined or sanitized; using fallback 'app'"
  norm="app"
fi

FUNCTION_APP_NAME="${norm}"
RESOURCE_GROUP="poc-gar-blocktracker"

log "repo_name=$repo_name -> normalized=$norm"
log "FUNCTION_APP_NAME=$FUNCTION_APP_NAME"
log "RESOURCE_GROUP=$RESOURCE_GROUP"

if [ -n "${GITHUB_ENV:-}" ]; then
  {
    echo "FUNCTION_APP_NAME=$FUNCTION_APP_NAME"
    echo "RESOURCE_GROUP=$RESOURCE_GROUP"
  } >> "$GITHUB_ENV"
fi

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  {
    echo "function_app_name=$FUNCTION_APP_NAME"
    echo "resource_group=$RESOURCE_GROUP"
  } >> "$GITHUB_OUTPUT"
fi
