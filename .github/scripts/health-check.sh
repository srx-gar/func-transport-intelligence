#!/usr/bin/env bash
set -euo pipefail

: "${WEB_APP_NAME:?WEB_APP_NAME is required}"

HEALTHCHECK_PATH="${HEALTHCHECK_PATH:-/healthz}"
EXPECT_CODE="${HEALTHCHECK_EXPECT:-200}"
DELAY="${HEALTHCHECK_DELAY_SEC:-5}"
RETRIES="${HEALTHCHECK_RETRIES:-30}"
BASE_URL="${HEALTHCHECK_BASE_URL:-https://${WEB_APP_NAME}.azurewebsites.net}"
URL="${BASE_URL}${HEALTHCHECK_PATH}"

echo "[health-check] Waiting for ${URL} to return ${EXPECT_CODE}..."

attempt=0
until [ "$attempt" -ge "$RETRIES" ]; do
  status=$(curl -s -o /dev/null -w "%{http_code}" "$URL" || true)
  echo "[health-check] Attempt $((attempt + 1))/$RETRIES -> HTTP ${status}"
  if [ "$status" = "$EXPECT_CODE" ]; then
    echo "[health-check] Healthy"
    exit 0
  fi
  attempt=$((attempt + 1))
  sleep "$DELAY"
done

echo "[health-check] Failed after ${RETRIES} attempts"
exit 1
