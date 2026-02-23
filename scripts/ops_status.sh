#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROJECT_NAME="${PROJECT_NAME:-deploy}"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT_DIR}/deploy/docker-compose.prod.yml}"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env.prod}"
LOG_TAIL="${LOG_TAIL:-120}"
REQUIRED_BRANCH="${REQUIRED_BRANCH:-runtime-stable}"

if command -v git >/dev/null 2>&1 && git -C "${ROOT_DIR}" rev-parse --git-dir >/dev/null 2>&1; then
  current_branch="$(git -C "${ROOT_DIR}" rev-parse --abbrev-ref HEAD)"
  if [ "${current_branch}" != "${REQUIRED_BRANCH}" ]; then
    echo "[ABORT] production branch guard failed: current=${current_branch}, required=${REQUIRED_BRANCH}" >&2
    exit 1
  fi
fi

echo "=== PROD-v0 STATUS ==="
echo "utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo
echo "--- docker compose ps ---"
docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" ps

echo
echo "--- api health ---"
curl -fsS "http://127.0.0.1:8101/health" || {
  echo "health_check=FAIL"
  exit 1
}
echo

echo
echo "--- worker logs (tail=${LOG_TAIL}) ---"
docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" logs --tail="${LOG_TAIL}" organizer-worker

echo
echo "--- bot logs (tail=${LOG_TAIL}) ---"
docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" logs --tail="${LOG_TAIL}" telegram-bot
