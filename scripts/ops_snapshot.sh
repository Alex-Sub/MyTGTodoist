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

echo "=== OPS SNAPSHOT ==="
echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "date_local: $(date +%Y-%m-%dT%H:%M:%S%z)"

if command -v git >/dev/null 2>&1; then
  if git -C "${ROOT_DIR}" rev-parse --git-dir >/dev/null 2>&1; then
    echo "git_sha: $(git -C "${ROOT_DIR}" rev-parse HEAD)"
  else
    echo "git_sha: n/a (not a git repo)"
  fi
else
  echo "git_sha: n/a (git not installed)"
fi

if [ -f "${COMPOSE_FILE}" ]; then
  echo "compose_file: ${COMPOSE_FILE}"
  echo "compose_sha256: $(sha256sum "${COMPOSE_FILE}" | awk '{print $1}')"
else
  echo "compose_file: missing (${COMPOSE_FILE})"
fi

echo
echo "--- ENV keys (.env.prod, values hidden) ---"
if [ -f "${ENV_FILE}" ]; then
  grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "${ENV_FILE}" | cut -d= -f1 | sort -u || true
else
  echo ".env file not found: ${ENV_FILE}"
fi

echo
echo "--- docker compose ps ---"
docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" ps

echo
echo "--- recent logs: organizer-worker ---"
docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" logs --tail="${LOG_TAIL}" organizer-worker

echo
echo "--- recent logs: telegram-bot ---"
docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" logs --tail="${LOG_TAIL}" telegram-bot
