#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROJECT_NAME="${PROJECT_NAME:-deploy}"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env.prod}"
COMPOSE_FILE_MAIN="${COMPOSE_FILE_MAIN:-${ROOT_DIR}/docker-compose.yml}"
COMPOSE_FILE_OVERRIDE="${COMPOSE_FILE_OVERRIDE:-${ROOT_DIR}/docker-compose.vps.override.yml}"
TARGET_BRANCH="${TARGET_BRANCH:-runtime-stable}"
STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP_BRANCH="backup/vps-${STAMP}"

echo "=== PROD SWITCH TO ${TARGET_BRANCH} ==="
echo "repo: ${ROOT_DIR}"

if ! git -C "${ROOT_DIR}" rev-parse --git-dir >/dev/null 2>&1; then
  echo "[ABORT] not a git repository: ${ROOT_DIR}" >&2
  exit 1
fi

CURRENT_BRANCH="$(git -C "${ROOT_DIR}" rev-parse --abbrev-ref HEAD)"
echo "current_branch=${CURRENT_BRANCH}"

if ! git -C "${ROOT_DIR}" diff --quiet || ! git -C "${ROOT_DIR}" diff --cached --quiet || [ -n "$(git -C "${ROOT_DIR}" ls-files --others --exclude-standard)" ]; then
  echo "[info] local changes detected; stashing (tracked + untracked)"
  git -C "${ROOT_DIR}" stash push -u -m "auto-stash before switch to ${TARGET_BRANCH} (${STAMP})"
else
  echo "[info] working tree clean; stash skipped"
fi

echo "[info] creating backup branch: ${BACKUP_BRANCH}"
git -C "${ROOT_DIR}" branch "${BACKUP_BRANCH}" HEAD

echo "[info] fetch origin"
git -C "${ROOT_DIR}" fetch origin

if git -C "${ROOT_DIR}" show-ref --verify --quiet "refs/heads/${TARGET_BRANCH}"; then
  echo "[info] checkout existing local branch ${TARGET_BRANCH}"
  git -C "${ROOT_DIR}" checkout "${TARGET_BRANCH}"
elif git -C "${ROOT_DIR}" show-ref --verify --quiet "refs/remotes/origin/${TARGET_BRANCH}"; then
  echo "[info] create local ${TARGET_BRANCH} from origin/${TARGET_BRANCH}"
  git -C "${ROOT_DIR}" checkout -b "${TARGET_BRANCH}" "origin/${TARGET_BRANCH}"
else
  echo "[ABORT] origin/${TARGET_BRANCH} not found; refusing to create orphan local branch" >&2
  exit 1
fi

echo "[info] pull --ff-only origin ${TARGET_BRANCH}"
git -C "${ROOT_DIR}" pull --ff-only origin "${TARGET_BRANCH}"

echo "[info] rebuild organizer-worker"
docker compose \
  -p "${PROJECT_NAME}" \
  --env-file "${ENV_FILE}" \
  -f "${COMPOSE_FILE_MAIN}" \
  -f "${COMPOSE_FILE_OVERRIDE}" \
  build organizer-worker

echo
echo "=== FINAL GIT STATUS ==="
git -C "${ROOT_DIR}" status --short --branch

echo
echo "=== COMPOSE PS ==="
docker compose \
  -p "${PROJECT_NAME}" \
  --env-file "${ENV_FILE}" \
  -f "${COMPOSE_FILE_MAIN}" \
  -f "${COMPOSE_FILE_OVERRIDE}" \
  ps
