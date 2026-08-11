#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="${PROJECT_NAME:-deploy}"
ENV_FILE="${ENV_FILE:-deploy/tenants/.env.alexey}"
COMPOSE_FILES=(-f docker-compose.yml -f docker-compose.vps.override.yml)
EXPECTED_DATA_VOLUME="${EXPECTED_DATA_VOLUME:-${PROJECT_NAME}_db_data}"

check_env_file_ready() {
  local path="$1"
  echo "[INFO] Using env file: $path"
  if [[ ! -f "$path" ]]; then
    echo "[FAIL] Env file is missing: $path" >&2
    exit 1
  fi

  local key line value
  for key in TELEGRAM_BOT_TOKEN GOOGLE_CALENDAR_ID; do
    line="$(grep -E "^[[:space:]]*${key}=" "$path" | head -n 1 || true)"
    if [[ -z "$line" ]]; then
      echo "[FAIL] ${key} is missing in $path" >&2
      exit 1
    fi
    value="${line#*=}"
    value="${value//$'\r'/}"
    value="${value//[[:space:]]/}"
    if [[ -z "$value" ]]; then
      echo "[FAIL] ${key} is empty in $path" >&2
      exit 1
    fi
    echo "[OK] ${key} is set in $path"
  done
  echo "[OK] env file exists: $path"
}

compose() {
  docker compose -p "${PROJECT_NAME}" --env-file "${ENV_FILE}" "${COMPOSE_FILES[@]}" "$@"
}

find_duplicate_stack_containers() {
  docker ps --format '{{.ID}} {{.Names}} {{.Label "com.docker.compose.project"}} {{.Label "com.docker.compose.service"}}' \
    | awk -v p="${PROJECT_NAME}" '
      ($4=="organizer-worker" || $4=="telegram-bot" || $4=="organizer-api") && $3 != p { print }
    '
}

check_no_duplicate_stacks() {
  local dup
  dup="$(find_duplicate_stack_containers || true)"
  if [[ -n "${dup}" ]]; then
    echo "[ABORT] Found containers from a different compose project running in parallel:" >&2
    echo "${dup}" >&2
    echo "[ABORT] Stop duplicate stack first, then rerun with -p ${PROJECT_NAME}." >&2
    exit 1
  fi
}

health_check_single_worker() {
  local workers count line cid project volume
  workers="$(docker ps --filter label=com.docker.compose.service=organizer-worker --format '{{.ID}} {{.Names}} {{.Label "com.docker.compose.project"}}')"
  count="$(echo "${workers}" | sed '/^\s*$/d' | wc -l | tr -d ' ')"

  if [[ "${count}" != "1" ]]; then
    echo "[FAIL] Expected exactly 1 running organizer-worker, got ${count}." >&2
    echo "${workers}" >&2
    exit 1
  fi

  line="$(echo "${workers}" | sed -n '1p')"
  cid="$(echo "${line}" | awk '{print $1}')"
  project="$(echo "${line}" | awk '{print $3}')"
  if [[ "${project}" != "${PROJECT_NAME}" ]]; then
    echo "[FAIL] Worker project is '${project}', expected '${PROJECT_NAME}'." >&2
    exit 1
  fi

  volume="$(docker inspect "${cid}" --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}')"
  if [[ "${volume}" != "${EXPECTED_DATA_VOLUME}" ]]; then
    echo "[FAIL] Worker /data volume is '${volume}', expected '${EXPECTED_DATA_VOLUME}'." >&2
    exit 1
  fi

  echo "[OK] single worker + volume check passed: project=${project}, worker=${cid}, volume=${volume}"
}

usage() {
  cat <<'USAGE'
Usage:
  ./run.sh up       # guard against duplicate stacks, then up -d --build
  ./run.sh ps       # docker compose ps (locked to -p deploy by default)
  ./run.sh logs     # logs --tail=200 organizer-worker telegram-bot organizer-api
  ./run.sh health   # verify exactly one worker and /data volume is deploy_db_data
USAGE
}

cmd="${1:-}"
case "${cmd}" in
  up)
    check_env_file_ready "${ENV_FILE}"
    check_no_duplicate_stacks
    compose up -d --build
    health_check_single_worker
    ;;
  ps)
    check_env_file_ready "${ENV_FILE}"
    compose ps
    ;;
  logs)
    check_env_file_ready "${ENV_FILE}"
    compose logs --tail=200 organizer-worker telegram-bot organizer-api
    ;;
  health)
    health_check_single_worker
    ;;
  *)
    usage
    exit 1
    ;;
esac
