#!/bin/sh
set -eu

MODE_RAW="${CALENDAR_SYNC_MODE:-full}"
MODE="$(printf '%s' "$MODE_RAW" | tr '[:upper:]' '[:lower:]')"
SA_PATH="${GOOGLE_SERVICE_ACCOUNT_FILE:-/data/google_sa.json}"
CAL_ID_RAW="${GOOGLE_CALENDAR_ID:-}"
CAL_ID="$(printf '%s' "$CAL_ID_RAW" | tr '[:upper:]' '[:lower:]')"

if [ "$MODE" = "off" ]; then
  exec python /app/worker.py
fi

if [ -z "$SA_PATH" ]; then
  echo "[preflight] ERROR: GOOGLE_SERVICE_ACCOUNT_FILE is empty (mode=$MODE_RAW)" >&2
  exit 1
fi
if [ ! -e "$SA_PATH" ]; then
  echo "[preflight] ERROR: service account file not found: $SA_PATH" >&2
  exit 1
fi
if [ -d "$SA_PATH" ]; then
  echo "[preflight] ERROR: $SA_PATH is a directory, expected JSON file" >&2
  exit 1
fi
if [ ! -f "$SA_PATH" ]; then
  echo "[preflight] ERROR: $SA_PATH is not a regular file" >&2
  exit 1
fi
if [ ! -s "$SA_PATH" ]; then
  echo "[preflight] ERROR: service account file is empty: $SA_PATH" >&2
  exit 1
fi
if [ -z "$CAL_ID_RAW" ]; then
  echo "[preflight] ERROR: GOOGLE_CALENDAR_ID is empty (service account mode requires explicit shared calendar id)" >&2
  exit 1
fi
if [ "$CAL_ID" = "primary" ]; then
  echo "[preflight] ERROR: GOOGLE_CALENDAR_ID=primary is invalid for service account mode; use shared calendar id" >&2
  exit 1
fi

python -c "import json,sys; p=sys.argv[1]; json.load(open(p,'r',encoding='utf-8'))" "$SA_PATH" || {
  echo "[preflight] ERROR: service account json invalid (likely missing leading '{' or extra non-json text): $SA_PATH" >&2
  exit 1
}

exec python /app/worker.py
