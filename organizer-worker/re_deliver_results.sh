#!/usr/bin/env bash
set -euo pipefail

# Host-side helper for VPS runtime.
# Runs re-delivery logic inside organizer-worker container.
LIMIT="${1:-50}"
docker compose exec -T organizer-worker python /app/worker.py --re-deliver --limit "${LIMIT}"
