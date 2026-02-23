#!/usr/bin/env bash
set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR" || exit 1

TS="$(date +%Y%m%d_%H%M%S)"
BASE_DIR="$ROOT_DIR/_diag"
BUNDLE_DIR="$BASE_DIR/voice_bundle_$TS"
ARCHIVE_PATH="$BASE_DIR/voice_bundle_$TS.tar.gz"
SUMMARY_PATH="$BUNDLE_DIR/SUMMARY.md"

mkdir -p "$BUNDLE_DIR"
mkdir -p "$BUNDLE_DIR/metadata" "$BUNDLE_DIR/health" "$BUNDLE_DIR/logs" "$BUNDLE_DIR/focus" "$BUNDLE_DIR/snapshots"

COMPOSE_CMD=(docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml)

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

run_capture() {
  local outfile="$1"
  shift
  if [ $# -eq 0 ]; then
    printf "SKIPPED: empty command\n" >"$outfile"
    return 1
  fi
  "$@" >"$outfile" 2>&1
  return $?
}

safe_head_lines() {
  local pattern="$1"
  local src="$2"
  local dst="$3"
  if [ ! -f "$src" ]; then
    printf "SKIPPED: source missing (%s)\n" "$src" >"$dst"
    return 1
  fi
  grep -nE "$pattern" "$src" | head -n 3 >"$dst" 2>/dev/null || true
  if [ ! -s "$dst" ]; then
    printf "no matches\n" >"$dst"
  fi
}

status_api_health="SKIPPED"
status_ml_health="SKIPPED"
status_ml_upstreams="SKIPPED"
status_exec_bot_env="SKIPPED"
status_exec_bot_core="SKIPPED"
status_exec_worker_core="SKIPPED"

run_capture "$BUNDLE_DIR/metadata/date.txt" date || true
run_capture "$BUNDLE_DIR/metadata/uname.txt" uname -a || true
run_capture "$BUNDLE_DIR/metadata/uptime.txt" uptime || true
run_capture "$BUNDLE_DIR/metadata/git_head.txt" git rev-parse HEAD || true
run_capture "$BUNDLE_DIR/metadata/git_status_porcelain.txt" git status --porcelain || true
run_capture "$BUNDLE_DIR/metadata/git_branch.txt" git branch --show-current || true
run_capture "$BUNDLE_DIR/metadata/docker_version.txt" docker version || true
run_capture "$BUNDLE_DIR/metadata/docker_compose_version.txt" docker compose version || true
run_capture "$BUNDLE_DIR/metadata/compose_ps.txt" "${COMPOSE_CMD[@]}" ps || true

if [ -f ".env.prod" ]; then
  grep -vE '^\s*#' .env.prod | grep -E '=' | cut -d= -f1 | sed '/^\s*$/d' | sort -u >"$BUNDLE_DIR/metadata/env_keys.txt" || true
else
  printf "SKIPPED: .env.prod not found\n" >"$BUNDLE_DIR/metadata/env_keys.txt"
fi

if have_cmd curl; then
  if curl -fsS "http://127.0.0.1:8101/health" >"$BUNDLE_DIR/health/api_health_8101.json" 2>"$BUNDLE_DIR/health/api_health_8101.err"; then
    status_api_health="OK"
  else
    status_api_health="FAIL"
  fi
  if curl -fsS "http://127.0.0.1:19000/health" >"$BUNDLE_DIR/health/ml_health_19000.json" 2>"$BUNDLE_DIR/health/ml_health_19000.err"; then
    status_ml_health="OK"
  else
    status_ml_health="FAIL"
  fi
  if curl -fsS "http://127.0.0.1:19000/diag/upstreams" >"$BUNDLE_DIR/health/ml_diag_upstreams_19000.json" 2>"$BUNDLE_DIR/health/ml_diag_upstreams_19000.err"; then
    status_ml_upstreams="OK"
  else
    status_ml_upstreams="FAIL"
  fi
else
  printf "SKIPPED: curl not installed\n" >"$BUNDLE_DIR/health/api_health_8101.err"
  printf "SKIPPED: curl not installed\n" >"$BUNDLE_DIR/health/ml_health_19000.err"
  printf "SKIPPED: curl not installed\n" >"$BUNDLE_DIR/health/ml_diag_upstreams_19000.err"
fi

if have_cmd ss; then
  ss -tulpn | grep -E ':(8101|19000)\b' >"$BUNDLE_DIR/health/ports_8101_19000.txt" 2>&1 || true
else
  printf "SKIPPED: ss not installed\n" >"$BUNDLE_DIR/health/ports_8101_19000.txt"
fi

run_capture "$BUNDLE_DIR/logs/telegram-bot.log" "${COMPOSE_CMD[@]}" logs --tail=800 telegram-bot || true
run_capture "$BUNDLE_DIR/logs/organizer-worker.log" "${COMPOSE_CMD[@]}" logs --tail=800 organizer-worker || true
run_capture "$BUNDLE_DIR/logs/organizer-api.log" "${COMPOSE_CMD[@]}" logs --tail=400 organizer-api || true

grep -Ei "voice|ASR|asr_|/voice-command|ML_CORE|diag|timeout|trace|update_id|file_id" "$BUNDLE_DIR/logs/telegram-bot.log" >"$BUNDLE_DIR/focus/telegram-bot_voice_focus.log" 2>&1 || true
grep -Ei "runtime/command|clarifying_question|ok=|intent|calendar|NOT_CONFIGURED" "$BUNDLE_DIR/logs/organizer-worker.log" >"$BUNDLE_DIR/focus/organizer-worker_focus.log" 2>&1 || true

if run_capture "$BUNDLE_DIR/snapshots/telegram-bot_python_env_masked.txt" "${COMPOSE_CMD[@]}" exec -T telegram-bot sh -lc "python -V; env | sort | sed -E 's/(TOKEN|KEY|SECRET|PASS)[^=]*=.*/\\1=***MASKED***/I'"; then
  status_exec_bot_env="OK"
else
  status_exec_bot_env="FAIL"
fi
if run_capture "$BUNDLE_DIR/snapshots/telegram-bot_ml_timeouts.txt" "${COMPOSE_CMD[@]}" exec -T telegram-bot sh -lc "echo ML_CORE_URL=\$ML_CORE_URL; echo TG_LONGPOLL_SEC=\$TG_LONGPOLL_SEC; echo TG_HTTP_READ_TIMEOUT=\$TG_HTTP_READ_TIMEOUT"; then
  status_exec_bot_core="OK"
else
  status_exec_bot_core="FAIL"
fi
if run_capture "$BUNDLE_DIR/snapshots/organizer-worker_ml_canon.txt" "${COMPOSE_CMD[@]}" exec -T organizer-worker sh -lc "echo ML_CORE_URL=\$ML_CORE_URL; ls -la /canon/intents_v2.yml"; then
  status_exec_worker_core="OK"
else
  status_exec_worker_core="FAIL"
fi

asr_unavailable_count="$(grep -c "asr_unavailable" "$BUNDLE_DIR/logs/telegram-bot.log" 2>/dev/null || true)"
asr_timeout_count="$(grep -c "asr_timeout" "$BUNDLE_DIR/logs/telegram-bot.log" 2>/dev/null || true)"
asr_empty_count="$(grep -c "asr_empty" "$BUNDLE_DIR/logs/telegram-bot.log" 2>/dev/null || true)"
llm_invalid_count="$(grep -c "llm_invalid_output" "$BUNDLE_DIR/logs/telegram-bot.log" 2>/dev/null || true)"
gateway_unreachable_count="$(grep -ciE "asr_unavailable|connection refused|name or service not known|failed to establish|timed out|timeout" "$BUNDLE_DIR/logs/telegram-bot.log" 2>/dev/null || true)"

safe_head_lines "asr_unavailable" "$BUNDLE_DIR/logs/telegram-bot.log" "$BUNDLE_DIR/focus/examples_asr_unavailable.txt"
safe_head_lines "asr_timeout" "$BUNDLE_DIR/logs/telegram-bot.log" "$BUNDLE_DIR/focus/examples_asr_timeout.txt"
safe_head_lines "asr_empty" "$BUNDLE_DIR/logs/telegram-bot.log" "$BUNDLE_DIR/focus/examples_asr_empty.txt"
safe_head_lines "llm_invalid_output" "$BUNDLE_DIR/logs/telegram-bot.log" "$BUNDLE_DIR/focus/examples_llm_invalid_output.txt"
safe_head_lines "asr_unavailable|connection refused|timed out|timeout" "$BUNDLE_DIR/logs/telegram-bot.log" "$BUNDLE_DIR/focus/examples_gateway_unreachable_or_timeout.txt"

hypothesis1="- Недостаточно фактов: смотрите health/upstreams и примеры логов."
hypothesis2="- Недостаточно фактов: проверьте прямые ASR проверки на ML-host."

if [ "$status_ml_health" = "FAIL" ] || [ "$status_ml_upstreams" = "FAIL" ]; then
  hypothesis1="- Вероятный первичный сбой между VPS и ML-Gateway: /health или /diag/upstreams на 19000 неуспешны (см. файлы в health/)."
elif [ "${asr_timeout_count:-0}" -gt 0 ]; then
  hypothesis1="- Вероятный первичный сбой: timeout при вызове gateway/ASR (подтверждается asr_timeout в telegram-bot логах)."
elif [ "${asr_unavailable_count:-0}" -gt 0 ]; then
  hypothesis1="- Вероятный первичный сбой: gateway недоступен или upstream ASR недоступен (подтверждается asr_unavailable)."
elif [ "${asr_empty_count:-0}" -gt 0 ]; then
  hypothesis1="- Вероятный первичный сбой: ASR отдает пустой transcript (подтверждается asr_empty)."
fi

if [ "${llm_invalid_count:-0}" -gt 0 ]; then
  hypothesis2="- Дополнительно: есть невалидный ответ gateway (llm_invalid_output), см. примеры в focus/examples_llm_invalid_output.txt."
elif [ "${gateway_unreachable_count:-0}" -gt 0 ]; then
  hypothesis2="- Дополнительно: есть признаки unreachable/timeout в логах telegram-bot (см. focus/examples_gateway_unreachable_or_timeout.txt)."
fi

cat >"$SUMMARY_PATH" <<EOF
# Voice Diagnostic Summary ($TS)

## What was checked
- [x] VPS metadata (date/uname/uptime/git/docker/compose)
- [x] Compose service state
- [x] Env keys from .env.prod (names only, no values)
- [x] API and ML health checks
- [x] Container logs (bot/worker/api)
- [x] Focused log extracts
- [x] Runtime snapshots from telegram-bot and organizer-worker

## Health status
- API http://127.0.0.1:8101/health: **$status_api_health**
- ML-Gateway http://127.0.0.1:19000/health: **$status_ml_health**
- ML-Gateway http://127.0.0.1:19000/diag/upstreams: **$status_ml_upstreams**
- telegram-bot exec snapshot: **$status_exec_bot_env**
- telegram-bot core vars snapshot: **$status_exec_bot_core**
- organizer-worker snapshot: **$status_exec_worker_core**

## Error markers in telegram-bot logs
- asr_unavailable: **$asr_unavailable_count**
- asr_timeout: **$asr_timeout_count**
- asr_empty: **$asr_empty_count**
- llm_invalid_output: **$llm_invalid_count**
- gateway unreachable/timeout markers: **$gateway_unreachable_count**

Examples:
- asr_unavailable: \`focus/examples_asr_unavailable.txt\`
- asr_timeout: \`focus/examples_asr_timeout.txt\`
- asr_empty: \`focus/examples_asr_empty.txt\`
- llm_invalid_output: \`focus/examples_llm_invalid_output.txt\`
- gateway unreachable/timeout: \`focus/examples_gateway_unreachable_or_timeout.txt\`

## Likeliest breakage (fact-based)
$hypothesis1
$hypothesis2
EOF

tar -czf "$ARCHIVE_PATH" -C "$BASE_DIR" "voice_bundle_$TS" >/dev/null 2>&1 || true

echo "Voice diagnostic bundle created:"
echo "  dir: $BUNDLE_DIR"
echo "  archive: $ARCHIVE_PATH"
echo "Health summary: API=$status_api_health ML_HEALTH=$status_ml_health ML_UPSTREAMS=$status_ml_upstreams"
echo "Markers: asr_unavailable=$asr_unavailable_count asr_timeout=$asr_timeout_count asr_empty=$asr_empty_count llm_invalid_output=$llm_invalid_count"
echo "Likeliest breakage:"
echo "  $hypothesis1"
echo "  $hypothesis2"
