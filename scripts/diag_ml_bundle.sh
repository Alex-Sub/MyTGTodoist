#!/usr/bin/env bash
set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR" || exit 1

TS="$(date +%Y%m%d_%H%M%S)"
BASE_DIR="$ROOT_DIR/_diag"
BUNDLE_DIR="$BASE_DIR/ml_bundle_$TS"
ARCHIVE_PATH="$BASE_DIR/ml_bundle_$TS.tar.gz"
SUMMARY_PATH="$BUNDLE_DIR/SUMMARY.md"

mkdir -p "$BUNDLE_DIR"
mkdir -p "$BUNDLE_DIR/health" "$BUNDLE_DIR/logs" "$BUNDLE_DIR/probes" "$BUNDLE_DIR/system"

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

status_gateway_health="SKIPPED"
status_gateway_upstreams="SKIPPED"
status_asr_health="SKIPPED"

run_capture "$BUNDLE_DIR/system/date.txt" date || true
run_capture "$BUNDLE_DIR/system/uname.txt" uname -a || true
run_capture "$BUNDLE_DIR/system/uptime.txt" uptime || true

if have_cmd curl; then
  if curl -fsS "http://127.0.0.1:9000/health" >"$BUNDLE_DIR/health/gateway_health_9000.json" 2>"$BUNDLE_DIR/health/gateway_health_9000.err"; then
    status_gateway_health="OK"
  else
    status_gateway_health="FAIL"
  fi
  if curl -fsS "http://127.0.0.1:9000/diag/upstreams" >"$BUNDLE_DIR/health/gateway_diag_upstreams_9000.json" 2>"$BUNDLE_DIR/health/gateway_diag_upstreams_9000.err"; then
    status_gateway_upstreams="OK"
  else
    status_gateway_upstreams="FAIL"
  fi
  if curl -fsS "http://127.0.0.1:8020/health" >"$BUNDLE_DIR/health/asr_health_8020.json" 2>"$BUNDLE_DIR/health/asr_health_8020.err"; then
    status_asr_health="OK"
  else
    status_asr_health="FAIL"
  fi
else
  printf "SKIPPED: curl not installed\n" >"$BUNDLE_DIR/health/gateway_health_9000.err"
  printf "SKIPPED: curl not installed\n" >"$BUNDLE_DIR/health/gateway_diag_upstreams_9000.err"
  printf "SKIPPED: curl not installed\n" >"$BUNDLE_DIR/health/asr_health_8020.err"
fi

if have_cmd docker; then
  run_capture "$BUNDLE_DIR/logs/docker_compose_ps.txt" docker compose ps || true
  run_capture "$BUNDLE_DIR/logs/docker_compose_logs_guess.log" docker compose logs --tail=800 ml-gateway asr || true
else
  printf "SKIPPED: docker not installed\n" >"$BUNDLE_DIR/logs/docker_compose_ps.txt"
  printf "SKIPPED: docker not installed\n" >"$BUNDLE_DIR/logs/docker_compose_logs_guess.log"
fi

if have_cmd journalctl; then
  run_capture "$BUNDLE_DIR/logs/journal_ml_gateway.log" journalctl -u ml-gateway -n 800 --no-pager || true
  run_capture "$BUNDLE_DIR/logs/journal_asr.log" journalctl -u asr -n 800 --no-pager || true
else
  printf "SKIPPED: journalctl not installed\n" >"$BUNDLE_DIR/logs/journal_ml_gateway.log"
  printf "SKIPPED: journalctl not installed\n" >"$BUNDLE_DIR/logs/journal_asr.log"
fi

TEST_WAV="$BUNDLE_DIR/probes/test.wav"
if have_cmd python3; then
  python3 - "$TEST_WAV" >"$BUNDLE_DIR/probes/create_test_wav.log" 2>&1 <<'PY'
import struct
import sys
import wave

path = sys.argv[1]
with wave.open(path, "wb") as w:
    w.setnchannels(1)
    w.setsampwidth(2)
    w.setframerate(16000)
    data = struct.pack("<h", 0) * 16000
    w.writeframes(data)
print("ok")
PY
elif have_cmd python; then
  python - "$TEST_WAV" >"$BUNDLE_DIR/probes/create_test_wav.log" 2>&1 <<'PY'
import struct
import sys
import wave

path = sys.argv[1]
with wave.open(path, "wb") as w:
    w.setnchannels(1)
    w.setsampwidth(2)
    w.setframerate(16000)
    data = struct.pack("<h", 0) * 16000
    w.writeframes(data)
print("ok")
PY
else
  printf "SKIPPED: python/python3 not available\n" >"$BUNDLE_DIR/probes/create_test_wav.log"
fi

if [ -f "$TEST_WAV" ] && have_cmd curl; then
  curl -sS -X POST "http://127.0.0.1:8020/asr" -F "file=@$TEST_WAV" | head -c 500 >"$BUNDLE_DIR/probes/asr_direct_probe.txt" 2>"$BUNDLE_DIR/probes/asr_direct_probe.err" || true
  curl -sS -X POST "http://127.0.0.1:9000/voice-command?profile=organizer" \
    -H "X-Timezone: Europe/Amsterdam" \
    -F "file=@$TEST_WAV" | head -c 800 >"$BUNDLE_DIR/probes/gateway_voice_command_probe.txt" 2>"$BUNDLE_DIR/probes/gateway_voice_command_probe.err" || true
else
  printf "SKIPPED: test.wav missing or curl not installed\n" >"$BUNDLE_DIR/probes/asr_direct_probe.err"
  printf "SKIPPED: test.wav missing or curl not installed\n" >"$BUNDLE_DIR/probes/gateway_voice_command_probe.err"
fi

hypothesis1="- Недостаточно фактов: проверьте health и upstreams."
hypothesis2="- Недостаточно фактов: проверьте логи gateway/asr."

if [ "$status_gateway_health" = "FAIL" ]; then
  hypothesis1="- Вероятный первичный сбой: ML-Gateway на 9000 не отвечает (/health FAIL)."
elif [ "$status_gateway_upstreams" = "FAIL" ]; then
  hypothesis1="- Вероятный первичный сбой: /diag/upstreams FAIL, upstream ASR/LLM неготов."
elif [ "$status_asr_health" = "FAIL" ]; then
  hypothesis1="- Вероятный первичный сбой: ASR на 8020 не отвечает (/health FAIL)."
fi

if grep -qiE "timeout|connection refused|error|traceback" "$BUNDLE_DIR/logs/docker_compose_logs_guess.log" "$BUNDLE_DIR/logs/journal_ml_gateway.log" "$BUNDLE_DIR/logs/journal_asr.log" 2>/dev/null; then
  hypothesis2="- В логах gateway/asr есть timeout/connection/error (см. logs/)."
fi

cat >"$SUMMARY_PATH" <<EOF
# ML Diagnostic Summary ($TS)

## Checks
- [x] Gateway health: /health
- [x] Gateway upstreams: /diag/upstreams
- [x] ASR health: /health
- [x] Logs (docker compose + journalctl best effort)
- [x] Probe: direct ASR /asr
- [x] Probe: gateway /voice-command

## Health status
- Gateway 9000 /health: **$status_gateway_health**
- Gateway 9000 /diag/upstreams: **$status_gateway_upstreams**
- ASR 8020 /health: **$status_asr_health**

## Likeliest breakage (fact-based)
$hypothesis1
$hypothesis2
EOF

tar -czf "$ARCHIVE_PATH" -C "$BASE_DIR" "ml_bundle_$TS" >/dev/null 2>&1 || true

echo "ML diagnostic bundle created:"
echo "  dir: $BUNDLE_DIR"
echo "  archive: $ARCHIVE_PATH"
echo "Health summary: GW_HEALTH=$status_gateway_health GW_UPSTREAMS=$status_gateway_upstreams ASR_HEALTH=$status_asr_health"
echo "Likeliest breakage:"
echo "  $hypothesis1"
echo "  $hypothesis2"
