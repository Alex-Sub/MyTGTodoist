# Voice Diagnostic Summary (20260223_181846)

## What was checked
- [x] VPS metadata
- [x] Compose service state
- [x] Env keys only (no values)
- [x] API and ML health checks
- [x] Container logs and focused extracts

## Health status
- API 8101 /health: **FAIL**
- ML 19000 /health: **FAIL**
- ML 19000 /diag/upstreams: **FAIL**

## Error markers in telegram-bot logs
- asr_unavailable: **0**
- asr_timeout: **0**
- asr_empty: **0**
- llm_invalid_output: **0**

## Likeliest breakage (fact-based)
- Likely primary failure between VPS and ML-Gateway: /health or /diag/upstreams on 19000 failed.
- Not enough facts yet: check ML-host (gateway/asr).
