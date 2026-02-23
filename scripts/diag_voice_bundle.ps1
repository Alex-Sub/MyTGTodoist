$ErrorActionPreference = "Continue"

$RootDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RootDir

$Ts = Get-Date -Format "yyyyMMdd_HHmmss"
$BaseDir = Join-Path $RootDir "_diag"
$BundleDir = Join-Path $BaseDir "voice_bundle_$Ts"
$SummaryPath = Join-Path $BundleDir "SUMMARY.md"
$ZipPath = Join-Path $BaseDir "voice_bundle_$Ts.zip"

New-Item -ItemType Directory -Force -Path $BundleDir | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $BundleDir "metadata") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $BundleDir "health") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $BundleDir "logs") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $BundleDir "focus") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $BundleDir "snapshots") | Out-Null

$ComposePrefix = "docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml"

function Write-Capture {
    param(
        [string]$Path,
        [string]$Command
    )
    try {
        $Output = & powershell -NoProfile -Command $Command 2>&1
        $Output | Out-File -FilePath $Path -Encoding UTF8
        return $true
    } catch {
        "FAIL: $($_.Exception.Message)" | Out-File -FilePath $Path -Encoding UTF8
        return $false
    }
}

function Write-HeadMatches {
    param(
        [string]$Source,
        [string]$Pattern,
        [string]$Target
    )
    if (-not (Test-Path $Source)) {
        "SKIPPED: source missing" | Out-File -FilePath $Target -Encoding UTF8
        return
    }
    $m = Select-String -Path $Source -Pattern $Pattern -AllMatches | Select-Object -First 3
    if ($null -eq $m -or $m.Count -eq 0) {
        "no matches" | Out-File -FilePath $Target -Encoding UTF8
        return
    }
    $m | ForEach-Object { "$($_.LineNumber):$($_.Line)" } | Out-File -FilePath $Target -Encoding UTF8
}

$statusApi = "SKIPPED"
$statusMlHealth = "SKIPPED"
$statusMlDiag = "SKIPPED"

Write-Capture -Path (Join-Path $BundleDir "metadata/date.txt") -Command "Get-Date -Format o" | Out-Null
Write-Capture -Path (Join-Path $BundleDir "metadata/uname.txt") -Command "uname -a" | Out-Null
Write-Capture -Path (Join-Path $BundleDir "metadata/uptime.txt") -Command "uptime" | Out-Null
Write-Capture -Path (Join-Path $BundleDir "metadata/git_head.txt") -Command "git rev-parse HEAD" | Out-Null
Write-Capture -Path (Join-Path $BundleDir "metadata/git_status_porcelain.txt") -Command "git status --porcelain" | Out-Null
Write-Capture -Path (Join-Path $BundleDir "metadata/git_branch.txt") -Command "git branch --show-current" | Out-Null
Write-Capture -Path (Join-Path $BundleDir "metadata/docker_version.txt") -Command "docker version" | Out-Null
Write-Capture -Path (Join-Path $BundleDir "metadata/docker_compose_version.txt") -Command "docker compose version" | Out-Null
Write-Capture -Path (Join-Path $BundleDir "metadata/compose_ps.txt") -Command "$ComposePrefix ps" | Out-Null

if (Test-Path ".env.prod") {
    Get-Content ".env.prod" |
        Where-Object { $_ -notmatch '^\s*#' -and $_ -match '=' } |
        ForEach-Object { ($_ -split '=', 2)[0].Trim() } |
        Where-Object { $_ -ne "" } |
        Sort-Object -Unique |
        Out-File -FilePath (Join-Path $BundleDir "metadata/env_keys.txt") -Encoding UTF8
} else {
    "SKIPPED: .env.prod not found" | Out-File -FilePath (Join-Path $BundleDir "metadata/env_keys.txt") -Encoding UTF8
}

try {
    (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:8101/health" -TimeoutSec 10).Content |
        Out-File -FilePath (Join-Path $BundleDir "health/api_health_8101.json") -Encoding UTF8
    $statusApi = "OK"
} catch {
    $_.Exception.Message | Out-File -FilePath (Join-Path $BundleDir "health/api_health_8101.err") -Encoding UTF8
    $statusApi = "FAIL"
}
try {
    (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:19000/health" -TimeoutSec 10).Content |
        Out-File -FilePath (Join-Path $BundleDir "health/ml_health_19000.json") -Encoding UTF8
    $statusMlHealth = "OK"
} catch {
    $_.Exception.Message | Out-File -FilePath (Join-Path $BundleDir "health/ml_health_19000.err") -Encoding UTF8
    $statusMlHealth = "FAIL"
}
try {
    (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:19000/diag/upstreams" -TimeoutSec 10).Content |
        Out-File -FilePath (Join-Path $BundleDir "health/ml_diag_upstreams_19000.json") -Encoding UTF8
    $statusMlDiag = "OK"
} catch {
    $_.Exception.Message | Out-File -FilePath (Join-Path $BundleDir "health/ml_diag_upstreams_19000.err") -Encoding UTF8
    $statusMlDiag = "FAIL"
}

Write-Capture -Path (Join-Path $BundleDir "health/ports_8101_19000.txt") -Command "ss -tulpn | egrep ':(8101|19000)\b' || true" | Out-Null

Write-Capture -Path (Join-Path $BundleDir "logs/telegram-bot.log") -Command "$ComposePrefix logs --tail=800 telegram-bot" | Out-Null
Write-Capture -Path (Join-Path $BundleDir "logs/organizer-worker.log") -Command "$ComposePrefix logs --tail=800 organizer-worker" | Out-Null
Write-Capture -Path (Join-Path $BundleDir "logs/organizer-api.log") -Command "$ComposePrefix logs --tail=400 organizer-api" | Out-Null

Get-Content (Join-Path $BundleDir "logs/telegram-bot.log") -ErrorAction SilentlyContinue |
    Select-String -Pattern "voice|ASR|asr_|/voice-command|ML_CORE|diag|timeout|trace|update_id|file_id" |
    ForEach-Object { $_.Line } |
    Out-File -FilePath (Join-Path $BundleDir "focus/telegram-bot_voice_focus.log") -Encoding UTF8

Get-Content (Join-Path $BundleDir "logs/organizer-worker.log") -ErrorAction SilentlyContinue |
    Select-String -Pattern "runtime/command|clarifying_question|ok=|intent|calendar|NOT_CONFIGURED" |
    ForEach-Object { $_.Line } |
    Out-File -FilePath (Join-Path $BundleDir "focus/organizer-worker_focus.log") -Encoding UTF8

Write-Capture -Path (Join-Path $BundleDir "snapshots/telegram-bot_ml_timeouts.txt") -Command "$ComposePrefix exec -T telegram-bot sh -lc 'echo ML_CORE_URL=\$ML_CORE_URL; echo TG_LONGPOLL_SEC=\$TG_LONGPOLL_SEC; echo TG_HTTP_READ_TIMEOUT=\$TG_HTTP_READ_TIMEOUT'" | Out-Null
Write-Capture -Path (Join-Path $BundleDir "snapshots/organizer-worker_ml_canon.txt") -Command "$ComposePrefix exec -T organizer-worker sh -lc 'echo ML_CORE_URL=\$ML_CORE_URL; ls -la /canon/intents_v2.yml'" | Out-Null

$asrUnavailable = (Select-String -Path (Join-Path $BundleDir "logs/telegram-bot.log") -Pattern "asr_unavailable" -AllMatches -ErrorAction SilentlyContinue).Count
$asrTimeout = (Select-String -Path (Join-Path $BundleDir "logs/telegram-bot.log") -Pattern "asr_timeout" -AllMatches -ErrorAction SilentlyContinue).Count
$asrEmpty = (Select-String -Path (Join-Path $BundleDir "logs/telegram-bot.log") -Pattern "asr_empty" -AllMatches -ErrorAction SilentlyContinue).Count
$llmInvalid = (Select-String -Path (Join-Path $BundleDir "logs/telegram-bot.log") -Pattern "llm_invalid_output" -AllMatches -ErrorAction SilentlyContinue).Count

Write-HeadMatches -Source (Join-Path $BundleDir "logs/telegram-bot.log") -Pattern "asr_unavailable" -Target (Join-Path $BundleDir "focus/examples_asr_unavailable.txt")
Write-HeadMatches -Source (Join-Path $BundleDir "logs/telegram-bot.log") -Pattern "asr_timeout" -Target (Join-Path $BundleDir "focus/examples_asr_timeout.txt")
Write-HeadMatches -Source (Join-Path $BundleDir "logs/telegram-bot.log") -Pattern "asr_empty" -Target (Join-Path $BundleDir "focus/examples_asr_empty.txt")
Write-HeadMatches -Source (Join-Path $BundleDir "logs/telegram-bot.log") -Pattern "llm_invalid_output" -Target (Join-Path $BundleDir "focus/examples_llm_invalid_output.txt")

$hyp1 = "- Not enough facts yet: check health/upstreams and log examples."
$hyp2 = "- Not enough facts yet: check ML-host (gateway/asr)."
if ($statusMlHealth -eq "FAIL" -or $statusMlDiag -eq "FAIL") {
    $hyp1 = "- Likely primary failure between VPS and ML-Gateway: /health or /diag/upstreams on 19000 failed."
} elseif ($asrTimeout -gt 0) {
    $hyp1 = "- Likely primary failure: timeout when calling gateway/ASR (asr_timeout in logs)."
} elseif ($asrUnavailable -gt 0) {
    $hyp1 = "- Likely primary failure: gateway unavailable or ASR upstream unavailable (asr_unavailable)."
} elseif ($asrEmpty -gt 0) {
    $hyp1 = "- Likely primary failure: ASR returned empty transcript (asr_empty)."
}
if ($llmInvalid -gt 0) {
    $hyp2 = "- Additional signal: invalid gateway payload detected (llm_invalid_output)."
}

$summary = @"
# Voice Diagnostic Summary ($Ts)

## What was checked
- [x] VPS metadata
- [x] Compose service state
- [x] Env keys only (no values)
- [x] API and ML health checks
- [x] Container logs and focused extracts

## Health status
- API 8101 /health: **$statusApi**
- ML 19000 /health: **$statusMlHealth**
- ML 19000 /diag/upstreams: **$statusMlDiag**

## Error markers in telegram-bot logs
- asr_unavailable: **$asrUnavailable**
- asr_timeout: **$asrTimeout**
- asr_empty: **$asrEmpty**
- llm_invalid_output: **$llmInvalid**

## Likeliest breakage (fact-based)
$hyp1
$hyp2
"@

$summary | Out-File -FilePath $SummaryPath -Encoding UTF8

if (Test-Path $ZipPath) {
    Remove-Item $ZipPath -Force
}
Compress-Archive -Path $BundleDir -DestinationPath $ZipPath -Force

Write-Output "Voice diagnostic bundle created:"
Write-Output "  dir: $BundleDir"
Write-Output "  archive: $ZipPath"
Write-Output "Health summary: API=$statusApi ML_HEALTH=$statusMlHealth ML_UPSTREAMS=$statusMlDiag"
Write-Output "Markers: asr_unavailable=$asrUnavailable asr_timeout=$asrTimeout asr_empty=$asrEmpty llm_invalid_output=$llmInvalid"
Write-Output "Likeliest breakage:"
Write-Output "  $hyp1"
Write-Output "  $hyp2"
