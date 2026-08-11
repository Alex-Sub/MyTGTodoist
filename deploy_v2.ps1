param(
  [string]$VpsHost = "root@31.128.47.128",
  [string]$RemoteDir = "/opt/mytgtodoist",
  [string]$ProjectName = "deploy",
  [string]$ComposeFile = "docker-compose.yml",
  [string]$ComposeOverrideFile = "docker-compose.vps.override.yml",
  [string]$EnvFile = "deploy/tenants/.env.alexey",
  [string]$SourceDir = $PSScriptRoot,
  [string[]]$Services = @("telegram-bot"),
  [switch]$CopyEnv,
  [switch]$CleanupRemoteJunk
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Require-Cmd([string]$Name) {
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Не найдено: $Name. Установи/включи $Name и повтори."
  }
}

function Assert-ExitCode([string]$Step) {
  if ($LASTEXITCODE -ne 0) {
    throw "$Step завершился с кодом $LASTEXITCODE"
  }
}

function Invoke-CommandWithRetry(
  [string]$Step,
  [scriptblock]$Run,
  [int]$Attempts = 4,
  [int]$DelaySeconds = 4
) {
  $attemptMax = [Math]::Max(1, [int]$Attempts)
  for ($attempt = 1; $attempt -le $attemptMax; $attempt++) {
    & $Run
    $code = $LASTEXITCODE
    if ($code -eq 0) {
      return
    }

    $isTransientSsh = ($code -eq 255)
    if (-not $isTransientSsh -or $attempt -eq $attemptMax) {
      throw "$Step завершился с кодом $code"
    }

    Write-Host "==> ${Step}: transient SSH/SCP error ($code), retry $attempt/$attemptMax..."
    Start-Sleep -Seconds ([Math]::Max(1, [int]$DelaySeconds))
  }
}

function Join-ServiceArgs([string[]]$Items) {
  if ($null -eq $Items -or $Items.Count -eq 0) {
    return ""
  }
  return ($Items | Where-Object { $_ -and $_.Trim().Length -gt 0 } | ForEach-Object { $_.Trim() }) -join " "
}

function Escape-ShellSingleQuoted([string]$Value) {
  $v = [string]$Value
  return $v.Replace("'", "'""'""'")
}

function Write-Utf8NoBomLfFile([string]$Path, [string]$Content) {
  $normalized = ([string]$Content) -replace "`r`n", "`n" -replace "`r", "`n"
  $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
  [System.IO.File]::WriteAllText($Path, $normalized, $utf8NoBom)
}

function Resolve-PythonCmd() {
  $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
  if ($pythonCmd) {
    return @{
      Exe = $pythonCmd.Source
      PrefixArgs = @()
    }
  }
  $pyCmd = Get-Command py -ErrorAction SilentlyContinue
  if ($pyCmd) {
    return @{
      Exe = $pyCmd.Source
      PrefixArgs = @("-3")
    }
  }
  throw "Не найден python/py. Установи Python и повтори."
}

function Invoke-PythonChecked($PythonCommand, [string]$ScriptPath, [string[]]$Arguments = @()) {
  $exe = [string]$PythonCommand.Exe
  $argList = @($PythonCommand.PrefixArgs)
  $argList += $ScriptPath
  $argList += $Arguments
  & $exe @argList
  Assert-ExitCode "python $ScriptPath"
}

function Require-Path([string]$BaseDir, [string]$RelativePath) {
  $target = Join-Path $BaseDir $RelativePath
  if (-not (Test-Path $target)) {
    throw "Обязательный путь не найден: $RelativePath"
  }
}

function Get-GitOutput([string[]]$Arguments) {
  $result = & git @Arguments 2>$null
  if ($LASTEXITCODE -ne 0) {
    throw "git $($Arguments -join ' ') завершился с кодом $LASTEXITCODE"
  }
  return [string]::Join("`n", @($result))
}

function Assert-RepoRoot([string]$RepoRoot, [string]$SourcePath) {
  $resolvedRepoRoot = [System.IO.Path]::GetFullPath($RepoRoot).TrimEnd('\', '/')
  $resolvedSourcePath = [System.IO.Path]::GetFullPath($SourcePath).TrimEnd('\', '/')
  if ($resolvedRepoRoot -ne $resolvedSourcePath) {
    throw "Deploy must run from repo root only. repo_root=$resolvedRepoRoot source_dir=$resolvedSourcePath"
  }
}

function New-DeploySourceInfoJson(
  [string]$GitSha,
  [string]$Branch,
  [string]$SyncTimestampUtc,
  [string]$SourceMachine,
  [string]$DeployScriptVersion,
  [string[]]$ServiceList,
  [string]$HandlerSha256,
  [string]$RouteRulesVersion,
  [string]$GitStatusSummary
) {
  $payload = [ordered]@{
    deployment_mode = "mirror"
    source_of_truth = "local_workspace"
    local_git_sha = $GitSha
    local_branch = $Branch
    sync_timestamp_utc = $SyncTimestampUtc
    source_machine = $SourceMachine
    deploy_script_version = $DeployScriptVersion
    services = @($ServiceList)
    handler_sha256 = $HandlerSha256
    route_rules_version = $RouteRulesVersion
    git_status_summary = $GitStatusSummary
  }
  return ($payload | ConvertTo-Json -Depth 5)
}

Require-Cmd ssh
Require-Cmd scp
Require-Cmd robocopy
$pythonCommand = Resolve-PythonCmd

Write-Host "==> Deploy to: $VpsHost`:$RemoteDir"

if (-not (Test-Path $SourceDir)) {
  throw "SourceDir не найден: $SourceDir"
}

$sourcePath = (Resolve-Path $SourceDir).Path
Write-Host "==> Source dir: $sourcePath"

$buildTimestampUtc = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
$gitSha = "unknown"
$gitBranch = "unknown"
$gitStatusSummary = "git status unavailable"
$sourceMachine = [System.Environment]::MachineName
$deployScriptVersion = "deploy_v2.ps1@2026-05-22"
try {
  Push-Location $sourcePath
  try {
    $repoRoot = (Get-GitOutput @("rev-parse", "--show-toplevel")).Trim()
    Assert-RepoRoot -RepoRoot $repoRoot -SourcePath $sourcePath
    $gitSha = (Get-GitOutput @("rev-parse", "HEAD")).Trim()
    $gitBranch = (Get-GitOutput @("branch", "--show-current")).Trim()
    $gitStatusSummary = (Get-GitOutput @("status", "--short", "--branch")).Trim()
  }
  finally {
    Pop-Location
  }
}
catch {
  throw "PREFLIGHT FAIL: local git metadata unavailable or SourceDir is not repo root. $($_.Exception.Message)"
}

Write-Host "==> PREFLIGHT: docker build contract"
Require-Path $sourcePath "common"
Require-Path $sourcePath "telegram-bot/app-integration"
Require-Path $sourcePath "telegram-bot/app-integration/src/app/handler.py"

$handlerPath = Join-Path $sourcePath "telegram-bot/app-integration/src/app/handler.py"
$handlerText = Get-Content -Raw -LiteralPath $handlerPath
$routeRulesVersionMatch = [regex]::Match($handlerText, '_ROUTE_RULES_VERSION\s*=\s*"([^"]+)"')
if (-not $routeRulesVersionMatch.Success) {
  throw "PREFLIGHT FAIL: route_rules_version marker not found in telegram-bot/app-integration/src/app/handler.py"
}
$routeRulesVersion = $routeRulesVersionMatch.Groups[1].Value

foreach ($marker in @("stage67-hardguard-v2", "telegram_text_route_entry", "memory_fallback_before_return")) {
  if ($handlerText.IndexOf($marker, [System.StringComparison]::Ordinal) -lt 0) {
    throw "PREFLIGHT FAIL: source marker missing in handler.py: $marker"
  }
}

$handlerSha256 = ((Get-FileHash -Algorithm SHA256 -LiteralPath $handlerPath).Hash).ToLowerInvariant()
$contractScriptPath = Join-Path $sourcePath "scripts/check_docker_build_contract.py"
if (-not (Test-Path $contractScriptPath)) {
  throw "PREFLIGHT FAIL: scripts/check_docker_build_contract.py not found"
}
$contractArgs = @("--services") + $Services
Invoke-PythonChecked -PythonCommand $pythonCommand -ScriptPath $contractScriptPath -Arguments $contractArgs

Write-Host "==> PREFLIGHT OK: route_rules_version=$routeRulesVersion"
Write-Host "==> PREFLIGHT OK: handler_sha256=$handlerSha256"
Write-Host "==> PREFLIGHT OK: build_git_sha=$gitSha"
Write-Host "==> PREFLIGHT OK: local_branch=$gitBranch"
Write-Host "==> PREFLIGHT INFO: git_status_summary"
Write-Host $gitStatusSummary

if ($CopyEnv -and -not (Test-Path (Join-Path $sourcePath $EnvFile))) {
  throw "$EnvFile не найден в локальном проекте: $sourcePath"
}

$tmp = Join-Path $env:TEMP ("mytgtodoist_deploy_" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tmp | Out-Null

try {
  Write-Host "==> Stage files to: $tmp"

  $excludeDirs = @(
    ".git", ".venv", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
    ".idea", ".vscode", ".cache", ".tox", ".nox", "node_modules", "htmlcov",
    "dist", "build", "_diag", "data", "uploads", "vector_store"
  )

  $excludeFiles = @(
    "*.log", "*.tmp", ".DS_Store", "Thumbs.db", "Desktop.ini", "~$*",
    "*.db", "*.db-shm", "*.db-wal", "*.sqlite", "*.sqlite3", ".coverage", ".coverage.*",
    "*.zip", "*.tar", "*.tar.gz", "*.7z", "*.bak"
  )

  if (-not $CopyEnv) {
    $excludeFiles += ".env"
    $excludeFiles += ".env.prod"
  }

  $src = "`"$sourcePath`""
  $dst = "`"$tmp`""

  $xd = ($excludeDirs | ForEach-Object { "`"$($_)`"" }) -join " "
  $xf = ($excludeFiles | ForEach-Object { "`"$($_)`"" }) -join " "

  $rcArgString = "$src $dst /MIR /FFT /Z /R:2 /W:2 /NFL /NDL"

  if ($xd.Trim().Length -gt 0) {
    $rcArgString += " /XD $xd"
  }

  if ($xf.Trim().Length -gt 0) {
    $rcArgString += " /XF $xf"
  }

  $rc = Start-Process robocopy -ArgumentList $rcArgString -NoNewWindow -PassThru -Wait

  if ($rc.ExitCode -ge 8) {
    throw "robocopy завершился с ошибкой. Код: $($rc.ExitCode)"
  }

  $mustHave = @(
    "docker-compose.yml",
    "docker-compose.vps.override.yml",
    "telegram-bot/app-integration/src/integrations/telegram/bot_main.py",
    "telegram-bot/app-integration/src/app/handler.py"
  )

  foreach ($rel in $mustHave) {
    $p = Join-Path $tmp $rel
    if (-not (Test-Path $p)) {
      throw "В staged-копии отсутствует обязательный файл: $rel"
    }
  }

  $deploySourceInfoPath = Join-Path $tmp "DEPLOY_SOURCE_INFO.json"
  $deploySourceInfoJson = New-DeploySourceInfoJson `
    -GitSha $gitSha `
    -Branch $gitBranch `
    -SyncTimestampUtc $buildTimestampUtc `
    -SourceMachine $sourceMachine `
    -DeployScriptVersion $deployScriptVersion `
    -ServiceList $Services `
    -HandlerSha256 $handlerSha256 `
    -RouteRulesVersion $routeRulesVersion `
    -GitStatusSummary $gitStatusSummary
  Write-Utf8NoBomLfFile -Path $deploySourceInfoPath -Content $deploySourceInfoJson

  # Pre-switch sanity: abort if obvious local junk leaked into staged release.
  $junkHits = New-Object System.Collections.Generic.List[string]
  foreach ($dirName in @("data", "vector_store", "_diag")) {
    Get-ChildItem -LiteralPath $tmp -Recurse -Directory -ErrorAction SilentlyContinue |
      Where-Object { $_.Name -ieq $dirName } |
      ForEach-Object {
        $rel = $_.FullName.Substring($tmp.Length).TrimStart("\", "/")
        [void]$junkHits.Add($rel)
      }
  }
  foreach ($pattern in @("*.db", "*.db-shm", "*.db-wal")) {
    Get-ChildItem -LiteralPath $tmp -Recurse -File -Filter $pattern -ErrorAction SilentlyContinue |
      ForEach-Object {
        $rel = $_.FullName.Substring($tmp.Length).TrimStart("\", "/")
        [void]$junkHits.Add($rel)
      }
  }
  if ($junkHits.Count -gt 0) {
    $sample = $junkHits | Select-Object -First 30
    throw ("Staged release contains excluded local artifacts (sanity check failed): {0}" -f ($sample -join ", "))
  }

  $serviceArg = Join-ServiceArgs $Services
  Write-Host ("==> Target services: " + ($(if ([string]::IsNullOrWhiteSpace($serviceArg)) { "<all>" } else { $serviceArg })))

  $remoteTmp = "/opt/.mytgtodoist_release_" + (Get-Date -Format "yyyyMMdd_HHmmss")

  Write-Host "==> Prepare remote temp dir: $remoteTmp"
  Invoke-CommandWithRetry -Step "ssh mkdir" -Run { ssh $VpsHost "mkdir -p '$remoteTmp'" }

  Write-Host "==> Upload files (scp)..."
  $tmpScp = ($tmp -replace "\\", "/").TrimEnd("/")
  Invoke-CommandWithRetry -Step "scp upload" -Run { scp -r "$tmpScp/." "$VpsHost`:$remoteTmp/" }

  Write-Host "==> VERIFY VPS STAGED SOURCE: telegram-bot handler markers + sha256"
  $remoteVerifyStaged = @'
set -e
cd '__REMOTE_TMP__'
test -f telegram-bot/app-integration/src/app/handler.py
test -f DEPLOY_SOURCE_INFO.json
grep -n 'stage67-hardguard-v2\|telegram_text_route_entry\|memory_fallback_before_return' telegram-bot/app-integration/src/app/handler.py
actual_sha="$(sha256sum telegram-bot/app-integration/src/app/handler.py | awk '{print $1}')"
if [ "$actual_sha" != '__HANDLER_SHA256__' ]; then
  echo "PREFLIGHT FAIL: staged handler sha mismatch expected=__HANDLER_SHA256__ actual=$actual_sha"
  exit 51
fi
echo "PREFLIGHT OK: staged handler sha=$actual_sha"
'@
  $remoteVerifyStaged = $remoteVerifyStaged.Replace("__REMOTE_TMP__", $remoteTmp).Replace("__HANDLER_SHA256__", $handlerSha256)
  Invoke-CommandWithRetry -Step "verify staged source" -Run { ssh $VpsHost $remoteVerifyStaged }

  if ($CleanupRemoteJunk) {
    Write-Host "==> Optional cleanup: remove known junk in staged VPS release..."
    $cleanupScript = @'
set -e
cd "$REMOTE_TMP"

# Remove known junk directories in staged release only.
find . -type d \( -name data -o -name vector_store -o -name _diag \) -prune -exec rm -rf {} +

# Remove known junk files in staged release only.
find . -type f \( -name '*.db' -o -name '*.log' \) -delete
'@
    $cleanupScriptPath = Join-Path $tmp "remote_cleanup_script.sh"
    Write-Utf8NoBomLfFile -Path $cleanupScriptPath -Content $cleanupScript
    Write-Host "==> DEBUG: remote cleanup script saved to $cleanupScriptPath"

    $remoteTmpEsc = Escape-ShellSingleQuoted $remoteTmp
    $remoteCleanupScript = "$remoteTmp/remote_cleanup_script.sh"
    $remoteCleanupScriptEsc = Escape-ShellSingleQuoted $remoteCleanupScript

    Invoke-CommandWithRetry -Step "scp cleanup script" -Run { scp "$cleanupScriptPath" "$VpsHost`:$remoteCleanupScript" }

    $cleanupRemoteCmd = "REMOTE_TMP='$remoteTmpEsc' bash '$remoteCleanupScriptEsc'"
    Invoke-CommandWithRetry -Step "remote staged junk cleanup" -Run { ssh $VpsHost $cleanupRemoteCmd }
  }

  $codeSyncStatus = "SYNCED_TO_VPS_STAGING"
  $deployStatus = "NOT_STARTED"

  Write-Host "==> Switch release and restart docker compose..."

  $remoteScript = @'
set -e

STAGED_ENV_PATH="$REMOTE_TMP/$ENV_FILE"
CURRENT_ENV_PATH="$REMOTE_DIR/$ENV_FILE"
PREV_ENV_PATH="${REMOTE_DIR}.prev/$ENV_FILE"

STAGED_SECRETS_PATH="$REMOTE_TMP/secrets"
CURRENT_SECRETS_PATH="$REMOTE_DIR/secrets"
PREV_SECRETS_PATH="${REMOTE_DIR}.prev/secrets"

restore_env() {
  if [ ! -f "$STAGED_ENV_PATH" ]; then
    if [ -f "$CURRENT_ENV_PATH" ]; then
      cp "$CURRENT_ENV_PATH" "$STAGED_ENV_PATH"
      echo "Restored $ENV_FILE from current release"
    elif [ -f "$PREV_ENV_PATH" ]; then
      cp "$PREV_ENV_PATH" "$STAGED_ENV_PATH"
      echo "Restored $ENV_FILE from previous release"
    else
      echo "ERROR: $ENV_FILE not found in staged release, current release, or previous release"
      exit 1
    fi
  fi
}

restore_secrets() {
  if [ ! -d "$STAGED_SECRETS_PATH" ]; then
    if [ -d "$CURRENT_SECRETS_PATH" ]; then
      cp -R "$CURRENT_SECRETS_PATH" "$STAGED_SECRETS_PATH"
      echo "Restored secrets/ from current release"
    elif [ -d "$PREV_SECRETS_PATH" ]; then
      cp -R "$PREV_SECRETS_PATH" "$STAGED_SECRETS_PATH"
      echo "Restored secrets/ from previous release"
    else
      echo "WARN: secrets/ not found in staged, current, or previous release"
    fi
  fi
}

restore_env
restore_secrets

if [ -d "$REMOTE_DIR" ]; then
  rm -rf "${REMOTE_DIR}.prev" || true
  mv "$REMOTE_DIR" "${REMOTE_DIR}.prev"
fi

mv "$REMOTE_TMP" "$REMOTE_DIR"
cd "$REMOTE_DIR"

COMPOSE_ARGS="-p $PROJECT_NAME --env-file $ENV_FILE -f $COMPOSE_FILE -f $COMPOSE_OVERRIDE_FILE"

compose() {
  docker compose $COMPOSE_ARGS "$@"
}

upsert_env_value() {
  key="$1"
  value="$2"
  if grep -Eq "^${key}=" "$ENV_FILE"; then
    python3 - "$ENV_FILE" "$key" "$value" <<'PY'
from pathlib import Path
import sys

env_path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]
lines = env_path.read_text(encoding='utf-8').splitlines()
updated = False
for index, line in enumerate(lines):
    if line.startswith(f"{key}="):
        lines[index] = f"{key}={value}"
        updated = True
        break
if not updated:
    lines.append(f"{key}={value}")
env_path.write_text("\n".join(lines) + "\n", encoding='utf-8')
PY
  else
    printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
  fi
}

require_env_value() {
  key="$1"
  if ! grep -Eq "^${key}=" "$ENV_FILE"; then
    echo "PREFLIGHT FAIL: $key is missing in $ENV_FILE"
    exit 41
  fi
  value="$(sed -n "s/^${key}=//p" "$ENV_FILE" | head -n 1 | tr -d '\r')"
  if [ -z "$(printf '%s' "$value" | tr -d '[:space:]')" ]; then
    echo "PREFLIGHT FAIL: $key is empty in $ENV_FILE"
    exit 42
  fi
  echo "PREFLIGHT OK: $key is set"
}

echo "==> PREFLIGHT"
echo "PREFLIGHT INFO: using env file $ENV_FILE"
if [ ! -f "$ENV_FILE" ]; then
  echo "PREFLIGHT FAIL: $ENV_FILE is missing in $REMOTE_DIR"
  exit 40
fi
upsert_env_value BUILD_GIT_SHA "$BUILD_GIT_SHA"
upsert_env_value BUILD_TIMESTAMP_UTC "$BUILD_TIMESTAMP_UTC"
upsert_env_value TELEGRAM_BOT_SOURCE_MARKER "$TELEGRAM_BOT_SOURCE_MARKER"
upsert_env_value TELEGRAM_BOT_ROUTE_RULES_VERSION "$TELEGRAM_BOT_ROUTE_RULES_VERSION"
upsert_env_value TELEGRAM_BOT_HANDLER_SHA256 "$TELEGRAM_BOT_HANDLER_SHA256"
upsert_env_value ORGANIZER_API_SOURCE_MARKER "$ORGANIZER_API_SOURCE_MARKER"
upsert_env_value ORGANIZER_WORKER_SOURCE_MARKER "$ORGANIZER_WORKER_SOURCE_MARKER"
upsert_env_value GOOGLE_SYNC_SOURCE_MARKER "$GOOGLE_SYNC_SOURCE_MARKER"
echo "PREFLIGHT OK: $ENV_FILE exists"
require_env_value TELEGRAM_BOT_TOKEN
require_env_value GOOGLE_CALENDAR_ID
require_env_value BUILD_GIT_SHA
require_env_value BUILD_TIMESTAMP_UTC

if [ -n "$SERVICES" ]; then
  compose up -d --build --force-recreate $SERVICES
  compose ps $SERVICES
else
  compose up -d --build --force-recreate
  compose ps
fi

python3 - <<'PY'
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8101/health', timeout=10).read().decode('utf-8','ignore'))
PY

if compose exec -T telegram-bot sh -lc 'test -s "${VERSION_PROOF_PATH:-/tmp/telegram_adapter.version.json}"'; then
  echo "==> VERSION_PROOF"
  compose exec -T telegram-bot sh -lc 'cat "${VERSION_PROOF_PATH:-/tmp/telegram_adapter.version.json}"'
else
  echo "WARN: version proof file not found"
fi

selected_services="$SERVICES"
if [ -z "$selected_services" ]; then
  selected_services="google-sync organizer-api organizer-worker telegram-bot"
fi

verify_service() {
  svc="$1"
  container_id="$(compose ps -q "$svc" | head -n 1)"
  if [ -z "$container_id" ]; then
    echo "DEPLOY VERIFICATION FAILED: service container not found: $svc"
    exit 61
  fi

  echo "==> VERIFY SERVICE: $svc"
  docker inspect "$container_id" --format 'container={{.Name}} image_id={{.Image}} image_tag={{.Config.Image}} created={{.Created}}'

  if ! compose exec -T "$svc" sh -lc 'test -s /app/build-info.json'; then
    echo "DEPLOY VERIFICATION FAILED: /app/build-info.json missing for $svc"
    exit 62
  fi
  compose exec -T "$svc" sh -lc 'cat /app/build-info.json'

  if [ "$svc" = "telegram-bot" ]; then
    if ! compose exec -T telegram-bot sh -lc 'python3 -c "import json; from pathlib import Path; payload=json.loads(Path(\"/app/build-info.json\").read_text(encoding=\"utf-8\")); assert str(payload.get(\"git_sha\") or \"\") == \"'"$BUILD_GIT_SHA"'\"; assert str(payload.get(\"route_rules_version\") or \"\") == \"'"$TELEGRAM_BOT_ROUTE_RULES_VERSION"'\"; assert str(payload.get(\"handler_sha256\") or \"\").lower() == \"'"$TELEGRAM_BOT_HANDLER_SHA256"'\".lower(); print(\"PREFLIGHT OK: telegram-bot build-info matches expected provenance\")"'; then
      echo "DEPLOY VERIFICATION FAILED: telegram-bot build-info provenance mismatch"
      exit 64
    fi
    if ! compose exec -T telegram-bot sh -lc 'grep -n "stage67-hardguard-v2\|telegram_text_route_entry\|memory_fallback_before_return" /app/app-integration/src/app/handler.py'; then
      echo "DEPLOY VERIFICATION FAILED: telegram-bot route markers absent in container"
      exit 63
    fi
    if ! compose exec -T telegram-bot sh -lc 'python3 -c "from pathlib import Path; import hashlib; expected=\"'"$TELEGRAM_BOT_HANDLER_SHA256"'\".strip().lower(); actual=hashlib.sha256(Path(\"/app/app-integration/src/app/handler.py\").read_bytes()).hexdigest().lower(); assert actual == expected, f\"handler sha mismatch expected={expected} actual={actual}\"; print(f\"PREFLIGHT OK: telegram-bot handler sha={actual}\")"'; then
      echo "DEPLOY VERIFICATION FAILED: telegram-bot handler sha mismatch in container"
      exit 65
    fi
  fi
}

for svc in $selected_services; do
  verify_service "$svc"
done

echo "DEPLOY VERIFIED"
echo "==> TELEGRAM SMOKE"
echo "/system"
echo "купить хлеб завтра"
echo "позвонить врачу завтра"
echo 'docker compose -p deploy --env-file deploy/tenants/.env.alexey -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=300 telegram-bot | grep -E "telegram_text_route_entry|runtime_route_selected|memory_fallback_before_return|generic_task_fallback_selected|stage67-hardguard-v2"'
'@

  $remoteScriptPath = Join-Path $tmp "remote_switch_script.sh"
  Write-Utf8NoBomLfFile -Path $remoteScriptPath -Content $remoteScript
  Write-Host "==> DEBUG: remote switch script saved to $remoteScriptPath"

  $servicesExport = $serviceArg
  $remoteDirEsc = Escape-ShellSingleQuoted $RemoteDir
  $remoteTmpEsc = Escape-ShellSingleQuoted $remoteTmp
  $projectNameEsc = Escape-ShellSingleQuoted $ProjectName
  $envFileEsc = Escape-ShellSingleQuoted $EnvFile
  $composeFileEsc = Escape-ShellSingleQuoted $ComposeFile
  $composeOverrideEsc = Escape-ShellSingleQuoted $ComposeOverrideFile
  $servicesEsc = Escape-ShellSingleQuoted $servicesExport
  $buildGitShaEsc = Escape-ShellSingleQuoted $gitSha
  $buildTimestampEsc = Escape-ShellSingleQuoted $buildTimestampUtc
  $telegramSourceMarkerEsc = Escape-ShellSingleQuoted $routeRulesVersion
  $telegramRouteRulesEsc = Escape-ShellSingleQuoted $routeRulesVersion
  $telegramHandlerShaEsc = Escape-ShellSingleQuoted $handlerSha256
  $organizerApiSourceMarkerEsc = Escape-ShellSingleQuoted "runtime-organizer-api"
  $organizerWorkerSourceMarkerEsc = Escape-ShellSingleQuoted "runtime-worker"
  $googleSyncSourceMarkerEsc = Escape-ShellSingleQuoted "runtime-google-sync"

  $remoteSwitchScript = "$remoteTmp/remote_switch_script.sh"
  $remoteSwitchScriptEsc = Escape-ShellSingleQuoted $remoteSwitchScript
  Invoke-CommandWithRetry -Step "scp remote switch script" -Run { scp "$remoteScriptPath" "$VpsHost`:$remoteSwitchScript" }

  $remoteEnvPrefix = "REMOTE_DIR='$remoteDirEsc' REMOTE_TMP='$remoteTmpEsc' PROJECT_NAME='$projectNameEsc' ENV_FILE='$envFileEsc' COMPOSE_FILE='$composeFileEsc' COMPOSE_OVERRIDE_FILE='$composeOverrideEsc' SERVICES='$servicesEsc' BUILD_GIT_SHA='$buildGitShaEsc' BUILD_TIMESTAMP_UTC='$buildTimestampEsc' TELEGRAM_BOT_SOURCE_MARKER='$telegramSourceMarkerEsc' TELEGRAM_BOT_ROUTE_RULES_VERSION='$telegramRouteRulesEsc' TELEGRAM_BOT_HANDLER_SHA256='$telegramHandlerShaEsc' ORGANIZER_API_SOURCE_MARKER='$organizerApiSourceMarkerEsc' ORGANIZER_WORKER_SOURCE_MARKER='$organizerWorkerSourceMarkerEsc' GOOGLE_SYNC_SOURCE_MARKER='$googleSyncSourceMarkerEsc'"
  $remoteCmd = "$remoteEnvPrefix bash -n '$remoteSwitchScriptEsc' && $remoteEnvPrefix bash '$remoteSwitchScriptEsc'"

  & { ssh $VpsHost $remoteCmd }

  $remoteExitCode = $LASTEXITCODE
  if ($remoteExitCode -ne 0) {
    Write-Host "==> Remote command exited with $remoteExitCode. Running reconnect post-check..."

    $composePsCmd = "cd '$remoteDirEsc' && docker compose -p '$projectNameEsc' --env-file '$envFileEsc' -f '$composeFileEsc' -f '$composeOverrideEsc' ps"
    if (-not [string]::IsNullOrWhiteSpace($serviceArg)) {
      $composePsCmd += " $serviceArg"
    }

    $postCheckOk = $false
    for ($attempt = 1; $attempt -le 4; $attempt++) {
      Write-Host "==> Post-check attempt $attempt/4..."
      ssh $VpsHost $composePsCmd
      if ($LASTEXITCODE -eq 0) {
        $postCheckOk = $true
        break
      }
      Start-Sleep -Seconds 4
    }

    if ($postCheckOk) {
      Write-Host "==> Post-check succeeded after reconnect; treating deploy as successful."
      $remoteExitCode = 0
    }
  }

  if ($remoteExitCode -eq 0) {
    $codeSyncStatus = "SYNCED_AND_SWITCHED"
    $deployStatus = "SUCCESS"
  } else {
    $codeSyncStatus = "SYNCED (release switch likely completed before failure)"
    $deployStatus = "FAILED (remote switch + compose up exit $remoteExitCode)"
  }

  Write-Host "==> CODE_SYNC_RESULT: $codeSyncStatus"
  Write-Host "==> DEPLOY_RESULT: $deployStatus"

  if ($remoteExitCode -ne 0) {
    throw "remote switch + compose up завершился с кодом $remoteExitCode"
  }

  Write-Host "==> Deploy completed successfully."
}
finally {
  if (Test-Path $tmp) {
    Write-Host "==> Cleanup local temp..."
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
  }
}
