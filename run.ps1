param(
    [ValidateSet("up", "ps", "logs", "health", "help")]
    [string]$Command = "help",
    [string]$ProjectName = $env:PROJECT_NAME,
    [string]$EnvFile = $env:ENV_FILE,
    [string]$ExpectedDataVolume = $env:EXPECTED_DATA_VOLUME
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ProjectName)) {
    $ProjectName = "deploy"
}
if ([string]::IsNullOrWhiteSpace($EnvFile)) {
    $EnvFile = "deploy/tenants/.env.alexey"
}
if ([string]::IsNullOrWhiteSpace($ExpectedDataVolume)) {
    $ExpectedDataVolume = "${ProjectName}_db_data"
}

$ComposeFiles = @("-f", "docker-compose.yml", "-f", "docker-compose.vps.override.yml")
$GuardedServices = @("organizer-worker", "telegram-bot", "organizer-api", "google-sync")

function Test-EnvFileReady {
    param([string]$Path)

    Write-Output "[INFO] Using env file: $Path"

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        Write-Error "[FAIL] Env file is missing: $Path"
    }

    $lines = Get-Content -LiteralPath $Path
    foreach ($key in @("TELEGRAM_BOT_TOKEN", "GOOGLE_CALENDAR_ID")) {
        $match = $lines | Where-Object { $_ -match "^\s*$([regex]::Escape($key))=" } | Select-Object -First 1
        if (-not $match) {
            Write-Error "[FAIL] $key is missing in $Path"
        }
        $value = (($match -split "=", 2)[1]).Trim()
        if ([string]::IsNullOrWhiteSpace($value)) {
            Write-Error "[FAIL] $key is empty in $Path"
        }
        Write-Output "[OK] $key is set in $Path"
    }
    Write-Output "[OK] env file exists: $Path"
}

function Invoke-Compose {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )
    & docker compose -p $ProjectName --env-file $EnvFile @ComposeFiles @Args
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

function Get-DuplicateStackContainers {
    $lines = & docker ps --format '{{.ID}}`t{{.Names}}`t{{.Label "com.docker.compose.project"}}`t{{.Label "com.docker.compose.service"}}'
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    $dup = @()
    foreach ($line in $lines) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }
        $parts = $line -split "`t", 4
        if ($parts.Count -lt 4) {
            continue
        }
        $project = $parts[2]
        $service = $parts[3]
        if (($GuardedServices -contains $service) -and ($project -ne $ProjectName)) {
            $dup += $line
        }
    }
    return $dup
}

function Assert-NoDuplicateStacks {
    $dup = Get-DuplicateStackContainers
    if ($dup.Count -gt 0) {
        Write-Error "[ABORT] Found containers from a different compose project running in parallel:`n$($dup -join '`n')`n[ABORT] Stop duplicate stack first, then rerun with -p $ProjectName."
    }
}

function Test-HealthSingleWorker {
    $workers = & docker ps --filter 'label=com.docker.compose.service=organizer-worker' --format '{{.ID}}`t{{.Names}}`t{{.Label "com.docker.compose.project"}}'
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
    $rows = @($workers | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($rows.Count -ne 1) {
        Write-Error "[FAIL] Expected exactly 1 running organizer-worker, got $($rows.Count).`n$($rows -join '`n')"
    }

    $parts = $rows[0] -split "`t", 3
    if ($parts.Count -lt 3) {
        Write-Error "[FAIL] Unable to parse docker ps output for worker."
    }
    $cid = $parts[0]
    $project = $parts[2]
    if ($project -ne $ProjectName) {
        Write-Error "[FAIL] Worker project is '$project', expected '$ProjectName'."
    }

    $volume = & docker inspect $cid --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}'
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
    $volume = "$volume".Trim()
    if ($volume -ne $ExpectedDataVolume) {
        Write-Error "[FAIL] Worker /data volume is '$volume', expected '$ExpectedDataVolume'."
    }

    Write-Output "[OK] single worker + volume check passed: project=$project, worker=$cid, volume=$volume"
}

function Show-Usage {
    @"
Usage:
  .\run.ps1 up       # guard against duplicate stacks, then up -d --build
  .\run.ps1 ps       # docker compose ps (locked to -p deploy by default)
  .\run.ps1 logs     # logs --tail=200 organizer-worker telegram-bot organizer-api google-sync
  .\run.ps1 health   # verify exactly one worker and /data volume is deploy_db_data
"@ | Write-Output
}

switch ($Command) {
    "up" {
        Test-EnvFileReady -Path $EnvFile
        Assert-NoDuplicateStacks
        Invoke-Compose up -d --build
        Test-HealthSingleWorker
        break
    }
    "ps" {
        Test-EnvFileReady -Path $EnvFile
        Invoke-Compose ps
        break
    }
    "logs" {
        Test-EnvFileReady -Path $EnvFile
        Invoke-Compose logs --tail=200 organizer-worker telegram-bot organizer-api google-sync
        break
    }
    "health" {
        Test-HealthSingleWorker
        break
    }
    default {
        Show-Usage
        break
    }
}
