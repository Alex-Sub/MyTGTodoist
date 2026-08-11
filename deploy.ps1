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

$canonicalScript = Join-Path $PSScriptRoot "deploy_v2.ps1"
if (-not (Test-Path -LiteralPath $canonicalScript)) {
  throw "Canonical deploy script not found: $canonicalScript"
}

Write-Warning "deploy.ps1 is a compatibility wrapper. Canonical deploy script: deploy_v2.ps1"

& $canonicalScript `
  -VpsHost $VpsHost `
  -RemoteDir $RemoteDir `
  -ProjectName $ProjectName `
  -ComposeFile $ComposeFile `
  -ComposeOverrideFile $ComposeOverrideFile `
  -EnvFile $EnvFile `
  -SourceDir $SourceDir `
  -Services $Services `
  -CopyEnv:$CopyEnv `
  -CleanupRemoteJunk:$CleanupRemoteJunk

if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}
