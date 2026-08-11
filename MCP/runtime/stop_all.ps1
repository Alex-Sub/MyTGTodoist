[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$cfg = & "$PSScriptRoot\runtime_config.ps1"

function Stop-WorkerByPidFile([string]$PidFilePath, [string]$Name) {
    if (-not (Test-Path $PidFilePath)) {
        Write-Host "${Name}: pid file not found."
        return
    }

    $raw = Get-Content -Path $PidFilePath -ErrorAction SilentlyContinue | Select-Object -First 1
    $targetPid = 0
    if (-not [int]::TryParse($raw, [ref]$targetPid)) {
        Write-Host "${Name}: invalid pid file content."
        Remove-Item -LiteralPath $PidFilePath -Force -ErrorAction SilentlyContinue
        return
    }

    $proc = Get-Process -Id $targetPid -ErrorAction SilentlyContinue
    if (-not $proc) {
        Write-Host "${Name}: process not found (PID=$targetPid)."
        Remove-Item -LiteralPath $PidFilePath -Force -ErrorAction SilentlyContinue
        return
    }

    if ($proc.ProcessName -notin @("powershell", "pwsh")) {
        Write-Host "${Name}: PID $targetPid is not a PowerShell runtime process. Skip."
        return
    }

    Stop-Process -Id $targetPid -Force -ErrorAction SilentlyContinue
    Write-Host "$Name stopped (PID=$targetPid)."
    Remove-Item -LiteralPath $PidFilePath -Force -ErrorAction SilentlyContinue
}

Stop-WorkerByPidFile -PidFilePath $cfg.McpPidFile -Name "start_mcp.ps1"
Stop-WorkerByPidFile -PidFilePath $cfg.TunnelPidFile -Name "start_tunnel.ps1"

Write-Host "Runtime stop command completed."
