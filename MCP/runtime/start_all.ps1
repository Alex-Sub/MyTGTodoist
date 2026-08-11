[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$cfg = & "$PSScriptRoot\runtime_config.ps1"

New-Item -ItemType Directory -Path $cfg.LogsDir -Force | Out-Null
New-Item -ItemType Directory -Path $cfg.RunDir -Force | Out-Null

function Start-Worker([string]$ScriptPath, [string]$PidFilePath, [string]$Name) {
    if (Test-Path $PidFilePath) {
        $raw = Get-Content -Path $PidFilePath -ErrorAction SilentlyContinue | Select-Object -First 1
        $existingPid = 0
        if ([int]::TryParse($raw, [ref]$existingPid)) {
            $existingProc = Get-Process -Id $existingPid -ErrorAction SilentlyContinue
            if ($existingProc) {
                Write-Host "$Name already running (PID=$existingPid)."
                return
            }
        }
    }

    $args = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", "`"$ScriptPath`"",
        "-Worker"
    )

    $proc = Start-Process -FilePath "powershell.exe" -ArgumentList $args -WindowStyle Hidden -PassThru
    "{0}" -f $proc.Id | Set-Content -Path $PidFilePath -Encoding UTF8
    Write-Host "$Name started. PID=$($proc.Id)"
}

Start-Worker -ScriptPath (Join-Path $PSScriptRoot "start_mcp.ps1") -PidFilePath $cfg.McpPidFile -Name "MCP worker"
Start-Worker -ScriptPath (Join-Path $PSScriptRoot "start_tunnel.ps1") -PidFilePath $cfg.TunnelPidFile -Name "Tunnel worker"

Start-Sleep -Seconds 2
& "$PSScriptRoot\healthcheck.ps1" | Format-List
