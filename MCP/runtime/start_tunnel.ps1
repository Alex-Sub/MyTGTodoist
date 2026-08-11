[CmdletBinding()]
param(
    [switch]$Worker
)

$ErrorActionPreference = "Stop"
$cfg = & "$PSScriptRoot\runtime_config.ps1"

New-Item -ItemType Directory -Path $cfg.LogsDir -Force | Out-Null
New-Item -ItemType Directory -Path $cfg.RunDir -Force | Out-Null

function Write-Log([string]$Message) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [start_tunnel] $Message"
    Add-Content -Path $cfg.TunnelLogFile -Value $line -Encoding UTF8
}

if (-not $Worker) {
    $workerArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", "`"$PSCommandPath`"",
        "-Worker"
    )
    $proc = Start-Process -FilePath "powershell.exe" -ArgumentList $workerArgs -WindowStyle Hidden -PassThru
    "{0}" -f $proc.Id | Set-Content -Path $cfg.TunnelPidFile -Encoding UTF8
    Write-Host "Tunnel runtime worker started. PID=$($proc.Id)"
    exit 0
}

if ($cfg.TunnelUserHost -like "<user>*" -or $cfg.TunnelUserHost -like "*<user>*") {
    throw "Set MCP_TUNNEL_USERHOST (or edit runtime_config.ps1): current value '$($cfg.TunnelUserHost)' is placeholder."
}

$WorkerProcessId = $PID
Write-Log "Worker started. PID=$WorkerProcessId"

while ($true) {
    try {
        $sshArgs = @(
            "-N",
            "-R", "$($cfg.TunnelRemotePort):127.0.0.1:$($cfg.McpPort)",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            "-o", "TCPKeepAlive=yes",
            "-o", "ExitOnForwardFailure=yes"
        )

        if ($cfg.SshKeyPath -and (Test-Path $cfg.SshKeyPath)) {
            $sshArgs = @("-i", $cfg.SshKeyPath) + $sshArgs
            Write-Log "Using SSH key: $($cfg.SshKeyPath)"
        }

        $sshArgs += $cfg.TunnelUserHost

        Write-Log "Starting reverse tunnel to $($cfg.TunnelUserHost): remote $($cfg.TunnelRemotePort) -> 127.0.0.1:$($cfg.McpPort)"
        $nativePrefVar = Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue
        if ($nativePrefVar) { $script:PSNativeCommandUseErrorActionPreference = $false }
        try {
            & $cfg.SshExe @sshArgs 2>&1 | ForEach-Object {
                Add-Content -Path $cfg.TunnelLogFile -Value "$_" -Encoding UTF8
            }
        } finally {
            if ($nativePrefVar) { $script:PSNativeCommandUseErrorActionPreference = $nativePrefVar.Value }
        }

        $exitCode = $LASTEXITCODE
        Write-Log "SSH tunnel exited with code $exitCode"
    }
    catch {
        Write-Log "Tunnel wrapper error: $($_.Exception.Message)"
    }

    Start-Sleep -Seconds ([int]$cfg.RestartDelaySec)
    Write-Log "Restarting tunnel after delay..."
}
