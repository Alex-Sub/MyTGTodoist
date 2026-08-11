[CmdletBinding()]
param(
    [switch]$Worker
)

$ErrorActionPreference = "Stop"
$cfg = & "$PSScriptRoot\runtime_config.ps1"

New-Item -ItemType Directory -Path $cfg.LogsDir -Force | Out-Null
New-Item -ItemType Directory -Path $cfg.RunDir -Force | Out-Null

function Write-Log([string]$Message) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [start_mcp] $Message"
    Add-Content -Path $cfg.McpWrapperLogFile -Value $line -Encoding UTF8
}

function Test-McpEndpoint {
    try {
        $url = "http://$($cfg.McpHost):$($cfg.McpPort)$($cfg.McpPath)"
        $body = '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"start_mcp_probe","version":"1"}}}'
        $resp = Invoke-WebRequest -Uri $url -Method POST -ContentType "application/json" -Body $body -TimeoutSec 8 -UseBasicParsing
        return ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300)
    } catch {
        return $false
    }
}

if (-not $Worker) {
    $workerArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", "`"$PSCommandPath`"",
        "-Worker"
    )
    $proc = Start-Process -FilePath "powershell.exe" -ArgumentList $workerArgs -WindowStyle Hidden -PassThru
    "{0}" -f $proc.Id | Set-Content -Path $cfg.McpPidFile -Encoding UTF8
    Write-Host "MCP runtime worker started. PID=$($proc.Id)"
    exit 0
}

$WorkerProcessId = $PID
Write-Log "Worker started. PID=$WorkerProcessId"

$ProbeStartupDelaySec = 3
$ProbeIntervalSec = 5
$MaxProbeFailures = 3

while ($true) {
    try {
        Set-Location $cfg.McpRoot

        $env:MCP_TRANSPORT = "http"
        $env:MCP_HOST = $cfg.McpHost
        $env:MCP_PORT = $cfg.McpPort
        $env:MCP_PATH = $cfg.McpPath
        $env:PYTHONPATH = $cfg.TodoistRoot

        Write-Log "Starting MCP server process: $($cfg.PythonExe) mcp_server.py"
        $McpProcess = Start-Process `
            -FilePath $cfg.PythonExe `
            -ArgumentList "mcp_server.py" `
            -WorkingDirectory $cfg.McpRoot `
            -WindowStyle Hidden `
            -RedirectStandardOutput $cfg.McpLogFile `
            -RedirectStandardError $cfg.McpErrorLogFile `
            -PassThru

        $McpProcessId = $McpProcess.Id
        Write-Log "MCP process started. PID=$McpProcessId"
        Start-Sleep -Seconds $ProbeStartupDelaySec

        $ConsecutiveProbeFailures = 0
        $startupProbeOk = Test-McpEndpoint
        if ($startupProbeOk) {
            Write-Log "Startup health probe OK."
        } else {
            $ConsecutiveProbeFailures = 1
            Write-Log "Startup health probe failed (1/$MaxProbeFailures)."
        }

        while ($true) {
            $CurrentMcpProcess = Get-Process -Id $McpProcessId -ErrorAction SilentlyContinue
            if (-not $CurrentMcpProcess) {
                Write-Log "MCP process is not running anymore. PID=$McpProcessId"
                break
            }

            if (Test-McpEndpoint) {
                if ($ConsecutiveProbeFailures -gt 0) {
                    Write-Log "Health probe recovered after previous failures."
                }
                $ConsecutiveProbeFailures = 0
            } else {
                $ConsecutiveProbeFailures += 1
                Write-Log "Health probe failed ($ConsecutiveProbeFailures/$MaxProbeFailures)."
                if ($ConsecutiveProbeFailures -ge $MaxProbeFailures) {
                    Write-Log "Health probe failure threshold reached. Restarting MCP process. PID=$McpProcessId"
                    Stop-Process -Id $McpProcessId -Force -ErrorAction SilentlyContinue
                    break
                }
            }

            Start-Sleep -Seconds $ProbeIntervalSec
        }
    }
    catch {
        Write-Log "MCP server wrapper error: $($_.Exception.Message)"
    }

    Start-Sleep -Seconds ([int]$cfg.RestartDelaySec)
    Write-Log "Restarting MCP server after delay..."
}
