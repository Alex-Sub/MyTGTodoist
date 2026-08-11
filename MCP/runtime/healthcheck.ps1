[CmdletBinding()]
param(
    [switch]$CheckExternal
)

$ErrorActionPreference = "Stop"
$cfg = & "$PSScriptRoot\runtime_config.ps1"

New-Item -ItemType Directory -Path $cfg.LogsDir -Force | Out-Null

function Write-Log([string]$Message) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [healthcheck] $Message"
    Add-Content -Path $cfg.HealthLogFile -Value $line -Encoding UTF8
}

function Test-JsonRpcEndpoint([string]$Url) {
    try {
        $body = '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"healthcheck","version":"1"}}}'
        $resp = Invoke-WebRequest -Uri $Url -Method POST -ContentType "application/json" -Body $body -TimeoutSec 8 -UseBasicParsing
        return $resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300
    } catch {
        return $false
    }
}

function Test-PidFile([string]$PidFilePath) {
    if (-not (Test-Path $PidFilePath)) {
        return $false
    }

    $raw = Get-Content -Path $PidFilePath -ErrorAction SilentlyContinue | Select-Object -First 1
    $WorkerProcessId = 0
    if (-not [int]::TryParse($raw, [ref]$WorkerProcessId)) {
        return $false
    }

    return $null -ne (Get-Process -Id $WorkerProcessId -ErrorAction SilentlyContinue)
}

$localUrl = "http://$($cfg.McpHost):$($cfg.McpPort)$($cfg.McpPath)"
$mcpUp = Test-JsonRpcEndpoint -Url $localUrl
$tunnelProcUp = Test-PidFile -PidFilePath $cfg.TunnelPidFile
$tunnelExternalUp = $false

if ($CheckExternal) {
    $tunnelExternalUp = Test-JsonRpcEndpoint -Url $cfg.PublicMcpUrl
}

$status = [pscustomobject]@{
    timestamp         = (Get-Date).ToString("s")
    mcp_server        = if ($mcpUp) { "up" } else { "down" }
    tunnel            = if ($CheckExternal) {
        if ($tunnelProcUp -and $tunnelExternalUp) { "up" } else { "down" }
    } else {
        if ($tunnelProcUp) { "up" } else { "down" }
    }
    local_endpoint    = $localUrl
    external_endpoint = if ($CheckExternal) { $cfg.PublicMcpUrl } else { "not_checked" }
}

$line = "mcp_server=$($status.mcp_server); tunnel=$($status.tunnel); local=$($status.local_endpoint); external=$($status.external_endpoint)"
Write-Log $line
$status
