$RuntimeRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$McpRoot = Split-Path -Parent $RuntimeRoot
$TodoistRoot = Split-Path -Parent $McpRoot

$config = [pscustomobject]@{
    RuntimeRoot        = $RuntimeRoot
    McpRoot            = ${env:MCP_ROOT}
    TodoistRoot        = ${env:MCP_PROJECT_ROOT}
    PythonExe          = ${env:MCP_PYTHON_EXE}
    SshExe             = ${env:MCP_SSH_EXE}
    TunnelUserHost     = ${env:MCP_TUNNEL_USERHOST}
    SshKeyPath         = ${env:MCP_SSH_KEY_PATH}
    TunnelRemotePort   = ${env:MCP_TUNNEL_REMOTE_PORT}
    McpHost            = ${env:MCP_HOST}
    McpPort            = ${env:MCP_PORT}
    McpPath            = ${env:MCP_PATH}
    PublicMcpUrl       = ${env:MCP_PUBLIC_URL}
    RestartDelaySec    = ${env:MCP_RESTART_DELAY_SEC}
    LogsDir            = Join-Path $RuntimeRoot "logs"
    RunDir             = Join-Path $RuntimeRoot "run"
    McpPidFile         = Join-Path $RuntimeRoot "run\mcp_wrapper.pid"
    TunnelPidFile      = Join-Path $RuntimeRoot "run\tunnel_wrapper.pid"
    McpLogFile         = Join-Path $RuntimeRoot "logs\mcp_server.log"
    McpErrorLogFile    = Join-Path $RuntimeRoot "logs\mcp_server_error.log"
    McpWrapperLogFile  = Join-Path $RuntimeRoot "logs\mcp_wrapper.log"
    TunnelLogFile      = Join-Path $RuntimeRoot "logs\tunnel.log"
    HealthLogFile      = Join-Path $RuntimeRoot "logs\healthcheck.log"
}

if (-not $config.McpRoot)          { $config.McpRoot = $McpRoot }
if (-not $config.TodoistRoot)      { $config.TodoistRoot = $TodoistRoot }
if (-not $config.SshExe)           { $config.SshExe = "ssh" }
if (-not $config.TunnelUserHost)   { $config.TunnelUserHost = "root@31.128.47.128" }
if (-not $config.SshKeyPath)       { $config.SshKeyPath = "" }
if (-not $config.TunnelRemotePort) { $config.TunnelRemotePort = "9101" }
if (-not $config.McpHost)          { $config.McpHost = "127.0.0.1" }
if (-not $config.McpPort)          { $config.McpPort = "8765" }
if (-not $config.McpPath)          { $config.McpPath = "/mcp" }
if (-not $config.PublicMcpUrl)     { $config.PublicMcpUrl = "https://mcp.a-subbotin.online/mcp" }
if (-not $config.RestartDelaySec)  { $config.RestartDelaySec = "5" }

if (-not $config.PythonExe) {
    $venvPython = Join-Path $config.TodoistRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        $config.PythonExe = $venvPython
    } else {
        $config.PythonExe = "python"
    }
}

$config
