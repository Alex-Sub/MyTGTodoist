# MCP

Весь MCP-контур проекта находится в папке `MCP/`:

- `MCP/mcp_server.py`
- `MCP/runtime/*.ps1`
- `MCP/runtime/logs/`

## Запуск (1 команда)

```powershell
powershell -ExecutionPolicy Bypass -File D:\My_AI_Prodgekt\MyTGTodoist\MCP\runtime\start_all.ps1
```

## Проверка (1 команда)

```powershell
powershell -ExecutionPolicy Bypass -File D:\My_AI_Prodgekt\MyTGTodoist\MCP\runtime\healthcheck.ps1
```

Ожидаемый локальный endpoint MCP:

- `http://127.0.0.1:8765/mcp`

## Остановка (1 команда)

```powershell
powershell -ExecutionPolicy Bypass -File D:\My_AI_Prodgekt\MyTGTodoist\MCP\runtime\stop_all.ps1
```

## Логи

- `MCP/runtime/logs/mcp_server.log` (stdout mcp_server)
- `MCP/runtime/logs/mcp_server_error.log` (stderr mcp_server)
- `MCP/runtime/logs/mcp_wrapper.log` (события watchdog)
- `MCP/runtime/logs/tunnel.log`
- `MCP/runtime/logs/healthcheck.log`

## Изменение VPS/портов

Файл конфигурации: `MCP/runtime/runtime_config.ps1`

Ключевые параметры:

- `MCP_TUNNEL_USERHOST`
- `MCP_TUNNEL_REMOTE_PORT`
- `MCP_HOST`
- `MCP_PORT`
- `MCP_PATH`
- `MCP_PUBLIC_URL`
- `MCP_SSH_KEY_PATH`
