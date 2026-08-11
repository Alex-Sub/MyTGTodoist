# TUNNEL
Руководство по reverse tunnel для ML Gateway в текущем production-контуре.

## Clarification note
- Если `tg_compose=UNAVAILABLE`, но функциональные проверки Telegram (`tg_health`, `tg_to_ml`, `adapter_mode`, `tg_polling`) зелёные, это observability gap, а не критический runtime failure.

## 1) Актуальная топология
- VM ML Gateway слушает `127.0.0.1:19000`.
- На VPS endpoint туннеля: `127.0.0.1:19000`.
- Контейнеры на VPS ходят в ML через:
  - `http://host.docker.internal:19000`

## 2) Каноническая команда (ручной режим)
Запускать только на VM:
```bash
ssh -i ~/.ssh/id_ed25519 -NT -R 127.0.0.1:19000:127.0.0.1:19000 root@31.128.47.128
```

Важно:
- Старые примеры с VM `9000` для текущего контура устарели.
- Tunnel source всегда VM, не VPS.

## 3) Политика на период стабилизации
- Рекомендуемый режим: manual tunnel.
- `autossh/systemd` не включать до подтверждённых live Telegram тестов:
  - text;
  - voice.

## 4) Проверка (source of truth)
```bash
## VM
curl -fsS http://127.0.0.1:19000/health

## VPS
curl -fsS http://127.0.0.1:19000/health
```

Проверка доступности из контейнера VPS:
```bash
docker exec -i deploy-organizer-worker-1 python - << 'PY'
import requests
print(requests.get("http://host.docker.internal:19000/health").text)
PY
```

## 5) Типовые проблемы
- SSH начал запрашивать пароль:
  - проверить соответствие локального public key и `/root/.ssh/authorized_keys` на VPS.
- Tunnel поднят на VPS вместо VM:
  - пересобрать схему; запускать reverse tunnel с VM.
- `/health` доступен на VPS host, но недоступен из контейнера:
  - проверить `extra_hosts: host.docker.internal:host-gateway` в compose для runtime-контейнеров.
