# MyTGTodoist Quick Start

## Быстрый запуск

0. Включить виртуальную машину.

1. Запуск Whisper на Windows.

```powershell
cd D:\VM\whispercpp
.\whisper-server.exe --host 0.0.0.0 --port 8020 --model ".\models\ggml-large-v3.bin" --no-gpu
```

Проверка 1:

```powershell
curl http://127.0.0.1:8020/health
```

2. Запуск LLM на Windows.

- открыть LM Studio
- загрузить модель
- включить Local Server на `1234`

Проверка 2:

```powershell
curl http://127.0.0.1:1234/v1/models
```

3. Проверка доступа к Whisper и LLM с VM.

```bash
curl http://192.168.91.1:8020/health
curl http://172.29.96.1:1234/v1/models
```

4. Запуск ML stack на VM.

```bash
cd /mnt/hgfs/VMShare/infra/ml-stack
docker compose up -d --build
docker compose ps
```

Проверка 4:

```bash
curl http://127.0.0.1:19000/health
curl http://127.0.0.1:19000/diag/upstreams
```

Норма: `asr=ok` и `llm=ok`.

5. Запуск tunnel VM -> VPS.

Ручной вариант:

```bash
ssh -i ~/.ssh/id_ed25519 -o IdentitiesOnly=yes -NT -R 127.0.0.1:19000:127.0.0.1:19000 root@31.128.47.128
```

Или через скрипт установки systemd:

```bash
cd /mnt/hgfs/VMShare/infra/ml-stack
bash scripts/install_reverse_tunnel_service.sh
sudo systemctl status ml-gateway-reverse-tunnel --no-pager
```

Проверка 5:

```bash
curl http://127.0.0.1:19000/health
```

6. Деплой TG на VPS.

Основной вариант:

```powershell
cd D:\My_AI_Prodgekt\MyTGTodoist
.\deploy_v2.ps1 -CopyEnv
```

Совместимый legacy-вариант:

```powershell
cd D:\My_AI_Prodgekt\MyTGTodoist
.\deploy.ps1 -CopyEnv
```

7. Проверка TG runtime.

```powershell
cd D:\My_AI_Prodgekt\MyTGTodoist
.\run.ps1 ps
.\run.ps1 health
.\run.ps1 logs
```

8. Проверка доступа `telegram-bot` к ML.

```bash
docker exec -i deploy-telegram-bot-1 python - << 'PY'
import requests
print(requests.get("http://host.docker.internal:19000/health").text)
PY
```

9. Финальная проверка.

- `http://127.0.0.1:8101/health` отвечает
- `telegram-bot` запущен
- в логах есть `runtime_core_direct`

## Скрипты из `D:\My_AI_Prodgekt\MyTGTodoist\scripts`

Основные:

- `scripts\start_baseline.ps1` — поднимает baseline целиком: ML, tunnel, TG, затем делает проверки.
- `scripts\check_baseline.ps1` — только проверка baseline без запуска.
- `scripts\baseline_doctor.ps1` — расширенная диагностика и verdict `OK/DEGRADED/FAIL`.
- `scripts\diag_ml_bundle.sh` — собирает ML-диагностику по `19000` и `8020`.
- `scripts\ops_status.sh` — быстрый production status.
- `scripts\ops_snapshot.sh` — snapshot compose, env keys и логов.

Быстрые команды:

```powershell
cd D:\My_AI_Prodgekt\MyTGTodoist
.\scripts\start_baseline.ps1
.\scripts\check_baseline.ps1
.\scripts\baseline_doctor.ps1
```

```bash
cd /opt/mytgtodoist
bash scripts/diag_ml_bundle.sh
bash scripts/ops_status.sh
bash scripts/ops_snapshot.sh
```
