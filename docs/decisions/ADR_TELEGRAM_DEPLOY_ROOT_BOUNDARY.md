# ADR: Telegram Deploy Root Boundary

Status: Accepted  
Date: 2026-03-16

## Context
- Продакшен-деплой Telegram Organizer выполняется из репозитория `D:\My_AI_Prodgekt\MyTGTodoist`.
- Ранее возник рассинхрон: изменения direct runtime/direct voice были внесены в `D:\VMShare\app-integration`, но deploy шел из другого корня.
- Это привело к тому, что на VPS запускался старый код Telegram adapter/runtime bridge.

## Decision
- `D:\My_AI_Prodgekt\MyTGTodoist` — единственный source of truth для Telegram-бота и всего deploy-контура.
- `D:\VMShare\app-integration` рассматривается как отдельный ML/runtime слой и не используется как источник изменений для Telegram deploy-кода.
- Для Telegram-specific задач изменения вносятся только в `MyTGTodoist/telegram-bot/app-integration/...`.
- Массовое копирование между корнями запрещено. Разрешен только осознанный point-to-point porting конкретных файлов/изменений.

## Porting Tasks (into MyTGTodoist)
- Добавить `direct_voice_enabled` в `RuntimeBridge.__init__` и хранение в `self.direct_voice_enabled`.
- Добавить env loader:
  - `DIRECT_VOICE_ENABLED`
  - `DIRECT_ASR_TIMEOUT_SEC`
  - `DIRECT_ASR_RETRIES`
- Добавить `direct_asr_client.py`.
- Подключить wiring `GatewayAsrClient` в direct runtime builder.
- Добавить voice resilience/logging:
  - download started/succeeded/failed
  - ASR started/completed/failed
  - runtime outcome for voice
- Удалить авто voice->compat fallback внутри `runtime_core_direct` (compat только как явный rollback mode).

## Workflow Guardrails
- Перед любой Telegram-задачей проверять рабочий корень: `D:\My_AI_Prodgekt\MyTGTodoist`.
- Перед deploy сверять, что измененные файлы находятся в `telegram-bot/app-integration/...` внутри `MyTGTodoist`.
- Если изменение сначала сделано в другом дереве, переносить его как отдельную задачу porting с явным списком файлов и проверкой.

## Consequences
- Исключается скрытый рассинхрон между локальной разработкой и VPS deploy.
- Упрощается трассировка регрессий: одна кодовая база, один deploy-root, один путь в прод.
