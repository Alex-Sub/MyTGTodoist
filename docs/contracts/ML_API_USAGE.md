# ML_API_USAGE
Правила использования внешней ML-платформы приложением MyTGTodoist.

## 1) Роль MyTGTodoist
MyTGTodoist является клиентом внешней ML-платформы.

ML-платформа расположена во внешнем репозитории:
- `D:\VMShare\ml-core`

Граница ответственности:
- ML интерпретирует вход;
- MyTGTodoist runtime валидирует и исполняет бизнес-действие.

## 2) Явный profile binding
Приложение обязано явно отправлять:
- `ML_PROFILE_ID`
- `ML_PROFILE_VERSION`

Эти параметры задаются конфигурацией приложения и считаются обязательными для runtime path.

## 3) Запрет auto-upgrade
Приложение не должно:
- молча переключаться на `latest`;
- автоматически обновлять профиль при появлении новой версии;
- подменять версию при ошибке.

Версия профиля всегда контролируется app config (конфигом приложения), а не ML auto-discovery.

## 4) Optional diagnostics через registry
Приложение может (опционально) запрашивать registry endpoint ML для диагностики:
- проверка доступности `profile_id/profile_version`;
- отображение поддерживаемых версий в `/system`/ops-диагностике.

Ограничение:
- registry используется только для diagnostics/observability;
- auto-upgrade версии по registry запрещён.

## 5) Example: organizer request
Пример запроса MyTGTodoist в ML Gateway:

```json
{
  "request_id": "req-org-001",
  "app_id": "telegram-organizer",
  "profile_id": "organizer",
  "profile_version": "v2",
  "tenant_id": "personal",
  "user": {
    "user_id": "tg_1001",
    "role": "owner",
    "locale": "ru-RU",
    "timezone": "Europe/Moscow"
  },
  "input": {
    "type": "text",
    "text": "запланируй встречу завтра в 11"
  }
}
```

Ожидаемый ответ ML:

```json
{
  "status": "clarification_required",
  "intent": {
    "name": "timeblock.create",
    "confidence": 0.92
  },
  "entities": {
    "start_at": "2026-03-09T11:00:00+03:00",
    "duration_minutes": null
  },
  "clarification": {
    "required": true,
    "missing_field": "duration_minutes",
    "question": "На сколько минут поставить блок?"
  },
  "meta": {
    "request_id": "req-org-001",
    "profile_id": "organizer",
    "profile_version": "v2",
    "contract_version": "1.0.0"
  }
}
```

## 6) Clarification follow-up behavior
Когда пользователь отвечает на уточнение (`"30"`), MyTGTodoist:
- продолжает исходную команду runtime-side;
- не создаёт новую независимую команду;
- сохраняет исходный idempotency context.

Пример follow-up запроса в ML:

```json
{
  "request_id": "req-org-002",
  "app_id": "telegram-organizer",
  "profile_id": "organizer",
  "profile_version": "v2",
  "tenant_id": "personal",
  "user": {
    "user_id": "tg_1001",
    "role": "owner",
    "locale": "ru-RU",
    "timezone": "Europe/Moscow"
  },
  "input": {
    "type": "text",
    "text": "30"
  },
  "context": {
    "clarification_key": "telegram-organizer:personal:tg_1001",
    "pending_intent": "timeblock.create",
    "pending_missing_field": "duration_minutes"
  }
}
```
