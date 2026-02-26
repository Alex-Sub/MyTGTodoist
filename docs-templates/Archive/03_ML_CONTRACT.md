ML CONTRACT

Версия: согласовано с ML-core v1.4

## 1) Главный принцип

ML понимает, Runtime исполняет.

- ML-core отвечает за интерпретацию естественного языка.
- Runtime отвечает за валидацию и изменение состояния.
- Runtime не "додумывает" смысл, ML не выполняет команды.

## 2) Формат входа в Runtime (CommandEnvelope)

```json
{
  "trace_id": "...",
  "source": {...},
  "text": {...},
  "command": {
    "intent": "...",
    "confidence": 0.0,
    "entities": {...}
  }
}
```

## 3) Обязанности ML-core

- Определить один intent.
- Извлечь entities.
- При неоднозначности передать candidates.
- Не выбирать сам при низкой уверенности.

Пример `task_ref`:

```json
{
  "query": "...",
  "candidates": [...],
  "chosen_id": null,
  "ask": "..."
}
```

Если `chosen_id` отсутствует, Runtime задаёт уточняющий вопрос.

## 4) Обязанности Runtime

- Проверить обязательные поля по `canon/intents_v2.yml`.
- При нехватке данных вернуть один уточняющий вопрос.
- При достаточных данных выполнить intent детерминированно.
- Вернуть структурированный результат выполнения.

## 5) Поведение при неуверенности/отказе ML

Пороги Runtime:
- `CLARIFY_CONFIDENCE = 0.40`
- `EXECUTE_CONFIDENCE = 0.75`

Decision rule:
- Если `rejected=true` или `confidence < 0.40` -> safe-fail (без исполнения).
- Если есть `candidates` и не передан `chosen_id` -> всегда clarification (с `choices`).
- Если `0.40 <= confidence < 0.75` -> clarification (предпочесть уточнение, не исполнять).
- Если `confidence >= 0.75` -> проверить required поля; при missing вернуть clarification.
- Исполнение только при `confidence >= 0.75` и полном наборе required полей.

При safe-fail Runtime не выполняет действие и возвращает безопасный ответ:

`"Не могу выполнить. Уточните запрос."`

## 6) Формат ответа Runtime

Успех:

```json
{
  "ok": true,
  "user_message": "..."
}
```

Уточнение:

```json
{
  "ok": false,
  "clarifying_question": "...",
  "choices": [...]
}
```

## VPS Production Routing

- Голосовой endpoint ML: `/voice-command`.
- VPS не обращается напрямую к ASR/LLM/REC сервисам.
- Весь ML-трафик с VPS идёт только через ML-Gateway.
- Для production обязателен reverse tunnel:
- VPS `127.0.0.1:19000` -> local `127.0.0.1:9000`.
- В контейнерах используется:
- `ML_CORE_URL=http://host.docker.internal:19000`.
- Reverse tunnel является обязательной runtime-зависимостью для ML-функций в production.

## Clarification Contract (Runtime UX)

- Runtime возвращает ровно один `clarifying_question` на шаг.
- Если есть ambiguity candidates без `chosen_id`, Runtime всегда возвращает clarification (не execution).
- Если есть варианты, Runtime добавляет `choices` (`id`, `label`) и не делает авто-выбор.
