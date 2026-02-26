# 00_DOCS_MAP

What: Каноническая карта документации MyTGTodoist.
When: Читать первым при входе в проект и перед изменением документации.
Not: Не заменяет runtime/canon/schemas.

## Single Source of Truth
- Каноническая папка документации: `D:\My_AI_Prodgekt\MyTGTodoist\docs-templates`
- Историческая папка `docs/` больше не является первичным источником.
- Все новые правки и handoff-обновления вносятся только в `docs-templates/`.

## Canon File Set
- `00_DOCS_MAP.md`: карта документации и правила обновления.
- `00_PHILOSOPHY.md`: продуктовая/управленческая философия системы.
- `01_canon.MD`: ML/Runtime контракт и канон входного CommandEnvelope.
- `02_RUNTIME_SPEC.md`: runtime-инварианты, disambiguation, стратегический слой, UX copybook.
- `03_adapters.md`: транспортный и UX-слой (Telegram/API/voice).
- `04_SYSTEM_OVERVIEW.md`: слои, границы, data-flow.
- `05_docs.md`: операционный runbook, деплой, прод-ограничения, troubleshooting.
- `05_ROADMAP_AND_PROGRESS.md`: текущий статус, журнал прогресса и инциденты.
- `06_DEVELOPMENT_BACKLOG.md`: backlog по архитектуре/продукту/операциям.
- `ML_STACK_CANON.md`: короткий master canon для быстрого входа в контекст.

## External Specs (authoritative)
- `canon/intents_v2.yml`: канон интентов и обязательных полей runtime.
- `schemas/command_envelope.schema.json`: JSON schema для входа в runtime.
- `schemas/command.schema.json`: legacy schema для parser/compat use-cases.

## Update Rules
- При изменении интента сначала обновлять `canon/intents_v2.yml`, затем синхронизировать `01_canon.MD` и `02_RUNTIME_SPEC.md`.
- При изменении прод-контура сначала обновлять `05_docs.md`, затем `ML_STACK_CANON.md`.
- Если меняется архитектурная граница слоя, обязательно обновлять `04_SYSTEM_OVERVIEW.md`.
- Любой инцидент в проде фиксируется в `05_ROADMAP_AND_PROGRESS.md`.
