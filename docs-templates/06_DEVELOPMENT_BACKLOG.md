# 06_DEVELOPMENT_BACKLOG

## 1) Видение
Система управления задачами и целями с жёстким разделением смыслового ML-слоя и детерминированного runtime-слоя, где состояние изменяется только в одном месте.

## 2) Вехи
- M1: Полная стабилизация runtime-only production контура.
- M2: Устойчивый voice pipeline через ML Gateway без format-related инцидентов.
- M3: Прозрачная управленческая аналитика по cycles/goals/tasks (digest + витрины).
- M4: Операционная зрелость: repeatable deploy/runbook без ручных отклонений.

## 3) Текущий спринт
- [ ] Закрыть период наблюдения 7 дней без критических рестартов.
- [ ] Проверить ежедневный digest и все list intents в Telegram UX.
- [ ] Провести smoke по calendar idempotency на повторных событиях.
- [ ] Верифицировать, что все сервисы используют единый `DB_PATH=/data/organizer.db`.
- [ ] Подтвердить корректный voice контракт (`/voice-command`) для Telegram audio форматов.

## 4) Архитектурные задачи
- [ ] Унифицировать документацию legacy schema (`schemas/command.schema.json`) vs runtime canon v2.
- [ ] Явно задокументировать versioning policy для `canon/intents_v2.yml`.
- [ ] Добавить авто-проверку mounts (`/canon`, `/app/migrations`) в preflight/CI.
- [ ] Формализовать regression suite для clarifying/disambiguation сценариев.

## 5) Операционные задачи
- [ ] Стандартизировать prod-checklist как исполняемый script (branch + volume + health).
- [ ] Ввести обязательный post-deploy report в формате OPS journal entry.
- [ ] Добавить alerting на недоступность ML tunnel и рост conflict queue.

## 6) Парковка идей (пока не в плане)
- Product analytics по эффективности cycle/goals.
- Web UX адаптер поверх текущего runtime API.
- Автоматический weekly management report из digest и goal metrics.
