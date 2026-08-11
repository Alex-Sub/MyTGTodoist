# ADAPTERS

Telegram adapter documentation for the active production contour.

## 1. Canonical Adapter Entry

- production module: `python -m src.integrations.telegram.bot_main`
- active mode: `runtime_core_direct`
- execution backend: `WORKER_COMMAND_URL`
- organizer read API backend: `ORGANIZER_API_URL`
- ML backend: `ML_GATEWAY_URL`

Historical:

- `telegram-bot/bot.py` is removed from the active contour
- compat bridge path is retired

## 2. Adapter Responsibilities

The Telegram adapter is responsible for:

- Telegram polling and update intake
- text/voice/callback normalization
- invoking the direct runtime handler
- binding clarification prompt metadata
- mapping runtime results into Telegram replies

The Telegram adapter is not responsible for:

- direct production DB writes
- bypassing worker execution
- final calendar commit logic

## 3. Runtime Bridge Role

`runtime_bridge.py` is the direct-only integration boundary between Telegram transport and the application runtime.

It does three things:

1. constructs the direct runtime handler from the current environment/settings
2. normalizes runtime input/output, including transport metadata
3. preserves worker endpoint routing because execution still happens through `organizer-worker /runtime/command`

It is not an alternative business-logic path.

## 4. Clarification And Session Routing

Session state is persisted in the local runtime DB and keyed by logical user context.

The adapter/runtime combination must support:

- clarification continuation
- summary rerendering
- button/text equivalence
- stale callback guards
- global cancel

## 5. Edit-Flow UX

Current active UX includes:

- temporal summary confirmation
- unified meeting/timeblock edit flow
- comment keep/clear actions
- duration quick choices `15/30/45/60`

Expected behavior:

- comment edits rerender summary and stop at confirmation
- duration quick choice updates the draft without bypassing confirmation
- callback labels must not leak into runtime draft values

## 6. Allocation Phrase Handling

Examples:

- `выдели время на задачу`
- `задача на завтра выдели время на нее`

Current adapter/runtime behavior:

- these phrases route to `timeblock.create`
- task hints from the phrase are optional, not mandatory
- the flow must remain in timeblock confirmation/edit states unless the user explicitly enters task-search/link flow

## 7. Transport Callback Contract

Callback format:

- `clarify:v1:<family>:<value>`

Current active families include:

- `temporal_edit`
- `temporal_duration`
- `temporal_comment`
- `meeting_update_duration`
- `timeblock_update_duration`

Callbacks are transport-equivalent to typed user replies.

## 8. Operational Notes

- `TELEGRAM_ADAPTER_MODE` must resolve to `runtime_core_direct`
- `ML_GATEWAY_URL` is required in direct mode
- `WORKER_COMMAND_URL` remains required because direct mode still executes through worker HTTP runtime

## 9. Removed / Deprecated References

Do not use as active guidance:

- `src/main.py`
- `src/api/*`
- `src/telegram/*`
- `telegram-bot/bot.py`
- `compat_worker_bridge`
