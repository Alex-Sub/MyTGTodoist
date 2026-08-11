# EVENT_FLOW

Event-flow reference for the current production contour.

## 1. Canonical Command Flow

`Telegram update -> update_mapper -> runtime_bridge -> app handler -> worker /runtime/command -> runtime DB -> worker calendar commit path -> reply_mapper -> Telegram reply`

## 2. Voice/Text Flow

1. Telegram receives `message`, `voice`, or `callback_query`
2. `update_mapper` normalizes the transport payload
3. `runtime_bridge` builds a direct runtime request
4. app runtime calls ML as needed
5. app runtime either:
   - returns clarification/confirmation/edit response, or
   - emits executable command to worker
6. worker executes and may perform calendar commit side effects
7. result is mapped into Telegram response

## 3. Clarification Session Flow

Session storage:

- `telegram-bot/app-integration` local DB
- `clarification_sessions` table

Flow:

1. runtime determines a missing field or confirmation gate
2. runtime persists session payload with context key and idempotency lineage
3. user replies by text or callback
4. runtime resolves continuation against the persisted payload
5. runtime either:
   - re-asks
   - rerenders summary
   - executes
   - cancels

## 4. Temporal Create Flow

For `timeblock.create` and `meeting.create`:

1. parse and normalize
2. materialize date/time fields
3. ask missing fields in order
4. apply past-date guard if needed
5. render summary
6. wait for `temporal_commit_confirm`
7. execute worker mutation
8. worker calendar commit path runs for temporal flows

## 5. Allocation Phrase Flow

Examples:

- `выдели время на задачу`
- `задача на завтра выдели время на нее`

Current flow:

1. Telegram runtime recognizes allocation phrasing
2. intent is forced to `timeblock.create`
3. task hint is marked optional
4. clarification continues as temporal scheduling, not task search
5. unmatched optional task hint does not surface `Не нашел подходящую задачу.`
6. worker creates a backing task automatically if no concrete task link is available

## 6. Unified Edit Flow

Applies to:

- `meeting.update`
- `timeblock.update`

Flow:

1. locate target entity
2. confirm target if needed
3. enter edit-choice state
4. choose field:
   - date
   - time
   - duration
   - comment
   - date+time
5. accept text or callback input
6. rerender final summary
7. confirm update
8. commit through worker

Observability:

- `temporal_edit_*` logs on selection, value receipt, summary rerender, callback guards, and confirmation

## 7. Runtime Commit Path Flow

Worker logs:

- `runtime_intent_dispatch_start`
- `runtime_intent_dispatch_result`
- `runtime_commit_path_result`

Commit path mapping:

- `timeblock.create` -> `calendar_create`
- `meeting.create` -> `calendar_create`
- `meeting.update` -> `calendar_patch`
- `meeting.comment.update` -> `calendar_patch_description`
- non-calendar commands -> runtime state only

## 8. Sync Conflict Lifecycle

1. worker detects or stores a sync conflict
2. conflict is persisted in `sync_conflicts`
3. Telegram/runtime can surface the conflict in an active flow
4. action is sent to `POST /runtime/sync_conflict/action`
5. worker updates conflict status and related state
6. organizer-api exposes read-side visibility via `/sync/conflicts`

## 9. Google Health Flow

1. operator calls `GET /google/health` on `organizer-api`
2. API attempts a real Google Calendar probe event creation
3. API immediately cancels the probe
4. response reports `calendar_write` status and error details if any

This is an active topology check, not a dummy liveness stub.

## 10. Historical / Removed Flows

Not part of the active production path:

- `compat_worker_bridge`
- legacy monolith `src/main.py`
- legacy `src/api/*`
- legacy `src/telegram/*`
- `telegram-bot/bot.py`
- legacy queue/items request routing
