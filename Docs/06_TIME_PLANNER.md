# P7 - Minimal Time Planner (v1)
Version: v1.0
Status: draft

## 1. Scope
Minimal time planner on top of existing tasks.

Non-goals:
- no auto planning;
- no priorities;
- no optimization.
- assistant_planner can suggest a day plan from aggregates, but does not create blocks automatically.

## CANON v1
- time_blocks do not change `tasks.state`
- time_blocks do not touch Calendar
- blocks stay within one local day (no cross-day)
- storage: UTC; UI: local `TIMEZONE_NAME`
- preset base time: NOW (round up to nearest 10 minutes)
- overlap rule: no overlaps within a day

## 2. Entity: time_block
Stores allocated time for a task.

Fields:
- `id` (INTEGER, PK)
- `task_id` (INTEGER, FK -> tasks.id)
- `start_at` (TEXT, ISO 8601, UTC)
- `end_at` (TEXT, ISO 8601, UTC)
- `created_at` (TEXT, ISO 8601, UTC)

## 3. Invariants
1. `start_at < end_at`.
2. No overlaps by day for one user.
   Single-user runtime: applies to all blocks.
3. A block must not cross local day boundary.

## 4. Commands (worker, write)
Feature flag: `P7_MODE=off|on`. When `off`, commands are unavailable.

### 4.1 Add block
`POST /p7/commands/add_block`

Payload:
- `task_id` (int, required)
- `start_at` (ISO 8601, required)
- `end_at` (ISO 8601, required)
- `source_msg_id` (string, optional)

Rules:
- validate `start_at < end_at`;
- validate overlaps on local day of `start_at`;
- `end_at` must be in the same local day.

Response: created `time_block`.

### 4.2 Move block
`POST /p7/commands/move_block`

Payload:
- `block_id` (int, required)
- `start_at` (ISO 8601, required)
- `end_at` (ISO 8601, required)
- `source_msg_id` (string, optional)

Rules:
- same invariants as create;
- overlap check excludes current `block_id`.

Response: updated `time_block`.

### 4.3 Delete block
`POST /p7/commands/delete_block`

Payload:
- `block_id` (int, required)
- `source_msg_id` (string, optional)

Response: `{ "deleted": true, "block_id": ... }`.

## 5. API (read-only)
Feature flag: `P7_MODE=off|on`. When `off`, endpoint is unavailable.

### 5.1 Day view
`GET /p7/day?date=YYYY-MM-DD`

Returns blocks intersecting local day `date`,
sorted by `start_at`, then `id`.

Response:
```json
{
  "date": "YYYY-MM-DD",
  "timezone": "UTC+03:00",
  "blocks": [
    { "id": 1, "task_id": 10, "start_at": "...", "end_at": "...", "created_at": "..." }
  ]
}
```

`timezone` = local fixed offset (see `LOCAL_TZ_OFFSET_MIN`).

## 6. Telegram MVP
Feature flag: `P7_MODE=off|on`. When `off`, buttons are hidden.

From `/today`:
- button "📍 Allocate time" near a task;
- quick presets: 30m / 60m / 90m.

Logic:
- start = NOW (round up to nearest 10 minutes);
- end = `start + preset`;
- overlap errors are returned to user.
