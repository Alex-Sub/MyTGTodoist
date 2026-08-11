# OPERATOR_SMOKE_CHECK

Purpose: concise operator checklist for validating the current production contour after deploy or incident recovery.

Source/deploy model:

- local git/workspace is the deployable code source of truth
- `/opt/mytgtodoist` on VPS is a deployment mirror
- deploy verification must confirm runtime/container state, not only synced files

## 1. Process / Mode Check

Expected mode:

- Telegram adapter mode: `runtime_core_direct`
- worker legacy queue disabled

Useful commands:

```bash
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml ps
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 telegram-bot
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 organizer-worker
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 organizer-api
```

Verify:

- Telegram logs include `runtime_core_direct`
- no operational use of `compat_worker_bridge`
- no startup path that enables `ALLOW_LEGACY_WORKER_QUEUE=1`
- route logs should show task/calendar routing before any explicit memory fallback

## 2. HTTP Health Check

```bash
curl http://127.0.0.1:8101/health
curl http://127.0.0.1:8102/health
curl http://127.0.0.1:8101/google/health
```

Verify:

- API health returns `ok`
- worker health returns `ok`
- Google health returns a real probe result, not a placeholder stub

Build proof check:

1. send `/system`
2. verify the response includes:
   - `route_rules_version`
   - short `git_sha`
   - short `handler_sha256`
   - `build_timestamp_utc`
3. if available, inspect `/app/build-info.json` inside the running `telegram-bot` container

## 3. Runtime Sheets Dry-Run

For Alexey production tenant, use `deploy/tenants/.env.alexey` as the active env file.

```bash
docker compose -p deploy --env-file deploy/tenants/.env.alexey -f docker-compose.yml -f docker-compose.vps.override.yml run --rm google-sync \
  python -m src.google.runtime_sheets_sync --dry-run
```

Verify:

- command finishes without writing errors
- logs show per-tab counts for `Задачи`, `InBox`, and `Календарь`
- rerunning the same dry-run does not show phantom growth in `created`
- logs show the runtime path as canonical; no legacy `items` Sheets mode should start
- do not use Google Tasks sidebar as a validation source for this check; Google Tasks is not in the active runtime sync contour
- `Задачи` exposes the user-facing task screen
  - expected hierarchy columns:
    - `Уровень`
    - `Родитель ID`
    - `Родитель`
- `InBox` is a triage-only subset; its row count should be less than or equal to `Задачи`
  - expected hierarchy columns:
    - `Уровень`
    - `Родитель ID`
    - `Родитель`
  - subtasks with `parent_task_id` are excluded from `InBox` in the current triage filter
- `Календарь` exposes unified calendar rows
- row types may include:
  - `Блок времени`
  - `Встреча`
  - `Событие календаря`
- IDs in `Календарь` must be typed:
  - `timeblock:...`
  - `meeting_event:...`
  - `external_event:...`
- `Календарь` remains human-readable:
  - rows sorted by `Начало` ascending
  - helper columns visible:
    - `Дата`
    - `День недели`
    - `Время`
  - technical columns remain at the end:
    - `ID`
    - `Calendar Event ID`
    - `Источник`
- canonical tabs also receive Sheets usability formatting:
  - header row is frozen
  - filter is enabled
  - columns are auto-resized when the Google API accepts it
  - sortable date/datetime columns are real Sheets date/time cells, not plain text
- formatting errors must not block export success

## 4. Runtime Sheets Apply Check

Enable:

- `GOOGLE_SHEETS_SYNC_ENABLED=1`
- `GOOGLE_SHEETS_SPREADSHEET_ID=<spreadsheet id>`

Then either restart `google-sync` or run a one-shot apply:

```bash
docker compose -p deploy --env-file deploy/tenants/.env.alexey -f docker-compose.yml -f docker-compose.vps.override.yml run --rm google-sync \
  python -m src.google.runtime_sheets_sync
```

Verify:

- no duplicate rows appear after repeated runs
- existing rows are updated by internal `entity_id`
- Sheets acts only as a read model; no reverse apply path is exercised
- if Google Tasks sidebar differs from Sheets, treat that as expected unless a separate Google Tasks projection task has been implemented
- do not set `GOOGLE_SYNC_ALLOW_LEGACY_ITEMS_MODE=1` for this check
- deprecated `RuntimeTasks` / `RuntimeTimeBlocks` are not the canonical operator view and should not be used for validation
- `InBox` does not duplicate all of `Задачи` unless the runtime DB genuinely contains only unplanned triage tasks
- check live sorting in Google Sheets:
  - `Календарь` sorted by `Начало`
  - `Задачи` sorted/filterable by `План`
  - `Логи` sorted/filterable by `Время`

Optional workbook cleanup:

```bash
docker compose -p deploy --env-file deploy/tenants/.env.alexey -f docker-compose.yml -f docker-compose.vps.override.yml run --rm google-sync \
  python -m src.google.runtime_sheets_sync --cleanup-tabs --delete-deprecated
```

Verify after cleanup:

- workbook contains exactly:
  - `Задачи`
  - `InBox`
  - `Календарь`
  - `Логи`
- normal sync still succeeds
- deprecated tabs are deleted and not recreated by the canonical exporter

Optional reverse-sync comparison smoke:

```bash
python -m src.google.runtime_sheets_reverse_sync --dry-run
python -m src.google.runtime_sheets_reverse_sync --review
python -m src.google.runtime_sheets_reverse_sync --review --output /data/sheets_review.json
```

Preferred Windows operator wrapper:

```powershell
.\scripts\sheets_review_apply.ps1 -Mode review
.\scripts\sheets_review_apply.ps1 -Mode review -OutputPath /data/sheets_review.json
```

Verify:

- command is run through `google-sync`, not `organizer-api`
- command reads `Задачи`, `InBox`, `Календарь`
- default artifact path is persistent container storage: `/data/sheets_review.json`
- `--review` prints a Russian human-readable summary plus machine-readable JSON
- script prints the artifact path and saved change counters
- `--output` writes a review JSON artifact
- exit code `0` means no changes, `1` means proposed/confirm-required, `2` means conflict/invalid, `3` means runtime/tool error
- if `total_changes=0`, do not run apply
- output contains only review / comparison results
- no worker runtime command is called
- no runtime DB mutation occurs
- a new task row with blank `ID` and non-empty `Задача` should appear as `Новая задача` / `proposed_create`
  - if the same row has a valid `Родитель ID`, it should appear as subtask `proposed_create`
  - changing `Родитель ID` on an existing task should appear as `confirm_required`
  - editing `Родитель` display text should be `invalid`

Manual Sheets apply:

Current first safe apply path:

- `python -m src.google.runtime_sheets_reverse_sync --review --output /data/sheets_review.json`
- `python -m src.google.runtime_sheets_reverse_sync --apply /data/sheets_review.json --change-id chg-00001`
- `python -m src.google.runtime_sheets_reverse_sync --apply review.json --all-proposed`
- preferred Windows operator wrapper:
  - `.\scripts\sheets_review_apply.ps1 -Mode apply -ChangeId chg-00001`
- apply should use the same `/data/...json` artifact produced by review unless the operator explicitly overrides `-OutputPath`
- do not type placeholder angle brackets literally; use a real `change_id` returned by review
- applies only safe task changes:
  - `Задача`
  - `Комментарий`
  - `Статус`
  - new task rows from `Задачи` / `InBox` by explicit `ChangeId`
    - valid `Родитель ID` should map to worker `task.create` with `parent_task_id`
    - after successful create, the consumed blank-`ID` proposal row should be removed automatically if review metadata still matches
- must use worker runtime commands only
- direct SQLite writes remain forbidden
- timeblock / meeting / calendar apply remains blocked
- `confirm_required`, `conflict`, and `invalid` entries must not be applied
- wrapper script uses fixed env `deploy/tenants/.env.alexey`
- wrapper script does not expose bulk apply by default
- `--all-proposed` must not auto-create new tasks from blank-ID rows
- parent reassignment from Sheets must not be applied in this smoke; it stays blocked as `confirm_required`

Post-create cleanup verification:

- after successful `proposed_create`, rerun review
- expected:
  - no repeated `proposed_create` for the consumed row
  - sheet contains only the real runtime task row with assigned `ID`
- if a blank-`ID` row remains:
  - treat that as a cleanup failure in Sheets, not as a DB failure

Google Sheets Manual Review Button:

- button/menu is review-only
- safe label: `Проверить изменения`
- recommended MVP is instructions-only Apps Script
- button must not:
  - call apply
  - write to DB
  - expose bulk apply
  - process calendar / timeblock / meeting apply

Operator expectation after clicking the button:

1. run `.\scripts\sheets_review_apply.ps1 -Mode review`
2. inspect `change_id`
3. if needed, run `.\scripts\sheets_review_apply.ps1 -Mode apply -ChangeId chg-00001`

Telegram review command:

- send `/sheets_review`
- verify the reply shows:
  - total changes
  - proposed
  - confirm required
  - conflict
  - invalid
  - first few `change_id`
- verify the reply instructs operator to use:
  - `.\scripts\sheets_review_apply.ps1 -Mode apply -ChangeId chg-00001`
- verify no apply happens from Telegram
- internal review endpoint for this command:
  - `http://google-sync:8010/review`
  - health: `http://google-sync:8010/health`
- verified VPS local checks:
  - `http://127.0.0.1:8010/health`
  - `http://127.0.0.1:8010/review?limit=5`
- note:
  - in-container formatting was verified on VPS
  - one real live inbound Telegram message is still required as a final operator check

## 5. Worker Helper Endpoint Check

```bash
curl -X POST http://127.0.0.1:8102/runtime/task/search \
  -H "Content-Type: application/json" \
  -d '{"user_id":"u-smoke","target_hint":"test"}'
```

Verify:

- endpoint responds with valid JSON
- no server-side 500

## 6. Telegram Functional Smoke

Run these manually in Telegram:

1. `задача на завтра выдели время на нее`
2. reply `15`
3. reply `30`
4. press or type `Комментарий`
5. send `купить молоко`
6. confirm

Verify:

- flow stays in `timeblock.create`
- summary rerenders after comment update
- no `Не нашел подходящую задачу.` after comment input
- command can complete even without an existing matched task

Also run live Telegram review smoke:

1. `/sheets_review`

Expected response includes:

- `Проверка Google Sheets`
- `Всего изменений: ...`
- `Предложено: ...`
- `Нужно подтверждение: ...`
- `Конфликт: ...`
- `Некорректно: ...`

And verify:

- if changes exist, response includes `.\scripts\sheets_review_apply.ps1 -Mode apply -ChangeId ...`
- no apply buttons are shown
- no DB mutation is triggered

Duplicate-create smoke:

1. ensure there is an active undated root task `Позвонить врачу`
2. send `позвонить врачу`
3. verify Telegram shows duplicate warning before the normal create summary
4. verify warning text says `Похоже, такая задача уже есть в InBox:`
5. verify reply offers `Создать ещё одну?`

Dated duplicate comparison smoke:

1. ensure there is an active task `Купить хлеб` on the target day
2. send `купить хлеб завтра`
3. verify duplicate warning is day-based, not InBox-based

## 7. Explicit Search Regression Check

Run a task-targeting flow that intentionally requires a task lookup with an unmatched task reference.

Verify:

- explicit task-search/link flow still returns `Не нашел подходящую задачу.` when no match exists

## 8. Temporal Edit Smoke

Run:

1. create a meeting or timeblock
2. reopen edit flow
3. change date, time, duration, and comment

Verify:

- summary/final confirmation rerenders correctly
- duration quick choices `15/30/45/60` work
- stale callback taps do not corrupt the draft
- logs contain `temporal_edit_*` observability signals

## 9. Sync Conflict Smoke

If a conflict exists:

```bash
curl http://127.0.0.1:8101/sync/conflicts
```

Then resolve through the active action path and verify:

- Telegram/runtime shows the pending conflict
- worker `/runtime/sync_conflict/action` accepts the action
- conflict status changes and does not remain stuck as active

## 10. Commit Path Check

Worker logs should show:

- `runtime_intent_dispatch_start`
- `runtime_intent_dispatch_result`
- `runtime_commit_path_result`

Telegram/runtime logs should show:

- `telegram_text_route_entry`
- `runtime_route_selected`
- `memory_fallback_before_return`
- `generic_task_fallback_selected`
- `task_duplicate_precheck_start`
- `task_duplicate_precheck_result`
- `task_create_date_resolution`
- `temporal_edit_*`
- `task_parent_resolution`

Verify that:

- `timeblock.create` and `meeting.create` use calendar create commit path
- `meeting.update` uses calendar patch commit path
- `meeting.comment.update` uses calendar description patch commit path

## 11. Failure Signals That Matter

Escalate if any of these are seen:

- repeated `runtime_commit_path_result ... ok=false`
- repeated `legacy_route_used`
- active traffic using historical compat markers
- worker startup with legacy queue enabled unintentionally
- `google/health` consistently down
- repeated `runtime_sheets_sync failed`
- repeated Sheets exports creating duplicate rows instead of `updated` or `skipped`
- `google-sync` starting in legacy `full_bidir` / `items` Sheets mode without an explicit migration/debug decision
