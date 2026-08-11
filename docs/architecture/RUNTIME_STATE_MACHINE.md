# RUNTIME_STATE_MACHINE

State-machine summary for the active Telegram app runtime.

## 1. Boundaries

ML layer:

- interprets user input
- returns intent/entities/hints
- does not mutate production state

App runtime:

- owns clarification and confirmation state
- owns edit-flow continuation
- decides when a command is executable

Worker:

- owns final mutation and calendar commit side effects

## 2. Top-Level States

1. `RECEIVED`
2. `PARSED`
3. `NORMALIZED`
4. `GUARDED`
5. `CLARIFY_REQUIRED` or `CONFIRM_REQUIRED` or `READY_TO_EXECUTE`
6. `EXECUTED` or `CANCELLED` or `REJECTED`
7. `RESPONDED`

## 3. Active Clarification / Confirmation Families

Core create flow:

- `start_at_date`
- `start_at_time`
- `duration_minutes`
- `start_at_date_past_confirm`
- `temporal_commit_confirm`
- `temporal_edit_field`
- `task_create_confirm`
- `task_create_edit`

Update/edit flow:

- `awaiting_target_confirm`
- `awaiting_final_confirm`
- `meeting_update_*`
- `timeblock_update_*`
- `sync_conflict_resolution`

## 4. Temporal Create Flow

For `timeblock.create` and `meeting.create`:

1. normalize intent and source text
2. materialize date/time fields
3. ask missing temporal fields
4. apply past-date guard when needed
5. build temporal draft
6. render summary
7. wait for `temporal_commit_confirm`
8. execute only after explicit confirmation

Current clarification order:

- `start_at_date -> start_at_time -> duration_minutes`

## 5. Allocation-Phrase Branch

For phrases like `выдели время на задачу`:

1. route to `timeblock.create`
2. preserve optional task hint only
3. stay inside the temporal create state machine
4. do not switch into task-search fallback unless the flow is explicitly a task-link/search flow

This is the branch fixed by the latest `task_ref_optional` behavior.

## 6. Summary / Edit Loop

At `temporal_commit_confirm`:

- `Да` -> execute
- `Нет` or explicit field change -> `temporal_edit_field`
- callback or typed field selection is normalized into the same state transitions

While editing:

- runtime stores the active edit target in `__temporal_edit_active_field`
- duration quick actions `15/30/45/60` are accepted for supported duration states
- comment edits rerender summary and stop on confirmation
- stale callbacks are guarded and must not corrupt the current draft

## 7. Update Flow

For `meeting.update` and `timeblock.update`:

1. identify target
2. confirm target if required
3. choose field to change
4. capture new value
5. rerender final summary
6. confirm update
7. execute through worker

This flow is intentionally aligned across meeting and timeblock updates.

## 8. Global Cancel

If an active scenario exists and user sends a cancel phrase:

- clear the active dialog/session state
- do not execute
- return stop response

Cancel has priority over normal field parsing.

## 9. Button Equivalence

Telegram callback replies are transport equivalents of typed replies.

Supported current families:

- past-date yes/no
- temporal commit yes/no
- task create yes/no
- temporal duration quick picks
- update duration quick picks
- comment keep/clear actions

No button-only business branch is allowed.

## 10. Execution Boundary

The state machine is allowed to call worker `/runtime/command` only when:

- required fields are complete
- the active confirmation gate is satisfied
- no sync-conflict resolution branch blocks the command

## 11. Observability

Important runtime logs:

- `temporal_edit_*`
- summary rerender markers
- stale callback guards
- worker-side `runtime_commit_path_result`
