# VOICE_PIPELINE

Voice and text pipeline for the active Telegram runtime.

## 1. End-To-End Path

1. Telegram update
2. `update_mapper`
3. `runtime_bridge` in direct mode
4. ML gateway calls as needed
5. app runtime handler
6. worker `/runtime/command` only when executable
7. `reply_mapper`
8. Telegram response

## 2. Active Mode

- active mode: `runtime_core_direct`
- compat bridge path is retired

## 3. Clarification / Confirmation

Current active behavior:

- temporal create order: `date -> time -> duration`
- task-create confirm-before-commit
- temporal summary confirm-before-commit
- unified meeting/timeblock update edit flow

## 4. Allocation Phrase Support

Voice or text phrases such as `выдели время на задачу` route into `timeblock.create`.

Current behavior:

- task hint is optional for allocation-style phrasing
- comment/title can be free text
- summary must rerender after comment edit
- no task-search fallback unless the user is explicitly in task link/search flow

## 5. Buttons

Current button families:

- past-date yes/no
- temporal confirm yes/no
- task-create yes/no
- duration quick choices `15/30/45/60`
- comment keep/clear

Callbacks are normalized into the same logical continuation path as typed replies.

## 6. Observability

Important runtime markers:

- `temporal_edit_*`
- stale callback guards
- summary rerender markers
- worker-side `runtime_commit_path_result`
