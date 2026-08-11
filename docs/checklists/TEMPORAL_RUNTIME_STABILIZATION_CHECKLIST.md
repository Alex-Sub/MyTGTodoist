# TEMPORAL_RUNTIME_STABILIZATION_CHECKLIST

Scope: active temporal create/update flows in the `runtime_core_direct` contour.

## Functional Checks

- [ ] allocation phrases like `выдели время на задачу` route to `timeblock.create`
- [ ] clarification order is `date -> time -> duration`
- [ ] time-only reply does not satisfy date
- [ ] past-date guard asks confirmation
- [ ] temporal summary renders before commit
- [ ] comment edit rerenders summary and stops at confirmation
- [ ] optional task linkage does not trigger task-search fallback in allocation flows
- [ ] explicit task-search/link flow still returns task-not-found when appropriate
- [ ] worker can complete allocation flow by creating a backing task when task link is optional

## Telegram UX Checks

- [ ] temporal confirm buttons are shown
- [ ] duration quick choices `15/30/45/60` are shown where supported
- [ ] typed reply and callback reply are equivalent
- [ ] comment keep/clear actions work
- [ ] stale callback taps do not corrupt the draft

## Update/Edit Checks

- [ ] `meeting.update` supports date/time/duration/comment/date+time edits
- [ ] `timeblock.update` supports date/time/duration/comment/date+time edits
- [ ] both flows rerender final summary before commit

## Operational Checks

- [ ] Telegram logs include `temporal_edit_*`
- [ ] worker logs include `runtime_commit_path_result`
- [ ] no accidental use of compat or legacy queue paths
