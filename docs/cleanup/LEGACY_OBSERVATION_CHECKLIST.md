# LEGACY_OBSERVATION_CHECKLIST

Historical completed-state checklist.

Status:

- compat retirement completed
- direct runtime contour active
- keep this file only as historical evidence for future cleanup passes

## Historical Signals

- `mode=runtime_core_direct`
- `mode=compat_worker_bridge`
- `path=direct`
- `path=compat`

## Current Interpretation

If any active production traffic still shows compat markers, treat that as a regression or stale automation issue, not as supported behavior.

## Active Legacy Risk That Still Matters

The only meaningful remaining quarantine risk in the worker contour is:

- legacy queue/items startup behind `ALLOW_LEGACY_WORKER_QUEUE=1`

Do not enable it in production.
