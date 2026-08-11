# COMPAT_ZERO_USAGE_REVIEW

Historical record.

Status:

- compat retirement completed
- document retained only as evidence that active production now runs direct-only

## Historical Outcome

- no required compat traffic remained
- no active rollback procedure depends on compat mode
- current supported production mode is `runtime_core_direct`

## Current Rule

Any new reference to `compat_worker_bridge` in active runbooks, compose overrides, or operator procedures should be treated as stale documentation and removed.
