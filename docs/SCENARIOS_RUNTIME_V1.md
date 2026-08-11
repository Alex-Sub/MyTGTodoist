# SCENARIOS_RUNTIME_V1

Historical product-scenario corpus.

Status:

- not a source-of-truth for the current production contour
- retained as a legacy scenario/reference file

Use instead for active behavior:

- `docs/architecture/CURRENT_PRODUCTION_ARCHITECTURE.md`
- `docs/contracts/APP_RUNTIME_CONTRACT.md`
- `docs/architecture/RUNTIME_STATE_MACHINE.md`
- `docs/operations/OPERATOR_SMOKE_CHECK.md`

Important drift from this historical file:

- active runtime is `runtime_core_direct`
- worker is modularized behind `/runtime/*`
- unified update edit flow exists
- temporal edit observability exists
- allocation phrases now support optional task linkage
- worker may create a backing task automatically for optional-linkage timeblock flows
- duration quick-selection UX exists in active Telegram runtime
