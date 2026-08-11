# DOCS_INDEX

Purpose: map the active documentation set for MyTGTodoist after the `runtime_core_direct` cutover and worker modularization.

## Current Canonical Documents

Read these first for the active production contour:

- `architecture/CURRENT_PRODUCTION_ARCHITECTURE.md`
- `operations/OPERATOR_SMOKE_CHECK.md`
- `contracts/APP_RUNTIME_CONTRACT.md`
- `contracts/SYSTEM_SPEC.md`
- `engineering/ADAPTERS.md`
- `engineering/CODE STRUCTURE.md`
- `STARTUP_RUNBOOK_VERIFIED.md`

## Architecture

- `architecture/CURRENT_PRODUCTION_ARCHITECTURE.md`
  - concise source-of-truth for the live production contour
- `architecture/SYSTEM_OVERVIEW.md`
  - layer model and production boundaries
- `architecture/SERVICES_MAP.md`
  - service responsibilities and explicit non-responsibilities
- `architecture/EVENT_FLOW.md`
  - command, clarification, edit, sync-conflict, and Google health flows
- `architecture/RUNTIME_STATE_MACHINE.md`
  - active clarification and confirmation lifecycle
- `architecture/VOICE_PIPELINE.md`
  - Telegram voice/text/runtime path
- `architecture/REPOSITORY_STRUCTURE.md`
  - repository layout
- `architecture/DATA_MODEL.md`
  - runtime state tables
- `architecture/DECISION_ENGINE_RULES.md`
  - decision-layer rules
- `architecture/CONFIRM_BEFORE_COMMIT.md`
  - confirmation invariants
- `architecture/RUNTIME_GUARDS.md`
  - operational runtime guardrails

## Contracts

- `contracts/APP_RUNTIME_CONTRACT.md`
- `contracts/SYSTEM_SPEC.md`
- `contracts/INTENT_CATALOG.md`
- `contracts/INTENT_ALIAS_LAYER.md`
- `contracts/ML_API_USAGE.md`

## Engineering

- `engineering/ADAPTERS.md`
- `engineering/CODE STRUCTURE.md`
- `engineering/CODE_STRUCTURE.md`
  - alias/compat pointer to the canonical file above

## Operations

- `operations/OPERATOR_SMOKE_CHECK.md`
- `operations/MYTGTODOIST_RUNBOOK.md`
- `operations/TUNNEL.md`
- `STARTUP_RUNBOOK_VERIFIED.md`

## Development / Historical Inventory

- `development/LEGACY_INVENTORY_RUNTIME_CORE_DIRECT_2026-05-10.md`
  - active legacy/quarantine inventory
- `development/LEGACY_REMOVAL_PREP.md`
  - historical removal-prep record; compat removal is already complete
- `cleanup/LEGACY_OBSERVATION_CHECKLIST.md`
  - historical completed-state checklist
- `cleanup/COMPAT_ZERO_USAGE_REVIEW.md`
  - historical completed-state review

## Archived / Historical

- `Archive/*`
  - historical operational notes only; not source-of-truth
- `SCENARIOS_RUNTIME_V1.md`
  - historical scenario corpus; useful for product intent review, not for operator truth

## External Sources Of Truth

- `canon/intents_v2.yml`
  - canonical required-fields and intent registry for runtime validation
- `canon/intent_aliases_v1.yml`
  - alias-layer mapping
- `schemas/command_envelope.schema.json`
  - runtime envelope schema

## Update Rules

- If production routing changes:
  - update `architecture/CURRENT_PRODUCTION_ARCHITECTURE.md`
  - update `architecture/SERVICES_MAP.md`
  - update `engineering/ADAPTERS.md`
- If clarification/edit/confirmation behavior changes:
  - update `contracts/APP_RUNTIME_CONTRACT.md`
  - update `architecture/RUNTIME_STATE_MACHINE.md`
  - update `operations/OPERATOR_SMOKE_CHECK.md`
- If deploy/runtime commands change:
  - update `operations/MYTGTODOIST_RUNBOOK.md`
  - update `STARTUP_RUNBOOK_VERIFIED.md`
- If a document is historical only:
  - mark it explicitly as historical instead of letting it read like active operator guidance
