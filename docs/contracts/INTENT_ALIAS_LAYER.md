# INTENT_ALIAS_LAYER

Описание data-driven alias layer для естественных пользовательских фраз.

## 1) Purpose

Alias layer позволяет маппить natural phrases в существующие canonical intents без введения новых intent names.

## 2) Source file

- `canon/intent_aliases_v1.yml`

## 3) Current phase coverage

### `timeblock.create`
Examples:
- `запланируй мероприятие`
- `забронируй время`
- `забронируй слот`
- `выдели время`
- `выдели слот`
- `поставь блок`
- `нужен слот`

### `task.create`
Examples:
- `создай задачу`
- `добавь задачу`
- `запиши задачу`
- `напомни сделать ...`
- `надо сделать ...`
- `нужно позвонить ...`

## 4) Matching model

Conservative matching:
- strong phrase match if explicit phrase present;
- fallback match by subject/action patterns where intent is clear.

Anti-false-positive guardrails:
- generic bare words without actionable content should not auto-map (`надо`, `нужно`, `хочу`, `потом`).

## 5) Runtime behavior contract

Alias resolution is applied when parser result is `intent=unknown`.

If alias matched:
- runtime command intent is set to existing canonical intent;
- execution/clarification logic remains the same;
- no new business logic branch is introduced.

## 6) Dependencies

Decision layer loads YAML via `yaml.safe_load`.
`telegram-bot` runtime image must include `PyYAML`.

## 7) Related docs

- `docs/contracts/INTENT_CATALOG.md`
- `docs/architecture/DECISION_ENGINE_RULES.md`
- `docs/contracts/APP_RUNTIME_CONTRACT.md`
