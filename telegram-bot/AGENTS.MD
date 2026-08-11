# AGENTS.md

This application is the local recognition / ML / interpretation layer.

## Responsibilities

- text and speech understanding
- extraction of structured data
- normalization of inputs
- classification
- retrieval-assisted interpretation
- producing structured outputs for downstream execution

## Non-responsibilities

This layer must NOT:

- execute production actions
- update runtime databases directly
- orchestrate workers or scheduling
- manage retries or replay logic
- introduce side effects in external systems unless explicitly designed

## Output rules

- Always return structured, machine-readable outputs
- Prefer explicit fields over free-form text
- Mark uncertainty explicitly
- If required data is missing, return a clarification requirement instead of guessing

## Design rules

- Keep recognition separate from execution
- Do not embed runtime business logic unless explicitly defined
- Avoid hidden assumptions or implicit defaults
- Preserve explainability of extracted fields

## Data contract

- Outputs must match expected schemas
- Do not silently change schema without coordination
- Maintain backward compatibility where possible

## Quality rules

When modifying logic:
1. Keep outputs deterministic where possible
2. Update tests for extraction behavior
3. Make uncertainty visible
4. Avoid overfitting to specific phrasing

## Tool usage

- Use MCP/tools only for reading or assisting interpretation
- Do not trigger execution actions through tools unless explicitly allowed

## Output expectations

Always return:
- extracted fields
- confidence or uncertainty (if applicable)
- missing required fields (if any)