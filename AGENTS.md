# LLM Agents Registry — MyTGTodoist

This document defines all LLM agents used in the project, their responsibilities,
interfaces, and data boundaries.

The system uses multiple LLM agents with strict separation of concerns.

---

## Agent: command_parser

**Purpose**  
Parse short user inputs (mainly voice → ASR output) into a single executable command.

**Responsibilities**
- Convert natural language into a structured command
- Normalize dates, times, priorities
- Detect missing data and request clarification
- NEVER execute logic or reason about projects

**Input**
- ASR text (string)
- Current datetime (ISO 8601, Europe/Moscow)

**Output**
- Strict JSON according to the Command Schema
- Exactly one intent per request

**Allowed intents**
- create_task
- update_task
- complete_task
- delete_task
- create_event
- update_event
- delete_event
- list_tasks
- list_events
- add_note
- unknown

**Rules**
- Output MUST be valid JSON
- No explanations, no markdown, no text outside JSON
- No access to task lists, projects, or personal history
- Deterministic behavior preferred (low temperature)

**Current provider**
- OpenRouter (model: openrouter/free)

**Future provider**
- Local LLM (planned)

---

## Agent: assistant_planner

**Purpose**  
Help the user review tasks, projects, and events, and propose next actions.

**Responsibilities**
- Summarize current workload
- Highlight risks, deadlines, overload
- Propose concrete next steps
- Generate suggested commands (but NOT execute them)

**Input**
- Aggregated tasks, projects, events
- User request (optional)

**Output**
- Structured JSON with:
  - summary
  - now_focus
  - today_plan
  - risks
  - questions
  - suggested_command objects (compatible with command_parser schema)

**Rules**
- Do not invent facts
- Do not execute commands
- Reasoning is allowed but must be reflected only in structured output
- Personal data handling must be explicit

**Current provider**
- OpenRouter (model: openrouter/free)

**Future provider**
- Local LLM (preferred for personal data)

---

## Data policy

- command_parser:
  - Receives ONLY raw command text
  - No personal data, no task history

- assistant_planner:
  - May receive aggregated personal data
  - Will be migrated to local LLM when stable

---

## Routing rules

- Voice input → command_parser
- Short imperative requests → command_parser
- Requests for overview, planning, suggestions → assistant_planner

---

## Notes

This file is a contract.
Any new agent must be added here before implementation.
