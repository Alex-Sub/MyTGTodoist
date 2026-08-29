# PAP Organizer Runtime Command Contract

Status: Stage 3D-A.2 companion ingress, version 1

## Purpose and ownership

This is the narrow internal HTTP boundary used by PersonalAgentPlatform (PAP)
to ask Organizer to execute a domain command. The owner is `organizer-worker`.
The endpoint is not a Telegram adapter and does not format or deliver user
messages.

```text
PAP Router -> organizer-worker:8002 -> Organizer domain runtime -> Organizer DB
```

Port `8002` is an internal Docker-network port. No public port is required or
permitted for this contract. The legacy `telegram-bot` and legacy `asr-service`
are not part of the PAP ingress path.

## Request

`POST /runtime/command` with `Content-Type: application/json`.

Required fields:

```json
{
  "trace_id": "transport correlation identity",
  "idempotency_key": "stable source-command identity",
  "command": {
    "intent": "task.create",
    "entities": {
      "title": "task title",
      "text": "optional original command text",
      "source_msg_id": "optional stable source message identity",
      "parent_type": "optional existing parent type",
      "parent_id": "optional existing parent id"
    }
  }
}
```

`source` is optional metadata. When supplied it is an object and may contain
`channel`, `service`, `bot_code`, `chat_id`, `user_id`, and `timezone`. The
runtime copies `source.user_id` into command entities when the domain command
does not already contain a user identity. `idempotency_key` may be derived from
`command.entities.source_msg_id` or `trace_id` for non-PAP callers, but PAP must
send its stable key explicitly.

The current published companion supports the PAP-required `task.create`
intent. Its domain adapter accepts `title` (or the existing command text as a
fallback), optional `source_msg_id`, and the existing P2 parent fields. It
does not copy the entire PAP routing envelope into Organizer.

## Response

Successful creation returns HTTP `200`:

```json
{
  "ok": true,
  "outcome": "succeeded",
  "duplicate": false,
  "idempotency_key": "...",
  "trace_id": "...",
  "result_identity": {"entity_type": "task", "entity_id": "123"},
  "result": {"id": 123, "title": "task title"}
}
```

The same durable command submitted again returns HTTP `200` with
`outcome: "duplicate"`, `duplicate: true`, and the same
`result_identity`/domain result. A conflicting payload under the same key
returns HTTP `409` with `outcome: "rejected"` and
`error: "idempotency_conflict"`.

Other outcomes are:

- `400 rejected`: malformed request schema;
- `422 rejected`: unsupported intent or domain validation failure;
- `503 retryable_failure`: transient runtime failure or another request is
  still processing;
- `500 terminal_failure`: safe generic server failure.

Clarification is not emitted by this narrow published `task.create` adapter;
PAP remains responsible for any conversational clarification policy.

Organizer returns domain semantics only. Telegram confirmation formatting and
delivery remain PAP responsibilities.

## Idempotency and persistence

The Organizer DB owns table `runtime_command_dedup`, keyed by
`idempotency_key`. The request hash covers the source metadata and domain
command. An immediate SQLite reservation prevents two worker request paths
from entering the domain dispatcher for the same key. The completed response
is stored durably. If a worker restarts after the domain mutation but before
the HTTP response, `source_msg_id` recovery returns the existing task and
finalizes the durable dedup record.

Organizer's existing P2 runtime remains the domain authority and its task
store remains the only task store. PAP never needs Organizer DB credentials.

## Ownership boundaries

- No `getUpdates` or Telegram polling is reachable from this endpoint.
- No ASR call or model is reachable from this endpoint; voice is transcribed
  before PAP dispatches a domain command.
- The legacy Telegram poller remains separate and is not required for this
  ingress.

## Minimal startup and health

From the repository root, the minimal future runtime is:

```text
docker compose up -d database organizer-api organizer-worker
```

This starts the database, the existing API dependency, and the worker only;
it does not require `telegram-bot` or `asr-service`. Verify the internal
worker health endpoint from the Organizer network:

```text
GET http://organizer-worker:8002/health
-> {"ok": true}
```

Do not publish port `8002` to the host or expose it outside the private
service network.
