# ADR 0015: Public Verification Task Handle Uses Creator and Bundle ID

## Status

Accepted

## Context

The first HTTP API exposed `TaskId` as the public verification polling handle:

```http
POST /proof-bundles -> { "task_id": "...", "submitted_at": "..." }
GET /verification-tasks/{task_id}
```

That made implementation simple, but it gave clients a second public identifier for the same viewer journey. The domain model already distinguishes the identifiers:

- `TaskId` is a server-generated operational UUID for short-lived Lock Server runtime work.
- `BundleId` is viewer-generated, durable, and used with the creator identity to anchor the verified proof bundle and credential issuance flow.

Credential issuance already uses `{ creator, bundle_id }`. The viewer also already knows the Bundle ID and can derive the creator from the submitted `pubky_lock_resource`. Making Task ID the public polling handle forces clients to retain an extra operational value that is not useful for credible exit and does not match the later access-credential API.

## Decision

Use `{ creator, bundle_id }` as the public verification attempt handle.

`TaskId` remains in the system only as an internal Lock Server runtime/correlation identifier. It may appear in:

- `VerificationTaskRecord`
- Postgres primary keys and worker claim/update internals
- worker/completion use cases
- internal logs/correlation, provided logs do not expose bearer-secret values

`TaskId` must not appear in public HTTP API routes, request bodies, responses, or public API docs as a client polling handle.

### Public submission response

`POST /proof-bundles` creates or finds a verification task for the submitted proof bundle and returns a public lifecycle view. It does not return `task_id`.

Response shape:

```json
{
  "creator": "pubky...",
  "bundle_id": "000G40R40M30E209185GR38E1W",
  "status": "pending",
  "submitted_at": "2026-05-29T12:00:00Z",
  "started_at": null,
  "completed_at": null,
  "failure_message": null
}
```

### Public polling endpoint

Public polling uses a JSON body so the bearer-secret-like Bundle ID does not appear in URL paths or query strings:

```http
POST /verification-task-lookups
{
  "creator": "pubky...",
  "bundle_id": "000G40R40M30E209185GR38E1W"
}
```

It returns the same public lifecycle view as submission.

A missing `{ creator, bundle_id }` returns the existing verification-task-not-found API error.

### Public lifecycle view

The public lifecycle view contains:

- `creator`
- `bundle_id`
- `status`
- `submitted_at`
- `started_at`
- `completed_at`
- `failure_message`

It does not contain:

- `task_id`
- raw submitted proof material
- raw access credentials
- verified proof bundle evidence
- worker claim metadata

`failure_message` is viewer-safe public explanation only. It must not contain raw proof fragments, credentials, storage/database errors, stack traces, worker IDs, claim metadata, or internal debug formatting.

### Public task-id route removal

Remove public task-id polling:

```http
GET /verification-tasks/{task_id}
```

Task ID lookup can remain as an internal service/repository capability if worker/runtime code needs it, but it is not a public HTTP route.

### Dev/internal completion route

The dev/internal completion route also shifts away from task-id paths:

```http
POST /verification-task-completions
{
  "creator": "pubky...",
  "bundle_id": "000G40R40M30E209185GR38E1W"
}
```

This route remains mounted only when runtime mode is `dev` and `expose_dev_completion_route = true`. It is absent from production routing.

### Idempotent submission and conflict semantics

`{ creator, bundle_id }` identifies one logical verification attempt lifecycle.

On `POST /proof-bundles`:

- If no task exists for `{ creator, bundle_id }`, create a new pending task.
- If a task exists and the submitted proof bundle matches the stored submitted proof bundle exactly after normal parsing/normalization, return the existing public lifecycle view without creating new work.
- If a task exists but the submitted proof bundle differs, return the existing conflict API shape: HTTP 409 with `task_state_conflict`.
- A different `pubky_lock_resource`, content lock path, proof set, proof payload, or verifier type for the same `{ creator, bundle_id }` is a conflict.
- This idempotency rule applies to every task status, including `failed` and `expired`.

Retrying after `failed` or `expired` requires a new Bundle ID.

### Postgres runtime invariant

Postgres stores first-class runtime columns for the public handle:

- `creator TEXT NOT NULL`
- `bundle_id TEXT NOT NULL`

and enforces:

```sql
UNIQUE (creator, bundle_id)
```

The migration must fail if duplicate `{ creator, bundle_id }` rows already exist. It must not silently deduplicate or delete verification task rows.

## Consequences

Positive:

- The public viewer workflow uses one stable handle: `{ creator, bundle_id }`.
- Credential issuance, task polling, and verified proof bundle identity align.
- Network retries of proof submission are idempotent when the proof bundle is unchanged.
- Changed proof material for an existing public attempt fails loudly instead of being ignored.
- Task IDs remain available for non-secret internal correlation without becoming client-facing state.
- Production routes no longer expose operational task IDs.

Negative:

- This is a public API contract change from the first skeleton.
- Service code needs a public lifecycle view separate from internal task records.
- Postgres needs a migration and unique constraint over `creator` and `bundle_id`.
- Clients must provide `{ creator, bundle_id }` for polling and dev completion.
- Failed/expired retries require clients to generate a new Bundle ID.

## Rejected alternatives

### Keep Task ID as public polling handle

Rejected because it gives clients a second operational identifier that does not participate in credential issuance or credible-exit recovery.

### Support both Task ID and Bundle ID public polling

Rejected because dual public handles invite drift and unnecessary client complexity. Task ID remains internal.

### Put Bundle ID in a GET path or query string

Rejected because Bundle ID is bearer-secret-like. JSON request bodies are less likely to leak through paths, query logs, browser history, metrics labels, or intermediary traces.

### Allow duplicate rows and return latest

Rejected. Product semantics are cleaner if `{ creator, bundle_id }` identifies exactly one logical verification attempt lifecycle. Duplicates are invalid runtime state.

### Create retry tasks for failed/expired attempts with the same Bundle ID

Rejected. Retrying after terminal failure or expiry requires a new Bundle ID so each Bundle ID maps to one attempt lifecycle.
