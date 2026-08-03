# ADR 0014: Postgres Owns Lock Server Runtime Persistence and Worker Coordination

## Status

Accepted

## Context

The in-memory Lock Server skeleton proves the retrieval/access flow over HTTP, but it does not yet provide durable runtime state, restart recovery, or a production-safe completion path. `POST /verification-task-completions` is intentionally a skeleton/dev trigger and must be replaced or guarded before deployable production runtime.

Future runtime work also needs a coordination mechanism between the HTTP API and verification worker execution. This may begin as an in-process worker loop and later become a separate worker process. The persistence substrate should support concurrent workers, task claiming, leases, retries, and operational inspection without changing the application use-case boundaries.

SQLite is not an appropriate default for this role. Its single-writer behavior is a poor fit for IPC-adjacent worker coordination and future concurrent service evolution.

## Decision

Use Postgres as the Lock Server runtime persistence and worker-coordination substrate.

Postgres stores Lock Server private operational state:

- verification task state
- verification task claim/lease metadata
- verification task attempt history when needed
- access credential lookup records

Postgres does not become the canonical source of truth for Pubky-owned domain resources in this phase. Keep these behind the existing ports and in-memory adapters until Pubky-backed adapters are specified:

- content locks
- guarded resources
- entitlement records / verified proof bundles

Do not add `dev_*` Postgres tables for content locks, guarded resources, or entitlement records in this phase. They would make local demos easier but blur source-of-truth boundaries and create throwaway schema. If the Lock Server later needs a production local index of creator-owned entitlement data, design it explicitly as an `entitlement_index` or similar cache/materialized view, with documented rebuild/freshness semantics.

Migrations are mandatory from the first Postgres table. Use a migration manager rather than ad hoc schema creation. Runtime startup may run migrations explicitly when configured to do so; tests and CI must also run migrations before using Postgres repositories.

Worker coordination uses Postgres row-level locking, not a custom IPC protocol at first. Claiming pending or expired verification tasks should use `FOR UPDATE SKIP LOCKED` or an equivalent single-statement claim/update pattern so multiple workers can safely compete for work.

The production completion path is a worker loop or worker process calling `CompleteVerificationTaskUseCase`. The dev completion HTTP route remains available only in test/dev routing and must not be exposed by default in production routing.

For the current implementation, the worker is in-process because content locks, guarded resources, and entitlements are still in-memory. A separate long-lived worker process is deferred until those completion dependencies are shared/durable. Per-request child-process verification is not the default architecture; subprocesses should be verifier-specific isolation/sandboxing if that need appears.

Runtime configuration resolves the database URL from either a literal `database.url` or an environment variable named by `database.url_env`, with checked-in examples preferring `PUBKY_LOCK_DATABASE_URL`. Production mode must not expose the dev completion route.

## Consequences

Positive:

- Runtime state survives process restarts.
- API and worker processes can coordinate through a durable source of truth.
- Multiple workers can be introduced without redesigning persistence.
- Task claiming, leases, retries, and operational inspection have a natural home.
- The unguarded completion route can be removed from production routing without blocking local/test workflows.
- Pubky-owned content and entitlement semantics remain isolated until Pubky-Core questions are resolved.

Negative:

- Local development and CI require Postgres for runtime-persistence tests.
- Migration management becomes part of the service lifecycle.
- Postgres introduces operational configuration that the in-memory skeleton did not need.
- Restart E2E tests must distinguish Lock Server private runtime persistence from Pubky-owned resource persistence, which remains in-memory or future Pubky-backed.
- Worker E2E tests must seed or otherwise provide in-memory Pubky-owned dependencies explicitly; a passing Postgres restart test only proves Lock Server private runtime persistence.

## Rejected alternatives

### SQLite

Rejected for runtime persistence and worker coordination because its single-writer behavior is a poor fit for API/worker IPC, concurrent task claiming, and future multi-worker execution.

### `dev_*` Postgres tables for Pubky-owned resources

Rejected for this phase. They provide local demo convenience but confuse canonical ownership of content locks, guarded resources, and verified proof bundles. Keep the existing in-memory adapters for these until real Pubky-backed adapters or an explicitly designed production index/cache is introduced.

### Message broker first

Rejected for the next milestone. A broker may be useful later for wakeups or high-throughput event delivery, but Postgres remains the source of truth. Start with Postgres claims/leases before adding broker infrastructure.
