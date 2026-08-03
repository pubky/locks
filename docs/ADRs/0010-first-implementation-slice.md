# ADR 0010: First Implementation Slice Uses In-Memory Retrieval Access

## Status

Accepted

## Context

Pubky-Core questions remain open around private-path writes, private namespace stability, and event visibility. Starting with creator publishing would force the first implementation to assume answers in areas that are still unresolved.

The first implementation still needs to prove the core Locks workflow: proof submission, asynchronous verification, entitlement persistence, access credential issuance, entitlement-backed access validation, and proxy-read behavior.

## Decision

The first implementation slice focuses on retrieval/access, not creator publishing.

The slice seeds a content lock and guarded resource directly into in-memory repositories. It does not perform real Pubky I/O, real homeserver writes, or creator publishing.

The first verifier is `dev-static`, a non-production verifier used only to exercise the workflow in dev/test runtime mode. It reads `params.satisfied: true/false` from a criterion and requires no meaningful submitted proof. Production-mode workers must not register it; a `dev-static` task in production fails through the normal unsupported-verifier path and creates no entitlement.

In-memory repositories and the `dev-static` verifier live in `locks-service`, not `locks-core`.

`locks-service` starts with a layer-first hexagonal structure, but it is not kept flat: use cases are split inside `application/`, and adapters are split by type inside `infrastructure/`. In-memory repositories live under `infrastructure/memory/`; `dev-static` lives under `infrastructure/verifiers/`.

First vertical slice flow:

1. Seed content lock.
2. Seed guarded resource.
3. Submit proof bundle with viewer-generated Bundle ID and create a pending server-owned verification task.
4. Verify using `dev-static`.
5. Store verified proof bundle on success.
6. Issue access credential.
7. Validate access credential.
8. Re-read content lock via `pubky_lock_resource`.
9. Verify content lock hash matches the Lock ID embedded in the path.
10. Proxy-read fake guarded resource.

The workspace contains an application-level regression test for this flow: `retrieval_access_slice_submits_verifies_issues_validates_and_proxy_reads`.

## Consequences

- The first implementation proves application/domain organization without depending on unresolved Pubky-Core behavior.
- Creator publishing remains deferred until guarded write, namespace, and event semantics are confirmed or explicitly mocked.
- The `dev-static` verifier is only registered for dev/test runtime wiring and is not available to production-mode workers.
- The in-memory adapters can later support a fake-adapter Lock Server skeleton before Pubky-backed adapters exist.
- Splitting by use case and adapter type avoids `application/services.rs`, `infrastructure/memory.rs`, and `infrastructure/dev_static.rs` becoming catch-all files.
