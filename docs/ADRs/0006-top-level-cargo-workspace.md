# ADR 0006: Top-Level Cargo Workspace

## Status

Accepted

## Context

Locks is expected to become more than a single Lock Server binary. The repository needs room for a Lock Server, shared protocol/domain payloads, a future developer SDK, a future creator UI, and possible operator tooling.

A single root crate would mix service runtime concerns with shared protocol contracts and would make it harder for SDK/UI/admin code to depend only on stable shared types.

## Decision

Use a Cargo workspace with top-level member folders, not a nested `crates/` layout.

Initial workspace members:

- `locks-core`
- `locks-service`

Document but do not create placeholder members until responsibilities are concrete:

- `locks-sdk`
- `locks-admin`
- `creator-ui`

`locks-core` owns shared protocol/domain payloads, value objects, validation, canonical JSON, hash derivation, and content lock path rules.

`locks-service` owns Lock Server orchestration, application ports, infrastructure adapters, in-memory repositories, dev-only verifier implementations, runtime config, and later Pubky-backed adapters.

Dependency direction:

```text
locks-service -> locks-core
locks-sdk     -> locks-core
locks-admin   -> locks-core and/or service API client, TBD
creator-ui    -> locks-sdk and/or Lock Service API, TBD
```

## Consequences

- The root `Cargo.toml` becomes a workspace manifest.
- Existing root `src/main.rs` should move into `locks-service` or be removed during workspace migration.
- Future products can be added without reshaping the repo again.
- Placeholder crates are avoided so undefined products do not create false API/ownership commitments.
- Shared contract/domain logic must not drift into Lock Server runtime implementation.
