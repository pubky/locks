# Local Creator Publishing Demo

This is a local/dev e2e demo over test-support composition. It exercises the authenticated creator publishing route contract without requiring live Pubky homeserver I/O.

The demo is currently an E2E test backed by `LocalCreatorPublishingClient`, a test-support client over the in-memory Axum router. That keeps the flow executable without implying a production SDK or networked HTTP client exists yet.

For a manual local HTTP walkthrough against the running `locks-server` binary, see [`LOCAL_OPERATOR_DEMO.md`](LOCAL_OPERATOR_DEMO.md). This file remains the test-support client demo.

## Run the creator-to-viewer flow

```bash
cargo test -p locks-e2e creator_publishing_http_flow -- --nocapture
```

The flow exercises:

1. `POST /creator/lock-service-config` with frontend-session bearer auth
2. `PUT /creator/priv-resources/content/<path>` with frontend-session bearer auth and raw bytes
3. `POST /creator/content-locks` with frontend-session bearer auth
4. `POST /proof-bundles`
5. dev-only `POST /verification-task-completions`
6. `POST /access-credentials`
7. `GET /priv-resources/content/<path>`

It proves that a local creator can register guarded bytes, create a content lock, submit and complete a dev-static proof, issue a bearer access credential, and proxy-read the guarded resource bytes with the stored MIME content type.

## Run the negative private-path contract check

```bash
cargo test -p locks-e2e creator_publishing_http_rejects_invalid_guarded_path -- --nocapture
```

This proves the local client exposes non-success responses as JSON error envelopes instead of hiding them behind panics. The invalid guarded path returns `400 invalid_request`.

## Boundaries

This demo does not claim:

- live production Pubky writes are exercised
- external pubky.app creator auth UX is exercised
- a networked creator publishing surface without frontend-session auth exists
- live Pubky homeserver I/O is exercised

Those are follow-up decisions after Pubky-Core integration questions are resolved.
