# Payment-only-v2 Compose E2E contract

The deterministic payment-only-v2 verification surface is:

```bash
./scripts/test-payment-only-v2-compose.sh
```

It starts isolated `locks-payment-only-v2-e2e` Compose project with PostgreSQL 16, deterministic fake provider-v2 HTTP service, actual `locks-server` process, plus `locks-e2e` driver. Each of two fresh rounds removes disposable state, starts Lock Server, admits payment work into shared PostgreSQL, observes lifecycle through Lock Server HTTP API, restarts Lock Server, then verifies terminal state and provider call counts remain unchanged.

Protocol-real server path verifies:

- released Lock Server binary composes PostgreSQL migrations, signed provider client, durable payment worker, HTTP health, and lifecycle lookup;
- provider-v2 `POST /v2/invoices` and `POST /v2/invoices/status` are each called once with signed requests;
- provider rejection maps to durable `expired / payment_request_rejected` lifecycle;
- actual service restart preserves terminal lifecycle and does not repeat provider calls; and
- two clean rounds produce same terminal lifecycle and call counts.

Separate Rust E2E in same target uses production signed `PaykitHttpClient`, `PaymentVerificationWorker`, and `PostgresPaymentTaskRepository` directly. It verifies exact-deadline response completion, repository/worker reconstruction, sticky first-threshold evidence across current-sample reorg, entitlement publication, replay, and deterministic repeated rounds.

The fake provider is intentionally protocol-focused: it validates JSON framing, echoes immutable request bindings, records signature-header presence, and emits deterministic create/status evidence. It does not validate signature cryptography or emulate Bitcoin consensus. Signature implementation has separate client tests; this surface proves the production client actually signs both composed requests.

## Static and compile checks without Docker

```bash
python3 -m py_compile scripts/payment-v2-e2e-fake-provider.py
cargo test --locked -p locks-e2e --test payment_only_v2_compose --no-run
```

`docker compose --file compose.payment-only-v2-e2e.yaml config --quiet` validates the rendered model when the Docker Compose CLI is available. A Docker daemon is required only for the full runner.
