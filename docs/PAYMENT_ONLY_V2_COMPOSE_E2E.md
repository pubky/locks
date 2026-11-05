# Payment-only-v2 Compose E2E contract

The deterministic payment-only-v2 verification surface is:

```bash
./scripts/test-payment-only-v2-compose.sh
```

It starts the isolated `locks-payment-only-v2-e2e` Compose project with PostgreSQL 16, a deterministic fake provider-v2 HTTP service, and the `locks-e2e` Rust test container. The runner always removes the project and its disposable database volume before and after a run.

The Rust E2E uses the production signed `PaykitHttpClient`, `PaymentVerificationWorker`, and `PostgresPaymentTaskRepository`. It verifies:

- provider-v2 `POST /v2/invoices` and `POST /v2/invoices/status` are each called once with signed requests;
- the Locks-owned 60-second deadline is preserved by create and status responses;
- the PostgreSQL repository and worker can be reconstructed between create and status;
- threshold evidence observed exactly at the deadline remains timely when the HTTP response completes after the deadline;
- a reorged current sample can return to `undetected` while immutable first-threshold evidence remains sticky and publishes a payment entitlement;
- replay of the completed admission does not call the provider again; and
- two fresh rounds produce the same terminal state, evaluator revision, and provider call counts.

The fake provider is intentionally protocol-focused: it validates JSON framing, echoes immutable request bindings, records signature-header presence, and emits deterministic create/status evidence. It does not validate signature cryptography or emulate Bitcoin consensus. Signature implementation has separate client tests; this surface proves the production client actually signs both composed requests.

## Static and compile checks without Docker

```bash
python3 -m py_compile scripts/payment-v2-e2e-fake-provider.py
cargo test --locked -p locks-e2e --test payment_only_v2_compose --no-run
```

`docker compose --file compose.payment-only-v2-e2e.yaml config --quiet` validates the rendered model when the Docker Compose CLI is available. A Docker daemon is required only for the full runner.
