# Lock Server Runtime

## Scope

This document describes current process/runtime behavior for local development and operator review.

`locks-server` is the process/runtime composition root. It owns HTTP routing, config loading, observability, AppState composition, PKARR publication, and worker startup. `locks-service` owns application use cases and ports, not process assembly.

## Startup

```bash
locks-server [--config <path>]
```

Without `--config`, the server resolves:

```text
~/.pubky-lock/config.toml
```

If default config is missing, startup initializes service home, writes generated config, and creates local Lock Server signing material. Custom config paths must already exist.

The filesystem identity provider validates that `credentials.lock_server_public_key` matches the public key derived from `credentials.lock_server_secret_key`.

## Config files

Current config example:

- `locks-server/config/example.dev.postgres.toml` — runnable development Postgres operation with worker, legacy-connect, Pubky testnet, and dev route gates.

No runnable in-memory binary config exists. In-memory runtime is test support only.

## Logging

```toml
[logging]
level = "info"
```

`logging.level` accepts `tracing_subscriber::EnvFilter` syntax. `RUST_LOG` still wins at process startup when set.

## Pubky network

```toml
[pubky]
network = "testnet"
```

Supported values:

- `testnet`: generated/default local development network.
- `mainnet`: public Pubky network for production/staging deployments.

## Runtime environment and route gates

```toml
[runtime]
environment = "development"
```

Supported values:

| Environment | Dev completion route | Raw JSON connect-flow routes | Hosted connect/session routes | Creator publishing | `dev-static` verifier |
| --- | --- | --- | --- | --- | --- |
| `development` | mounted | mounted when `[creator_authority_acquisition].enabled = true` | mounted when `[creator_authority_acquisition].enabled = true` | authenticated, Pubky homeserver-backed | registered |
| `staging` | not mounted | not mounted | mounted when `[creator_authority_acquisition].enabled = true` | authenticated, Pubky homeserver-backed | not registered |
| `production` | not mounted | not mounted | mounted when `[creator_authority_acquisition].enabled = true` | authenticated, Pubky homeserver-backed | not registered |

`staging` behaves like `production`; it is a separate operator label.

Removed config keys fail during config parsing:

```toml
[runtime]
mode = "dev"
expose_dev_completion_route = true
expose_creator_connect_routes = false

[creator_repositories]
backend = "local-memory"
```

Creator repositories are not operator-selectable. Server runtime always uses Pubky homeserver-backed creator repositories. There is no `[creator_repositories]` config.

## Creator Authority Acquisition

```toml
[creator_authority_acquisition]
enabled = true
method = "legacy-connect"
frontend_session_ttl_seconds = 86400
frontend_session_code_ttl_seconds = 120

[creator_authority_acquisition.legacy_connect]
allowed_return_origins = ["http://localhost:3000"]
```

`legacy-connect` is the only accepted method. Removed or unknown methods, including `legacy-self-relay`, are rejected during config loading.

When enabled, the Lock Server starts and completes legacy Pubky auth, stores encrypted creator authority, then issues a short-lived code that pubky.app/browser clients exchange for a Locks-local frontend session token.

`legacy_connect.allowed_return_origins` entries are exact `http`/`https` origins with scheme and authority only. Paths, query strings, and fragments are rejected. A single wildcard entry, `allowed_return_origins = ["*"]`, is accepted and means `/connect` may use the origin from `return_to`; wildcard must not be mixed with concrete origins.

## PKARR and browser SDK reachability

The Lock Server public key is also the PKARR service pointer clients resolve to reach the Lock Server. Locks publishes and republishes that record from configured signing material.

```toml
[pkdns]
public_ip = "203.0.113.10"
public_pubky_tls_port = 6287
public_icann_http_port = 80
icann_domain = "locks.example"
pkarr_relays = []
key_republisher_interval_seconds = 3600
```

`public_pubky_tls_port` and `public_icann_http_port` advertise externally reachable ports. `icann_domain` is browser/ICANN fallback target. Local testnet operators should set `pkarr_relays = ["http://localhost:15411"]`.

PKARR publishing starts when environment is `staging`/`production` or creator-authority acquisition is enabled, and republishes every `key_republisher_interval_seconds` seconds.

`credentials.lock_server_secret_key` must contain:

```text
keypair-seed:<base64url-no-pad-32-byte-seed>
```

The derived public key must equal `credentials.lock_server_public_key`; startup fails on mismatch.

## Paykit integration

`paykit-payment` support is enabled only when the optional `[paykit]` section is present:

```toml
[paykit]
server_url = "http://127.0.0.1:3001"
minimum_confirmations = 0
```

`server_url` is the standalone Paykit Server base URL. Any configured path prefix is preserved when appending `invoices` and `transactions/status`, with or without a trailing slash. For a new `{ creator, bundle_id }` lifecycle identity, the Lock Server calls `POST /invoices` during `POST /proof-bundles` before creating a verification task; exact persisted submission replay does not call Paykit. Workers call `POST /transactions/status` while completing pending payment verification tasks. Both request bodies are canonical JSON signed through `X-Paykit-Signature` with the existing Lock Server keypair; therefore `credentials.lock_server_secret_key` must use the `keypair-seed:<base64url-no-pad-32-byte-seed>` format when `[paykit]` is configured.

Paykit HTTP connections have a 5-second connect timeout and every request has a 20-second whole-request timeout. Invoice timeouts fail submission with `paykit_invoice_creation_failed`; status-query timeouts remain pending/retryable. When `[paykit]` and the in-process worker are both enabled, `worker.claim_timeout_seconds` must be greater than 20 so a Paykit request cannot outlive the worker claim lease. External worker deployments must preserve the same timeout/lease relationship operationally.

Every claimed verification task receives a fresh opaque claim token. Retry, completion, and failure transitions require the exact token, worker ID, `in_progress` state, and an unexpired lease, so a stale process cannot write after the same worker ID reclaims the task. Pubky entitlement publication cannot be atomic with the Postgres transition: a stale worker may publish a valid entitlement but cannot persist terminal task state. After any publication error, Locks reads the entitlement back; the current owner recovers only when the stored entitlement decision matches in every field except verifier-owned `verified_at` timestamps. A missing or mismatched entitlement preserves the failure. The Pubky adapter remains check-then-put, so claim fencing does not make concurrent homeserver writes atomic; it only fences Postgres task state.

`minimum_confirmations = 0` accepts a Paykit status of `detected` or `confirmed` when `amount_matched = true`. Values above zero require `status = "confirmed"` and at least that many confirmations. `undetected`, insufficient confirmations, or `amount_matched = false` keep the task pending/retryable.

Omitting `[paykit]` prevents creation of new payment lifecycle identities. In that state, non-payment verifier flows continue to run, an exact persisted `paykit-payment` submission replay can still return its lifecycle after current canonical preflight, and a new `paykit-payment` submission returns `422 paykit_not_configured`. Staging deployments should omit `[paykit]` until a Paykit Server is deployed and reachable for that environment.

## Runtime storage

Operator-facing readiness uses semantic storage labels:

- `ephemeral`: in-process/in-memory private runtime composition.
- `persisted`: Postgres-backed private runtime composition.

Postgres is private runtime storage for verification tasks, task claiming, access credentials, frontend sessions, and creator-granted homeserver session material. It is not storage for Pubky-owned content locks, guarded resources, Lock Service Pointers, or verified proof bundles.

Creator-granted session material is encrypted before storage. The server-side encryption key comes from an env var named by config:

```toml
[secrets]
creator_authority_key_env = "PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY"
```

The named env var must contain a 32-byte key encoded as base64url without padding:

```bash
python3 - <<'PY'
import base64, os
print(base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip('='))
PY
```

## Development integration shape

```toml
[pubky]
network = "testnet"

[runtime]
environment = "development"

[creator_authority_acquisition]
enabled = true
method = "legacy-connect"
```

This mounts authenticated Pubky-backed creator publishing routes, hosted legacy-connect routes, raw JSON dev connect routes, and the dev-only manual verification completion route.

## Staging/production shape

```toml
[pubky]
network = "mainnet"

[runtime]
environment = "staging" # or "production"

[creator_authority_acquisition]
enabled = true
method = "legacy-connect"

[creator_authority_acquisition.legacy_connect]
allowed_return_origins = ["https://pubky.app"]
```

This mounts authenticated Pubky-backed creator publishing routes and hosted legacy-connect/session routes. It does not mount the dev completion route.

## Worker

```toml
[worker]
enabled = true
poll_interval_ms = 250
```

`worker.poll_interval_ms` must be greater than zero whether the in-process worker is enabled or disabled. When enabled, the worker claims due pending verification tasks through private runtime storage and completes them through application use cases. Newly submitted tasks are immediately due. A retryable verification result returns the task to durable `pending` state with `next_attempt_at` 30 seconds in the future; it is not claimable again before that timestamp. Queue polling remains independently controlled by `worker.poll_interval_ms`. Retry scheduling preserves the current attempt count and releases the active claim.

`dev-static` verification is registered only in `environment = "development"`. `paykit-payment` verification is registered when `[paykit]` is configured, regardless of environment. Staging/production completion uses the worker path, not the dev-only `POST /verification-task-completions` route.

For `paykit-payment`, every Paykit status-call failure schedules a normal pending retry and is logged as retry telemetry rather than a verification failure. This includes network errors, timeouts, all non-2xx responses (including `404` and authentication/authorization failures), and malformed success bodies. V1 has no terminal Paykit payment-failure status.

Scheduled retries and crash recovery are separate mechanisms. Expected retryable results explicitly release the current claim and set `next_attempt_at`. If a worker crashes while a task is `in_progress`, another worker may reclaim it only after `claim_expires_at`; only the worker that still owns an active claim may schedule its retry.

## Health and readiness

`GET /healthz` reports process liveness:

```json
{ "status": "ok" }
```

`GET /readyz` reports runtime dependency readiness:

```json
{
  "status": "ready",
  "runtime_storage": "persisted",
  "worker_enabled": true
}
```

Health/readiness responses must remain secret-free. They must not include database URLs, secret paths, worker IDs, task counts, public keys, credentials, raw errors, or submitted proof material.
