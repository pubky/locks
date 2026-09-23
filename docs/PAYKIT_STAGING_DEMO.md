# Paykit staging browser demo

This demo runs only local Creator and reader browser apps. It uses deployed staging services:

- Locks: `https://locks.staging.pubky.app`
- Paykit: `https://paykit.staging.pubky.app`

It does not run or reset Locks, Paykit, PostgreSQL, Bitcoin, Fulcrum, Pubky testnet, or wallet-helper services. It uses the standard public Pubky network and SDK defaults.

## Prerequisites

- Docker Compose with BuildKit support.
- Two distinct Bitkit staging identities/devices connected to the regtest used by Paykit staging:
  - Creator/payee Bitkit;
  - reader/payer Bitkit.
- Reader Bitkit is funded and handles payment through its own staging/regtest flow.

Do not use one Bitkit identity for both roles.

## Start

From repository root:

```bash
docker compose --file compose.paykit-staging-demo.yaml up -d --build
```

No environment file or URL override is accepted. Startup:

1. builds one helper-free JS/WASM image;
2. fetches and validates Locks `/.well-known/locks-server` over HTTPS;
3. writes public config under `.local/paykit-staging-demo/config/`;
4. starts Creator and reader apps only after config succeeds.

Open:

- Creator: <http://127.0.0.1:8080/examples/js-sdk/>
- Reader: <http://127.0.0.1:8088/reader/>

Creator scoped Pubky session state persists under `.local/paykit-staging-demo/creator-session/` with owner-only permissions. Locks frontend session and pasted reader Pubky remain browser-memory only.

## Walkthrough

### Creator/payee device

1. Open Creator app.
2. Authenticate the demo Creator with Creator Bitkit.
3. Authenticate to deployed Lock Server through hosted connect.
4. Select `paykit-payment`.
5. Complete Paykit setup with Creator Bitkit.
6. Publish guarded content and copy resulting content-lock resource.

### Reader/payer device

1. Ensure Paykit is enabled in separate reader Bitkit identity.
2. Copy reader Bitkit's canonical public Pubky.
3. Open reader app and load Creator's content-lock resource.
4. Paste reader Pubky. Reader and Creator identities must differ.
5. Click **Check Paykit data**.
   - no data: submission remains blocked; enable Paykit in Bitkit, then retry;
   - lookup unavailable: submission remains blocked; retry later;
   - data present: submission is enabled, but usable receiver validation still occurs during invoice creation.
6. Submit payment proof bundle.
7. Receive and pay Payment Request in reader Bitkit.
8. Resume payment verification polling.
9. After Locks reports completion, issue access credential.
10. Read guarded content and verify expected bytes.

Payment Request receipt and payment confirmation are intermediate milestones. Full E2E passes only after access credential issuance and successful guarded-content read.

## Data-presence limitation

`Locks.hasPaykitData` checks whether any child exists under reader's public Paykit v0 namespace. `true` does not prove marker validity, supported capabilities, freshness, wallet readiness, or payment success. Paykit invoice creation remains authoritative.

## Reset local client state

```bash
npm --prefix examples/js-sdk run reset-paykit-staging-demo-local
```

This stops only local staging-demo containers and removes only `.local/paykit-staging-demo/`. It does not call remote endpoints or reset deployed staging state.

## Known external blocker

At last verification, this secret-free probe returned `400 {"error":"invalid_request"}`:

```bash
curl --silent --show-error --output /dev/null --write-out '%{http_code}\n' \
  'https://paykit.staging.pubky.app/setup?return_to=http%3A%2F%2F127.0.0.1%3A8080&state=staging-demo-probe'
```

Deployment-policy changes are outside this branch. If Creator setup still fails, report:

- exact source branch/commit or tree identity;
- failed stage;
- endpoint path;
- HTTP status and coarse public error;
- timestamp;
- reproducible secret-free command.

Never include authorization URLs, bearer/session tokens, one-time codes, wallet material, private content, or remote configuration values.

## Verification

```bash
npm --prefix examples/js-sdk run test:staging-config
npm --prefix examples/js-sdk run test:staging-compose
npm --prefix examples/js-sdk run test:staging-creator-mode
npm --prefix examples/js-sdk run test:staging-reader-mode
npm --prefix examples/js-sdk run test:reset-paykit-staging-demo-local
npm --prefix examples/js-sdk run check
npm --prefix locks-sdk/bindings/js run smoke:examples
git diff --check
```

Image/runtime verification additionally requires Docker daemon access for the exact startup command.
