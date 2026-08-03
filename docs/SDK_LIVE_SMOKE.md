# SDK live browser smoke

This document defines the live Pubky/testnet smoke boundary for the JS/WASM browser SDK.

Normal SDK verification does **not** require live Pubky credentials or endpoints. Live smoke is a separate manual/integration activity because it depends on real PKARR records, homeserver data, allowed browser origins, and a configured Lock Server.

## Prerequisite audit

Run:

```bash
npm --prefix locks-sdk/bindings/js run live:smoke:check
```

The command is informational and exits 0. It reports whether the following environment variables are set:

| Variable | Meaning |
| --- | --- |
| `LOCKS_LIVE_LOCK_SERVER` | Lock Server Pubky with a browser-usable PKARR endpoint |
| `LOCKS_LIVE_PKARR_RELAY` | PKARR relay URL. Local `pubky-testnet` uses `http://localhost:15411` |
| `LOCKS_LIVE_CREATOR` | Creator Pubky publishing `/pub/locks.app/config.json` |
| `LOCKS_LIVE_CONTENT_LOCK_RESOURCE` | Canonical `pubky.../pub/locks.app/<lock_id>.json` resource |
| `LOCKS_LIVE_DEMO_ORIGIN` | Browser origin allowed by `creator_authority_acquisition.legacy_connect.allowed_return_origins` |

## Manual smoke sequence

Once prerequisites exist:

```bash
npm --prefix locks-sdk/bindings/js run build
python3 -m http.server 8080 --directory locks-sdk/bindings/js
```

Open:

```text
http://localhost:8080/demo/
```

Then verify:

1. Build SDK options with the PKARR relay:

   ```ts
   const options = new LocksOptions();
   options.addPkarrRelay(process.env.LOCKS_LIVE_PKARR_RELAY ?? "http://localhost:15411");
   ```

2. `Locks.forServerWithOptions(lockServer, options)` displays the configured Lock Server Pubky.
3. `createConnectUrl` generates a Lock-Server-hosted `legacy-connect` URL without exposing a raw legacy Pubky `authorization_url` to the app.
4. The connect callback path can parse `code` and `state`, validate caller-managed state, and exchange the code for a frontend session.
5. `session.exportSecret()` and `locks.restoreSession(secret)` round-trip the session and retain Lock Server context.
6. `session.signout()` revokes the current frontend session.
7. `Locks.forCreatorWithOptions(creator, options)` resolves creator PKARR, fetches `/pub/locks.app/config.json`, validates it, and selects the creator default Lock Server.
8. `Locks.readContentLockWithOptions(resource, options)` resolves the creator homeserver, fetches the public content lock, validates that it matches the requested resource, and returns JSON.
9. `Locks.forContentLockWithOptions(resource, options)` selects the Lock Server from per-lock override or creator pointer fallback.
10. `viewer.submitProofBundle(...)`, `lookupVerificationTask`, `issueAccessCredential`, and `proxyReadGuardedResource` work for a known satisfiable proof bundle.

## Proof helper boundary

The SDK intentionally does not ship lock-type-specific proof builders yet. `submitProofBundle` accepts the protocol object, validates/canonicalizes it through Rust types, and sends it to the Lock Server.

Future helpers should be added only when a verifier type has stable product semantics. Examples:

- payment receipt proof helper
- membership/subscription proof helper
- dev-only fixture proof helper

Do not add generic helpers that pretend to understand proof semantics before the verifier contract is explicit.
