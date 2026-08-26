# ADR 0019: Creator Authority Acquisition Shell

## Status

Accepted

## Context

Plan 0019 implemented the backend Creator Authority Acquisition protocol for the interim legacy Pubky auth path:

- `POST /creator/connect-flows`
- `POST /creator/connect-flows/{flow_id}/completions`
- `POST /frontend-sessions`
- `GET /creator/authority-status`

The protocol keeps three auth relationships separate:

```text
1. pubky.app/browser -> creator homeserver
2. Lock Server -> creator homeserver
3. pubky.app/browser -> Lock Server
```

For legacy-connect, crates.io `pubky` 0.10.x returns a cookie-auth-flow `authorization_url` that is secret-bearing because it embeds Lock-Server-owned relay/client secret material needed to resume and await approval. The Lock Server must own that flow. `pubky.app` must not receive, render, log, forward, or postMessage the legacy `authorization_url`.

The product shell decision is therefore security-sensitive. The choices considered were:

1. pubky.app-native rendering from Lock Server metadata.
2. Lock-Server-hosted redirect/popup.
3. Lock-Server-hosted iframe.
4. Keep only the JSON protocol and defer the shell.

## Decision

For the current legacy-connect implementation, the first human-facing acquisition shell is a **Lock-Server-hosted redirect/popup page**.

`pubky.app` starts the flow by opening a Lock Server URL from a user gesture, for example:

```text
https://<lock-server>/connect?return_to=<pubky-app-callback>&state=<state>
```

The Lock Server page then:

1. starts the legacy Pubky auth flow;
2. keeps the secret-bearing `authorization_url` on the Lock Server side;
3. renders the QR/deeplink itself on the Lock Server origin;
4. awaits creator approval;
5. stores creator-granted homeserver authority in encrypted private runtime storage;
6. redirects back to `return_to` with only a short-lived one-time code and original state:

```text
https://<pubky-app-callback>?state=<state>&code=<one-time-code>
```

`pubky.app` then exchanges the code through `POST /frontend-sessions` and uses the returned Locks-local frontend session token for creator-facing Lock Server APIs.

The shell may be a full-page redirect or popup/new tab. Popup is acceptable when opened directly from a creator click; fallback to full redirect is required for popup blockers or constrained mobile browsers.

## Rejected / Deferred Alternatives

### pubky.app-native rendering for legacy-connect

Rejected for legacy-connect.

Native pubky.app rendering would require pubky.app to receive enough data to display the QR/deeplink. With the current legacy auth flow, that display URL is secret-bearing. Showing it in pubky.app would expose Lock-Server-owned relay/client secret material to pubky.app, violating the auth boundary.

pubky.app-native rendering remains a good target for a future grant-based or Pubky-Core-provided non-secret metadata flow where the display artifact is safe for pubky.app to render.

### Lock-Server-hosted iframe first


An iframe can preserve the Lock Server origin boundary if the iframe content is entirely Lock-Server-hosted and never sends the secret-bearing URL to the parent. But iframe adds extra security and browser constraints before the product needs them:

- `postMessage` schema and origin validation;
- parent origin allowlist;
- child origin validation;
- no long-lived secrets in messages;
- CSP `frame-ancestors`;
- clickjacking controls;
- mobile browser behavior;
- redirect fallback anyway.

**Update (issue #15):** the same Lock-Server-hosted `/connect` shell now supports an opt-in `?delivery=postmessage` mode that posts `{ type: 'locks-auth-callback', state, code }` to the parent instead of redirecting. Redirect (302) stays the default, so this is additive. The constraints above are satisfied: only `{ state, code }` (one-time code, never the secret-bearing `authorization_url` or a session token) crosses; `postMessage` `targetOrigin` and CSP `frame-ancestors` are both scoped to the flow's validated `return_to` origin (never `*`), reusing the same `allowed_return_origins` allowlist as redirect. See `locks-server/src/api/creator_authority.rs` and `locks-e2e/tests/legacy_connect_shell_http.rs`.

### JSON protocol only

Rejected as insufficient for the next integration slice.

The backend JSON protocol is implemented and tested, but a creator needs a human-facing approval surface. The smallest safe shell over the implemented protocol is Lock-Server-hosted redirect/popup.

## Mandatory Security Requirements

The Lock-Server-hosted legacy shell must satisfy:

- the popup/redirect entry URL must not contain the legacy Pubky `authorization_url` or creator-authority secret;
- the Lock Server must generate, persist, render, and resume the secret-bearing legacy `authorization_url` itself;
- the `authorization_url` must not appear in pubky.app-visible URLs, `postMessage` payloads, error envelopes, logs, readiness responses, or debug output;
- `return_to` must be validated against configured allowed pubky.app origins before redirecting;
- `state` must be bound to the pending flow and returned unchanged;
- the callback to pubky.app must carry only a short-lived, single-use frontend session code plus state;
- the one-time code is not a Pubky homeserver credential and not creator-granted homeserver authority;
- `POST /frontend-sessions` remains the only place pubky.app receives a Locks-local frontend session token;
- full redirect fallback is required if popup behavior is blocked or unreliable.

## Consequences

Positive:

- preserves the Lock Server's ownership of legacy Pubky auth-flow secret material;
- avoids exposing the secret-bearing QR/deeplink to pubky.app;
- avoids iframe-specific CSP/postMessage/mobile complexity for the first shell;
- works with the already implemented JSON protocol and one-time code exchange;
- keeps future grant/non-secret metadata rendering options open.

Negative:

- Lock Servers must serve a small browser-facing connect page, not only JSON APIs;
- UI styling/branding may be less native to pubky.app until future grant/non-secret metadata flow exists;
- popup blockers and mobile browser behavior require a full-redirect fallback;
- `return_to` origin allowlisting becomes required operator/runtime configuration before production exposure.

## Follow-ups

- Implement the Lock-Server-hosted `/connect` page/shell as a separate plan slice.
- Add explicit allowed `return_to` origin config before enabling the shell outside local/dev tests.
- Decide whether the first shell should default to popup or full-page redirect in pubky.app UX.
- Revisit pubky.app-native rendering when Pubky-Core grant-based auth or a non-secret QR/deeplink metadata API is available.
- ~~If iframe embedding is requested later, add an iframe-specific ADR/checklist before implementation.~~ Done (issue #15): iframe `?delivery=postmessage` shipped as an additive mode on the same shell; constraints documented under "Lock-Server-hosted iframe first" above. A follow-up may still add per-app `frame-ancestors` registration for multi-tenant framing (currently scoped to the single `return_to` origin per flow).
