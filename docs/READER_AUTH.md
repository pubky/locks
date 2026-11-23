# Reader authentication and sessions

Locks supports `custom-pop-session-v1` for browser Reader authentication and retains `folded-auth-v2` as the compatibility fallback. Discovery advertises both, in that order, in `reader_auth` from `GET /.well-known/locks-server`.

## API flow

1. From an explicitly allowed, exact browser `Origin`, send the strict JSON body `{}` to `POST /reader-auth/challenges`.
2. The response contains `challenge_id`, the one-time `challenge`, `lock_server_pubky`, the configured canonical `lock_server_origin`, purpose `reader-session-v1`, and `expires_at`. A challenge expires after two minutes and is single-use.
3. Sign the exact closed custom-PoP data object: `type`, `lock_server_pubky`, `lock_server_origin`, `challenge_id`, `challenge`, and `browser_origin`. `type` is `pubky-locks-reader-session-v1`.
4. Send `{ "challenge_id": "...", "credentials": { "grant": "...", "pop": "..." } }` to `POST /reader-sessions`. The server verifies the Pubky 0.18 grant and custom PoP offline and derives Reader identity only from verified `grant.iss`.
5. The response returns `reader`, `session_token`, `idle_expires_at`, and `expires_at`. The token is a Reader bearer for subsequent graph/mixed admission and graph/mixed access-credential issuance.
6. Renew by repeating the challenge/PoP exchange while sending the current `Authorization: Bearer <reader-session-token>` header. Sign out with `DELETE /reader-sessions/current` and the same bearer header.

Sessions have a 15-minute idle timeout and a 60-minute absolute timeout, both capped by `grant.exp`. The server stores challenge and session-token hashes, not plaintext values. Reader endpoints require one non-opaque, canonical, explicitly allowed Origin; reject missing, duplicate, `null`, malformed, and unapproved origins; return `Cache-Control: no-store`; and never set cookies. Every use of a Reader bearer also requires the same exact approved Origin recorded at exchange time; use from another Origin does not authenticate or extend idle expiry.

Reader challenge and exchange traffic is bounded per Lock Server node: each trusted client IP plus exact approved browser Origin may make 60 combined challenge/exchange requests per 60 seconds, at most 10,000 active client/Origin windows are retained, and at most 16 proof exchanges run concurrently. These admission bounds are defense-in-depth; durable challenge and proof replay protection remains authoritative across restarts and nodes.

## Compatibility and no downgrade

Graph-only and mixed-v2 submission without a Reader bearer continues through folded auth. A valid Reader bearer directly creates the same durable graph/mixed snapshot with immutable Reader binding. Credential issuance dispatches once between disjoint bearer forms: canonical 32-byte base64url Reader tokens and canonical UUID folded-auth states. If a presented Reader bearer is malformed, unknown, expired, invalidated, or bound to another Reader, Locks fails closed and never reinterprets it as folded auth. Reader credential issuance uses the exact session Origin CORS boundary and `Cache-Control: no-store`; payment-only and Creator flows are unchanged. Existing lifecycle/status lookup remains capability-based rather than Reader-session authenticated.

Roll out Lock Server support and discovery first, then clients. A client selects the typed `ReaderSessionPreferred` mode only when discovery advertises `custom-pop-session-v1`; it may explicitly select folded auth when `folded-auth-v2` is also advertised and its wallet cannot produce a supported direct-grant Custom PoP. Never retry a failed or rejected presented Reader bearer through folded auth.

## Security and privacy

Custom-PoP verification is offline. It checks direct-grant/proof signatures, binding, and `grant.exp`, but cannot observe homeserver revocation before expiry. Current Pubky Custom PoP carries one direct grant and no verifiable parent chain, so Accounts parent/child grants are rejected as unsupported; clients must use advertised folded auth for that case. Locks does not claim delegated-grant compatibility. Keep grant lifetimes short enough for the offline revocation window. Challenges are single-use replay fences. Session renewal has one winning predecessor invalidation, and logout invalidates the current token. Browser applications must keep challenges, grants, PoPs, and session tokens out of URLs, logs, analytics, exception telemetry, and shared storage. Do not send Reader session tokens to Creator routes or third-party origins. Reusing one Reader session across requests lets the Lock Server correlate those requests to one Reader until rotation or expiry; exact browser-Origin binding limits where the bearer works but does not provide unlinkability from the Lock Server.
