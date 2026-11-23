# @synonymdev/locks-sdk

Browser JS/WASM SDK for Pubky Locks.

## Install

Release candidates use the `rc` dist-tag:

```bash
npm install @synonymdev/locks-sdk@rc
```

Stable releases use the default `latest` dist-tag:

```bash
npm install @synonymdev/locks-sdk
```

## Initialize

```js
import init, { Locks } from '@synonymdev/locks-sdk';

await init();
const locks = Locks.forServer(lockServerPubky);
if (!(await locks.isReady())) {
  throw new Error('Lock Server is not ready');
}
```

The package targets browsers and loads its bundled WebAssembly module through the generated ES module initializer.

`isReady()` resolves the selected Lock Server through PKARR and calls `/readyz`. It returns `true` for HTTP 2xx (including the server's current `degraded` response), `false` for non-2xx, and rejects with `LockServerResolutionFailed` or `LockServerFetchFailed` for those distinct failure classes. It does not expose the resolved origin.

API and integration documentation: <https://github.com/pubky/locks/blob/master/docs/SDK.md>

Authenticated creator sessions expose `creatorAuthorityStatus()` for a live, secret-free grant
status check. Operational homeserver failures reject; only confirmed missing/revoked/expired grants
resolve with `authorized: false`.

## Reader sessions

When `/.well-known/locks-server` advertises
`reader_auth: ["custom-pop-session-v1", "folded-auth-v2"]`, authenticate protected Reader calls:

```js
const viewer = locks.viewer;
const challenge = await viewer.createReaderAuthChallenge();
const credentials = await wallet.answerLocksChallenge(challenge); // { grant, pop }
let readerSession = await viewer.createReaderSession(challenge.challenge_id, credentials);

const submitted = await readerSession.submitGraphProofBundle(graphBundle);
const mixed = await readerSession.submitMixedProofBundle(mixedBundle);
const access = await readerSession.issueAccessCredential(handle);

// The application owns persistence, as with creator sessions.
const savedToken = readerSession.exportSecret();
readerSession = viewer.restoreReaderSession(savedToken);

// Rotation authenticates with the current Reader token.
const nextChallenge = await viewer.createReaderAuthChallenge();
readerSession = await readerSession.rotate(
  nextChallenge.challenge_id,
  await wallet.answerLocksChallenge(nextChallenge),
);
await readerSession.signout();
```

`createReaderAuthChallenge()` sends the strict empty JSON body `{}`. Reader-session creation sends
`{ challenge_id, credentials: { grant, pop } }`; rotation additionally sends the existing Reader
bearer. Graph and mixed submission plus access-credential issuance belong on `ReaderSession` and use
that bearer. Payment-only submission remains on `Viewer.submitPaymentProofBundle()` and anonymous.

Use folded auth when Reader sessions are absent or when both modes are advertised but the wallet
cannot produce the currently supported direct-grant Custom PoP (for example, it has an Accounts
parent/child grant). Once a Reader bearer is presented, surface challenge, credential, session, and
protected-call failures; never retry that failed bearer anonymously. Session and credential secrets
are caller-persisted and redacted from Rust `Debug` output.

## License

MIT
