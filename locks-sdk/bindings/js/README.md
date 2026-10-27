# @synonymdev/locks-sdk

Browser JS/WASM SDK for Pubky Locks.

## Install

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

## License

MIT
