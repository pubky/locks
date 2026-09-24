# @synonymdev/locks-sdk

Browser JS/WASM SDK for Pubky Locks.

## Install

```bash
npm install @synonymdev/locks-sdk@rc
```

## Initialize

```js
import init, { Locks } from '@synonymdev/locks-sdk';

await init();
const locks = Locks.forServer(lockServerPubky);
```

The package targets browsers and loads its bundled WebAssembly module through the generated ES module initializer.

API and integration documentation: <https://github.com/pubky/locks/blob/master/docs/SDK.md>

## License

MIT
