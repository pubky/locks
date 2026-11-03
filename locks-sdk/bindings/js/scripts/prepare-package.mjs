import { unlink } from 'node:fs/promises';

// wasm-pack writes this file for standalone output. It makes npm exclude
// every generated artifact even though package.json explicitly lists them.
await unlink(new URL('../pkg/.gitignore', import.meta.url));
