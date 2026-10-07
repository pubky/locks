# Locks SDK Git snapshot

Built from [pubky/locks `c692fb70522cf75025b86ad72a3b98fa7ef84e72`](https://github.com/pubky/locks/commit/c692fb70522cf75025b86ad72a3b98fa7ef84e72).
This commit contains the browser JS/WASM package for Git-pinned consumers. It is not an npm release.

Rebuild from the source commit with Rust 1.91.1 and wasm-pack 0.13.1:

```sh
npm --prefix locks-sdk/bindings/js run build
```

The four generated files are copied unchanged from `locks-sdk/bindings/js/pkg`.
The package manifest comes from `locks-sdk/bindings/js/package.json`; build and publishing scripts are omitted because consumers use the built package.

## Artifact checksums (SHA-256)

```text
3461c48a91bad530ab2f75a160d561adec33c9f11648e8418111cf5884061291  pkg/locks_sdk_wasm.js
6044d750de494fdf08545e7feeb1b9676b550dd1479d5f6852ee806481eb212b  pkg/locks_sdk_wasm_bg.wasm
fb4a7e77f74d15a4253a4d233d193139f4db0d5ef2647f1b51a0d267cb8999f0  pkg/locks_sdk_wasm.d.ts
f7fbd7b93deae2053917e724c11b911da68b2f2a50d74600e25c7d0e1c27f0e5  pkg/locks_sdk_wasm_bg.wasm.d.ts
```
